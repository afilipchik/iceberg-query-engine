//! Pack dual integer group keys into one 64-bit key.
//!
//! `GROUP BY a, b` with both columns non-negative, bounded integers (footer
//! statistics) becomes `GROUP BY a*K + b` (K a power of two above max(b)),
//! putting the aggregation on the fast single-int raw path instead of the
//! per-row Vec<ScalarValue> GroupKey path, with a Project unpacking the key
//! via shift/mask afterwards. Q20's decorrelated lineitem aggregate groups
//! 18M rows by (l_partkey, l_suppkey) — 590ms of its 1.1s runtime.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::physical::operators::TableStatistics;
use crate::planner::{
    AggregateNode, BinaryOp, Column, Expr, LogicalPlan, PlanSchema, ProjectNode, ScalarFunction,
    ScalarValue, SchemaField,
};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PackedGroupKeys {
    table_stats: HashMap<String, TableStatistics>,
}

impl PackedGroupKeys {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
        }
    }

    pub fn with_table_statistics(table_stats: HashMap<String, TableStatistics>) -> Self {
        Self { table_stats }
    }

    /// Footer stats for a plain column, resolved by unambiguous name.
    fn column_bounds(&self, name: &str) -> Option<(i64, i64)> {
        let lname = name.to_lowercase();
        let mut found = None;
        for stats in self.table_stats.values() {
            if let Some(cs) = stats.column_stats.get(&lname) {
                if found.is_some() {
                    return None;
                }
                found = Some((cs.min_i64?, cs.max_i64?));
            }
        }
        found
    }
}

impl Default for PackedGroupKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for PackedGroupKeys {
    fn name(&self) -> &str {
        "PackedGroupKeys"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.rewrite(plan)
    }
}

impl PackedGroupKeys {
    fn rewrite(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let children: Vec<Arc<LogicalPlan>> = plan
            .children()
            .iter()
            .map(|c| self.rewrite(c).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let plan = plan.with_new_children(children);

        if let LogicalPlan::Aggregate(agg) = &plan {
            if let Some(rewritten) = self.try_pack(agg) {
                return Ok(rewritten);
            }
        }
        Ok(plan)
    }

    fn try_pack(&self, agg: &AggregateNode) -> Option<LogicalPlan> {
        if agg.group_by.len() != 2 || self.table_stats.is_empty() {
            return None;
        }
        let cols: Vec<&Column> = agg
            .group_by
            .iter()
            .map(|e| match e {
                Expr::Column(c) => Some(c),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;

        // Output field types must be plain ints (also guards non-column
        // schemas); nullable keys are fine — NULLs in either column make the
        // packed expression NULL, which groups them together. That matches
        // SQL GROUP BY semantics only when at most one column is nullable
        // per... be strict: require both columns null-free.
        let fields = agg.schema.fields();
        for (i, c) in cols.iter().enumerate() {
            let f = &fields[i];
            if f.name != c.name {
                return None;
            }
            if !matches!(f.data_type, DataType::Int64 | DataType::Int32) {
                return None;
            }
            // null-free per footer stats
            let lname = c.name.to_lowercase();
            let mut nf = false;
            for stats in self.table_stats.values() {
                if let Some(cs) = stats.column_stats.get(&lname) {
                    nf = cs.null_count == Some(0);
                }
            }
            if !nf {
                return None;
            }
        }

        let (a_min, a_max) = self.column_bounds(&cols[0].name)?;
        let (b_min, b_max) = self.column_bounds(&cols[1].name)?;
        if a_min < 0 || b_min < 0 {
            return None;
        }
        let k = (b_max as u64 + 1).next_power_of_two();
        if (a_max as i128) * (k as i128) + (b_max as i128) > i64::MAX as i128 {
            return None;
        }
        let shift = k.trailing_zeros() as i64;
        let mask = (k - 1) as i64;

        let cast_i64 = |e: Expr| Expr::Cast {
            expr: Box::new(e),
            data_type: DataType::Int64,
        };
        let packed = Expr::BinaryExpr {
            left: Box::new(Expr::BinaryExpr {
                left: Box::new(cast_i64(agg.group_by[0].clone())),
                op: BinaryOp::Multiply,
                right: Box::new(Expr::Literal(ScalarValue::Int64(k as i64))),
            }),
            op: BinaryOp::Add,
            right: Box::new(cast_i64(agg.group_by[1].clone())),
        };

        let mut inner_fields = vec![SchemaField {
            name: "__pk".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            relation: None,
        }];
        inner_fields.extend(fields.iter().skip(2).cloned());
        let inner_agg = LogicalPlan::Aggregate(AggregateNode {
            input: agg.input.clone(),
            group_by: vec![Expr::Alias {
                expr: Box::new(packed),
                name: "__pk".to_string(),
            }],
            aggregates: agg.aggregates.clone(),
            schema: PlanSchema::new(inner_fields),
        });

        // Unpack: a = __pk >> shift, b = __pk & mask, both cast back
        let pk = || {
            Expr::Column(Column {
                relation: None,
                name: "__pk".to_string(),
            })
        };
        let unpack_a = Expr::ScalarFunc {
            func: ScalarFunction::BitwiseRightShift,
            args: vec![pk(), Expr::Literal(ScalarValue::Int64(shift))],
        };
        let unpack_b = Expr::ScalarFunc {
            func: ScalarFunction::BitwiseAnd,
            args: vec![pk(), Expr::Literal(ScalarValue::Int64(mask))],
        };
        let cast_to = |e: Expr, dt: &DataType| {
            if *dt == DataType::Int64 {
                e
            } else {
                Expr::Cast {
                    expr: Box::new(e),
                    data_type: dt.clone(),
                }
            }
        };
        let mut project_exprs = vec![
            Expr::Alias {
                expr: Box::new(cast_to(unpack_a, &fields[0].data_type)),
                name: fields[0].name.clone(),
            },
            Expr::Alias {
                expr: Box::new(cast_to(unpack_b, &fields[1].data_type)),
                name: fields[1].name.clone(),
            },
        ];
        for f in fields.iter().skip(2) {
            project_exprs.push(Expr::Column(Column {
                relation: f.relation.clone(),
                name: f.name.clone(),
            }));
        }
        Some(LogicalPlan::Project(ProjectNode {
            input: Arc::new(inner_agg),
            exprs: project_exprs,
            schema: agg.schema.clone(),
        }))
    }
}
