//! Pack integer group keys only with an injectivity proof from logical semantics.
//! Type domains and explicit integer predicates may prove bounds; table
//! statistics and matching column names never prove ranges or null freedom.

use crate::error::Result;
use crate::optimizer::properties::column_index;
use crate::optimizer::OptimizerRule;
use crate::physical::operators::TableStatistics;
use crate::planner::{
    AggregateNode, BinaryOp, Column, Expr, LogicalPlan, PlanSchema, ProjectNode, ScalarFunction,
    ScalarValue, SchemaField,
};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PackedGroupKeys;

impl PackedGroupKeys {
    pub fn new() -> Self {
        Self
    }

    /// Retained constructor compatibility. Estimates are never packing proofs.
    pub fn with_table_statistics(_table_stats: HashMap<String, TableStatistics>) -> Self {
        Self
    }
}

/// Proven range of non-NULL values; nullable describes whether NULL can occur.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IntegerDomain {
    pub min: i128,
    pub max: i128,
    pub nullable: bool,
}

/// Follow an unambiguous column through operators that preserve its values.
/// Deliberately decline joins (including NULL extension), aggregates, computed
/// projections and other boundaries without an implemented lineage contract.
pub(crate) fn integer_domain(plan: &LogicalPlan, column: &Column) -> Option<IntegerDomain> {
    domain_at(plan, column_index(&plan.schema(), column)?)
}

fn identity_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias { expr, .. } => identity_column(expr),
        _ => None,
    }
}

fn type_domain(data_type: &DataType, nullable: bool) -> Option<IntegerDomain> {
    let (min, max) = match data_type {
        DataType::Int8 => (i8::MIN as i128, i8::MAX as i128),
        DataType::Int16 => (i16::MIN as i128, i16::MAX as i128),
        DataType::Int32 => (i32::MIN as i128, i32::MAX as i128),
        DataType::Int64 => (i64::MIN as i128, i64::MAX as i128),
        DataType::UInt8 => (0, u8::MAX as i128),
        DataType::UInt16 => (0, u16::MAX as i128),
        DataType::UInt32 => (0, u32::MAX as i128),
        DataType::UInt64 => (0, u64::MAX as i128),
        _ => return None,
    };
    Some(IntegerDomain { min, max, nullable })
}

fn domain_at(plan: &LogicalPlan, index: usize) -> Option<IntegerDomain> {
    let schema = plan.schema();
    let field = schema.fields().get(index)?;
    let domain = match plan {
        LogicalPlan::Scan(node) => {
            let mut domain = type_domain(&field.data_type, field.nullable)?;
            if let Some(predicate) = &node.filter {
                narrow_domain(&mut domain, predicate, &schema, index);
            }
            domain
        }
        LogicalPlan::Filter(node) => {
            let mut domain = domain_at(&node.input, index)?;
            narrow_domain(&mut domain, &node.predicate, &schema, index);
            domain
        }
        LogicalPlan::Project(node) => {
            let column = identity_column(node.exprs.get(index)?)?;
            let input_schema = node.input.schema();
            let input_index = column_index(&input_schema, column)?;
            if input_schema.fields().get(input_index)?.data_type != field.data_type {
                return None;
            }
            domain_at(&node.input, input_index)?
        }
        LogicalPlan::SubqueryAlias(node) => {
            if node.input.schema().fields().get(index)?.data_type != field.data_type {
                return None;
            }
            domain_at(&node.input, index)?
        }
        LogicalPlan::Sort(node) => domain_at(&node.input, index)?,
        LogicalPlan::Limit(node) => domain_at(&node.input, index)?,
        _ => return None,
    };
    // Empty or contradictory bounds can safely decline instead of introducing
    // a special vacuous-proof representation into packing arithmetic.
    if domain.min > domain.max {
        return None;
    }
    Some(domain)
}

fn integer_literal(expr: &Expr) -> Option<i128> {
    match expr {
        Expr::Literal(ScalarValue::Int8(v)) => Some(*v as i128),
        Expr::Literal(ScalarValue::Int16(v)) => Some(*v as i128),
        Expr::Literal(ScalarValue::Int32(v)) => Some(*v as i128),
        Expr::Literal(ScalarValue::Int64(v)) => Some(*v as i128),
        Expr::Literal(ScalarValue::UInt8(v)) => Some(*v as i128),
        Expr::Literal(ScalarValue::UInt16(v)) => Some(*v as i128),
        Expr::Literal(ScalarValue::UInt32(v)) => Some(*v as i128),
        Expr::Literal(ScalarValue::UInt64(v)) => Some(*v as i128),
        _ => None,
    }
}

fn narrow_domain(domain: &mut IntegerDomain, predicate: &Expr, schema: &PlanSchema, index: usize) {
    let is_column = |expr: &Expr| matches!(expr, Expr::Column(column) if column_index(schema, column) == Some(index));
    match predicate {
        Expr::BinaryExpr {
            left,
            op: BinaryOp::And,
            right,
        } => {
            narrow_domain(domain, left, schema, index);
            narrow_domain(domain, right, schema, index);
        }
        Expr::UnaryExpr {
            op: crate::planner::UnaryOp::IsNotNull,
            expr,
        } if is_column(expr) => {
            domain.nullable = false;
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } if is_column(expr) => {
            if let (Some(low), Some(high)) = (integer_literal(low), integer_literal(high)) {
                domain.min = domain.min.max(low);
                domain.max = domain.max.min(high);
                domain.nullable = false;
            }
        }
        Expr::BinaryExpr { left, op, right } => {
            let (value, op) = if is_column(left) {
                (integer_literal(right), *op)
            } else if is_column(right) {
                let reversed = match op {
                    BinaryOp::Eq => BinaryOp::Eq,
                    BinaryOp::Lt => BinaryOp::Gt,
                    BinaryOp::LtEq => BinaryOp::GtEq,
                    BinaryOp::Gt => BinaryOp::Lt,
                    BinaryOp::GtEq => BinaryOp::LtEq,
                    _ => return,
                };
                (integer_literal(left), reversed)
            } else {
                return;
            };
            let Some(value) = value else {
                return;
            };
            match op {
                BinaryOp::Eq => {
                    domain.min = domain.min.max(value);
                    domain.max = domain.max.min(value);
                }
                BinaryOp::Lt => {
                    let Some(bound) = value.checked_sub(1) else {
                        return;
                    };
                    domain.max = domain.max.min(bound);
                }
                BinaryOp::LtEq => domain.max = domain.max.min(value),
                BinaryOp::Gt => {
                    let Some(bound) = value.checked_add(1) else {
                        return;
                    };
                    domain.min = domain.min.max(bound);
                }
                BinaryOp::GtEq => domain.min = domain.min.max(value),
                _ => return,
            }
            domain.nullable = false;
        }
        _ => {}
    }
}

/// Positive power-of-two radix representable as an Int64 literal, with every
/// multiplication/addition proven to fit Int64. Callers use the same domains
/// on both sides of a join so equivalent key pairs receive identical encodings.
pub(crate) fn packing_radix(first: IntegerDomain, second: IntegerDomain) -> Option<i64> {
    if first.min < 0 || second.min < 0 || first.min > first.max || second.min > second.max {
        return None;
    }
    let radix = u64::try_from(second.max)
        .ok()?
        .checked_add(1)?
        .checked_next_power_of_two()?;
    let radix = i64::try_from(radix).ok()?;
    let maximum = first
        .max
        .checked_mul(radix as i128)?
        .checked_add(second.max)?;
    (maximum <= i64::MAX as i128).then_some(radix)
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
        if agg.group_by.len() != 2 {
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

        let fields = agg.schema.fields();
        if fields.len() < 2 || fields.iter().any(|field| field.name == "__pk") {
            return None;
        }
        for (index, column) in cols.iter().enumerate() {
            let field = &fields[index];
            if field.name != column.name
                || agg.group_by[index].data_type(&agg.input.schema()).ok()? != field.data_type
            {
                return None;
            }
        }
        let first = integer_domain(&agg.input, cols[0])?;
        let second = integer_domain(&agg.input, cols[1])?;
        if first.nullable || second.nullable {
            return None;
        }
        let k = packing_radix(first, second)?;
        let shift = k.trailing_zeros() as i64;
        let mask = k - 1;

        let cast_i64 = |e: Expr| Expr::Cast {
            expr: Box::new(e),
            data_type: DataType::Int64,
            mode: crate::planner::CastMode::Strict,
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
                    mode: crate::planner::CastMode::Strict,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::ColumnStatistics;
    use crate::planner::{FilterNode, JoinNode, JoinType, ScanNode};

    fn input() -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Scan(ScanNode {
            table_name: "t".into(),
            schema: PlanSchema::new(vec![
                SchemaField::new("a", DataType::Int32),
                SchemaField::new("b", DataType::Int32),
            ]),
            projection: None,
            filter: None,
        }))
    }

    fn aggregate(input: Arc<LogicalPlan>) -> AggregateNode {
        AggregateNode {
            schema: input.schema(),
            input,
            group_by: vec![Expr::column("a"), Expr::column("b")],
            aggregates: Vec::new(),
        }
    }

    fn misleading_stats() -> HashMap<String, TableStatistics> {
        [(
            "t".into(),
            TableStatistics {
                row_count: 2,
                total_byte_size: 16,
                column_stats: ["a", "b"]
                    .into_iter()
                    .map(|name| {
                        (
                            name.into(),
                            ColumnStatistics {
                                min_i64: Some(0),
                                max_i64: Some(1),
                                null_count: Some(0),
                                ..Default::default()
                            },
                        )
                    })
                    .collect(),
            },
        )]
        .into_iter()
        .collect()
    }

    #[test]
    fn estimates_do_not_prove_signed_bounds_or_null_freedom() {
        let rule = PackedGroupKeys::with_table_statistics(misleading_stats());
        assert!(rule.try_pack(&aggregate(input())).is_none());
    }

    #[test]
    fn computed_alias_cannot_inherit_base_column_range() {
        // Actual keys (0, 2) and (1, 0) collide with the old K=2 proof.
        let base = input();
        let project = Arc::new(LogicalPlan::Project(ProjectNode {
            schema: base.schema(),
            input: base,
            exprs: vec![
                Expr::column("a"),
                Expr::Alias {
                    name: "b".into(),
                    expr: Box::new(Expr::BinaryExpr {
                        left: Box::new(Expr::column("b")),
                        op: BinaryOp::Multiply,
                        right: Box::new(Expr::Literal(ScalarValue::Int32(2))),
                    }),
                },
            ],
        }));
        let rule = PackedGroupKeys::with_table_statistics(misleading_stats());
        assert!(rule.try_pack(&aggregate(project)).is_none());
    }

    #[test]
    fn outer_join_null_extension_cannot_use_base_null_counts() {
        // Distinct preserved-side keys paired with an introduced NULL must
        // remain distinct GROUP BY tuples; arithmetic would collapse to NULL.
        let scan = |name: &str| {
            Arc::new(LogicalPlan::Scan(ScanNode {
                table_name: name.into(),
                schema: PlanSchema::new(vec![
                    SchemaField::new(name, DataType::Int32).with_nullable(false)
                ]),
                projection: None,
                filter: None,
            }))
        };
        let joined = Arc::new(LogicalPlan::Join(JoinNode {
            schema: PlanSchema::new(vec![
                SchemaField::new("a", DataType::Int32).with_nullable(false),
                SchemaField::new("b", DataType::Int32),
            ]),
            left: scan("a"),
            right: scan("b"),
            join_type: JoinType::Left,
            on: vec![(Expr::column("a"), Expr::column("b"))],
            filter: None,
        }));
        let agg = AggregateNode {
            input: joined,
            group_by: vec![Expr::column("a"), Expr::column("b")],
            aggregates: vec![],
            schema: input().schema(),
        };
        assert!(PackedGroupKeys::with_table_statistics(misleading_stats())
            .try_pack(&agg)
            .is_none());
    }

    #[test]
    fn explicit_predicates_prove_nonnegative_nonnull_int32_keys() {
        let nonnegative = |name| Expr::BinaryExpr {
            left: Box::new(Expr::column(name)),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::Int32(0))),
        };
        let filtered = Arc::new(LogicalPlan::Filter(FilterNode {
            input: input(),
            predicate: Expr::BinaryExpr {
                left: Box::new(nonnegative("a")),
                op: BinaryOp::And,
                right: Box::new(nonnegative("b")),
            },
        }));
        assert!(PackedGroupKeys::new()
            .try_pack(&aggregate(filtered))
            .is_some());
    }

    #[test]
    fn identity_alias_preserves_only_proven_input_bounds() {
        let predicate = Expr::BinaryExpr {
            left: Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::column("a")),
                op: BinaryOp::GtEq,
                right: Box::new(Expr::Literal(ScalarValue::Int32(0))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Between {
                expr: Box::new(Expr::column("b")),
                low: Box::new(Expr::Literal(ScalarValue::Int32(0))),
                high: Box::new(Expr::Literal(ScalarValue::Int32(3))),
                negated: false,
            }),
        };
        let filtered = Arc::new(LogicalPlan::Filter(FilterNode {
            input: input(),
            predicate,
        }));
        let schema = PlanSchema::new(vec![
            SchemaField::new("x", DataType::Int32),
            SchemaField::new("y", DataType::Int32),
        ]);
        let projected = Arc::new(LogicalPlan::Project(ProjectNode {
            input: filtered,
            schema: schema.clone(),
            exprs: vec![
                Expr::Alias {
                    expr: Box::new(Expr::column("a")),
                    name: "x".into(),
                },
                Expr::Alias {
                    expr: Box::new(Expr::column("b")),
                    name: "y".into(),
                },
            ],
        }));
        let agg = AggregateNode {
            input: projected,
            group_by: vec![Expr::column("x"), Expr::column("y")],
            aggregates: vec![],
            schema,
        };
        assert!(PackedGroupKeys::new().try_pack(&agg).is_some());
    }

    #[test]
    fn inexact_literal_and_disjunction_do_not_narrow_integer_domains() {
        let predicate = Expr::BinaryExpr {
            left: Box::new(Expr::column("a")),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::Float64(
                ordered_float::OrderedFloat(0.0),
            ))),
        };
        let filtered = LogicalPlan::Filter(FilterNode {
            input: input(),
            predicate,
        });
        assert_eq!(
            integer_domain(&filtered, &Column::new("a")).unwrap().min,
            i32::MIN as i128
        );
        let predicate = Expr::BinaryExpr {
            left: Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::column("a")),
                op: BinaryOp::GtEq,
                right: Box::new(Expr::Literal(ScalarValue::Int32(0))),
            }),
            op: BinaryOp::Or,
            right: Box::new(Expr::Literal(ScalarValue::Boolean(true))),
        };
        let filtered = LogicalPlan::Filter(FilterNode {
            input: input(),
            predicate,
        });
        let domain = integer_domain(&filtered, &Column::new("a")).unwrap();
        assert_eq!(domain.min, i32::MIN as i128);
        assert!(domain.nullable);
    }

    #[test]
    fn packing_radix_never_wraps_or_emits_negative_multiplier() {
        let zero = IntegerDomain {
            min: 0,
            max: 0,
            nullable: false,
        };
        assert!(packing_radix(
            zero,
            IntegerDomain {
                min: 0,
                max: i64::MAX as i128,
                nullable: false
            }
        )
        .is_none());
        assert!(packing_radix(
            zero,
            IntegerDomain {
                min: 0,
                max: u64::MAX as i128,
                nullable: false
            }
        )
        .is_none());
        let full_u32 = IntegerDomain {
            min: 0,
            max: u32::MAX as i128,
            nullable: false,
        };
        assert!(packing_radix(full_u32, full_u32).is_none());
        assert_eq!(packing_radix(zero, zero), Some(1));
    }
}
