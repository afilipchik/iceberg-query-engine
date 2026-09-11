//! Reduce grouping columns only using structurally proven keys.
//!
//! Statistics do not establish functional dependencies. In particular NDV
//! estimates and dense min/max ranges cannot prove uniqueness or referential
//! integrity. Base-scan reduction, deferred decoration re-joins and join
//! elimination therefore remain disabled until enforced constraints exist.
use crate::error::Result;
use crate::optimizer::properties::{column_index, proven_keys};
use crate::optimizer::OptimizerRule;
use crate::physical::operators::TableStatistics;
use crate::planner::{
    AggregateFunction, AggregateNode, Column, Expr, LogicalPlan, PlanSchema, ProjectNode,
    SchemaField,
};
use std::collections::HashMap;
use std::sync::Arc;

pub struct GroupKeyReduction;

impl GroupKeyReduction {
    pub fn new() -> Self {
        Self
    }

    /// Kept for compatibility; estimates are deliberately not proof inputs.
    pub fn with_table_statistics(_table_stats: HashMap<String, TableStatistics>) -> Self {
        Self
    }

    fn reduce(&self, agg: &AggregateNode) -> Option<LogicalPlan> {
        if agg.group_by.len() < 2 {
            return None;
        }
        let input_schema = agg.input.schema();
        let columns = agg
            .group_by
            .iter()
            .map(|expr| match expr {
                Expr::Column(column) => column_index(&input_schema, column),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        // Do not turn a grouped aggregate into a global aggregate: empty input
        // has different cardinality. Select the shortest contained nonempty key.
        let key = proven_keys(&agg.input)
            .into_iter()
            .filter(|key| {
                !key.is_empty()
                    && key.len() < columns.len()
                    && key.iter().all(|part| columns.contains(part))
            })
            .min_by_key(Vec::len)?;
        let kept = key
            .iter()
            .map(|part| columns.iter().position(|col| col == part).unwrap())
            .collect::<Vec<_>>();
        let old_fields = agg.schema.fields();
        // Restoration uses named expressions until the binder has stable
        // column IDs. Never manufacture ambiguous references.
        if old_fields.iter().any(|field| {
            column_index(
                &agg.schema,
                &Column {
                    name: field.name.clone(),
                    relation: field.relation.clone(),
                },
            )
            .is_none()
        }) {
            return None;
        }

        let mut fields = kept
            .iter()
            .map(|&index| old_fields[index].clone())
            .collect::<Vec<_>>();
        fields.extend_from_slice(&old_fields[agg.group_by.len()..]);
        let mut aggregates = agg.aggregates.clone();
        let mut decorations = HashMap::new();
        for (index, expr) in agg.group_by.iter().enumerate() {
            if kept.contains(&index) {
                continue;
            }
            let mut name = format!("__proven_fd_{index}");
            while old_fields
                .iter()
                .chain(fields.iter())
                .any(|field| field.name == name)
            {
                name.push('_');
            }
            aggregates.push(Expr::Alias {
                expr: Box::new(Expr::Aggregate {
                    func: AggregateFunction::AnyValue,
                    args: vec![expr.clone()],
                    distinct: false,
                }),
                name: name.clone(),
            });
            fields.push(SchemaField {
                name: name.clone(),
                data_type: old_fields[index].data_type.clone(),
                nullable: old_fields[index].nullable,
                relation: None,
            });
            decorations.insert(index, name);
        }
        let exprs = old_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if let Some(name) = decorations.get(&index) {
                    Expr::Alias {
                        expr: Box::new(Expr::Column(Column::new(name))),
                        name: field.name.clone(),
                    }
                } else {
                    Expr::Column(Column {
                        name: field.name.clone(),
                        relation: field.relation.clone(),
                    })
                }
            })
            .collect();
        Some(LogicalPlan::Project(ProjectNode {
            input: Arc::new(LogicalPlan::Aggregate(AggregateNode {
                input: agg.input.clone(),
                group_by: kept
                    .iter()
                    .map(|&index| agg.group_by[index].clone())
                    .collect(),
                aggregates,
                schema: PlanSchema::new(fields),
            })),
            exprs,
            schema: agg.schema.clone(),
        }))
    }
}
impl Default for GroupKeyReduction {
    fn default() -> Self {
        Self::new()
    }
}
impl OptimizerRule for GroupKeyReduction {
    fn name(&self) -> &str {
        "GroupKeyReduction"
    }
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let children = plan
            .children()
            .iter()
            .map(|child| self.optimize(child).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let plan = plan.with_new_children(children);
        if let LogicalPlan::Aggregate(agg) = &plan {
            if let Some(reduced) = self.reduce(agg) {
                return Ok(reduced);
            }
        }
        Ok(plan)
    }
}
