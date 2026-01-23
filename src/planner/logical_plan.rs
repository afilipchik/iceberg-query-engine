//! Logical plan types

use crate::planner::{Expr, PlanSchema, SchemaField, SortExpr};
use std::fmt;
use std::sync::Arc;

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Full => write!(f, "FULL"),
            JoinType::Semi => write!(f, "SEMI"),
            JoinType::Anti => write!(f, "ANTI"),
            JoinType::Cross => write!(f, "CROSS"),
        }
    }
}

/// Logical plan node
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Table scan
    Scan(ScanNode),
    /// Filter (WHERE clause)
    Filter(FilterNode),
    /// Projection (SELECT columns)
    Project(ProjectNode),
    /// Join
    Join(JoinNode),
    /// Aggregation (GROUP BY)
    Aggregate(AggregateNode),
    /// Sort (ORDER BY)
    Sort(SortNode),
    /// Limit
    Limit(LimitNode),
    /// Distinct
    Distinct(DistinctNode),
    /// Union
    Union(UnionNode),
    /// Subquery alias
    SubqueryAlias(SubqueryAliasNode),
    /// Empty relation (for empty results)
    EmptyRelation(EmptyRelationNode),
    /// Values (inline data)
    Values(ValuesNode),
}

impl LogicalPlan {
    /// Get the output schema of this plan node
    pub fn schema(&self) -> PlanSchema {
        match self {
            LogicalPlan::Scan(node) => node.schema.clone(),
            LogicalPlan::Filter(node) => node.input.schema(),
            LogicalPlan::Project(node) => node.schema.clone(),
            LogicalPlan::Join(node) => node.schema.clone(),
            LogicalPlan::Aggregate(node) => node.schema.clone(),
            LogicalPlan::Sort(node) => node.input.schema(),
            LogicalPlan::Limit(node) => node.input.schema(),
            LogicalPlan::Distinct(node) => node.input.schema(),
            LogicalPlan::Union(node) => node.schema.clone(),
            LogicalPlan::SubqueryAlias(node) => node.schema.clone(),
            LogicalPlan::EmptyRelation(node) => node.schema.clone(),
            LogicalPlan::Values(node) => node.schema.clone(),
        }
    }

    /// Get child plans
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(_) | LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => vec![],
            LogicalPlan::Filter(node) => vec![&node.input],
            LogicalPlan::Project(node) => vec![&node.input],
            LogicalPlan::Join(node) => vec![&node.left, &node.right],
            LogicalPlan::Aggregate(node) => vec![&node.input],
            LogicalPlan::Sort(node) => vec![&node.input],
            LogicalPlan::Limit(node) => vec![&node.input],
            LogicalPlan::Distinct(node) => vec![&node.input],
            LogicalPlan::Union(node) => node.inputs.iter().map(|x| x.as_ref()).collect(),
            LogicalPlan::SubqueryAlias(node) => vec![&node.input],
        }
    }

    /// Create a new plan with children replaced
    pub fn with_new_children(&self, children: Vec<Arc<LogicalPlan>>) -> Self {
        match self {
            LogicalPlan::Scan(node) => LogicalPlan::Scan(node.clone()),
            LogicalPlan::EmptyRelation(node) => LogicalPlan::EmptyRelation(node.clone()),
            LogicalPlan::Values(node) => LogicalPlan::Values(node.clone()),
            LogicalPlan::Filter(node) => LogicalPlan::Filter(FilterNode {
                input: children.into_iter().next().unwrap(),
                predicate: node.predicate.clone(),
            }),
            LogicalPlan::Project(node) => LogicalPlan::Project(ProjectNode {
                input: children.into_iter().next().unwrap(),
                exprs: node.exprs.clone(),
                schema: node.schema.clone(),
            }),
            LogicalPlan::Join(node) => {
                let mut iter = children.into_iter();
                LogicalPlan::Join(JoinNode {
                    left: iter.next().unwrap(),
                    right: iter.next().unwrap(),
                    join_type: node.join_type,
                    on: node.on.clone(),
                    filter: node.filter.clone(),
                    schema: node.schema.clone(),
                })
            }
            LogicalPlan::Aggregate(node) => LogicalPlan::Aggregate(AggregateNode {
                input: children.into_iter().next().unwrap(),
                group_by: node.group_by.clone(),
                aggregates: node.aggregates.clone(),
                schema: node.schema.clone(),
            }),
            LogicalPlan::Sort(node) => LogicalPlan::Sort(SortNode {
                input: children.into_iter().next().unwrap(),
                order_by: node.order_by.clone(),
            }),
            LogicalPlan::Limit(node) => LogicalPlan::Limit(LimitNode {
                input: children.into_iter().next().unwrap(),
                skip: node.skip,
                fetch: node.fetch,
            }),
            LogicalPlan::Distinct(node) => LogicalPlan::Distinct(DistinctNode {
                input: children.into_iter().next().unwrap(),
            }),
            LogicalPlan::Union(node) => LogicalPlan::Union(UnionNode {
                inputs: children,
                schema: node.schema.clone(),
                all: node.all,
            }),
            LogicalPlan::SubqueryAlias(node) => LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: children.into_iter().next().unwrap(),
                alias: node.alias.clone(),
                schema: node.schema.clone(),
            }),
        }
    }

    /// Builder: add a filter
    pub fn filter(self, predicate: Expr) -> Self {
        LogicalPlan::Filter(FilterNode {
            input: Arc::new(self),
            predicate,
        })
    }

    /// Builder: add a projection
    pub fn project(self, exprs: Vec<Expr>) -> crate::error::Result<Self> {
        let input = Arc::new(self);
        let input_schema = input.schema();

        let fields: crate::error::Result<Vec<SchemaField>> = exprs
            .iter()
            .map(|e| e.to_field(&input_schema))
            .collect();

        Ok(LogicalPlan::Project(ProjectNode {
            input,
            exprs,
            schema: PlanSchema::new(fields?),
        }))
    }

    /// Builder: add a limit
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Self {
        LogicalPlan::Limit(LimitNode {
            input: Arc::new(self),
            skip,
            fetch,
        })
    }

    /// Builder: add a sort
    pub fn sort(self, order_by: Vec<SortExpr>) -> Self {
        LogicalPlan::Sort(SortNode {
            input: Arc::new(self),
            order_by,
        })
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indent(f, 0)
    }
}

impl LogicalPlan {
    fn fmt_indent(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let prefix = "  ".repeat(indent);

        match self {
            LogicalPlan::Scan(node) => {
                writeln!(f, "{}Scan: {} [{}]", prefix, node.table_name,
                    node.schema.fields().iter().map(|f| f.name.as_str()).collect::<Vec<_>>().join(", "))?;
                if let Some(filter) = &node.filter {
                    writeln!(f, "{}  filter: {}", prefix, filter)?;
                }
            }
            LogicalPlan::Filter(node) => {
                writeln!(f, "{}Filter: {}", prefix, node.predicate)?;
                node.input.fmt_indent(f, indent + 1)?;
            }
            LogicalPlan::Project(node) => {
                let exprs: Vec<String> = node.exprs.iter().map(|e| e.to_string()).collect();
                writeln!(f, "{}Project: [{}]", prefix, exprs.join(", "))?;
                node.input.fmt_indent(f, indent + 1)?;
            }
            LogicalPlan::Join(node) => {
                writeln!(f, "{}{} Join", prefix, node.join_type)?;
                if !node.on.is_empty() {
                    let on_str: Vec<String> = node.on.iter()
                        .map(|(l, r)| format!("{} = {}", l, r))
                        .collect();
                    writeln!(f, "{}  on: {}", prefix, on_str.join(" AND "))?;
                }
                if let Some(filter) = &node.filter {
                    writeln!(f, "{}  filter: {}", prefix, filter)?;
                }
                node.left.fmt_indent(f, indent + 1)?;
                node.right.fmt_indent(f, indent + 1)?;
            }
            LogicalPlan::Aggregate(node) => {
                let group_by: Vec<String> = node.group_by.iter().map(|e| e.to_string()).collect();
                let aggs: Vec<String> = node.aggregates.iter().map(|e| e.to_string()).collect();
                writeln!(f, "{}Aggregate: group_by=[{}], aggs=[{}]", prefix, group_by.join(", "), aggs.join(", "))?;
                node.input.fmt_indent(f, indent + 1)?;
            }
            LogicalPlan::Sort(node) => {
                let order: Vec<String> = node.order_by.iter()
                    .map(|s| format!("{} {:?}", s.expr, s.direction))
                    .collect();
                writeln!(f, "{}Sort: [{}]", prefix, order.join(", "))?;
                node.input.fmt_indent(f, indent + 1)?;
            }
            LogicalPlan::Limit(node) => {
                writeln!(f, "{}Limit: skip={}, fetch={:?}", prefix, node.skip, node.fetch)?;
                node.input.fmt_indent(f, indent + 1)?;
            }
            LogicalPlan::Distinct(node) => {
                writeln!(f, "{}Distinct", prefix)?;
                node.input.fmt_indent(f, indent + 1)?;
            }
            LogicalPlan::Union(node) => {
                writeln!(f, "{}Union", prefix)?;
                for input in &node.inputs {
                    input.fmt_indent(f, indent + 1)?;
                }
            }
            LogicalPlan::SubqueryAlias(node) => {
                writeln!(f, "{}SubqueryAlias: {}", prefix, node.alias)?;
                node.input.fmt_indent(f, indent + 1)?;
            }
            LogicalPlan::EmptyRelation(node) => {
                writeln!(f, "{}EmptyRelation: produce_one_row={}", prefix, node.produce_one_row)?;
            }
            LogicalPlan::Values(node) => {
                writeln!(f, "{}Values: {} rows", prefix, node.values.len())?;
            }
        }
        Ok(())
    }
}

/// Table scan node
#[derive(Debug, Clone, PartialEq)]
pub struct ScanNode {
    pub table_name: String,
    pub schema: PlanSchema,
    /// Optional projection (column indices)
    pub projection: Option<Vec<usize>>,
    /// Optional filter to push down
    pub filter: Option<Expr>,
}

/// Filter node
#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode {
    pub input: Arc<LogicalPlan>,
    pub predicate: Expr,
}

/// Projection node
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectNode {
    pub input: Arc<LogicalPlan>,
    pub exprs: Vec<Expr>,
    pub schema: PlanSchema,
}

/// Join node
#[derive(Debug, Clone, PartialEq)]
pub struct JoinNode {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub join_type: JoinType,
    /// Equi-join conditions (left_col, right_col)
    pub on: Vec<(Expr, Expr)>,
    /// Additional filter conditions
    pub filter: Option<Expr>,
    pub schema: PlanSchema,
}

/// Aggregate node
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateNode {
    pub input: Arc<LogicalPlan>,
    pub group_by: Vec<Expr>,
    pub aggregates: Vec<Expr>,
    pub schema: PlanSchema,
}

/// Sort node
#[derive(Debug, Clone, PartialEq)]
pub struct SortNode {
    pub input: Arc<LogicalPlan>,
    pub order_by: Vec<SortExpr>,
}

/// Limit node
#[derive(Debug, Clone, PartialEq)]
pub struct LimitNode {
    pub input: Arc<LogicalPlan>,
    pub skip: usize,
    pub fetch: Option<usize>,
}

/// Distinct node
#[derive(Debug, Clone, PartialEq)]
pub struct DistinctNode {
    pub input: Arc<LogicalPlan>,
}

/// Union node
#[derive(Debug, Clone, PartialEq)]
pub struct UnionNode {
    pub inputs: Vec<Arc<LogicalPlan>>,
    pub schema: PlanSchema,
    /// If true, this is UNION ALL (keep duplicates); if false, UNION (remove duplicates)
    pub all: bool,
}

/// Subquery alias node
#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryAliasNode {
    pub input: Arc<LogicalPlan>,
    pub alias: String,
    pub schema: PlanSchema,
}

/// Empty relation node
#[derive(Debug, Clone, PartialEq)]
pub struct EmptyRelationNode {
    pub produce_one_row: bool,
    pub schema: PlanSchema,
}

/// Values node (inline data)
#[derive(Debug, Clone, PartialEq)]
pub struct ValuesNode {
    pub values: Vec<Vec<Expr>>,
    pub schema: PlanSchema,
}

/// Builder for creating logical plans
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    pub fn scan(table_name: impl Into<String>, schema: PlanSchema) -> Self {
        Self {
            plan: LogicalPlan::Scan(ScanNode {
                table_name: table_name.into(),
                schema,
                projection: None,
                filter: None,
            }),
        }
    }

    pub fn filter(mut self, predicate: Expr) -> Self {
        self.plan = self.plan.filter(predicate);
        self
    }

    pub fn project(mut self, exprs: Vec<Expr>) -> crate::error::Result<Self> {
        self.plan = self.plan.project(exprs)?;
        Ok(self)
    }

    pub fn aggregate(
        mut self,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
    ) -> crate::error::Result<Self> {
        let input = Arc::new(self.plan);
        let input_schema = input.schema();

        let mut fields = Vec::new();
        for expr in &group_by {
            fields.push(expr.to_field(&input_schema)?);
        }
        for expr in &aggregates {
            fields.push(expr.to_field(&input_schema)?);
        }

        self.plan = LogicalPlan::Aggregate(AggregateNode {
            input,
            group_by,
            aggregates,
            schema: PlanSchema::new(fields),
        });
        Ok(self)
    }

    pub fn sort(mut self, order_by: Vec<SortExpr>) -> Self {
        self.plan = self.plan.sort(order_by);
        self
    }

    pub fn limit(mut self, skip: usize, fetch: Option<usize>) -> Self {
        self.plan = self.plan.limit(skip, fetch);
        self
    }

    pub fn join(
        mut self,
        right: LogicalPlan,
        join_type: JoinType,
        on: Vec<(Expr, Expr)>,
        filter: Option<Expr>,
    ) -> Self {
        let left = Arc::new(self.plan);
        let right = Arc::new(right);

        let left_schema = left.schema();
        let right_schema = right.schema();

        let schema = match join_type {
            JoinType::Semi | JoinType::Anti => left_schema,
            _ => left_schema.merge(&right_schema),
        };

        self.plan = LogicalPlan::Join(JoinNode {
            left,
            right,
            join_type,
            on,
            filter,
            schema,
        });
        self
    }

    pub fn build(self) -> LogicalPlan {
        self.plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::SchemaField;
    use arrow::datatypes::DataType;

    fn sample_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("id", DataType::Int64),
            SchemaField::new("name", DataType::Utf8),
            SchemaField::new("amount", DataType::Float64),
        ])
    }

    #[test]
    fn test_scan_builder() {
        let plan = LogicalPlanBuilder::scan("orders", sample_schema()).build();

        assert!(matches!(plan, LogicalPlan::Scan(_)));
        assert_eq!(plan.schema().len(), 3);
    }

    #[test]
    fn test_filter_builder() {
        let plan = LogicalPlanBuilder::scan("orders", sample_schema())
            .filter(Expr::column("amount").gt(Expr::literal(crate::planner::ScalarValue::Float64(100.0.into()))))
            .build();

        assert!(matches!(plan, LogicalPlan::Filter(_)));
    }

    #[test]
    fn test_plan_display() {
        let plan = LogicalPlanBuilder::scan("orders", sample_schema())
            .filter(Expr::column("amount").gt(Expr::literal(crate::planner::ScalarValue::Float64(100.0.into()))))
            .build();

        let display = format!("{}", plan);
        assert!(display.contains("Filter"));
        assert!(display.contains("Scan"));
    }
}
