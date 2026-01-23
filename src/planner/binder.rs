//! SQL AST to Logical Plan binder

use crate::error::{QueryError, Result};
use crate::parser::{self, ObjectNameExt};
use crate::planner::{
    AggregateFunction, AggregateNode, BinaryOp, Column, DistinctNode, Expr, FilterNode, JoinNode,
    JoinType, LimitNode, LogicalPlan, PlanSchema, ProjectNode, ScalarFunction, ScalarValue,
    ScanNode, SchemaField, SortDirection, SortExpr, SortNode, SubqueryAliasNode, UnaryOp,
    NullOrdering,
};
use arrow::datatypes::DataType as ArrowDataType;
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use sqlparser::ast::{self, Expr as SqlExpr, SelectItem, SetExpr, Statement, TableFactor};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

/// Catalog for table schemas
pub trait Catalog: Send + Sync {
    fn get_table_schema(&self, name: &str) -> Option<PlanSchema>;
    fn table_exists(&self, name: &str) -> bool;
}

/// Simple in-memory catalog
#[derive(Default)]
pub struct InMemoryCatalog {
    tables: HashMap<String, PlanSchema>,
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_table(&mut self, name: impl Into<String>, schema: PlanSchema) {
        self.tables.insert(name.into(), schema);
    }
}

impl Catalog for InMemoryCatalog {
    fn get_table_schema(&self, name: &str) -> Option<PlanSchema> {
        self.tables.get(name).cloned()
    }

    fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

/// SQL Binder - converts SQL AST to LogicalPlan
pub struct Binder<'a> {
    catalog: &'a dyn Catalog,
    /// Current scope's column aliases
    aliases: HashMap<String, Expr>,
    /// Table aliases in scope
    table_aliases: HashMap<String, String>,
    /// CTE definitions (WITH clauses)
    ctes: HashMap<String, Arc<LogicalPlan>>,
    /// Outer scope columns for correlated subqueries (name -> (type, relation))
    outer_scope: HashMap<String, (ArrowDataType, Option<String>)>,
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a dyn Catalog) -> Self {
        Self {
            catalog,
            aliases: HashMap::new(),
            table_aliases: HashMap::new(),
            ctes: HashMap::new(),
            outer_scope: HashMap::new(),
        }
    }

    /// Create a binder with outer scope for correlated subqueries
    fn with_outer_scope(
        catalog: &'a dyn Catalog,
        outer_scope: HashMap<String, (ArrowDataType, Option<String>)>,
        ctes: HashMap<String, Arc<LogicalPlan>>,
    ) -> Self {
        Self {
            catalog,
            aliases: HashMap::new(),
            table_aliases: HashMap::new(),
            ctes,
            outer_scope,
        }
    }

    /// Collect outer scope columns from a schema
    fn collect_outer_scope(schema: &PlanSchema) -> HashMap<String, (ArrowDataType, Option<String>)> {
        let mut scope = HashMap::new();
        for field in schema.fields() {
            scope.insert(
                field.name.clone(),
                (field.data_type.clone(), field.relation.clone()),
            );
        }
        scope
    }

    /// Bind a SQL statement to a logical plan
    pub fn bind(&mut self, stmt: &Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => self.bind_query(query),
            _ => Err(QueryError::NotImplemented(format!(
                "Statement type not supported: {:?}",
                stmt
            ))),
        }
    }

    /// Bind a SQL string to a logical plan
    pub fn bind_sql(&mut self, sql: &str) -> Result<LogicalPlan> {
        let stmt = parser::parse_sql(sql)?;
        self.bind(&stmt)
    }

    fn bind_query(&mut self, query: &ast::Query) -> Result<LogicalPlan> {
        // Process CTEs (WITH clause) first
        if let Some(ref with_clause) = query.with {
            self.bind_ctes(with_clause)?;
        }

        // Start with the body (SELECT, UNION, etc.)
        let mut plan = self.bind_set_expr(&query.body)?;

        // Apply ORDER BY
        if let Some(ref order_by_clause) = query.order_by {
            if !order_by_clause.exprs.is_empty() {
                let order_by = self.bind_order_by(&order_by_clause.exprs, &plan.schema())?;
                plan = LogicalPlan::Sort(SortNode {
                    input: Arc::new(plan),
                    order_by,
                });
            }
        }

        // Apply LIMIT/OFFSET
        if query.limit.is_some() || query.offset.is_some() {
            let skip = query
                .offset
                .as_ref()
                .and_then(|o| self.expr_to_usize(&o.value).ok())
                .unwrap_or(0);
            let fetch = query
                .limit
                .as_ref()
                .and_then(|l| self.expr_to_usize(l).ok());

            plan = LogicalPlan::Limit(LimitNode {
                input: Arc::new(plan),
                skip,
                fetch,
            });
        }

        Ok(plan)
    }

    /// Bind CTEs (WITH clause) and register them
    fn bind_ctes(&mut self, with_clause: &ast::With) -> Result<()> {
        for cte in &with_clause.cte_tables {
            let alias_name = cte.alias.name.value.clone();
            let cte_plan = self.bind_query(&cte.query)?;

            // Store the CTE with its alias
            self.ctes.insert(alias_name.clone(), Arc::new(cte_plan));
        }
        Ok(())
    }

    fn bind_set_expr(&mut self, set_expr: &SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(select) => self.bind_select(select),
            SetExpr::Query(query) => self.bind_query(query),
            SetExpr::SetOperation { op, left, right, .. } => {
                let left_plan = self.bind_set_expr(left)?;
                let right_plan = self.bind_set_expr(right)?;

                match op {
                    ast::SetOperator::Union => {
                        let schema = left_plan.schema();
                        Ok(LogicalPlan::Union(crate::planner::UnionNode {
                            inputs: vec![Arc::new(left_plan), Arc::new(right_plan)],
                            schema,
                        }))
                    }
                    _ => Err(QueryError::NotImplemented(format!(
                        "Set operation not supported: {:?}",
                        op
                    ))),
                }
            }
            SetExpr::Values(values) => {
                // VALUES clause
                let mut rows = Vec::new();
                for row in &values.rows {
                    let exprs: Result<Vec<Expr>> = row.iter().map(|e| self.bind_expr(e, &PlanSchema::empty())).collect();
                    rows.push(exprs?);
                }

                // Infer schema from first row
                let schema = if let Some(first_row) = rows.first() {
                    let fields: Vec<SchemaField> = first_row
                        .iter()
                        .enumerate()
                        .map(|(i, e)| {
                            let dt = e.data_type(&PlanSchema::empty()).unwrap_or(ArrowDataType::Utf8);
                            SchemaField::new(format!("column{}", i), dt)
                        })
                        .collect();
                    PlanSchema::new(fields)
                } else {
                    PlanSchema::empty()
                };

                Ok(LogicalPlan::Values(crate::planner::ValuesNode { values: rows, schema }))
            }
            _ => Err(QueryError::NotImplemented(format!(
                "Set expression not supported: {:?}",
                set_expr
            ))),
        }
    }

    fn bind_select(&mut self, select: &ast::Select) -> Result<LogicalPlan> {
        // 1. FROM clause
        let mut plan = self.bind_from(&select.from)?;

        // 2. WHERE clause
        if let Some(selection) = &select.selection {
            let input_schema = plan.schema();
            let predicate = self.bind_expr(selection, &input_schema)?;
            plan = LogicalPlan::Filter(FilterNode {
                input: Arc::new(plan),
                predicate,
            });
        }

        // 3. GROUP BY and aggregates
        let input_schema = plan.schema();
        let (group_by, aggregates, aggregate_aliases, has_aggregates) =
            self.extract_aggregates(&select.projection, &select.group_by, &input_schema)?;

        if has_aggregates || !group_by.is_empty() {
            let mut agg_fields = Vec::new();
            for expr in &group_by {
                agg_fields.push(expr.to_field(&input_schema)?);
            }
            for (i, expr) in aggregates.iter().enumerate() {
                // Use the alias if available, otherwise use the expression's output name
                let field_name = aggregate_aliases.get(i)
                    .and_then(|a| a.as_ref().cloned())
                    .unwrap_or_else(|| expr.output_name());
                let data_type = expr.data_type(&input_schema)?;
                agg_fields.push(SchemaField::new(field_name, data_type));
            }

            plan = LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(plan),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: PlanSchema::new(agg_fields),
            });
        }

        // 4. HAVING clause
        if let Some(having) = &select.having {
            let having_schema = plan.schema();
            let predicate = self.bind_expr(having, &having_schema)?;
            plan = LogicalPlan::Filter(FilterNode {
                input: Arc::new(plan),
                predicate,
            });
        }

        // 5. SELECT projection
        let proj_schema = plan.schema();

        // If we have an aggregate, we need to rewrite the projection expressions
        // to reference the aggregate's output columns by name instead of re-evaluating
        // the aggregate expressions.
        let (proj_exprs, proj_fields) = if has_aggregates || !group_by.is_empty() {
            self.bind_projection_after_aggregate(
                &select.projection,
                &proj_schema,
                &group_by,
                &aggregates,
                &input_schema,
            )?
        } else {
            self.bind_projection(&select.projection, &proj_schema)?
        };

        plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(plan),
            exprs: proj_exprs,
            schema: PlanSchema::new(proj_fields),
        });

        // 6. DISTINCT
        if let Some(distinct) = &select.distinct {
            match distinct {
                ast::Distinct::Distinct => {
                    plan = LogicalPlan::Distinct(DistinctNode {
                        input: Arc::new(plan),
                    });
                }
                ast::Distinct::On(_) => {
                    return Err(QueryError::NotImplemented("DISTINCT ON not supported".to_string()));
                }
            }
        }

        Ok(plan)
    }

    fn bind_from(&mut self, from: &[ast::TableWithJoins]) -> Result<LogicalPlan> {
        if from.is_empty() {
            // No FROM clause - return empty relation that produces one row
            return Ok(LogicalPlan::EmptyRelation(crate::planner::EmptyRelationNode {
                produce_one_row: true,
                schema: PlanSchema::empty(),
            }));
        }

        let mut plan = self.bind_table_with_joins(&from[0])?;

        // Cross join any additional tables
        for table_with_joins in from.iter().skip(1) {
            let right = self.bind_table_with_joins(table_with_joins)?;
            let left_schema = plan.schema();
            let right_schema = right.schema();

            plan = LogicalPlan::Join(JoinNode {
                left: Arc::new(plan),
                right: Arc::new(right),
                join_type: JoinType::Cross,
                on: vec![],
                filter: None,
                schema: left_schema.merge(&right_schema),
            });
        }

        Ok(plan)
    }

    fn bind_table_with_joins(&mut self, table: &ast::TableWithJoins) -> Result<LogicalPlan> {
        let mut plan = self.bind_table_factor(&table.relation)?;

        for join in &table.joins {
            let right = self.bind_table_factor(&join.relation)?;
            plan = self.bind_join(plan, right, join)?;
        }

        Ok(plan)
    }

    fn bind_table_factor(&mut self, factor: &TableFactor) -> Result<LogicalPlan> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.table_name();

                // Apply table alias to schema fields
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());

                self.table_aliases
                    .insert(alias_name.clone(), table_name.clone());

                // Check if this is a CTE reference first
                if let Some(cte_plan) = self.ctes.get(&table_name) {
                    // CTEs are full logical plans, clone and apply alias
                    let schema = cte_plan.schema();
                    let aliased_schema = PlanSchema::new(
                        schema
                            .fields()
                            .iter()
                            .map(|f| f.clone().with_relation(alias_name.clone()))
                            .collect(),
                    );

                    return Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                        input: Arc::clone(cte_plan),
                        alias: alias_name,
                        schema: aliased_schema,
                    }));
                }

                // Regular table scan
                let schema = self
                    .catalog
                    .get_table_schema(&table_name)
                    .ok_or_else(|| QueryError::TableNotFound(table_name.clone()))?;

                let aliased_schema = PlanSchema::new(
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.clone().with_relation(alias_name.clone()))
                        .collect(),
                );

                let scan = LogicalPlan::Scan(ScanNode {
                    table_name: table_name.clone(),
                    schema: aliased_schema.clone(),
                    projection: None,
                    filter: None,
                });

                if alias.is_some() {
                    Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                        input: Arc::new(scan),
                        alias: alias_name,
                        schema: aliased_schema,
                    }))
                } else {
                    Ok(scan)
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let plan = self.bind_query(subquery)?;
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "subquery".to_string());

                let schema = plan.schema();
                let aliased_schema = PlanSchema::new(
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.clone().with_relation(alias_name.clone()))
                        .collect(),
                );

                Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                    input: Arc::new(plan),
                    alias: alias_name,
                    schema: aliased_schema,
                }))
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                self.bind_table_with_joins(table_with_joins)
            }
            _ => Err(QueryError::NotImplemented(format!(
                "Table factor not supported: {:?}",
                factor
            ))),
        }
    }

    fn bind_join(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        join: &ast::Join,
    ) -> Result<LogicalPlan> {
        let join_type = match &join.join_operator {
            ast::JoinOperator::Inner(_) => JoinType::Inner,
            ast::JoinOperator::LeftOuter(_) => JoinType::Left,
            ast::JoinOperator::RightOuter(_) => JoinType::Right,
            ast::JoinOperator::FullOuter(_) => JoinType::Full,
            ast::JoinOperator::CrossJoin => JoinType::Cross,
            ast::JoinOperator::LeftSemi(_) => JoinType::Semi,
            ast::JoinOperator::LeftAnti(_) => JoinType::Anti,
            _ => {
                return Err(QueryError::NotImplemented(format!(
                    "Join type not supported: {:?}",
                    join.join_operator
                )))
            }
        };

        let left_schema = left.schema();
        let right_schema = right.schema();
        let combined_schema = left_schema.merge(&right_schema);

        let (on, filter) = match &join.join_operator {
            ast::JoinOperator::Inner(constraint)
            | ast::JoinOperator::LeftOuter(constraint)
            | ast::JoinOperator::RightOuter(constraint)
            | ast::JoinOperator::FullOuter(constraint)
            | ast::JoinOperator::LeftSemi(constraint)
            | ast::JoinOperator::LeftAnti(constraint) => {
                self.bind_join_constraint(constraint, &combined_schema)?
            }
            ast::JoinOperator::CrossJoin => (vec![], None),
            _ => (vec![], None),
        };

        let schema = match join_type {
            JoinType::Semi | JoinType::Anti => left_schema,
            _ => combined_schema,
        };

        Ok(LogicalPlan::Join(JoinNode {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type,
            on,
            filter,
            schema,
        }))
    }

    fn bind_join_constraint(
        &mut self,
        constraint: &ast::JoinConstraint,
        schema: &PlanSchema,
    ) -> Result<(Vec<(Expr, Expr)>, Option<Expr>)> {
        match constraint {
            ast::JoinConstraint::On(expr) => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let (equi_conditions, filter) = self.extract_equi_join_conditions(bound_expr);
                Ok((equi_conditions, filter))
            }
            ast::JoinConstraint::Using(cols) => {
                let on: Vec<(Expr, Expr)> = cols
                    .iter()
                    .map(|col| {
                        let name = &col.value;
                        (Expr::column(name.clone()), Expr::column(name.clone()))
                    })
                    .collect();
                Ok((on, None))
            }
            ast::JoinConstraint::Natural => {
                Err(QueryError::NotImplemented("NATURAL JOIN not supported".to_string()))
            }
            ast::JoinConstraint::None => Ok((vec![], None)),
        }
    }

    fn extract_equi_join_conditions(&self, expr: Expr) -> (Vec<(Expr, Expr)>, Option<Expr>) {
        let mut equi_conditions = Vec::new();
        let mut other_conditions = Vec::new();

        self.extract_equi_conditions_recursive(expr, &mut equi_conditions, &mut other_conditions);

        let filter = if other_conditions.is_empty() {
            None
        } else {
            Some(
                other_conditions
                    .into_iter()
                    .reduce(|a, b| a.and(b))
                    .unwrap(),
            )
        };

        (equi_conditions, filter)
    }

    fn extract_equi_conditions_recursive(
        &self,
        expr: Expr,
        equi: &mut Vec<(Expr, Expr)>,
        other: &mut Vec<Expr>,
    ) {
        match expr {
            Expr::BinaryExpr { left, op, right } => match op {
                BinaryOp::And => {
                    self.extract_equi_conditions_recursive(*left, equi, other);
                    self.extract_equi_conditions_recursive(*right, equi, other);
                }
                BinaryOp::Eq => {
                    // Check if this is column = column
                    if matches!(&*left, Expr::Column(_)) && matches!(&*right, Expr::Column(_)) {
                        equi.push((*left, *right));
                    } else {
                        other.push(Expr::BinaryExpr {
                            left,
                            op: BinaryOp::Eq,
                            right,
                        });
                    }
                }
                _ => {
                    other.push(Expr::BinaryExpr { left, op, right });
                }
            },
            _ => {
                other.push(expr);
            }
        }
    }

    fn bind_projection(
        &mut self,
        items: &[SelectItem],
        schema: &PlanSchema,
    ) -> Result<(Vec<Expr>, Vec<SchemaField>)> {
        let mut exprs = Vec::new();
        let mut fields = Vec::new();

        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let bound = self.bind_expr(expr, schema)?;
                    let field = bound.to_field(schema)?;
                    fields.push(field);
                    exprs.push(bound);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let bound = self.bind_expr(expr, schema)?;
                    let aliased = bound.alias(alias.value.clone());
                    let field = SchemaField::new(
                        alias.value.clone(),
                        aliased.data_type(schema)?,
                    );
                    fields.push(field);
                    exprs.push(aliased);
                }
                SelectItem::Wildcard(_) => {
                    for (i, field) in schema.fields().iter().enumerate() {
                        exprs.push(Expr::Column(Column {
                            relation: field.relation.clone(),
                            name: field.name.clone(),
                        }));
                        fields.push(schema.fields()[i].clone());
                    }
                }
                SelectItem::QualifiedWildcard(name, _) => {
                    let table_name = name.table_name();
                    for field in schema.fields() {
                        if field.relation.as_deref() == Some(&table_name) {
                            exprs.push(Expr::Column(Column {
                                relation: field.relation.clone(),
                                name: field.name.clone(),
                            }));
                            fields.push(field.clone());
                        }
                    }
                }
            }
        }

        Ok((exprs, fields))
    }

    /// Bind projection expressions after an aggregate.
    /// This converts aggregate expressions to column references that match the aggregate's output.
    fn bind_projection_after_aggregate(
        &mut self,
        items: &[SelectItem],
        agg_schema: &PlanSchema,
        group_by: &[Expr],
        aggregates: &[Expr],
        input_schema: &PlanSchema,
    ) -> Result<(Vec<Expr>, Vec<SchemaField>)> {
        let mut exprs = Vec::new();
        let mut fields = Vec::new();

        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    // Bind against the original input schema to get the full expression
                    let bound = self.bind_expr(expr, input_schema)?;
                    // Convert to a reference to the aggregate output
                    let (converted, field) = self.convert_to_agg_output(
                        &bound, agg_schema, group_by, aggregates, input_schema,
                    )?;
                    exprs.push(converted);
                    fields.push(field);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let bound = self.bind_expr(expr, input_schema)?;
                    let (converted, mut field) = self.convert_to_agg_output(
                        &bound, agg_schema, group_by, aggregates, input_schema,
                    )?;
                    let aliased = converted.alias(alias.value.clone());
                    field.name = alias.value.clone();
                    exprs.push(aliased);
                    fields.push(field);
                }
                SelectItem::Wildcard(_) => {
                    for field in agg_schema.fields() {
                        exprs.push(Expr::Column(Column {
                            relation: field.relation.clone(),
                            name: field.name.clone(),
                        }));
                        fields.push(field.clone());
                    }
                }
                SelectItem::QualifiedWildcard(name, _) => {
                    let table_name = name.table_name();
                    for field in agg_schema.fields() {
                        if field.relation.as_deref() == Some(&table_name) {
                            exprs.push(Expr::Column(Column {
                                relation: field.relation.clone(),
                                name: field.name.clone(),
                            }));
                            fields.push(field.clone());
                        }
                    }
                }
            }
        }

        Ok((exprs, fields))
    }

    /// Convert an expression to reference the aggregate output.
    fn convert_to_agg_output(
        &self,
        expr: &Expr,
        agg_schema: &PlanSchema,
        group_by: &[Expr],
        aggregates: &[Expr],
        input_schema: &PlanSchema,
    ) -> Result<(Expr, SchemaField)> {
        // If it's a group by expression, convert to column reference
        for (i, gb) in group_by.iter().enumerate() {
            if expr == gb {
                let field = &agg_schema.fields()[i];
                return Ok((
                    Expr::Column(Column::new(field.name.clone())),
                    field.clone(),
                ));
            }
        }

        // If it's an aggregate expression, convert to column reference
        let group_by_len = group_by.len();
        for (i, agg) in aggregates.iter().enumerate() {
            if expr == agg {
                let field = &agg_schema.fields()[group_by_len + i];
                return Ok((
                    Expr::Column(Column::new(field.name.clone())),
                    field.clone(),
                ));
            }
        }

        // If it contains aggregates, we need to recursively convert
        if expr.contains_aggregate() {
            let output_name = expr.output_name();
            // Try to find a matching aggregate output column
            for (i, agg) in aggregates.iter().enumerate() {
                if agg.output_name() == output_name || expr == agg {
                    let field = &agg_schema.fields()[group_by_len + i];
                    return Ok((
                        Expr::Column(Column::new(field.name.clone())),
                        field.clone(),
                    ));
                }
            }
            // If it's an expression containing an aggregate (like SUM(x) + 1),
            // we need to recursively convert
            let converted = self.convert_expr_with_aggregates(
                expr, agg_schema, group_by, aggregates, input_schema,
            )?;
            let field = converted.to_field(agg_schema)?;
            return Ok((converted, field));
        }

        // For non-aggregate expressions that reference group by columns
        let converted = self.convert_expr_with_aggregates(
            expr, agg_schema, group_by, aggregates, input_schema,
        )?;
        let field = converted.to_field(agg_schema)?;
        Ok((converted, field))
    }

    /// Recursively convert expressions containing aggregates to reference aggregate outputs.
    fn convert_expr_with_aggregates(
        &self,
        expr: &Expr,
        agg_schema: &PlanSchema,
        group_by: &[Expr],
        aggregates: &[Expr],
        input_schema: &PlanSchema,
    ) -> Result<Expr> {
        // Check if this is a group by column
        for (i, gb) in group_by.iter().enumerate() {
            if expr == gb {
                let field = &agg_schema.fields()[i];
                return Ok(Expr::Column(Column::new(field.name.clone())));
            }
        }

        // Check if this is an aggregate
        let group_by_len = group_by.len();
        for (i, agg) in aggregates.iter().enumerate() {
            if expr == agg {
                let field = &agg_schema.fields()[group_by_len + i];
                return Ok(Expr::Column(Column::new(field.name.clone())));
            }
        }

        // Recursively process sub-expressions
        match expr {
            Expr::BinaryExpr { left, op, right } => {
                let left_conv = self.convert_expr_with_aggregates(
                    left, agg_schema, group_by, aggregates, input_schema,
                )?;
                let right_conv = self.convert_expr_with_aggregates(
                    right, agg_schema, group_by, aggregates, input_schema,
                )?;
                Ok(Expr::BinaryExpr {
                    left: Box::new(left_conv),
                    op: *op,
                    right: Box::new(right_conv),
                })
            }
            Expr::UnaryExpr { op, expr: inner } => {
                let inner_conv = self.convert_expr_with_aggregates(
                    inner, agg_schema, group_by, aggregates, input_schema,
                )?;
                Ok(Expr::UnaryExpr {
                    op: *op,
                    expr: Box::new(inner_conv),
                })
            }
            Expr::Alias { expr: inner, name } => {
                let inner_conv = self.convert_expr_with_aggregates(
                    inner, agg_schema, group_by, aggregates, input_schema,
                )?;
                Ok(Expr::Alias {
                    expr: Box::new(inner_conv),
                    name: name.clone(),
                })
            }
            Expr::Cast { expr: inner, data_type } => {
                let inner_conv = self.convert_expr_with_aggregates(
                    inner, agg_schema, group_by, aggregates, input_schema,
                )?;
                Ok(Expr::Cast {
                    expr: Box::new(inner_conv),
                    data_type: data_type.clone(),
                })
            }
            // For columns and literals, return as-is
            Expr::Column(_) | Expr::Literal(_) => Ok(expr.clone()),
            // For aggregates that weren't matched above, find by output name
            Expr::Aggregate { .. } => {
                let output_name = expr.output_name();
                for (i, agg) in aggregates.iter().enumerate() {
                    if agg.output_name() == output_name {
                        let field = &agg_schema.fields()[group_by_len + i];
                        return Ok(Expr::Column(Column::new(field.name.clone())));
                    }
                }
                // If not found in aggregates, keep as-is (shouldn't happen in well-formed queries)
                Ok(expr.clone())
            }
            // For other expressions, return as-is
            _ => Ok(expr.clone()),
        }
    }

    fn extract_aggregates(
        &mut self,
        projection: &[SelectItem],
        group_by: &ast::GroupByExpr,
        schema: &PlanSchema,
    ) -> Result<(Vec<Expr>, Vec<Expr>, Vec<Option<String>>, bool)> {
        let mut group_by_exprs = Vec::new();
        let mut aggregate_exprs = Vec::new();
        let mut aggregate_aliases = Vec::new();
        let mut has_aggregates = false;

        // Parse GROUP BY
        if let ast::GroupByExpr::Expressions(exprs, _) = group_by {
            for expr in exprs {
                group_by_exprs.push(self.bind_expr(expr, schema)?);
            }
        }

        // Extract aggregates from projection
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let bound = self.bind_expr(expr, schema)?;
                    // Only add alias if this item contains aggregates
                    if bound.contains_aggregate() {
                        self.collect_aggregates(&bound, &mut aggregate_exprs);
                        // No alias for unnamed expressions
                        aggregate_aliases.push(None);
                        has_aggregates = true;
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let bound = self.bind_expr(expr, schema)?;
                    // Only add alias if this item contains aggregates
                    if bound.contains_aggregate() {
                        self.collect_aggregates(&bound, &mut aggregate_exprs);
                        // Store the alias
                        aggregate_aliases.push(Some(alias.value.clone()));
                        has_aggregates = true;
                    }
                }
                _ => {}
            }
        }

        Ok((group_by_exprs, aggregate_exprs, aggregate_aliases, has_aggregates))
    }

    fn collect_aggregates(&self, expr: &Expr, aggregates: &mut Vec<Expr>) {
        match expr {
            Expr::Aggregate { .. } => {
                if !aggregates.contains(expr) {
                    aggregates.push(expr.clone());
                }
            }
            Expr::BinaryExpr { left, right, .. } => {
                self.collect_aggregates(left, aggregates);
                self.collect_aggregates(right, aggregates);
            }
            Expr::UnaryExpr { expr, .. } => {
                self.collect_aggregates(expr, aggregates);
            }
            Expr::ScalarFunc { args, .. } => {
                for arg in args {
                    self.collect_aggregates(arg, aggregates);
                }
            }
            Expr::Cast { expr, .. } => {
                self.collect_aggregates(expr, aggregates);
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    self.collect_aggregates(op, aggregates);
                }
                for (w, t) in when_then {
                    self.collect_aggregates(w, aggregates);
                    self.collect_aggregates(t, aggregates);
                }
                if let Some(e) = else_expr {
                    self.collect_aggregates(e, aggregates);
                }
            }
            Expr::Alias { expr, .. } => {
                self.collect_aggregates(expr, aggregates);
            }
            _ => {}
        }
    }

    fn bind_order_by(
        &mut self,
        order_by: &[ast::OrderByExpr],
        schema: &PlanSchema,
    ) -> Result<Vec<SortExpr>> {
        order_by
            .iter()
            .map(|o| {
                let expr = self.bind_expr(&o.expr, schema)?;
                let direction = if o.asc.unwrap_or(true) {
                    SortDirection::Asc
                } else {
                    SortDirection::Desc
                };
                let nulls = match o.nulls_first {
                    Some(true) => NullOrdering::NullsFirst,
                    Some(false) => NullOrdering::NullsLast,
                    None => NullOrdering::NullsLast,
                };
                Ok(SortExpr {
                    expr,
                    direction,
                    nulls,
                })
            })
            .collect()
    }

    fn bind_expr(&mut self, expr: &SqlExpr, schema: &PlanSchema) -> Result<Expr> {
        match expr {
            SqlExpr::Identifier(ident) => {
                let name = &ident.value;
                // Check aliases first
                if let Some(aliased) = self.aliases.get(name) {
                    return Ok(aliased.clone());
                }
                Ok(Expr::Column(Column::new(name.clone())))
            }
            SqlExpr::CompoundIdentifier(idents) => {
                if idents.len() == 2 {
                    let table = &idents[0].value;
                    let column = &idents[1].value;
                    Ok(Expr::Column(Column::new_qualified(table.clone(), column.clone())))
                } else {
                    Err(QueryError::Bind(format!(
                        "Unsupported compound identifier: {:?}",
                        idents
                    )))
                }
            }
            SqlExpr::Value(value) => self.bind_value(value),
            SqlExpr::BinaryOp { left, op, right } => {
                let left_expr = self.bind_expr(left, schema)?;
                let right_expr = self.bind_expr(right, schema)?;
                let binary_op = self.convert_binary_op(op)?;
                Ok(Expr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: binary_op,
                    right: Box::new(right_expr),
                })
            }
            SqlExpr::UnaryOp { op, expr } => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let unary_op = match op {
                    ast::UnaryOperator::Not => UnaryOp::Not,
                    ast::UnaryOperator::Minus => UnaryOp::Negate,
                    ast::UnaryOperator::Plus => return Ok(bound_expr),
                    _ => {
                        return Err(QueryError::NotImplemented(format!(
                            "Unary operator not supported: {:?}",
                            op
                        )))
                    }
                };
                Ok(Expr::UnaryExpr {
                    op: unary_op,
                    expr: Box::new(bound_expr),
                })
            }
            SqlExpr::IsNull(expr) => {
                let bound = self.bind_expr(expr, schema)?;
                Ok(Expr::UnaryExpr {
                    op: UnaryOp::IsNull,
                    expr: Box::new(bound),
                })
            }
            SqlExpr::IsNotNull(expr) => {
                let bound = self.bind_expr(expr, schema)?;
                Ok(Expr::UnaryExpr {
                    op: UnaryOp::IsNotNull,
                    expr: Box::new(bound),
                })
            }
            SqlExpr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let bound_low = self.bind_expr(low, schema)?;
                let bound_high = self.bind_expr(high, schema)?;
                Ok(Expr::Between {
                    expr: Box::new(bound_expr),
                    low: Box::new(bound_low),
                    high: Box::new(bound_high),
                    negated: *negated,
                })
            }
            SqlExpr::InList {
                expr,
                list,
                negated,
            } => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let bound_list: Result<Vec<Expr>> =
                    list.iter().map(|e| self.bind_expr(e, schema)).collect();
                Ok(Expr::InList {
                    expr: Box::new(bound_expr),
                    list: bound_list?,
                    negated: *negated,
                })
            }
            SqlExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let subquery_plan = self.bind_query(subquery)?;
                Ok(Expr::InSubquery {
                    expr: Box::new(bound_expr),
                    subquery: Arc::new(subquery_plan),
                    negated: *negated,
                })
            }
            SqlExpr::Exists { subquery, negated } => {
                let subquery_plan = self.bind_query(subquery)?;
                Ok(Expr::Exists {
                    subquery: Arc::new(subquery_plan),
                    negated: *negated,
                })
            }
            SqlExpr::Subquery(subquery) => {
                let subquery_plan = self.bind_query(subquery)?;
                Ok(Expr::ScalarSubquery(Arc::new(subquery_plan)))
            }
            SqlExpr::Function(func) => self.bind_function(func, schema),
            SqlExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let bound_operand = operand
                    .as_ref()
                    .map(|e| self.bind_expr(e, schema))
                    .transpose()?
                    .map(Box::new);

                let when_then: Result<Vec<(Expr, Expr)>> = conditions
                    .iter()
                    .zip(results.iter())
                    .map(|(when, then)| {
                        let bound_when = self.bind_expr(when, schema)?;
                        let bound_then = self.bind_expr(then, schema)?;
                        Ok((bound_when, bound_then))
                    })
                    .collect();

                let bound_else = else_result
                    .as_ref()
                    .map(|e| self.bind_expr(e, schema))
                    .transpose()?
                    .map(Box::new);

                Ok(Expr::Case {
                    operand: bound_operand,
                    when_then: when_then?,
                    else_expr: bound_else,
                })
            }
            SqlExpr::Cast { expr, data_type, .. } => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let arrow_type = self.convert_data_type(data_type)?;
                Ok(Expr::Cast {
                    expr: Box::new(bound_expr),
                    data_type: arrow_type,
                })
            }
            SqlExpr::Extract { field, expr, .. } => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let field_name = format!("{:?}", field).to_uppercase();
                Ok(Expr::ScalarFunc {
                    func: ScalarFunction::Extract,
                    args: vec![Expr::Literal(ScalarValue::Utf8(field_name)), bound_expr],
                })
            }
            SqlExpr::Substring { expr, substring_from, substring_for, .. } => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let mut args = vec![bound_expr];

                // Handle FROM clause
                if let Some(from_expr) = substring_from {
                    args.push(self.bind_expr(from_expr, schema)?);
                } else {
                    // Default to 1 if not specified
                    args.push(Expr::Literal(ScalarValue::Int64(1)));
                }

                // Handle FOR clause (optional length)
                if let Some(for_expr) = substring_for {
                    args.push(self.bind_expr(for_expr, schema)?);
                }

                Ok(Expr::ScalarFunc {
                    func: ScalarFunction::Substring,
                    args,
                })
            }
            SqlExpr::Nested(inner) => self.bind_expr(inner, schema),
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                let bound_expr = self.bind_expr(expr, schema)?;
                let bound_pattern = self.bind_expr(pattern, schema)?;
                let op = if *negated {
                    BinaryOp::NotLike
                } else {
                    BinaryOp::Like
                };
                Ok(Expr::BinaryExpr {
                    left: Box::new(bound_expr),
                    op,
                    right: Box::new(bound_pattern),
                })
            }
            SqlExpr::ILike {
                negated,
                expr,
                pattern,
                ..
            } => {
                // ILIKE - case insensitive like
                let bound_expr = self.bind_expr(expr, schema)?;
                let bound_pattern = self.bind_expr(pattern, schema)?;
                let op = if *negated {
                    BinaryOp::NotLike
                } else {
                    BinaryOp::Like
                };
                // For now, treat ILIKE as LIKE (proper implementation would need UPPER())
                Ok(Expr::BinaryExpr {
                    left: Box::new(bound_expr),
                    op,
                    right: Box::new(bound_pattern),
                })
            }
            SqlExpr::Interval(interval) => {
                // Simple interval handling
                let value = self.bind_expr(&interval.value, schema)?;
                if let Expr::Literal(ScalarValue::Utf8(s)) = &value {
                    // Parse interval string like "1 day" or "3 month"
                    let parts: Vec<&str> = s.split_whitespace().collect();
                    if parts.len() >= 1 {
                        if let Ok(num) = parts[0].parse::<i64>() {
                            return Ok(Expr::Literal(ScalarValue::Interval(num)));
                        }
                    }
                }
                Ok(value)
            }
            SqlExpr::TypedString { data_type, value } => {
                match data_type {
                    ast::DataType::Date => {
                        // Parse date string
                        if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                            let days = date.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                            return Ok(Expr::Literal(ScalarValue::Date32(days)));
                        }
                    }
                    _ => {}
                }
                Ok(Expr::Literal(ScalarValue::Utf8(value.clone())))
            }
            _ => Err(QueryError::NotImplemented(format!(
                "Expression not supported: {:?}",
                expr
            ))),
        }
    }

    fn bind_value(&self, value: &ast::Value) -> Result<Expr> {
        match value {
            ast::Value::Number(n, _) => {
                // Try parsing as different numeric types
                if let Ok(i) = n.parse::<i64>() {
                    return Ok(Expr::Literal(ScalarValue::Int64(i)));
                }
                if let Ok(f) = n.parse::<f64>() {
                    return Ok(Expr::Literal(ScalarValue::Float64(OrderedFloat(f))));
                }
                if let Ok(d) = Decimal::from_str(n) {
                    return Ok(Expr::Literal(ScalarValue::Decimal128(d)));
                }
                Err(QueryError::Parse(format!("Cannot parse number: {}", n)))
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                Ok(Expr::Literal(ScalarValue::Utf8(s.clone())))
            }
            ast::Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(*b))),
            ast::Value::Null => Ok(Expr::Literal(ScalarValue::Null)),
            _ => Err(QueryError::NotImplemented(format!(
                "Value type not supported: {:?}",
                value
            ))),
        }
    }

    fn bind_function(&mut self, func: &ast::Function, schema: &PlanSchema) -> Result<Expr> {
        let name = func.name.to_string().to_uppercase();

        // Extract arguments from the FunctionArguments
        let func_args: Vec<&ast::FunctionArg> = match &func.args {
            ast::FunctionArguments::None => vec![],
            ast::FunctionArguments::Subquery(_) => {
                return Err(QueryError::NotImplemented("Subquery function arguments".into()));
            }
            ast::FunctionArguments::List(arg_list) => arg_list.args.iter().collect(),
        };

        let args: Result<Vec<Expr>> = func_args
            .iter()
            .map(|arg| match arg {
                ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                    ast::FunctionArgExpr::Expr(e) => self.bind_expr(e, schema),
                    ast::FunctionArgExpr::Wildcard => Ok(Expr::Wildcard),
                    ast::FunctionArgExpr::QualifiedWildcard(name) => {
                        Ok(Expr::QualifiedWildcard(name.to_string()))
                    }
                },
                ast::FunctionArg::Named { arg, .. } => match arg {
                    ast::FunctionArgExpr::Expr(e) => self.bind_expr(e, schema),
                    ast::FunctionArgExpr::Wildcard => Ok(Expr::Wildcard),
                    ast::FunctionArgExpr::QualifiedWildcard(name) => {
                        Ok(Expr::QualifiedWildcard(name.to_string()))
                    }
                },
            })
            .collect();
        let args = args?;

        // Check for DISTINCT in function args
        let distinct = match &func.args {
            ast::FunctionArguments::List(arg_list) => {
                matches!(arg_list.duplicate_treatment, Some(ast::DuplicateTreatment::Distinct))
            }
            _ => false,
        };

        // Check for aggregate functions
        match name.as_str() {
            "COUNT" => {
                let func_type = if distinct {
                    AggregateFunction::CountDistinct
                } else {
                    AggregateFunction::Count
                };
                Ok(Expr::Aggregate {
                    func: func_type,
                    args,
                    distinct,
                })
            }
            "SUM" => Ok(Expr::Aggregate {
                func: AggregateFunction::Sum,
                args,
                distinct,
            }),
            "AVG" => Ok(Expr::Aggregate {
                func: AggregateFunction::Avg,
                args,
                distinct,
            }),
            "MIN" => Ok(Expr::Aggregate {
                func: AggregateFunction::Min,
                args,
                distinct: false,
            }),
            "MAX" => Ok(Expr::Aggregate {
                func: AggregateFunction::Max,
                args,
                distinct: false,
            }),
            // Scalar functions
            "UPPER" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Upper,
                args,
            }),
            "LOWER" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Lower,
                args,
            }),
            "TRIM" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Trim,
                args,
            }),
            "LTRIM" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Ltrim,
                args,
            }),
            "RTRIM" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Rtrim,
                args,
            }),
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Length,
                args,
            }),
            "SUBSTRING" | "SUBSTR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Substring,
                args,
            }),
            "CONCAT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Concat,
                args,
            }),
            "REPLACE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Replace,
                args,
            }),
            "ABS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Abs,
                args,
            }),
            "CEIL" | "CEILING" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Ceil,
                args,
            }),
            "FLOOR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Floor,
                args,
            }),
            "ROUND" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Round,
                args,
            }),
            "YEAR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Year,
                args,
            }),
            "MONTH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Month,
                args,
            }),
            "DAY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Day,
                args,
            }),
            "DATE_TRUNC" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DateTrunc,
                args,
            }),
            "DATE_PART" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DatePart,
                args,
            }),
            "COALESCE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Coalesce,
                args,
            }),
            "NULLIF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::NullIf,
                args,
            }),
            "EXTRACT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Extract,
                args,
            }),
            _ => Err(QueryError::NotImplemented(format!(
                "Function not supported: {}",
                name
            ))),
        }
    }

    fn convert_binary_op(&self, op: &ast::BinaryOperator) -> Result<BinaryOp> {
        match op {
            ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOp::Subtract),
            ast::BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
            ast::BinaryOperator::Divide => Ok(BinaryOp::Divide),
            ast::BinaryOperator::Modulo => Ok(BinaryOp::Modulo),
            ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
            ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
            ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
            ast::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
            ast::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
            ast::BinaryOperator::And => Ok(BinaryOp::And),
            ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            ast::BinaryOperator::StringConcat => Ok(BinaryOp::StringConcat),
            _ => Err(QueryError::NotImplemented(format!(
                "Binary operator not supported: {:?}",
                op
            ))),
        }
    }

    fn convert_data_type(&self, dt: &ast::DataType) -> Result<ArrowDataType> {
        match dt {
            ast::DataType::Boolean => Ok(ArrowDataType::Boolean),
            ast::DataType::TinyInt(_) => Ok(ArrowDataType::Int8),
            ast::DataType::SmallInt(_) => Ok(ArrowDataType::Int16),
            ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(ArrowDataType::Int32),
            ast::DataType::BigInt(_) => Ok(ArrowDataType::Int64),
            ast::DataType::Real => Ok(ArrowDataType::Float32),
            ast::DataType::Float(_) | ast::DataType::Double | ast::DataType::DoublePrecision => {
                Ok(ArrowDataType::Float64)
            }
            ast::DataType::Decimal(info) | ast::DataType::Numeric(info) => {
                match info {
                    ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                        Ok(ArrowDataType::Decimal128(*p as u8, *s as i8))
                    }
                    ast::ExactNumberInfo::Precision(p) => {
                        Ok(ArrowDataType::Decimal128(*p as u8, 0))
                    }
                    ast::ExactNumberInfo::None => {
                        Ok(ArrowDataType::Decimal128(38, 10))
                    }
                }
            }
            ast::DataType::Char(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
                Ok(ArrowDataType::Utf8)
            }
            ast::DataType::Date => Ok(ArrowDataType::Date32),
            ast::DataType::Timestamp(_, _) => Ok(ArrowDataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            )),
            _ => Err(QueryError::NotImplemented(format!(
                "Data type not supported: {:?}",
                dt
            ))),
        }
    }

    fn expr_to_usize(&self, expr: &SqlExpr) -> Result<usize> {
        match expr {
            SqlExpr::Value(ast::Value::Number(n, _)) => n
                .parse::<usize>()
                .map_err(|_| QueryError::Parse(format!("Cannot parse as usize: {}", n))),
            _ => Err(QueryError::Parse(format!(
                "Expected numeric literal, got: {:?}",
                expr
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn create_test_catalog() -> InMemoryCatalog {
        let mut catalog = InMemoryCatalog::new();

        catalog.register_table(
            "orders",
            PlanSchema::new(vec![
                SchemaField::new("o_orderkey", DataType::Int64),
                SchemaField::new("o_custkey", DataType::Int64),
                SchemaField::new("o_orderstatus", DataType::Utf8),
                SchemaField::new("o_totalprice", DataType::Float64),
                SchemaField::new("o_orderdate", DataType::Date32),
            ]),
        );

        catalog.register_table(
            "lineitem",
            PlanSchema::new(vec![
                SchemaField::new("l_orderkey", DataType::Int64),
                SchemaField::new("l_partkey", DataType::Int64),
                SchemaField::new("l_quantity", DataType::Float64),
                SchemaField::new("l_extendedprice", DataType::Float64),
                SchemaField::new("l_discount", DataType::Float64),
                SchemaField::new("l_tax", DataType::Float64),
                SchemaField::new("l_returnflag", DataType::Utf8),
                SchemaField::new("l_linestatus", DataType::Utf8),
                SchemaField::new("l_shipdate", DataType::Date32),
            ]),
        );

        catalog.register_table(
            "customer",
            PlanSchema::new(vec![
                SchemaField::new("c_custkey", DataType::Int64),
                SchemaField::new("c_name", DataType::Utf8),
                SchemaField::new("c_nationkey", DataType::Int64),
            ]),
        );

        catalog
    }

    #[test]
    fn test_bind_simple_select() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let plan = binder.bind_sql("SELECT o_orderkey, o_totalprice FROM orders").unwrap();
        assert!(matches!(plan, LogicalPlan::Project(_)));
    }

    #[test]
    fn test_bind_select_with_where() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let plan = binder
            .bind_sql("SELECT o_orderkey FROM orders WHERE o_totalprice > 100")
            .unwrap();

        // Should be Project -> Filter -> Scan
        if let LogicalPlan::Project(proj) = plan {
            assert!(matches!(&*proj.input, LogicalPlan::Filter(_)));
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_bind_aggregate() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let plan = binder
            .bind_sql("SELECT COUNT(*), SUM(o_totalprice) FROM orders")
            .unwrap();

        // Should have aggregate
        if let LogicalPlan::Project(proj) = plan {
            assert!(matches!(&*proj.input, LogicalPlan::Aggregate(_)));
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_bind_group_by() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let plan = binder
            .bind_sql("SELECT o_orderstatus, COUNT(*) FROM orders GROUP BY o_orderstatus")
            .unwrap();

        if let LogicalPlan::Project(proj) = plan {
            if let LogicalPlan::Aggregate(agg) = &*proj.input {
                assert_eq!(agg.group_by.len(), 1);
            } else {
                panic!("Expected Aggregate");
            }
        } else {
            panic!("Expected Project");
        }
    }

    #[test]
    fn test_bind_join() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let plan = binder
            .bind_sql(
                "SELECT o.o_orderkey, l.l_quantity
                 FROM orders o
                 JOIN lineitem l ON o.o_orderkey = l.l_orderkey",
            )
            .unwrap();

        // Find the join in the plan
        fn has_join(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Join(_) => true,
                LogicalPlan::Project(p) => has_join(&p.input),
                LogicalPlan::Filter(f) => has_join(&f.input),
                LogicalPlan::SubqueryAlias(s) => has_join(&s.input),
                _ => false,
            }
        }

        assert!(has_join(&plan));
    }

    #[test]
    fn test_bind_order_by() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let plan = binder
            .bind_sql("SELECT o_orderkey FROM orders ORDER BY o_totalprice DESC")
            .unwrap();

        // Should have sort at top
        fn has_sort(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Sort(_) => true,
                LogicalPlan::Limit(l) => has_sort(&l.input),
                _ => false,
            }
        }

        assert!(has_sort(&plan));
    }

    #[test]
    fn test_bind_limit() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let plan = binder
            .bind_sql("SELECT o_orderkey FROM orders LIMIT 10")
            .unwrap();

        assert!(matches!(plan, LogicalPlan::Limit(_)));
    }

    #[test]
    fn test_table_not_found() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let result = binder.bind_sql("SELECT * FROM nonexistent");
        assert!(result.is_err());
    }
}
