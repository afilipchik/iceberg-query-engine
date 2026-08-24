//! SQL AST to Logical Plan binder

use crate::error::{QueryError, Result};
use crate::parser::{self, ObjectNameExt};
use crate::planner::{
    AggregateFunction, AggregateNode, BinaryOp, Column, DistinctNode, Expr, FilterNode, JoinNode,
    JoinType, LimitNode, LogicalPlan, NullOrdering, PlanSchema, ProjectNode, ScalarFunction,
    ScalarValue, ScanNode, SchemaField, SortDirection, SortExpr, SortNode, SubqueryAliasNode,
    UnaryOp,
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
    #[allow(dead_code)] // Reserved for correlated subquery type checking
    outer_scope: HashMap<String, (ArrowDataType, Option<String>)>,
    /// Named WINDOW clause definitions of the SELECT currently being bound.
    named_windows: HashMap<String, ast::WindowSpec>,
    /// True only while the SELECT list is being bound — the one place window
    /// functions are allowed (v1; the standard also allows ORDER BY).
    allow_window: bool,
}

/// `a IS [NOT] DISTINCT FROM b` as a CASE over IS NULL tests — null-safe
/// equality without new evaluator machinery.
fn is_distinct_expr(a: Expr, b: Expr, negated: bool) -> Expr {
    let is_null = |e: &Expr| Expr::UnaryExpr {
        op: UnaryOp::IsNull,
        expr: Box::new(e.clone()),
    };
    let both_null = Expr::BinaryExpr {
        left: Box::new(is_null(&a)),
        op: BinaryOp::And,
        right: Box::new(is_null(&b)),
    };
    let either_null = Expr::BinaryExpr {
        left: Box::new(is_null(&a)),
        op: BinaryOp::Or,
        right: Box::new(is_null(&b)),
    };
    let (t_both, t_one, cmp_op) = if negated {
        // IS NOT DISTINCT FROM: null-safe equality
        (true, false, BinaryOp::Eq)
    } else {
        (false, true, BinaryOp::NotEq)
    };
    Expr::Case {
        operand: None,
        when_then: vec![
            (both_null, Expr::Literal(ScalarValue::Boolean(t_both))),
            (either_null, Expr::Literal(ScalarValue::Boolean(t_one))),
        ],
        else_expr: Some(Box::new(Expr::BinaryExpr {
            left: Box::new(a),
            op: cmp_op,
            right: Box::new(b),
        })),
    }
}

/// A frame offset must be a non-negative integer literal (the standard allows
/// expressions; v1 does not, and says so).
fn frame_offset(e: &ast::Expr) -> Result<u64> {
    match e {
        ast::Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Number(n, _),
            ..
        }) => n.parse::<u64>().map_err(|_| {
            QueryError::Bind(format!(
                "frame offset must be a non-negative integer, got {n}"
            ))
        }),
        other => Err(QueryError::NotImplemented(format!(
            "non-literal window frame offset ({other})"
        ))),
    }
}

/// If `stmt` is a `CREATE TABLE` statement, its target table name —
/// regardless of whether the statement is otherwise a shape this binder's
/// `bind()` will accept. Deliberately does NOT validate: that happens once,
/// inside `bind()` (`require_supported_create_table_shape`), so there is a
/// single source of truth for "is this CREATE TABLE supported." Callers
/// (e.g. `ExecutionContext`'s CTAS entrypoint) call this first to learn what
/// to register the written table as, then call `bind()` to both validate
/// the statement's shape and bind its inner `SELECT`.
pub fn create_table_target_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::CreateTable(ct) => Some(ct.name.table_name()),
        _ => None,
    }
}

/// If `stmt` is an `INSERT` statement, its target table's name —
/// regardless of whether the statement is otherwise a shape this binder's
/// `bind()` will accept (mirrors [`create_table_target_name`]'s own
/// "extraction is separate from validation" split, so a caller can learn
/// the target before, or independent of, calling `bind()`). `None` for
/// `TableObject::TableFunction` (ClickHouse `INSERT INTO TABLE
/// FUNCTION(...)`), which has no plain table name —
/// `require_supported_insert_shape` (called from `bind()`) refuses that
/// shape by name too, so a caller that checks this first never reaches a
/// validated bind for it.
pub fn insert_target_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Insert(insert) => match &insert.table {
            ast::TableObject::TableName(name) => Some(name.table_name()),
            // `TableFunction` (ClickHouse `INSERT INTO TABLE FUNCTION(...)`)
            // and `TableQuery` (Oracle `INSERT INTO (subquery) VALUES ...`)
            // have no plain table name -- `require_supported_insert_shape`
            // (called from `bind()`) refuses both shapes by name too.
            ast::TableObject::TableFunction(_) | ast::TableObject::TableQuery(_) => None,
        },
        _ => None,
    }
}

/// Refuse, by name, every `INSERT` clause this epic (native-tables-
/// mutation, task 002) does not implement. Mirrors
/// `require_supported_create_table_shape`'s exact discipline, mechanically
/// checked against sqlparser 0.62's real `ast::Insert` struct (~26 fields
/// total; confirmed via `.scratch/mutation_sqlparser_spike/`, see task
/// 001's Outcome, Decision 6). Only `table` (name only) and `source` are
/// consumed (by `Binder::bind()`'s `Statement::Insert` arm); every other
/// field must be at its "not specified" value or this returns a specific
/// `QueryError::NotImplemented` naming the clause — never a silent
/// downgrade to "ignore the clause and proceed." `insert_token`, `into`
/// and `has_table_keyword` are pure syntax/span markers with no semantic
/// effect (`INSERT INTO t ...` vs. dialect-specific `INSERT t ...`/
/// `INSERT TABLE t ...` mean the same thing) and are deliberately not
/// checked, mirroring `require_supported_create_table_shape`'s own
/// treatment of `ct.hive_distribution`'s "NONE" default.
fn require_supported_insert_shape(insert: &ast::Insert) -> Result<()> {
    fn refuse(clause: &str) -> QueryError {
        QueryError::NotImplemented(format!(
            "INSERT: `{clause}` is not supported — this epic supports only \
             `INSERT INTO <table> SELECT ...` / `INSERT INTO <table> VALUES (...)` against an \
             existing native table (no upsert, no partial column lists, no full-table \
             overwrite via INSERT)"
        ))
    }
    if !matches!(insert.table, ast::TableObject::TableName(_)) {
        return Err(refuse(
            "an INSERT target that is not a plain table name (TABLE FUNCTION(...) or a \
             sub-query target)",
        ));
    }
    if !insert.optimizer_hints.is_empty() {
        return Err(refuse("query optimizer hints"));
    }
    if insert.or.is_some() {
        return Err(refuse("SQLite ON CONFLICT (INSERT OR ...)"));
    }
    if insert.ignore {
        return Err(refuse("MySQL INSERT IGNORE"));
    }
    if insert.table_alias.is_some() {
        return Err(refuse("a table alias on the INSERT target"));
    }
    if !insert.columns.is_empty() {
        return Err(refuse(
            "an explicit column list (INSERT INTO t (a, b) SELECT/VALUES ...)",
        ));
    }
    if insert.overwrite {
        return Err(refuse("Hive INSERT OVERWRITE TABLE"));
    }
    if !insert.assignments.is_empty() {
        return Err(refuse("MySQL INSERT ... SET"));
    }
    if insert.partitioned.is_some() {
        return Err(refuse("Hive PARTITION"));
    }
    if !insert.after_columns.is_empty() {
        return Err(refuse("Hive columns defined after PARTITION"));
    }
    if insert.on.is_some() {
        return Err(refuse("ON CONFLICT / ON DUPLICATE KEY UPDATE"));
    }
    if insert.returning.is_some() {
        return Err(refuse("RETURNING"));
    }
    if insert.output.is_some() {
        return Err(refuse("OUTPUT (MSSQL)"));
    }
    if insert.replace_into {
        return Err(refuse("MySQL REPLACE INTO"));
    }
    if insert.priority.is_some() {
        return Err(refuse(
            "MySQL INSERT priority (LOW_PRIORITY/DELAYED/HIGH_PRIORITY)",
        ));
    }
    if insert.insert_alias.is_some() {
        return Err(refuse("MySQL INSERT ... AS alias"));
    }
    if insert.settings.is_some() {
        return Err(refuse("ClickHouse SETTINGS"));
    }
    if insert.format_clause.is_some() {
        return Err(refuse("ClickHouse FORMAT"));
    }
    if insert.multi_table_insert_type.is_some() {
        return Err(refuse("Snowflake multi-table INSERT ALL/FIRST"));
    }
    if !insert.multi_table_into_clauses.is_empty() {
        return Err(refuse(
            "Snowflake multi-table INSERT additional INTO clauses",
        ));
    }
    if !insert.multi_table_when_clauses.is_empty() {
        return Err(refuse("Snowflake multi-table INSERT WHEN clauses"));
    }
    if insert.multi_table_else_clause.is_some() {
        return Err(refuse("Snowflake multi-table INSERT ELSE clause"));
    }
    Ok(())
}

/// The single `TableFactor` a `DELETE`'s `FROM` clause names — the FIRST
/// entry of `delete.from`'s table list, regardless of whether the
/// statement is otherwise a shape this binder accepts (mirrors
/// [`create_table_target_name`]/[`insert_target_name`]'s own "extraction is
/// separate from validation" split: a multi-table or joined DELETE still
/// has an extractable first target, and [`require_supported_delete_shape`]
/// — not this function — is what refuses those shapes with a specific,
/// actionable error). `None` only when there is no FROM table at all to
/// extract (structurally impossible for a real parse, but handled rather
/// than panicking) or its `TableFactor` variant carries no plain name
/// (e.g. a derived/function table factor — also refused by name inside
/// `require_supported_delete_shape`).
fn delete_from_table_factor(delete: &ast::Delete) -> Option<&ast::TableFactor> {
    let tables = match &delete.from {
        ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
    };
    Some(&tables.first()?.relation)
}

/// If `stmt` is a `DELETE` statement, its target table's name — see
/// [`delete_from_table_factor`]'s doc for the exact extraction rule this
/// delegates to. Used by `main.rs`'s REPL routing and
/// `ExecutionContext::delete_from_native_table` (both call this BEFORE any
/// validation, exactly like [`insert_target_name`]).
pub fn delete_target_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Delete(delete) => match delete_from_table_factor(delete)? {
            ast::TableFactor::Table { name, .. } => Some(name.table_name()),
            _ => None,
        },
        _ => None,
    }
}

/// Refuse, by name, every `DELETE` clause this epic does not implement.
/// Mirrors [`require_supported_insert_shape`]'s exact discipline,
/// mechanically checked against sqlparser 0.62's real 10-field
/// `ast::Delete` struct (confirmed via `.scratch/mutation_sqlparser_spike/`,
/// see task 001's Outcome, Decision 6) plus the real `TableFactor::Table`
/// variant's 10 fields. Only `from`'s single target table name/alias and
/// `selection` are consumed (by [`Binder::bind_delete`]); every other
/// field must be at its "not specified" value or this returns a specific
/// `QueryError::NotImplemented` naming the clause.
fn require_supported_delete_shape(delete: &ast::Delete) -> Result<()> {
    fn refuse(clause: &str) -> QueryError {
        QueryError::NotImplemented(format!(
            "DELETE: `{clause}` is not supported — this epic supports only `DELETE FROM \
             <table> [WHERE ...]` against a single existing native table (no multi-table \
             delete, no USING, no ORDER BY/LIMIT, no RETURNING/OUTPUT)"
        ))
    }
    if !delete.optimizer_hints.is_empty() {
        return Err(refuse("query optimizer hints"));
    }
    if !delete.tables.is_empty() {
        return Err(refuse(
            "a MySQL multi-table DELETE (comma-separated target list)",
        ));
    }
    let tables = match &delete.from {
        ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
    };
    if tables.len() != 1 {
        return Err(refuse("a DELETE naming more than one FROM table"));
    }
    if !tables[0].joins.is_empty() {
        return Err(refuse("a JOIN in the DELETE target"));
    }
    match &tables[0].relation {
        ast::TableFactor::Table {
            name: _,
            alias: _,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            if args.is_some() {
                return Err(refuse("a table-valued function as the DELETE target"));
            }
            if !with_hints.is_empty() {
                return Err(refuse("MSSQL table hints (WITH (...))"));
            }
            if version.is_some() {
                return Err(refuse("a table time-travel version qualifier"));
            }
            if *with_ordinality {
                return Err(refuse("WITH ORDINALITY"));
            }
            if !partitions.is_empty() {
                return Err(refuse("MySQL PARTITION selection"));
            }
            if json_path.is_some() {
                return Err(refuse("a PartiQL JSON path"));
            }
            if sample.is_some() {
                return Err(refuse("a TABLESAMPLE clause"));
            }
            if !index_hints.is_empty() {
                return Err(refuse("MySQL index hints"));
            }
        }
        _ => return Err(refuse("a DELETE target that is not a plain table name")),
    }
    if delete.using.is_some() {
        return Err(refuse(
            "USING (Postgres/Snowflake/MySQL join-shaped delete)",
        ));
    }
    if delete.returning.is_some() {
        return Err(refuse("RETURNING"));
    }
    if delete.output.is_some() {
        return Err(refuse("OUTPUT (MSSQL)"));
    }
    if !delete.order_by.is_empty() {
        return Err(refuse("MySQL ORDER BY on DELETE"));
    }
    if delete.limit.is_some() {
        return Err(refuse("MySQL LIMIT on DELETE"));
    }
    Ok(())
}

/// Refuse, by name, every `CREATE TABLE` clause this epic does not
/// implement. Mirrors this binder's existing "match the supported shape,
/// `NotImplemented` the rest" convention (used throughout for window
/// functions, GROUPING SETS, join types, etc.) applied to sqlparser's
/// `CreateTable` struct, which carries ~60 fields covering Postgres, Hive,
/// Snowflake, BigQuery, Redshift and ClickHouse syntax extensions this
/// engine has no semantic equivalent for. Only `name` and `query` are
/// consumed (by `Binder::bind()`'s `Statement::CreateTable` arm); every
/// other field must be at its "not specified" value or this returns a
/// specific `QueryError::NotImplemented` naming the clause — never a
/// silent downgrade to "ignore the clause and proceed."
fn require_supported_create_table_shape(ct: &ast::CreateTable) -> Result<()> {
    fn refuse(clause: &str) -> QueryError {
        QueryError::NotImplemented(format!(
            "CREATE TABLE ... AS SELECT: `{clause}` is not supported — this epic is \
             full-table bulk-load/replace only (no partitioning, no external/temporary \
             tables, no engine-specific storage/policy clauses)"
        ))
    }
    if ct.or_replace {
        return Err(refuse("OR REPLACE"));
    }
    if ct.temporary {
        return Err(refuse("TEMPORARY"));
    }
    if ct.external {
        return Err(refuse("EXTERNAL"));
    }
    if ct.dynamic {
        return Err(refuse("DYNAMIC"));
    }
    if ct.global.is_some() {
        return Err(refuse("GLOBAL/LOCAL"));
    }
    if ct.if_not_exists {
        // Honoring the real semantics (skip silently if the table already
        // exists) would require detecting an existing table and NOT
        // replacing it — this epic's write path always replaces, so
        // accepting this flag and ignoring it would silently change what
        // the statement means rather than doing what it says.
        return Err(refuse("IF NOT EXISTS"));
    }
    if ct.transient {
        return Err(refuse("TRANSIENT"));
    }
    if ct.volatile {
        return Err(refuse("VOLATILE"));
    }
    if ct.iceberg {
        return Err(refuse("ICEBERG"));
    }
    if ct.snapshot {
        return Err(refuse("SNAPSHOT"));
    }
    if !ct.columns.is_empty() {
        return Err(refuse(
            "an explicit column list (CREATE TABLE t (a, b) AS SELECT ...)",
        ));
    }
    if !ct.constraints.is_empty() {
        return Err(refuse("table constraints"));
    }
    if ct.hive_distribution != ast::HiveDistributionStyle::NONE {
        return Err(refuse("Hive distribution clauses"));
    }
    if ct.hive_formats.is_some() {
        return Err(refuse("Hive ROW FORMAT clauses"));
    }
    if ct.table_options != ast::CreateTableOptions::None {
        return Err(refuse("table options (WITH/OPTIONS/TBLPROPERTIES)"));
    }
    if ct.file_format.is_some() {
        return Err(refuse("STORED AS <file format>"));
    }
    if ct.location.is_some() {
        return Err(refuse("LOCATION"));
    }
    if ct.without_rowid {
        return Err(refuse("WITHOUT ROWID"));
    }
    if ct.like.is_some() {
        return Err(refuse("LIKE"));
    }
    if ct.clone.is_some() {
        return Err(refuse("CLONE"));
    }
    if ct.version.is_some() {
        return Err(refuse("a table version (FOR VERSION AS OF)"));
    }
    if ct.comment.is_some() {
        return Err(refuse("COMMENT"));
    }
    if ct.on_commit.is_some() {
        return Err(refuse("ON COMMIT"));
    }
    if ct.on_cluster.is_some() {
        return Err(refuse("ON CLUSTER"));
    }
    if ct.primary_key.is_some() {
        return Err(refuse("PRIMARY KEY"));
    }
    if ct.order_by.is_some() {
        return Err(refuse("table-level ORDER BY"));
    }
    if ct.partition_by.is_some() {
        return Err(refuse("PARTITION BY"));
    }
    if ct.cluster_by.is_some() {
        return Err(refuse("CLUSTER BY"));
    }
    if ct.clustered_by.is_some() {
        return Err(refuse("CLUSTERED BY"));
    }
    if ct.inherits.is_some() {
        return Err(refuse("INHERITS"));
    }
    if ct.partition_of.is_some() {
        return Err(refuse("PARTITION OF"));
    }
    if ct.for_values.is_some() {
        return Err(refuse("FOR VALUES"));
    }
    if ct.strict {
        return Err(refuse("STRICT"));
    }
    if ct.copy_grants {
        return Err(refuse("COPY GRANTS"));
    }
    if ct.enable_schema_evolution.is_some() {
        return Err(refuse("ENABLE_SCHEMA_EVOLUTION"));
    }
    if ct.change_tracking.is_some() {
        return Err(refuse("CHANGE_TRACKING"));
    }
    if ct.data_retention_time_in_days.is_some() {
        return Err(refuse("DATA_RETENTION_TIME_IN_DAYS"));
    }
    if ct.max_data_extension_time_in_days.is_some() {
        return Err(refuse("MAX_DATA_EXTENSION_TIME_IN_DAYS"));
    }
    if ct.default_ddl_collation.is_some() {
        return Err(refuse("DEFAULT_DDL_COLLATION"));
    }
    if ct.with_aggregation_policy.is_some() {
        return Err(refuse("WITH AGGREGATION POLICY"));
    }
    if ct.with_row_access_policy.is_some() {
        return Err(refuse("WITH ROW ACCESS POLICY"));
    }
    if ct.with_storage_lifecycle_policy.is_some() {
        return Err(refuse("WITH STORAGE LIFECYCLE POLICY"));
    }
    if ct.with_tags.is_some() {
        return Err(refuse("WITH TAG"));
    }
    if ct.external_volume.is_some() {
        return Err(refuse("EXTERNAL_VOLUME"));
    }
    if ct.base_location.is_some() {
        return Err(refuse("BASE_LOCATION"));
    }
    if ct.catalog.is_some() {
        return Err(refuse("CATALOG"));
    }
    if ct.catalog_sync.is_some() {
        return Err(refuse("CATALOG_SYNC"));
    }
    if ct.storage_serialization_policy.is_some() {
        return Err(refuse("STORAGE_SERIALIZATION_POLICY"));
    }
    if ct.target_lag.is_some() {
        return Err(refuse("TARGET_LAG"));
    }
    if ct.warehouse.is_some() {
        return Err(refuse("WAREHOUSE"));
    }
    if ct.refresh_mode.is_some() {
        return Err(refuse("REFRESH_MODE"));
    }
    if ct.initialize.is_some() {
        return Err(refuse("INITIALIZE"));
    }
    if ct.require_user {
        return Err(refuse("REQUIRE USER"));
    }
    if ct.diststyle.is_some() {
        return Err(refuse("DISTSTYLE"));
    }
    if ct.distkey.is_some() {
        return Err(refuse("DISTKEY"));
    }
    if ct.sortkey.is_some() {
        return Err(refuse("SORTKEY"));
    }
    if ct.backup.is_some() {
        return Err(refuse("BACKUP"));
    }
    Ok(())
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a dyn Catalog) -> Self {
        Self {
            catalog,
            aliases: HashMap::new(),
            table_aliases: HashMap::new(),
            ctes: HashMap::new(),
            outer_scope: HashMap::new(),
            named_windows: HashMap::new(),
            allow_window: false,
        }
    }

    /// Create a binder with outer scope for correlated subqueries
    #[allow(dead_code)] // Reserved for correlated subquery type checking
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
            named_windows: HashMap::new(),
            allow_window: false,
        }
    }

    /// Collect outer scope columns from a schema
    #[allow(dead_code)] // Reserved for correlated subquery type checking
    fn collect_outer_scope(
        schema: &PlanSchema,
    ) -> HashMap<String, (ArrowDataType, Option<String>)> {
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
            // `CREATE TABLE <name> AS SELECT ...` (native-tables-foundation
            // epic, task 004). This binds and returns ONLY the inner
            // SELECT's LogicalPlan — the same shape `Statement::Query`
            // produces — because `LogicalPlan` has no DDL node and none is
            // needed: the target table name is recovered separately (see
            // `create_table_target_name`) by whichever `ExecutionContext`
            // entrypoint drives the CTAS write, since binding a query and
            // deciding what to do with its result are different jobs. Every
            // `CreateTable` struct field this epic does not support is
            // refused BY NAME (`require_supported_create_table_shape`),
            // never silently ignored — see that function for the full list
            // (OR REPLACE, TEMPORARY, PARTITION BY, LIKE, CLONE, and ~50
            // more Hive/Snowflake/BigQuery/Redshift/ClickHouse-specific
            // clauses this engine has no equivalent for).
            Statement::CreateTable(ct) => {
                require_supported_create_table_shape(ct)?;
                let query = ct.query.as_deref().ok_or_else(|| {
                    QueryError::NotImplemented(
                        "CREATE TABLE without AS SELECT (a columns-only definition, to be \
                         populated later via INSERT) is not supported — this epic is \
                         full-table bulk-load/replace only. Use \
                         CREATE TABLE <name> AS SELECT ..."
                            .to_string(),
                    )
                })?;
                self.bind_query(query)
            }
            // `INSERT INTO <table> SELECT/VALUES ...` (native-tables-
            // mutation epic, task 002). Mirrors the `CreateTable` arm's
            // shape exactly: binds and returns ONLY the source query's
            // LogicalPlan (no DML node — `LogicalPlan` gains none for the
            // same reason `CreateTable` needed none), with the target
            // table name recovered separately (`insert_target_name`) by
            // whichever `ExecutionContext` entrypoint drives the write,
            // since binding a query and deciding what to do with its
            // result are different jobs. Every `Insert` struct field this
            // epic does not support is refused BY NAME
            // (`require_supported_insert_shape`), never silently ignored —
            // see that function for the full list (explicit column lists,
            // Hive INSERT OVERWRITE, ON CONFLICT/ON DUPLICATE KEY UPDATE,
            // MySQL INSERT...SET, RETURNING/OUTPUT, multi-table INSERT
            // ALL/FIRST, and more). `source` binds via the SAME
            // `bind_query()` CreateTable already uses — covers both
            // `INSERT ... SELECT` and `INSERT ... VALUES` (`SetExpr::
            // Values` already has its own arm in `bind_set_expr`), so no
            // extra binder work is needed for either shape.
            Statement::Insert(insert) => {
                require_supported_insert_shape(insert)?;
                let query = insert.source.as_deref().ok_or_else(|| {
                    QueryError::NotImplemented(
                        "INSERT without a SELECT/VALUES source (e.g. DEFAULT VALUES) is not \
                         supported — use INSERT INTO <table> SELECT ... or INSERT INTO \
                         <table> VALUES (...)"
                            .to_string(),
                    )
                })?;
                self.bind_query(query)
            }
            // `DELETE FROM <table> [WHERE ...]` (native-tables-mutation
            // epic, task 003). Unlike `CreateTable`/`Insert` above, this
            // arm is NOT a small wrapper around `bind_query` — DELETE has
            // no source query to bind, and `LogicalPlan`'s
            // scan/filter/project pipeline has no way to carry a matched
            // row's (segment, local position) back out (see
            // `native_delete.rs`'s module doc for exactly why). This arm
            // therefore only VALIDATES the statement's shape
            // (`require_supported_delete_shape` — every clause this epic
            // does not support refused by name, mirroring the Insert/
            // CreateTable arms above) and then points at the real
            // entrypoint: `Binder::bind_delete` (which returns the target
            // table name + bound predicate, not a `LogicalPlan`) is what
            // `ExecutionContext::delete_from_native_table` actually calls
            // to drive the bespoke row-identification loop. Reachable
            // here only via `logical_plan()`/other direct `bind()` callers
            // — `ExecutionContext::sql()` refuses a DELETE by name before
            // ever calling `bind()`, exactly like its CreateTable/Insert
            // guards.
            Statement::Delete(delete) => {
                require_supported_delete_shape(delete)?;
                Err(QueryError::NotImplemented(
                    "DELETE FROM ... must run through \
                     ExecutionContext::delete_from_native_table (the REPL calls this \
                     automatically) — Binder::bind() only validates a DELETE statement's \
                     shape; identifying and removing matched rows needs a bespoke \
                     row-identification loop over the target table's actual current data, not \
                     a LogicalPlan (see Binder::bind_delete)."
                        .to_string(),
                ))
            }
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

    /// Bind a `DELETE FROM <table> WHERE <predicate>` statement
    /// (native-tables-mutation epic, task 003): validates the statement's
    /// shape (`require_supported_delete_shape`), then binds the WHERE
    /// predicate (if any) via the SAME `bind_expr` method `bind_select`'s
    /// own WHERE-clause binding uses (`selection: None` binds to `None` —
    /// "match every row," a real supported case, not an error), against
    /// the target table's OWN schema (no join, no other table in scope;
    /// aliased via the DELETE's own alias if present, mirroring
    /// `bind_table_factor`'s convention, so `WHERE alias.col = ...` binds
    /// exactly like `WHERE col = ...` does). Returns the target table name
    /// and the bound predicate — the caller
    /// (`ExecutionContext::delete_from_native_table`) evaluates this
    /// predicate directly via `physical::operators::evaluate_expr` inside
    /// `native_delete::identify_matching_rows`, NOT through the generic
    /// `LogicalPlan`/`PhysicalOperator` pipeline.
    pub fn bind_delete(&mut self, stmt: &ast::Delete) -> Result<(String, Option<Expr>)> {
        require_supported_delete_shape(stmt)?;
        let tables = match &stmt.from {
            ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
        };
        // `require_supported_delete_shape` already guarantees exactly one
        // entry whose relation is `TableFactor::Table`.
        let (table_name, alias) = match &tables[0].relation {
            TableFactor::Table { name, alias, .. } => (
                name.table_name(),
                alias.as_ref().map(|a| a.name.value.clone()),
            ),
            other => {
                return Err(QueryError::Bind(format!(
                    "DELETE: expected a plain table name, got {other:?}"
                )))
            }
        };

        let schema = self
            .catalog
            .get_table_schema(&table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.clone()))?;
        let alias_name = alias.unwrap_or_else(|| table_name.clone());
        let aliased_schema = PlanSchema::new(
            schema
                .fields()
                .iter()
                .map(|f| f.clone().with_relation(alias_name.clone()))
                .collect(),
        );

        let predicate = match &stmt.selection {
            Some(expr) => Some(self.bind_expr(expr, &aliased_schema)?),
            None => None,
        };

        // A subquery in the WHERE clause (`DELETE FROM t WHERE id IN
        // (SELECT ...)`) BINDS successfully -- `bind_expr` is the same
        // method `bind_select`'s own WHERE binding uses, and task 001's
        // spike confirmed sqlparser parses this shape cleanly -- but
        // EVALUATING it needs a `SubqueryExecutor`, which this epic's
        // bespoke row-identification loop (`native_delete::
        // identify_matching_rows`, deliberately NOT the generic
        // LogicalPlan/PhysicalOperator pipeline `SubqueryExecutor` is
        // normally wired through) does not have and is out of this
        // task's scope to add. Refuse HERE, by name, at bind time --
        // matching this codebase's own "refuse cleanly and early" ethos
        // (mirrors `require_supported_delete_shape`'s discipline) --
        // rather than let it reach `physical::operators::evaluate_expr`
        // deep inside the identification loop and fail with a less
        // specific "no executor available" error.
        if predicate.as_ref().is_some_and(|p| p.contains_subquery()) {
            return Err(QueryError::NotImplemented(
                "DELETE: a subquery in the WHERE clause is not supported -- this epic's \
                 row-identification loop evaluates the predicate directly (via \
                 physical::operators::evaluate_expr), which has no subquery executor; \
                 rewrite the predicate without a subquery (e.g. a plain column comparison)"
                    .to_string(),
            ));
        }

        Ok((table_name, predicate))
    }

    fn bind_query(&mut self, query: &ast::Query) -> Result<LogicalPlan> {
        // Process CTEs (WITH clause) first
        if let Some(ref with_clause) = query.with {
            self.bind_ctes(with_clause)?;
        }

        // Start with the body (SELECT, UNION, etc.)
        let mut plan = self.bind_set_expr(&query.body)?;

        // Apply ORDER BY.
        //
        // `ORDER BY` may reference columns the SELECT list does not output —
        // `SELECT id FROM t ORDER BY price` is legal SQL, and it is exactly the
        // shape of a vector search (`SELECT id, text ... ORDER BY
        // cosine_distance(embedding, [...])`, where the 384-float embedding is
        // emphatically not something you want in the result set). Those extra
        // columns are added to the projection, sorted on, then trimmed back off
        // above the LIMIT. See `extend_projection_for_sort`.
        let mut trim_to: Option<PlanSchema> = None;
        if let Some(ref order_by_clause) = query.order_by {
            let order_exprs: &[ast::OrderByExpr] = match &order_by_clause.kind {
                ast::OrderByKind::Expressions(exprs) => exprs,
                ast::OrderByKind::All(_) => {
                    return Err(QueryError::NotImplemented("ORDER BY ALL".into()))
                }
            };
            if !order_exprs.is_empty() {
                let order_by = self.bind_order_by(order_exprs, &plan.schema())?;
                let (extended, trim) = extend_projection_for_sort(plan, &order_by)?;
                trim_to = trim;
                plan = LogicalPlan::Sort(SortNode {
                    input: Arc::new(extended),
                    order_by,
                });
            }
        }

        // Apply LIMIT/OFFSET.
        //
        // The trimming projection is deliberately applied *after* LIMIT: a
        // projection between Sort and Limit would break the physical planner's
        // Sort+Limit fusion and turn a top-k into a full sort.
        let (q_limit, q_offset) = match &query.limit_clause {
            None => (None, None),
            Some(ast::LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return Err(QueryError::NotImplemented("LIMIT ... BY".into()));
                }
                (limit.as_ref(), offset.as_ref())
            }
            Some(other) => {
                return Err(QueryError::NotImplemented(format!(
                    "this LIMIT form ({other:?})"
                )))
            }
        };
        if q_limit.is_some() || q_offset.is_some() {
            let skip = q_offset
                .and_then(|o| self.expr_to_usize(&o.value).ok())
                .unwrap_or(0);
            let fetch = q_limit.and_then(|l| self.expr_to_usize(l).ok());

            plan = LogicalPlan::Limit(LimitNode {
                input: Arc::new(plan),
                skip,
                fetch,
            });
        }

        if let Some(schema) = trim_to {
            let exprs: Vec<Expr> = schema
                .fields()
                .iter()
                .map(|f| {
                    Expr::Column(Column {
                        relation: f.relation.clone(),
                        name: f.name.clone(),
                    })
                })
                .collect();
            plan = LogicalPlan::Project(ProjectNode {
                input: Arc::new(plan),
                exprs,
                schema,
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
            SetExpr::SetOperation {
                op,
                left,
                right,
                set_quantifier,
                ..
            } => {
                let left_plan = self.bind_set_expr(left)?;
                let right_plan = self.bind_set_expr(right)?;

                match op {
                    ast::SetOperator::Minus => {
                        return Err(QueryError::NotImplemented("MINUS (use EXCEPT)".to_string()))
                    }
                    ast::SetOperator::Union => {
                        let schema = left_plan.schema();
                        // UNION ALL keeps duplicates, plain UNION removes them
                        let all = matches!(set_quantifier, ast::SetQuantifier::All);
                        // Plain UNION de-duplicates on every column, which a
                        // nested column cannot do; UNION ALL just concatenates
                        // and is fine.
                        if !all {
                            crate::planner::vector_types::require_scalar_row(&schema, "UNION")?;
                        }
                        Ok(LogicalPlan::Union(crate::planner::UnionNode {
                            inputs: vec![Arc::new(left_plan), Arc::new(right_plan)],
                            schema,
                            all,
                        }))
                    }
                    ast::SetOperator::Intersect => {
                        // INTERSECT is implemented as a semi-join on all columns
                        let left_schema = left_plan.schema();
                        let right_schema = right_plan.schema();
                        // Every column becomes a join key, and a nested value
                        // has no hashable equality.
                        crate::planner::vector_types::require_scalar_row(
                            &left_schema,
                            "INTERSECT",
                        )?;

                        // Create join conditions on all columns
                        let on: Vec<(Expr, Expr)> = left_schema
                            .fields()
                            .iter()
                            .zip(right_schema.fields().iter())
                            .map(|(l, r)| {
                                (
                                    Expr::Column(Column::new(l.name.clone())),
                                    Expr::Column(Column::new(r.name.clone())),
                                )
                            })
                            .collect();

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left_plan),
                            right: Arc::new(right_plan),
                            join_type: crate::planner::JoinType::Semi,
                            on,
                            filter: None,
                            schema: left_schema.clone(),
                        });

                        // INTERSECT removes duplicates by default (unless INTERSECT ALL)
                        let all = matches!(set_quantifier, ast::SetQuantifier::All);
                        if !all {
                            Ok(LogicalPlan::Distinct(crate::planner::DistinctNode {
                                input: Arc::new(join),
                            }))
                        } else {
                            Ok(join)
                        }
                    }
                    ast::SetOperator::Except => {
                        // EXCEPT is implemented as an anti-join on all columns
                        let left_schema = left_plan.schema();
                        let right_schema = right_plan.schema();
                        crate::planner::vector_types::require_scalar_row(&left_schema, "EXCEPT")?;

                        // Create join conditions on all columns
                        let on: Vec<(Expr, Expr)> = left_schema
                            .fields()
                            .iter()
                            .zip(right_schema.fields().iter())
                            .map(|(l, r)| {
                                (
                                    Expr::Column(Column::new(l.name.clone())),
                                    Expr::Column(Column::new(r.name.clone())),
                                )
                            })
                            .collect();

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left_plan),
                            right: Arc::new(right_plan),
                            join_type: crate::planner::JoinType::Anti,
                            on,
                            filter: None,
                            schema: left_schema.clone(),
                        });

                        // EXCEPT removes duplicates by default (unless EXCEPT ALL)
                        let all = matches!(set_quantifier, ast::SetQuantifier::All);
                        if !all {
                            Ok(LogicalPlan::Distinct(crate::planner::DistinctNode {
                                input: Arc::new(join),
                            }))
                        } else {
                            Ok(join)
                        }
                    }
                }
            }
            SetExpr::Values(values) => {
                // VALUES clause
                let mut rows = Vec::new();
                for row in &values.rows {
                    let exprs: Result<Vec<Expr>> = row
                        .iter()
                        .map(|e| self.bind_expr(e, &PlanSchema::empty()))
                        .collect();
                    rows.push(exprs?);
                }

                // Infer schema from first row
                let schema = if let Some(first_row) = rows.first() {
                    let fields: Vec<SchemaField> = first_row
                        .iter()
                        .enumerate()
                        .map(|(i, e)| {
                            let dt = e
                                .data_type(&PlanSchema::empty())
                                .unwrap_or(ArrowDataType::Utf8);
                            SchemaField::new(format!("column{}", i), dt)
                        })
                        .collect();
                    PlanSchema::new(fields)
                } else {
                    PlanSchema::empty()
                };

                Ok(LogicalPlan::Values(crate::planner::ValuesNode {
                    values: rows,
                    schema,
                }))
            }
            _ => Err(QueryError::NotImplemented(format!(
                "Set expression not supported: {:?}",
                set_expr
            ))),
        }
    }

    fn bind_select(&mut self, select: &ast::Select) -> Result<LogicalPlan> {
        // Named WINDOW definitions for this SELECT; windows are allowed only
        // while the SELECT list binds (set below), never in FROM/WHERE.
        self.allow_window = false;
        let saved_windows = std::mem::take(&mut self.named_windows);
        for def in &select.named_window {
            let key = def.0.value.to_lowercase();
            match &def.1 {
                ast::NamedWindowExpr::WindowSpec(spec) => {
                    self.named_windows.insert(key, spec.clone());
                }
                ast::NamedWindowExpr::NamedWindow(base) => {
                    let base_key = base.value.to_lowercase();
                    let resolved = self.named_windows.get(&base_key).cloned().ok_or_else(|| {
                        QueryError::Bind(format!(
                            "window \"{}\" references undefined window \"{}\"",
                            def.0.value, base.value
                        ))
                    })?;
                    self.named_windows.insert(key, resolved);
                }
            }
        }

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

        // 3a. GROUPING SETS / ROLLUP / CUBE desugar to a UNION ALL of plain
        // aggregates (one branch per set) — no new physical operator.
        if let ast::GroupByExpr::Expressions(gexprs, _) = &select.group_by {
            let has_sets = gexprs.iter().any(|e| {
                matches!(
                    e,
                    SqlExpr::GroupingSets(_) | SqlExpr::Rollup(_) | SqlExpr::Cube(_)
                )
            });
            if has_sets {
                self.allow_window = false;
                self.named_windows = saved_windows;
                return self.bind_grouping_sets(select, plan, gexprs);
            }
        }

        // 3. GROUP BY and aggregates. Window functions become legal to bind
        // from here on (they live in the SELECT list; extract_aggregates
        // pre-binds it).
        self.allow_window = true;
        let input_schema = plan.schema();
        let (group_by, mut aggregates, aggregate_aliases, mut has_aggregates) =
            self.extract_aggregates(&select.projection, &select.group_by, &input_schema)?;

        // Also extract aggregates from HAVING clause (e.g., HAVING SUM(x) > 300)
        if let Some(having) = &select.having {
            let having_expr = self.bind_expr(having, &input_schema)?;
            if having_expr.contains_aggregate() {
                self.collect_aggregates(&having_expr, &mut aggregates);
                has_aggregates = true;
            }
        }

        if has_aggregates || !group_by.is_empty() {
            let mut agg_fields = Vec::new();
            for expr in &group_by {
                // Hashing a 384-float embedding as a group key is never what
                // the user meant; reject it with the column name.
                crate::planner::vector_types::require_scalar(expr, &input_schema, "GROUP BY")?;
                agg_fields.push(expr.to_field(&input_schema)?);
            }
            for (i, expr) in aggregates.iter().enumerate() {
                if let Expr::Aggregate { func, args, .. } = expr {
                    for arg in args {
                        crate::planner::vector_types::require_scalar(
                            arg,
                            &input_schema,
                            &func.to_string(),
                        )?;
                    }
                }
                // Use the alias if available, otherwise use the expression's output name
                let field_name = aggregate_aliases
                    .get(i)
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
            // Bind against original input schema to get the full expression
            let predicate = self.bind_expr(having, &input_schema)?;

            // If we have aggregates, convert the HAVING expression to reference aggregate outputs
            let predicate = if has_aggregates || !group_by.is_empty() {
                self.convert_expr_with_aggregates(
                    &predicate,
                    &plan.schema(),
                    &group_by,
                    &aggregates,
                    &input_schema,
                )?
            } else {
                predicate
            };

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

        self.allow_window = false;
        self.named_windows = saved_windows;

        // 5b. Window extraction: pull every window expression out of the
        // projection into a Window node below it; the projection keeps a
        // column reference. See WindowNode.
        let mut window_exprs: Vec<(String, crate::planner::logical_expr::WindowExpr)> = Vec::new();
        let proj_exprs: Vec<Expr> = proj_exprs
            .into_iter()
            .map(|e| Self::extract_windows(e, &mut window_exprs))
            .collect();
        if !window_exprs.is_empty() {
            let win_input_schema = plan.schema();
            let mut fields = win_input_schema.fields().to_vec();
            for (name, w) in &window_exprs {
                let dt = Expr::WindowFunction(Box::new(w.clone())).data_type(&win_input_schema)?;
                fields.push(SchemaField::new(name.clone(), dt));
            }
            plan = LogicalPlan::Window(crate::planner::WindowNode {
                input: Arc::new(plan),
                window_exprs,
                schema: PlanSchema::new(fields),
            });
        }

        plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(plan),
            exprs: proj_exprs,
            schema: PlanSchema::new(proj_fields),
        });

        // 6. DISTINCT
        if let Some(distinct) = &select.distinct {
            match distinct {
                ast::Distinct::Distinct => {
                    // DISTINCT is planned as "group by every output column", so
                    // the columns are implicit and never reach the per-expression
                    // guards. `SELECT DISTINCT *` over a table with a struct
                    // column would otherwise hash a group key the aggregate does
                    // not understand.
                    crate::planner::vector_types::require_scalar_row(&plan.schema(), "DISTINCT")?;
                    plan = LogicalPlan::Distinct(DistinctNode {
                        input: Arc::new(plan),
                    });
                }
                ast::Distinct::On(_) => {
                    return Err(QueryError::NotImplemented(
                        "DISTINCT ON not supported".to_string(),
                    ));
                }
                ast::Distinct::All => {
                    return Err(QueryError::NotImplemented(
                        "DISTINCT ALL not supported".to_string(),
                    ));
                }
            }
        }

        Ok(plan)
    }

    /// `expr ± INTERVAL 'n' unit` as a DATE_ADD call.
    fn bind_interval_add(
        &mut self,
        base: Expr,
        iv: &ast::Interval,
        negate: bool,
        schema: &PlanSchema,
    ) -> Result<Expr> {
        let value = self.bind_expr(&iv.value, schema)?;
        // INTERVAL '3' DAY -> value literal "3"; INTERVAL '3 days' -> "3 days".
        let (mut n, mut unit) = match &value {
            Expr::Literal(ScalarValue::Utf8(s)) => {
                let parts: Vec<&str> = s.split_whitespace().collect();
                let n: i64 = parts
                    .first()
                    .and_then(|p| p.parse().ok())
                    .ok_or_else(|| QueryError::Bind(format!("malformed interval {s:?}")))?;
                (
                    n,
                    parts.get(1).map(|u| u.trim_end_matches('s').to_lowercase()),
                )
            }
            Expr::Literal(ScalarValue::Int64(n)) => (*n, None),
            other => {
                return Err(QueryError::NotImplemented(format!(
                    "non-literal INTERVAL value ({other})"
                )))
            }
        };
        if unit.is_none() {
            unit = iv.leading_field.as_ref().map(|f| match f {
                ast::DateTimeField::Year => "year".to_string(),
                ast::DateTimeField::Month => "month".to_string(),
                ast::DateTimeField::Week(_) => "week".to_string(),
                ast::DateTimeField::Day => "day".to_string(),
                ast::DateTimeField::Hour => "hour".to_string(),
                ast::DateTimeField::Minute => "minute".to_string(),
                ast::DateTimeField::Second => "second".to_string(),
                other => format!("{other:?}").to_lowercase(),
            });
        }
        let unit = unit.unwrap_or_else(|| "day".to_string());
        if negate {
            n = -n;
        }
        Ok(Expr::ScalarFunc {
            func: ScalarFunction::DateAdd,
            args: vec![
                Expr::Literal(ScalarValue::Utf8(unit)),
                Expr::Literal(ScalarValue::Int64(n)),
                base,
            ],
        })
    }

    /// `x op ANY/SOME/ALL (subquery)` desugar. `= ANY` and `<> ALL` are
    /// exactly IN / NOT IN; ordering comparisons reduce to MIN/MAX of the
    /// subquery with an emptiness guard (ANY over nothing = FALSE, ALL over
    /// nothing = TRUE). NULL elements in the subquery follow MIN/MAX
    /// semantics (ignored), which deviates from the standard's three-valued
    /// answer only in NULL-element corner cases.
    fn bind_quantified(
        &mut self,
        left: &SqlExpr,
        op: &ast::BinaryOperator,
        right: &SqlExpr,
        any: bool,
        schema: &PlanSchema,
    ) -> Result<Expr> {
        let SqlExpr::Subquery(query) = right else {
            return Err(QueryError::NotImplemented(
                "ANY/ALL over a non-subquery expression".into(),
            ));
        };
        let lhs = self.bind_expr(left, schema)?;
        let subplan = Arc::new(self.bind_query(query)?);
        use ast::BinaryOperator as B;
        match (op, any) {
            (B::Eq, true) => Ok(Expr::InSubquery {
                expr: Box::new(lhs),
                subquery: subplan,
                negated: false,
            }),
            (B::NotEq, false) => Ok(Expr::InSubquery {
                expr: Box::new(lhs),
                subquery: subplan,
                negated: true,
            }),
            (B::Gt | B::GtEq | B::Lt | B::LtEq, _) => {
                // ANY: compare against the easiest element (MIN for >/>=,
                // MAX for </<=); ALL: against the hardest.
                let want_min = match (op, any) {
                    (B::Gt | B::GtEq, true) => true,
                    (B::Lt | B::LtEq, true) => false,
                    (B::Gt | B::GtEq, false) => false,
                    (B::Lt | B::LtEq, false) => true,
                    _ => unreachable!(),
                };
                let sub_schema = subplan.schema();
                if sub_schema.fields().len() != 1 {
                    return Err(QueryError::Bind(
                        "ANY/ALL subquery must return exactly one column".into(),
                    ));
                }
                let col = Expr::Column(Column::new(sub_schema.fields()[0].name.clone()));
                let agg = Expr::Aggregate {
                    func: if want_min {
                        AggregateFunction::Min
                    } else {
                        AggregateFunction::Max
                    },
                    args: vec![col.clone()],
                    distinct: false,
                };
                let cnt = Expr::Aggregate {
                    func: AggregateFunction::Count,
                    args: vec![col.clone()],
                    distinct: false,
                };
                let sub_arrow = subplan.schema();
                let agg_field = SchemaField::new(agg.output_name(), agg.data_type(&sub_arrow)?);
                let extreme_plan = Arc::new(LogicalPlan::Aggregate(AggregateNode {
                    input: subplan.clone(),
                    group_by: vec![],
                    aggregates: vec![agg],
                    schema: PlanSchema::new(vec![agg_field]),
                }));
                let cnt_field = SchemaField::new(cnt.output_name(), ArrowDataType::Int64);
                let count_plan = Arc::new(LogicalPlan::Aggregate(AggregateNode {
                    input: subplan.clone(),
                    group_by: vec![],
                    aggregates: vec![cnt],
                    schema: PlanSchema::new(vec![cnt_field]),
                }));
                let bop = self.convert_binary_op(op)?;
                let cmp = Expr::BinaryExpr {
                    left: Box::new(lhs),
                    op: bop,
                    right: Box::new(Expr::ScalarSubquery(extreme_plan)),
                };
                let empty = Expr::BinaryExpr {
                    left: Box::new(Expr::ScalarSubquery(count_plan)),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
                };
                // ANY over empty = FALSE; ALL over empty = TRUE.
                Ok(Expr::Case {
                    operand: None,
                    when_then: vec![(empty, Expr::Literal(ScalarValue::Boolean(!any)))],
                    else_expr: Some(Box::new(cmp)),
                })
            }
            other => Err(QueryError::NotImplemented(format!(
                "quantified comparison {other:?}"
            ))),
        }
    }

    /// Desugar `GROUP BY GROUPING SETS / ROLLUP / CUBE` into
    /// `UNION ALL` of ordinary aggregates. Each branch pads the group columns
    /// absent from its set with typed NULLs; `GROUPING(...)` calls in the
    /// SELECT list become per-branch constants.
    fn bind_grouping_sets(
        &mut self,
        select: &ast::Select,
        input: LogicalPlan,
        gexprs: &[SqlExpr],
    ) -> Result<LogicalPlan> {
        if gexprs.len() != 1 {
            return Err(QueryError::NotImplemented(
                "GROUPING SETS/ROLLUP/CUBE combined with other GROUP BY items".into(),
            ));
        }
        if select.having.is_some() {
            return Err(QueryError::NotImplemented(
                "HAVING with GROUPING SETS/ROLLUP/CUBE".into(),
            ));
        }
        let input_schema = input.schema();
        let input = Arc::new(input);

        // Expand to the list of grouping sets (each: the AST exprs grouped).
        let sets: Vec<Vec<&SqlExpr>> = match &gexprs[0] {
            SqlExpr::GroupingSets(groups) => groups.iter().map(|g| g.iter().collect()).collect(),
            SqlExpr::Rollup(groups) => {
                // ROLLUP(a, b) -> [a,b], [a], []
                let mut out: Vec<Vec<&SqlExpr>> = Vec::new();
                for k in (0..=groups.len()).rev() {
                    out.push(groups[..k].iter().flatten().collect());
                }
                out
            }
            SqlExpr::Cube(groups) => {
                // CUBE(a, b) -> every subset, standard order.
                let m = groups.len();
                let mut out: Vec<Vec<&SqlExpr>> = Vec::new();
                for mask in (0..(1usize << m)).rev() {
                    let mut set = Vec::new();
                    for (bit, g) in groups.iter().enumerate() {
                        if mask & (1 << (m - 1 - bit)) != 0 {
                            set.extend(g.iter());
                        }
                    }
                    out.push(set);
                }
                out
            }
            _ => unreachable!("caller checked"),
        };

        // The ordered union of all group expressions across the sets.
        let mut union_exprs: Vec<Expr> = Vec::new();
        let mut union_names: Vec<String> = Vec::new();
        let mut set_membership: Vec<Vec<bool>> = Vec::new();
        let mut bound_sets: Vec<Vec<Expr>> = Vec::new();
        for set in &sets {
            let mut bound = Vec::new();
            for e in set {
                let b = self.bind_expr(e, &input_schema)?;
                if !union_exprs.contains(&b) {
                    union_names.push(b.output_name());
                    union_exprs.push(b.clone());
                }
                bound.push(b);
            }
            bound_sets.push(bound);
        }
        for bound in &bound_sets {
            set_membership.push(union_exprs.iter().map(|u| bound.contains(u)).collect());
        }

        // Aggregates and GROUPING(...) calls from the SELECT list.
        let mut aggregates: Vec<Expr> = Vec::new();
        let mut grouping_calls: Vec<Vec<Expr>> = Vec::new(); // arg lists
        let mut out_items: Vec<(Expr, String)> = Vec::new(); // shape of final projection
        enum Item {
            Group(usize),
            Agg(usize),
            Grouping(usize),
        }
        let mut item_plan: Vec<(Item, String)> = Vec::new();
        for item in &select.projection {
            let (raw, alias) = match item {
                SelectItem::UnnamedExpr(e) => (e, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => {
                    return Err(QueryError::NotImplemented(
                        "wildcard projection with GROUPING SETS".into(),
                    ))
                }
            };
            // GROUPING(...) — constant per branch.
            if let SqlExpr::Function(f) = raw {
                if f.name.to_string().to_uppercase() == "GROUPING" {
                    let args: Vec<Expr> = match &f.args {
                        ast::FunctionArguments::List(l) => l
                            .args
                            .iter()
                            .map(|a| match a {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                    self.bind_expr(e, &input_schema)
                                }
                                other => {
                                    Err(QueryError::Bind(format!("GROUPING argument {other:?}")))
                                }
                            })
                            .collect::<Result<_>>()?,
                        _ => vec![],
                    };
                    for a in &args {
                        if !union_exprs.contains(a) {
                            return Err(QueryError::Bind(format!(
                                "GROUPING argument {a} is not a grouping column"
                            )));
                        }
                    }
                    let name = alias.unwrap_or_else(|| "grouping".to_string());
                    item_plan.push((Item::Grouping(grouping_calls.len()), name));
                    grouping_calls.push(args);
                    continue;
                }
            }
            let bound = self.bind_expr(raw, &input_schema)?;
            let name = alias.unwrap_or_else(|| bound.output_name());
            if let Some(gi) = union_exprs.iter().position(|u| *u == bound) {
                item_plan.push((Item::Group(gi), name));
            } else if bound.contains_aggregate() {
                if !matches!(bound, Expr::Aggregate { .. }) {
                    return Err(QueryError::NotImplemented(
                        "expressions over aggregates with GROUPING SETS".into(),
                    ));
                }
                let ai = aggregates
                    .iter()
                    .position(|a| *a == bound)
                    .unwrap_or_else(|| {
                        aggregates.push(bound.clone());
                        aggregates.len() - 1
                    });
                item_plan.push((Item::Agg(ai), name));
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "SELECT item {bound} with GROUPING SETS must be a grouping column, \
                     an aggregate, or GROUPING(...)"
                )));
            }
        }
        drop(out_items);

        // One aggregate + projection branch per set, then UNION ALL.
        let mut branches: Vec<Arc<LogicalPlan>> = Vec::new();
        let mut union_schema_fields: Option<Vec<SchemaField>> = None;
        for (si, bound) in bound_sets.iter().enumerate() {
            let mut agg_fields = Vec::new();
            for e in bound {
                agg_fields.push(e.to_field(&input_schema)?);
            }
            for a in &aggregates {
                agg_fields.push(SchemaField::new(
                    a.output_name(),
                    a.data_type(&input_schema)?,
                ));
            }
            let agg_plan = LogicalPlan::Aggregate(AggregateNode {
                input: input.clone(),
                group_by: bound.clone(),
                aggregates: aggregates.clone(),
                schema: PlanSchema::new(agg_fields),
            });
            let agg_schema = agg_plan.schema();

            // Projection: the final SELECT shape, NULL-padding absent groups.
            let mut proj_exprs = Vec::new();
            let mut proj_fields = Vec::new();
            for (item, name) in &item_plan {
                let e = match item {
                    Item::Group(gi) => {
                        if set_membership[si][*gi] {
                            Expr::Column(Column::new(union_names[*gi].clone()))
                        } else {
                            Expr::Cast {
                                expr: Box::new(Expr::Literal(ScalarValue::Null)),
                                data_type: union_exprs[*gi].data_type(&input_schema)?,
                            }
                        }
                    }
                    Item::Agg(ai) => Expr::Column(Column::new(aggregates[*ai].output_name())),
                    Item::Grouping(ci) => {
                        let mut v: i64 = 0;
                        for a in &grouping_calls[*ci] {
                            let gi = union_exprs.iter().position(|u| u == a).expect("checked");
                            v = (v << 1) | (!set_membership[si][gi]) as i64;
                        }
                        Expr::Literal(ScalarValue::Int64(v))
                    }
                };
                let dt = match item {
                    Item::Group(gi) => union_exprs[*gi].data_type(&input_schema)?,
                    Item::Agg(_) => e.data_type(&agg_schema)?,
                    Item::Grouping(_) => ArrowDataType::Int64,
                };
                proj_fields.push(SchemaField::new(name.clone(), dt));
                proj_exprs.push(Expr::Alias {
                    expr: Box::new(e),
                    name: name.clone(),
                });
            }
            if union_schema_fields.is_none() {
                union_schema_fields = Some(proj_fields.clone());
            }
            branches.push(Arc::new(LogicalPlan::Project(ProjectNode {
                input: Arc::new(agg_plan),
                exprs: proj_exprs,
                schema: PlanSchema::new(proj_fields),
            })));
        }

        Ok(LogicalPlan::Union(crate::planner::UnionNode {
            inputs: branches,
            schema: PlanSchema::new(union_schema_fields.expect("at least one set")),
            all: true,
        }))
    }

    fn bind_from(&mut self, from: &[ast::TableWithJoins]) -> Result<LogicalPlan> {
        if from.is_empty() {
            // No FROM clause - return empty relation that produces one row
            return Ok(LogicalPlan::EmptyRelation(
                crate::planner::EmptyRelationNode {
                    produce_one_row: true,
                    schema: PlanSchema::empty(),
                },
            ));
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
                        alias: alias_name.clone(),
                        schema: aliased_schema,
                        cte_name: Some(table_name.clone()),
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
                        cte_name: None,
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
                    cte_name: None,
                }))
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.bind_table_with_joins(table_with_joins),
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
            ast::JoinOperator::Join(_) | ast::JoinOperator::Inner(_) => JoinType::Inner,
            ast::JoinOperator::Left(_) | ast::JoinOperator::LeftOuter(_) => JoinType::Left,
            ast::JoinOperator::Right(_) | ast::JoinOperator::RightOuter(_) => JoinType::Right,
            ast::JoinOperator::FullOuter(_) => JoinType::Full,
            ast::JoinOperator::CrossJoin(_) => JoinType::Cross,
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
            ast::JoinOperator::Join(constraint)
            | ast::JoinOperator::Inner(constraint)
            | ast::JoinOperator::Left(constraint)
            | ast::JoinOperator::Right(constraint)
            | ast::JoinOperator::LeftOuter(constraint)
            | ast::JoinOperator::RightOuter(constraint)
            | ast::JoinOperator::FullOuter(constraint)
            | ast::JoinOperator::LeftSemi(constraint)
            | ast::JoinOperator::LeftAnti(constraint) => {
                self.bind_join_constraint(constraint, &combined_schema)?
            }
            ast::JoinOperator::CrossJoin(_) => (vec![], None),
            _ => (vec![], None),
        };

        let (on, filter) = normalize_join_on(on, filter, &left_schema, &right_schema);

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

    #[allow(clippy::type_complexity)]
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
                        let name = &match col.0.first() {
                            Some(ast::ObjectNamePart::Identifier(i)) => i.value.clone(),
                            _ => col.to_string(),
                        };
                        (Expr::column(name.clone()), Expr::column(name.clone()))
                    })
                    .collect();
                Ok((on, None))
            }
            ast::JoinConstraint::Natural => Err(QueryError::NotImplemented(
                "NATURAL JOIN not supported".to_string(),
            )),
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
                    let field = SchemaField::new(alias.value.clone(), aliased.data_type(schema)?);
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
                    let table_name = match name {
                        ast::SelectItemQualifiedWildcardKind::ObjectName(o) => o.table_name(),
                        other => other.to_string(),
                    };
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
                SelectItem::ExprWithAliases { .. } => {
                    return Err(QueryError::NotImplemented(
                        "multi-alias SELECT items".into(),
                    ));
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
                        &bound,
                        agg_schema,
                        group_by,
                        aggregates,
                        input_schema,
                    )?;
                    exprs.push(converted);
                    fields.push(field);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let bound = self.bind_expr(expr, input_schema)?;
                    let (converted, mut field) = self.convert_to_agg_output(
                        &bound,
                        agg_schema,
                        group_by,
                        aggregates,
                        input_schema,
                    )?;
                    let aliased = converted.alias(alias.value.clone());
                    field.name = alias.value.clone();
                    // An explicit alias names a NEW output column; keeping the
                    // source column's qualifier would render it "n1.alias" and
                    // make the alias unaddressable by its own name.
                    field.relation = None;
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
                    let table_name = match name {
                        ast::SelectItemQualifiedWildcardKind::ObjectName(o) => o.table_name(),
                        other => other.to_string(),
                    };
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
                SelectItem::ExprWithAliases { .. } => {
                    return Err(QueryError::NotImplemented(
                        "multi-alias SELECT items".into(),
                    ));
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
                // Preserve relation qualifier for self-joins (n1.n_name vs n2.n_name)
                let col = Column {
                    relation: field.relation.clone(),
                    name: field.name.clone(),
                };
                return Ok((Expr::Column(col), field.clone()));
            }
        }

        // If it's an aggregate expression, convert to column reference
        let group_by_len = group_by.len();
        for (i, agg) in aggregates.iter().enumerate() {
            if expr == agg {
                let field = &agg_schema.fields()[group_by_len + i];
                return Ok((Expr::Column(Column::new(field.name.clone())), field.clone()));
            }
        }

        // If it contains aggregates, we need to recursively convert
        if expr.contains_aggregate() {
            let output_name = expr.output_name();
            // Try to find a matching aggregate output column
            for (i, agg) in aggregates.iter().enumerate() {
                if agg.output_name() == output_name || expr == agg {
                    let field = &agg_schema.fields()[group_by_len + i];
                    return Ok((Expr::Column(Column::new(field.name.clone())), field.clone()));
                }
            }
            // If it's an expression containing an aggregate (like SUM(x) + 1),
            // we need to recursively convert
            let converted = self.convert_expr_with_aggregates(
                expr,
                agg_schema,
                group_by,
                aggregates,
                input_schema,
            )?;
            let field = converted.to_field(agg_schema)?;
            return Ok((converted, field));
        }

        // For non-aggregate expressions that reference group by columns
        let converted = self.convert_expr_with_aggregates(
            expr,
            agg_schema,
            group_by,
            aggregates,
            input_schema,
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
        _input_schema: &PlanSchema,
    ) -> Result<Expr> {
        // Check if this is a group by column
        for (i, gb) in group_by.iter().enumerate() {
            if expr == gb {
                let field = &agg_schema.fields()[i];
                // Preserve relation qualifier for self-joins
                return Ok(Expr::Column(Column {
                    relation: field.relation.clone(),
                    name: field.name.clone(),
                }));
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
                    left,
                    agg_schema,
                    group_by,
                    aggregates,
                    _input_schema,
                )?;
                let right_conv = self.convert_expr_with_aggregates(
                    right,
                    agg_schema,
                    group_by,
                    aggregates,
                    _input_schema,
                )?;
                Ok(Expr::BinaryExpr {
                    left: Box::new(left_conv),
                    op: *op,
                    right: Box::new(right_conv),
                })
            }
            Expr::UnaryExpr { op, expr: inner } => {
                let inner_conv = self.convert_expr_with_aggregates(
                    inner,
                    agg_schema,
                    group_by,
                    aggregates,
                    _input_schema,
                )?;
                Ok(Expr::UnaryExpr {
                    op: *op,
                    expr: Box::new(inner_conv),
                })
            }
            Expr::Alias { expr: inner, name } => {
                let inner_conv = self.convert_expr_with_aggregates(
                    inner,
                    agg_schema,
                    group_by,
                    aggregates,
                    _input_schema,
                )?;
                Ok(Expr::Alias {
                    expr: Box::new(inner_conv),
                    name: name.clone(),
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
            } => {
                let inner_conv = self.convert_expr_with_aggregates(
                    inner,
                    agg_schema,
                    group_by,
                    aggregates,
                    _input_schema,
                )?;
                Ok(Expr::Cast {
                    expr: Box::new(inner_conv),
                    data_type: data_type.clone(),
                })
            }
            // A scalar function OVER an aggregate — ROUND(SUM(x), 2) — must
            // have its arguments rewritten to the aggregate's output columns,
            // exactly like the operands of `SUM(x) * 2`. These four variants
            // used to fall into the catch-all below, which left the raw
            // SUM(x) to fail against the aggregate output schema
            // ("Column not found: x").
            Expr::ScalarFunc { func, args } => Ok(Expr::ScalarFunc {
                func: func.clone(),
                args: args
                    .iter()
                    .map(|a| {
                        self.convert_expr_with_aggregates(
                            a,
                            agg_schema,
                            group_by,
                            aggregates,
                            _input_schema,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            }),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let conv = |e: &Expr| {
                    self.convert_expr_with_aggregates(
                        e,
                        agg_schema,
                        group_by,
                        aggregates,
                        _input_schema,
                    )
                };
                Ok(Expr::Case {
                    operand: operand.as_deref().map(&conv).transpose()?.map(Box::new),
                    when_then: when_then
                        .iter()
                        .map(|(w, t)| Ok((conv(w)?, conv(t)?)))
                        .collect::<Result<Vec<_>>>()?,
                    else_expr: else_expr.as_deref().map(&conv).transpose()?.map(Box::new),
                })
            }
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let conv = |e: &Expr| {
                    self.convert_expr_with_aggregates(
                        e,
                        agg_schema,
                        group_by,
                        aggregates,
                        _input_schema,
                    )
                };
                Ok(Expr::InList {
                    expr: Box::new(conv(inner)?),
                    list: list.iter().map(&conv).collect::<Result<Vec<_>>>()?,
                    negated: *negated,
                })
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                let conv = |e: &Expr| {
                    self.convert_expr_with_aggregates(
                        e,
                        agg_schema,
                        group_by,
                        aggregates,
                        _input_schema,
                    )
                };
                Ok(Expr::Between {
                    expr: Box::new(conv(inner)?),
                    low: Box::new(conv(low)?),
                    high: Box::new(conv(high)?),
                    negated: *negated,
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

    #[allow(clippy::type_complexity)]
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

        // Parse GROUP BY. An integer literal is an ORDINAL into the SELECT
        // list (standard), never a constant group key.
        if let ast::GroupByExpr::Expressions(exprs, _) = group_by {
            for expr in exprs {
                if let SqlExpr::Value(ast::ValueWithSpan {
                    value: ast::Value::Number(n, _),
                    ..
                }) = expr
                {
                    if let Ok(ord) = n.parse::<usize>() {
                        if ord == 0 || ord > projection.len() {
                            return Err(QueryError::Bind(format!(
                                "GROUP BY position {ord} is out of range (1-{})",
                                projection.len()
                            )));
                        }
                        let item = &projection[ord - 1];
                        let target = match item {
                            SelectItem::UnnamedExpr(e) => e,
                            SelectItem::ExprWithAlias { expr, .. } => expr,
                            other => {
                                return Err(QueryError::Bind(format!(
                                    "GROUP BY position {ord} refers to {other}, which is not \
                                     an expression"
                                )))
                            }
                        };
                        group_by_exprs.push(self.bind_expr(target, schema)?);
                        continue;
                    }
                }
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
                SelectItem::ExprWithAliases { .. } => {
                    return Err(QueryError::NotImplemented(
                        "multi-alias SELECT items".into(),
                    ));
                }
                _ => {}
            }
        }

        Ok((
            group_by_exprs,
            aggregate_exprs,
            aggregate_aliases,
            has_aggregates,
        ))
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
                // Handle column number references (e.g., ORDER BY 2)
                let expr = match &o.expr {
                    SqlExpr::Value(ast::ValueWithSpan {
                        value: ast::Value::Number(n, _),
                        ..
                    }) => {
                        if let Ok(col_num) = n.parse::<usize>() {
                            if col_num > 0 && col_num <= schema.fields().len() {
                                // Column numbers are 1-indexed
                                let field = &schema.fields()[col_num - 1];
                                Expr::Column(Column::new(field.name.clone()))
                            } else {
                                return Err(QueryError::Bind(format!(
                                    "ORDER BY column {} is out of range (1-{})",
                                    col_num,
                                    schema.fields().len()
                                )));
                            }
                        } else {
                            self.bind_expr(&o.expr, schema)?
                        }
                    }
                    _ => self.bind_expr(&o.expr, schema)?,
                };
                // A vector has no total order; sorting by one would silently
                // order by whatever byte-wise comparison the kernel happens to
                // do. Reject it, naming the column.
                crate::planner::vector_types::require_scalar(&expr, schema, "ORDER BY")?;
                let direction = if o.options.asc.unwrap_or(true) {
                    SortDirection::Asc
                } else {
                    SortDirection::Desc
                };
                let nulls = match o.options.nulls_first {
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
                // `a.b` is overwhelmingly `table.column`, but over a Lance (or
                // any nested) table it may be struct field access. Those look
                // identical to the parser, so disambiguate against the schema:
                // if `a` is itself a nested column here, the user asked for a
                // field and deserves to be told that field access is missing
                // rather than "column a.b not found", which points at the wrong
                // thing entirely.
                if let Some(head) = idents.first() {
                    if let Some((_, f)) = schema.resolve_column(&Column::new(head.value.clone())) {
                        if crate::planner::vector_types::is_opaque_nested(&f.data_type) {
                            return Err(QueryError::NotImplemented(format!(
                                "field access `{}` is not implemented: column `{}` has nested \
                                 type {}, and the engine carries nested values opaquely \
                                 (selectable and projectable, but not decomposable). Select the \
                                 whole column instead, or flatten the field when writing the \
                                 dataset.",
                                idents
                                    .iter()
                                    .map(|i| i.value.as_str())
                                    .collect::<Vec<_>>()
                                    .join("."),
                                head.value,
                                crate::planner::vector_types::describe_type(&f.data_type)
                            )));
                        }
                    }
                }
                if idents.len() == 2 {
                    let table = &idents[0].value;
                    let column = &idents[1].value;
                    Ok(Expr::Column(Column::new_qualified(
                        table.clone(),
                        column.clone(),
                    )))
                } else {
                    Err(QueryError::Bind(format!(
                        "Unsupported compound identifier: {:?}",
                        idents
                    )))
                }
            }
            SqlExpr::Value(value) => self.bind_value(value),
            // `[1.0, 2.0, ...]` / `ARRAY[...]`. The realistic use is a 384- or
            // 1536-element query vector, so this must stay linear and cheap:
            // elements are folded straight into one `ScalarValue::List`
            // literal, never into a 384-node expression tree that every
            // optimizer rule would then walk.
            SqlExpr::Array(arr) => self.bind_array_literal(&arr.elem, schema),
            SqlExpr::BinaryOp { left, op, right } => {
                // `expr + INTERVAL 'n' unit` / `expr - INTERVAL ...` desugar
                // to DATE_ADD, which does calendar-correct month arithmetic.
                if matches!(op, ast::BinaryOperator::Plus | ast::BinaryOperator::Minus) {
                    let negate = matches!(op, ast::BinaryOperator::Minus);
                    if let SqlExpr::Interval(iv) = right.as_ref() {
                        let base = self.bind_expr(left, schema)?;
                        return self.bind_interval_add(base, iv, negate, schema);
                    }
                    if let SqlExpr::Interval(iv) = left.as_ref() {
                        if !negate {
                            let base = self.bind_expr(right, schema)?;
                            return self.bind_interval_add(base, iv, false, schema);
                        }
                    }
                }
                let left_expr = self.bind_expr(left, schema)?;
                let right_expr = self.bind_expr(right, schema)?;
                let binary_op = self.convert_binary_op(op)?;
                // `embedding = embedding` / `embedding > 3` have no meaning; the
                // comparison kernels would either error opaquely or coerce.
                crate::planner::vector_types::require_scalar_operands(
                    &left_expr,
                    &right_expr,
                    schema,
                    &binary_op.to_string(),
                )?;
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
                else_result,
                ..
            } => {
                let bound_operand = operand
                    .as_ref()
                    .map(|e| self.bind_expr(e, schema))
                    .transpose()?
                    .map(Box::new);

                let when_then: Result<Vec<(Expr, Expr)>> = conditions
                    .iter()
                    .map(|cw| {
                        let bound_when = self.bind_expr(&cw.condition, schema)?;
                        let bound_then = self.bind_expr(&cw.result, schema)?;
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
            SqlExpr::Cast {
                expr, data_type, ..
            } => {
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
            SqlExpr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
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
            SqlExpr::IsDistinctFrom(a, b) => {
                let a = self.bind_expr(a, schema)?;
                let b = self.bind_expr(b, schema)?;
                Ok(is_distinct_expr(a, b, false))
            }
            SqlExpr::IsNotDistinctFrom(a, b) => {
                let a = self.bind_expr(a, schema)?;
                let b = self.bind_expr(b, schema)?;
                Ok(is_distinct_expr(a, b, true))
            }
            SqlExpr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => self.bind_quantified(left, compare_op, right, true, schema),
            SqlExpr::AllOp {
                left,
                compare_op,
                right,
            } => self.bind_quantified(left, compare_op, right, false, schema),
            SqlExpr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                // OVERLAY(s PLACING r FROM p [FOR l]) ==
                // SUBSTRING(s, 1, p-1) || r || SUBSTRING(s, p + l)
                // with l defaulting to LENGTH(r).
                let s = self.bind_expr(expr, schema)?;
                let r = self.bind_expr(overlay_what, schema)?;
                let p = self.bind_expr(overlay_from, schema)?;
                let l = match overlay_for {
                    Some(e) => self.bind_expr(e, schema)?,
                    None => Expr::ScalarFunc {
                        func: ScalarFunction::Length,
                        args: vec![r.clone()],
                    },
                };
                let one = || Expr::Literal(ScalarValue::Int64(1));
                let prefix = Expr::ScalarFunc {
                    func: ScalarFunction::Substring,
                    args: vec![
                        s.clone(),
                        one(),
                        Expr::BinaryExpr {
                            left: Box::new(p.clone()),
                            op: BinaryOp::Subtract,
                            right: Box::new(one()),
                        },
                    ],
                };
                let suffix = Expr::ScalarFunc {
                    func: ScalarFunction::Substring,
                    args: vec![
                        s,
                        Expr::BinaryExpr {
                            left: Box::new(p),
                            op: BinaryOp::Add,
                            right: Box::new(l),
                        },
                    ],
                };
                Ok(Expr::ScalarFunc {
                    func: ScalarFunction::Concat,
                    args: vec![prefix, r, suffix],
                })
            }
            SqlExpr::Interval(interval) => {
                // Simple interval handling
                let value = self.bind_expr(&interval.value, schema)?;
                if let Expr::Literal(ScalarValue::Utf8(s)) = &value {
                    // Parse interval string like "1 day" or "3 month"
                    let parts: Vec<&str> = s.split_whitespace().collect();
                    if !parts.is_empty() {
                        if let Ok(num) = parts[0].parse::<i64>() {
                            return Ok(Expr::Literal(ScalarValue::Interval(num)));
                        }
                    }
                }
                Ok(value)
            }
            SqlExpr::TypedString(ast::TypedString {
                data_type, value, ..
            }) => {
                if data_type == &ast::DataType::Date {
                    // Parse date string
                    let value = match &value.value {
                        ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                            s.clone()
                        }
                        other => other.to_string(),
                    };
                    let value = value.as_str();
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                        let days = date
                            .signed_duration_since(
                                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                            )
                            .num_days() as i32;
                        return Ok(Expr::Literal(ScalarValue::Date32(days)));
                    }
                }
                Ok(Expr::Literal(ScalarValue::Utf8(value.to_string())))
            }
            SqlExpr::Ceil { expr, .. } => {
                let arg = self.bind_expr(expr, schema)?;
                Ok(Expr::ScalarFunc {
                    func: ScalarFunction::Ceil,
                    args: vec![arg],
                })
            }
            SqlExpr::Floor { expr, .. } => {
                let arg = self.bind_expr(expr, schema)?;
                Ok(Expr::ScalarFunc {
                    func: ScalarFunction::Floor,
                    args: vec![arg],
                })
            }
            SqlExpr::Trim {
                expr,
                trim_where,
                trim_what: _,
                ..
            } => {
                // For now, simple trim without specific characters
                let arg = self.bind_expr(expr, schema)?;
                match trim_where {
                    Some(ast::TrimWhereField::Leading) => Ok(Expr::ScalarFunc {
                        func: ScalarFunction::Ltrim,
                        args: vec![arg],
                    }),
                    Some(ast::TrimWhereField::Trailing) => Ok(Expr::ScalarFunc {
                        func: ScalarFunction::Rtrim,
                        args: vec![arg],
                    }),
                    Some(ast::TrimWhereField::Both) | None => Ok(Expr::ScalarFunc {
                        func: ScalarFunction::Trim,
                        args: vec![arg],
                    }),
                }
            }
            // POSITION(substr IN str) syntax
            SqlExpr::Position { expr, r#in } => {
                let str_expr = self.bind_expr(r#in, schema)?;
                let substr_expr = self.bind_expr(expr, schema)?;
                Ok(Expr::ScalarFunc {
                    func: ScalarFunction::Position,
                    args: vec![str_expr, substr_expr],
                })
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

    /// Bind `[a, b, c]` / `ARRAY[a, b, c]` to a single `ScalarValue::List`.
    ///
    /// Elements must be constants. A 384-float query vector is the motivating
    /// case, and folding it into one literal (rather than an N-child expression
    /// node) keeps every later plan traversal O(1) in the vector's dimension
    /// instead of O(N) per rule per pass.
    ///
    /// The element type is the widest of the element types present, so
    /// `[1, 2.5]` is a Float64 list rather than a type error.
    fn bind_array_literal(&mut self, elems: &[SqlExpr], schema: &PlanSchema) -> Result<Expr> {
        let mut values: Vec<ScalarValue> = Vec::with_capacity(elems.len());
        let mut any_float = false;
        let mut any_string = false;

        for (i, e) in elems.iter().enumerate() {
            let bound = self.bind_expr(e, schema)?;
            let scalar = const_scalar(&bound).ok_or_else(|| {
                QueryError::NotImplemented(format!(
                    "array literal element {} is not a constant ({}); \
                     array literals must be built from literals",
                    i, bound
                ))
            })?;
            match &scalar {
                ScalarValue::Float32(_) | ScalarValue::Float64(_) | ScalarValue::Decimal128(_) => {
                    any_float = true
                }
                ScalarValue::Utf8(_) => any_string = true,
                _ => {}
            }
            values.push(scalar);
        }

        let elem_type = if any_string {
            ArrowDataType::Utf8
        } else if any_float {
            ArrowDataType::Float64
        } else if values.is_empty() {
            ArrowDataType::Null
        } else {
            ArrowDataType::Int64
        };

        Ok(Expr::Literal(ScalarValue::List(
            values,
            Box::new(elem_type),
        )))
    }

    /// Bind `func(args) OVER (spec | name)` into an `Expr::WindowFunction`.
    fn bind_window_function(
        &mut self,
        name: &str,
        func: &ast::Function,
        over: &ast::WindowType,
        schema: &PlanSchema,
    ) -> Result<Expr> {
        use crate::planner::logical_expr::{FrameBound, FrameUnits, WindowFrame, WindowFunc};

        // Resolve the window spec, following one level of naming.
        let spec: ast::WindowSpec = match over {
            ast::WindowType::WindowSpec(s) => s.clone(),
            ast::WindowType::NamedWindow(ident) => {
                let key = ident.value.to_lowercase();
                self.named_windows.get(&key).cloned().ok_or_else(|| {
                    QueryError::Bind(format!("window \"{}\" is not defined", ident.value))
                })?
            }
        };
        if spec.window_name.is_some() {
            return Err(QueryError::NotImplemented(
                "window specification inheritance (OVER (base_window ...))".into(),
            ));
        }

        // The function itself.
        let wfunc = match name {
            "ROW_NUMBER" => WindowFunc::RowNumber,
            "RANK" => WindowFunc::Rank,
            "DENSE_RANK" => WindowFunc::DenseRank,
            "PERCENT_RANK" => WindowFunc::PercentRank,
            "CUME_DIST" => WindowFunc::CumeDist,
            "NTILE" => WindowFunc::Ntile,
            "LAG" => WindowFunc::Lag,
            "LEAD" => WindowFunc::Lead,
            "FIRST_VALUE" => WindowFunc::FirstValue,
            "LAST_VALUE" => WindowFunc::LastValue,
            "NTH_VALUE" => WindowFunc::NthValue,
            "COUNT" => WindowFunc::Aggregate(AggregateFunction::Count),
            "SUM" => WindowFunc::Aggregate(AggregateFunction::Sum),
            "AVG" => WindowFunc::Aggregate(AggregateFunction::Avg),
            "MIN" => WindowFunc::Aggregate(AggregateFunction::Min),
            "MAX" => WindowFunc::Aggregate(AggregateFunction::Max),
            other => {
                return Err(QueryError::NotImplemented(format!(
                    "window function {other} OVER (...)"
                )))
            }
        };

        // Arguments (COUNT(*) carries a Wildcard argument).
        let args: Vec<Expr> = match &func.args {
            ast::FunctionArguments::None => vec![],
            ast::FunctionArguments::Subquery(_) => {
                return Err(QueryError::NotImplemented(
                    "subquery arguments to a window function".into(),
                ))
            }
            ast::FunctionArguments::List(list) => list
                .args
                .iter()
                .map(|arg| match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                    | ast::FunctionArg::Named {
                        arg: ast::FunctionArgExpr::Expr(e),
                        ..
                    } => self.bind_expr(e, schema),
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)
                    | ast::FunctionArg::Named {
                        arg: ast::FunctionArgExpr::Wildcard,
                        ..
                    } => Ok(Expr::Wildcard),
                    other => Err(QueryError::NotImplemented(format!(
                        "window function argument {other:?}"
                    ))),
                })
                .collect::<Result<_>>()?,
        };

        let partition_by: Vec<Expr> = spec
            .partition_by
            .iter()
            .map(|e| self.bind_expr(e, schema))
            .collect::<Result<_>>()?;
        let order_by = self.bind_order_by(&spec.order_by, schema)?;

        // The frame, stored RESOLVED (see WindowFrame docs).
        let frame = match &spec.window_frame {
            None => {
                if order_by.is_empty() {
                    WindowFrame {
                        units: FrameUnits::Rows,
                        start: FrameBound::UnboundedPreceding,
                        end: FrameBound::UnboundedFollowing,
                        explicit: false,
                    }
                } else {
                    WindowFrame {
                        units: FrameUnits::Range,
                        start: FrameBound::UnboundedPreceding,
                        end: FrameBound::CurrentRow,
                        explicit: false,
                    }
                }
            }
            Some(f) => {
                let units = match f.units {
                    ast::WindowFrameUnits::Rows => FrameUnits::Rows,
                    ast::WindowFrameUnits::Range => FrameUnits::Range,
                    ast::WindowFrameUnits::Groups => {
                        return Err(QueryError::NotImplemented("GROUPS window frames".into()))
                    }
                };
                let bound = |b: &ast::WindowFrameBound, is_start: bool| -> Result<FrameBound> {
                    Ok(match b {
                        ast::WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                        ast::WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                        ast::WindowFrameBound::Following(None) => {
                            if is_start {
                                return Err(QueryError::Bind(
                                    "frame cannot start at UNBOUNDED FOLLOWING".into(),
                                ));
                            }
                            FrameBound::UnboundedFollowing
                        }
                        ast::WindowFrameBound::Preceding(Some(e)) => {
                            FrameBound::Preceding(frame_offset(e)?)
                        }
                        ast::WindowFrameBound::Following(Some(e)) => {
                            FrameBound::Following(frame_offset(e)?)
                        }
                    })
                };
                let start = bound(&f.start_bound, true)?;
                let end = match &f.end_bound {
                    Some(e) => bound(e, false)?,
                    None => FrameBound::CurrentRow,
                };
                if matches!(end, FrameBound::UnboundedPreceding) {
                    return Err(QueryError::Bind(
                        "frame cannot end at UNBOUNDED PRECEDING".into(),
                    ));
                }
                WindowFrame {
                    units,
                    start,
                    end,
                    explicit: true,
                }
            }
        };

        Ok(Expr::WindowFunction(Box::new(
            crate::planner::logical_expr::WindowExpr {
                func: wfunc,
                args,
                partition_by,
                order_by,
                frame,
            },
        )))
    }

    /// Replace every window function in `expr` with a column reference,
    /// registering it (deduplicated) in `acc` as (`__wN`, expression).
    fn extract_windows(
        expr: Expr,
        acc: &mut Vec<(String, crate::planner::logical_expr::WindowExpr)>,
    ) -> Expr {
        match expr {
            Expr::WindowFunction(w) => {
                if let Some((name, _)) = acc.iter().find(|(_, x)| x == w.as_ref()) {
                    return Expr::Column(Column::new(name.clone()));
                }
                let name = format!("__w{}", acc.len());
                acc.push((name.clone(), *w));
                Expr::Column(Column::new(name))
            }
            Expr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
                left: Box::new(Self::extract_windows(*left, acc)),
                op,
                right: Box::new(Self::extract_windows(*right, acc)),
            },
            Expr::UnaryExpr { op, expr } => Expr::UnaryExpr {
                op,
                expr: Box::new(Self::extract_windows(*expr, acc)),
            },
            Expr::Cast { expr, data_type } => Expr::Cast {
                expr: Box::new(Self::extract_windows(*expr, acc)),
                data_type,
            },
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(Self::extract_windows(*expr, acc)),
                name,
            },
            Expr::ScalarFunc { func, args } => Expr::ScalarFunc {
                func,
                args: args
                    .into_iter()
                    .map(|a| Self::extract_windows(a, acc))
                    .collect(),
            },
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Expr::Case {
                operand: operand.map(|o| Box::new(Self::extract_windows(*o, acc))),
                when_then: when_then
                    .into_iter()
                    .map(|(w, t)| (Self::extract_windows(w, acc), Self::extract_windows(t, acc)))
                    .collect(),
                else_expr: else_expr.map(|e| Box::new(Self::extract_windows(*e, acc))),
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(Self::extract_windows(*expr, acc)),
                list: list
                    .into_iter()
                    .map(|e| Self::extract_windows(e, acc))
                    .collect(),
                negated,
            },
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(Self::extract_windows(*expr, acc)),
                low: Box::new(Self::extract_windows(*low, acc)),
                high: Box::new(Self::extract_windows(*high, acc)),
                negated,
            },
            other => other,
        }
    }

    fn bind_function(&mut self, func: &ast::Function, schema: &PlanSchema) -> Result<Expr> {
        let name = func.name.to_string().to_uppercase();

        // Window functions bind to Expr::WindowFunction — never as a plain
        // aggregate, which would silently return wrong results.
        if let Some(over) = &func.over {
            if !self.allow_window {
                return Err(QueryError::Bind(format!(
                    "{name} OVER (...) is only allowed in the SELECT list"
                )));
            }
            return self.bind_window_function(&name, func, over, schema);
        }

        // Extract arguments from the FunctionArguments
        let func_args: Vec<&ast::FunctionArg> = match &func.args {
            ast::FunctionArguments::None => vec![],
            ast::FunctionArguments::Subquery(_) => {
                return Err(QueryError::NotImplemented(
                    "Subquery function arguments".into(),
                ));
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
                    ast::FunctionArgExpr::WildcardWithOptions(_) => {
                        Err(QueryError::NotImplemented(
                            "wildcard with options as a function argument".into(),
                        ))
                    }
                },
                ast::FunctionArg::Named { arg, .. } => match arg {
                    ast::FunctionArgExpr::Expr(e) => self.bind_expr(e, schema),
                    ast::FunctionArgExpr::Wildcard => Ok(Expr::Wildcard),
                    ast::FunctionArgExpr::QualifiedWildcard(name) => {
                        Ok(Expr::QualifiedWildcard(name.to_string()))
                    }
                    ast::FunctionArgExpr::WildcardWithOptions(_) => {
                        Err(QueryError::NotImplemented(
                            "wildcard with options as a function argument".into(),
                        ))
                    }
                },
                ast::FunctionArg::ExprNamed { .. } => Err(QueryError::NotImplemented(
                    "expression-named function arguments".into(),
                )),
            })
            .collect();
        let args = args?;

        // Check for DISTINCT in function args
        let distinct = match &func.args {
            ast::FunctionArguments::List(arg_list) => {
                matches!(
                    arg_list.duplicate_treatment,
                    Some(ast::DuplicateTreatment::Distinct)
                )
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
            // Statistical aggregates
            "STDDEV" => Ok(Expr::Aggregate {
                func: AggregateFunction::Stddev,
                args,
                distinct: false,
            }),
            "STDDEV_POP" => Ok(Expr::Aggregate {
                func: AggregateFunction::StddevPop,
                args,
                distinct: false,
            }),
            "STDDEV_SAMP" => Ok(Expr::Aggregate {
                func: AggregateFunction::StddevSamp,
                args,
                distinct: false,
            }),
            "VARIANCE" | "VAR" => Ok(Expr::Aggregate {
                func: AggregateFunction::Variance,
                args,
                distinct: false,
            }),
            "VAR_POP" => Ok(Expr::Aggregate {
                func: AggregateFunction::VarPop,
                args,
                distinct: false,
            }),
            "VAR_SAMP" => Ok(Expr::Aggregate {
                func: AggregateFunction::VarSamp,
                args,
                distinct: false,
            }),
            // Boolean aggregates
            "BOOL_AND" | "EVERY" => Ok(Expr::Aggregate {
                func: AggregateFunction::BoolAnd,
                args,
                distinct: false,
            }),
            "BOOL_OR" | "ANY" => Ok(Expr::Aggregate {
                func: AggregateFunction::BoolOr,
                args,
                distinct: false,
            }),
            // New simple aggregates
            "COUNT_IF" => Ok(Expr::Aggregate {
                func: AggregateFunction::CountIf,
                args,
                distinct: false,
            }),
            "ANY_VALUE" => Ok(Expr::Aggregate {
                func: AggregateFunction::AnyValue,
                args,
                distinct: false,
            }),
            "ARBITRARY" => Ok(Expr::Aggregate {
                func: AggregateFunction::Arbitrary,
                args,
                distinct: false,
            }),
            "GEOMETRIC_MEAN" => Ok(Expr::Aggregate {
                func: AggregateFunction::GeometricMean,
                args,
                distinct: false,
            }),
            "CHECKSUM" => Ok(Expr::Aggregate {
                func: AggregateFunction::Checksum,
                args,
                distinct: false,
            }),
            // Bitwise aggregates
            "BITWISE_AND_AGG" => Ok(Expr::Aggregate {
                func: AggregateFunction::BitwiseAndAgg,
                args,
                distinct: false,
            }),
            "BITWISE_OR_AGG" => Ok(Expr::Aggregate {
                func: AggregateFunction::BitwiseOrAgg,
                args,
                distinct: false,
            }),
            "BITWISE_XOR_AGG" => Ok(Expr::Aggregate {
                func: AggregateFunction::BitwiseXorAgg,
                args,
                distinct: false,
            }),
            // String aggregates
            "LISTAGG" | "STRING_AGG" | "GROUP_CONCAT" => Ok(Expr::Aggregate {
                func: AggregateFunction::Listagg,
                args,
                distinct,
            }),
            // Correlation and regression aggregates
            "CORR" => Ok(Expr::Aggregate {
                func: AggregateFunction::Corr,
                args,
                distinct: false,
            }),
            "COVAR_POP" => Ok(Expr::Aggregate {
                func: AggregateFunction::CovarPop,
                args,
                distinct: false,
            }),
            "COVAR_SAMP" => Ok(Expr::Aggregate {
                func: AggregateFunction::CovarSamp,
                args,
                distinct: false,
            }),
            "KURTOSIS" => Ok(Expr::Aggregate {
                func: AggregateFunction::Kurtosis,
                args,
                distinct: false,
            }),
            "SKEWNESS" => Ok(Expr::Aggregate {
                func: AggregateFunction::Skewness,
                args,
                distinct: false,
            }),
            "REGR_SLOPE" => Ok(Expr::Aggregate {
                func: AggregateFunction::RegrSlope,
                args,
                distinct: false,
            }),
            "REGR_INTERCEPT" => Ok(Expr::Aggregate {
                func: AggregateFunction::RegrIntercept,
                args,
                distinct: false,
            }),
            "REGR_COUNT" => Ok(Expr::Aggregate {
                func: AggregateFunction::RegrCount,
                args,
                distinct: false,
            }),
            "REGR_AVGX" => Ok(Expr::Aggregate {
                func: AggregateFunction::RegrAvgx,
                args,
                distinct: false,
            }),
            "REGR_AVGY" => Ok(Expr::Aggregate {
                func: AggregateFunction::RegrAvgy,
                args,
                distinct: false,
            }),
            // Approximate aggregates
            "APPROX_PERCENTILE" => Ok(Expr::Aggregate {
                func: AggregateFunction::ApproxPercentile,
                args,
                distinct: false,
            }),
            "APPROX_DISTINCT" | "APPROX_COUNT_DISTINCT" => Ok(Expr::Aggregate {
                func: AggregateFunction::ApproxDistinct,
                args,
                distinct: false,
            }),
            // Multi-value aggregates
            "MAX_BY" => Ok(Expr::Aggregate {
                func: AggregateFunction::MaxBy,
                args,
                distinct: false,
            }),
            "MIN_BY" => Ok(Expr::Aggregate {
                func: AggregateFunction::MinBy,
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
            "POWER" | "POW" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Power,
                args,
            }),
            "SQRT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Sqrt,
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
            // Math functions
            "MOD" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Mod,
                args,
            }),
            "SIGN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Sign,
                args,
            }),
            "TRUNCATE" | "TRUNC" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Truncate,
                args,
            }),
            "LN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Ln,
                args,
            }),
            "LOG" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Log,
                args,
            }),
            "LOG2" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Log2,
                args,
            }),
            "LOG10" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Log10,
                args,
            }),
            "EXP" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Exp,
                args,
            }),
            "RANDOM" | "RAND" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Random,
                args,
            }),
            "SIN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Sin,
                args,
            }),
            "COS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Cos,
                args,
            }),
            "TAN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Tan,
                args,
            }),
            "ASIN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Asin,
                args,
            }),
            "ACOS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Acos,
                args,
            }),
            "ATAN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Atan,
                args,
            }),
            "ATAN2" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Atan2,
                args,
            }),
            "DEGREES" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Degrees,
                args,
            }),
            "RADIANS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Radians,
                args,
            }),
            "PI" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Pi,
                args,
            }),
            "E" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::E,
                args,
            }),
            "CBRT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Cbrt,
                args,
            }),
            // String functions
            "POSITION" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Position,
                args,
            }),
            "STRPOS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Strpos,
                args,
            }),
            "REVERSE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Reverse,
                args,
            }),
            "LPAD" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Lpad,
                args,
            }),
            "RPAD" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Rpad,
                args,
            }),
            "SPLIT_PART" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::SplitPart,
                args,
            }),
            "STARTS_WITH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::StartsWith,
                args,
            }),
            "ENDS_WITH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::EndsWith,
                args,
            }),
            "CHR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Chr,
                args,
            }),
            "ASCII" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Ascii,
                args,
            }),
            "CONCAT_WS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ConcatWs,
                args,
            }),
            "LEFT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Left,
                args,
            }),
            "RIGHT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Right,
                args,
            }),
            "REPEAT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Repeat,
                args,
            }),
            // Date/Time functions
            "CURRENT_DATE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::CurrentDate,
                args,
            }),
            "CURRENT_TIMESTAMP" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::CurrentTimestamp,
                args,
            }),
            "NOW" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Now,
                args,
            }),
            "DATE_ADD" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DateAdd,
                args,
            }),
            "DATE_DIFF" | "DATEDIFF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DateDiff,
                args,
            }),
            "HOUR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Hour,
                args,
            }),
            "MINUTE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Minute,
                args,
            }),
            "SECOND" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Second,
                args,
            }),
            "QUARTER" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Quarter,
                args,
            }),
            "WEEK" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Week,
                args,
            }),
            "DAY_OF_WEEK" | "DAYOFWEEK" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DayOfWeek,
                args,
            }),
            "DAY_OF_YEAR" | "DAYOFYEAR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DayOfYear,
                args,
            }),
            // Conditional functions
            "IF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::If,
                args,
            }),
            "GREATEST" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Greatest,
                args,
            }),
            "LEAST" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Least,
                args,
            }),
            // Regex functions
            "REGEXP_LIKE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::RegexpLike,
                args,
            }),
            "REGEXP_EXTRACT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::RegexpExtract,
                args,
            }),
            "REGEXP_REPLACE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::RegexpReplace,
                args,
            }),
            "REGEXP_SPLIT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::RegexpSplit,
                args,
            }),
            "REGEXP_COUNT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::RegexpCount,
                args,
            }),
            // Binary/Encoding functions
            "TO_HEX" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToHex,
                args,
            }),
            "FROM_HEX" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromHex,
                args,
            }),
            "TO_BASE64" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToBase64,
                args,
            }),
            "FROM_BASE64" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromBase64,
                args,
            }),
            "MD5" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Md5,
                args,
            }),
            "SHA256" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Sha256,
                args,
            }),
            "SHA1" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Sha1,
                args,
            }),
            "SHA512" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Sha512,
                args,
            }),
            // Bitwise functions
            "BITWISE_AND" | "BIT_AND" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BitwiseAnd,
                args,
            }),
            "BITWISE_OR" | "BIT_OR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BitwiseOr,
                args,
            }),
            "BITWISE_XOR" | "BIT_XOR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BitwiseXor,
                args,
            }),
            "BITWISE_NOT" | "BIT_NOT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BitwiseNot,
                args,
            }),
            "BIT_COUNT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BitCount,
                args,
            }),
            // URL functions
            "URL_EXTRACT_HOST" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlExtractHost,
                args,
            }),
            "URL_EXTRACT_PATH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlExtractPath,
                args,
            }),
            "URL_EXTRACT_PORT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlExtractPort,
                args,
            }),
            "URL_EXTRACT_PROTOCOL" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlExtractProtocol,
                args,
            }),
            "URL_EXTRACT_QUERY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlExtractQuery,
                args,
            }),
            "URL_ENCODE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlEncode,
                args,
            }),
            "URL_DECODE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlDecode,
                args,
            }),
            // Other functions
            "TYPEOF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Typeof,
                args,
            }),
            "UUID" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Uuid,
                args,
            }),
            // New Math functions - Trigonometric
            "SINH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Sinh,
                args,
            }),
            "COSH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Cosh,
                args,
            }),
            "TANH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Tanh,
                args,
            }),
            "COT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Cot,
                args,
            }),
            // New Math functions - Special values
            "INFINITY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Infinity,
                args,
            }),
            "NAN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Nan,
                args,
            }),
            "IS_FINITE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::IsFinite,
                args,
            }),
            "IS_NAN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::IsNan,
                args,
            }),
            "IS_INFINITE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::IsInfinite,
                args,
            }),
            // New Math functions - Base conversion
            "FROM_BASE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromBase,
                args,
            }),
            "TO_BASE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToBase,
                args,
            }),
            "WIDTH_BUCKET" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::WidthBucket,
                args,
            }),
            // New Math functions - Statistical distributions
            "BETA_CDF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BetaCdf,
                args,
            }),
            "INVERSE_BETA_CDF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::InverseBetaCdf,
                args,
            }),
            "NORMAL_CDF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::NormalCdf,
                args,
            }),
            "INVERSE_NORMAL_CDF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::InverseNormalCdf,
                args,
            }),
            "T_CDF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::TCdf,
                args,
            }),
            "T_PDF" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::TPdf,
                args,
            }),
            "WILSON_INTERVAL_LOWER" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::WilsonIntervalLower,
                args,
            }),
            "WILSON_INTERVAL_UPPER" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::WilsonIntervalUpper,
                args,
            }),
            // New Math functions - Vector operations
            "COSINE_SIMILARITY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::CosineSimilarity,
                args,
            }),
            "COSINE_DISTANCE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::CosineDistance,
                args,
            }),
            // Vector distance / similarity. See ScalarFunction docs for the
            // sign convention: *_DISTANCE is smaller-is-closer, DOT_PRODUCT is
            // a similarity where larger is closer.
            "L2_DISTANCE" | "EUCLIDEAN_DISTANCE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::L2Distance,
                args,
            }),
            "DOT_PRODUCT" | "INNER_PRODUCT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DotProduct,
                args,
            }),
            // New String functions
            "SPLIT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Split,
                args,
            }),
            "CODEPOINT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Codepoint,
                args,
            }),
            "HAMMING_DISTANCE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::HammingDistance,
                args,
            }),
            "LEVENSHTEIN_DISTANCE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::LevenshteinDistance,
                args,
            }),
            "SOUNDEX" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Soundex,
                args,
            }),
            "TRANSLATE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Translate,
                args,
            }),
            "LUHN_CHECK" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::LuhnCheck,
                args,
            }),
            "NORMALIZE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Normalize,
                args,
            }),
            "TO_UTF8" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToUtf8,
                args,
            }),
            "FROM_UTF8" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromUtf8,
                args,
            }),
            "WORD_STEM" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::WordStem,
                args,
            }),
            // New Date/Time functions - Extraction
            "MILLISECOND" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Millisecond,
                args,
            }),
            "YEAR_OF_WEEK" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::YearOfWeek,
                args,
            }),
            "TIMEZONE_HOUR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::TimezoneHour,
                args,
            }),
            "TIMEZONE_MINUTE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::TimezoneMinute,
                args,
            }),
            // New Date/Time functions - Current
            "CURRENT_TIME" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::CurrentTime,
                args,
            }),
            "CURRENT_TIMEZONE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::CurrentTimezone,
                args,
            }),
            "LOCALTIME" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Localtime,
                args,
            }),
            "LOCALTIMESTAMP" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Localtimestamp,
                args,
            }),
            // New Date/Time functions - Arithmetic
            "LAST_DAY_OF_MONTH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::LastDayOfMonth,
                args,
            }),
            // New Date/Time functions - Parsing and formatting
            "FROM_UNIXTIME" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromUnixtime,
                args,
            }),
            "TO_UNIXTIME" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToUnixtime,
                args,
            }),
            "FROM_ISO8601_TIMESTAMP" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromIso8601Timestamp,
                args,
            }),
            "FROM_ISO8601_DATE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromIso8601Date,
                args,
            }),
            "TO_ISO8601" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToIso8601,
                args,
            }),
            "DATE_FORMAT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DateFormat,
                args,
            }),
            "DATE_PARSE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::DateParse,
                args,
            }),
            "PARSE_DATETIME" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ParseDatetime,
                args,
            }),
            "PARSE_DURATION" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ParseDuration,
                args,
            }),
            "HUMAN_READABLE_SECONDS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::HumanReadableSeconds,
                args,
            }),
            // New Date/Time functions - Timezone
            "AT_TIMEZONE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::AtTimezone,
                args,
            }),
            "WITH_TIMEZONE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::WithTimezone,
                args,
            }),
            "TIMEZONE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Timezone,
                args,
            }),
            // New Type conversion
            "TRY_CAST" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::TryCast,
                args,
            }),
            // New Conditional functions
            "TRY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Try,
                args,
            }),
            // New Formatting functions
            "FORMAT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Format,
                args,
            }),
            "FORMAT_NUMBER" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FormatNumber,
                args,
            }),
            "PARSE_DATA_SIZE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ParseDataSize,
                args,
            }),
            // New Regex functions
            "REGEXP_EXTRACT_ALL" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::RegexpExtractAll,
                args,
            }),
            "REGEXP_POSITION" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::RegexpPosition,
                args,
            }),
            // New Binary/Encoding functions - Base64
            "TO_BASE64URL" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToBase64Url,
                args,
            }),
            "FROM_BASE64URL" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromBase64Url,
                args,
            }),
            // New Binary/Encoding functions - Base32
            "TO_BASE32" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToBase32,
                args,
            }),
            "FROM_BASE32" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromBase32,
                args,
            }),
            // New Binary/Encoding functions - Hash
            "CRC32" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Crc32,
                args,
            }),
            "XXHASH64" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Xxhash64,
                args,
            }),
            "MURMUR3" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Murmur3,
                args,
            }),
            "SPOOKY_HASH_V2_32" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::SpookyHashV2_32,
                args,
            }),
            "SPOOKY_HASH_V2_64" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::SpookyHashV2_64,
                args,
            }),
            // New Binary/Encoding functions - HMAC
            "HMAC_MD5" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::HmacMd5,
                args,
            }),
            "HMAC_SHA1" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::HmacSha1,
                args,
            }),
            "HMAC_SHA256" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::HmacSha256,
                args,
            }),
            "HMAC_SHA512" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::HmacSha512,
                args,
            }),
            // New Binary/Encoding functions - Endian conversion
            "FROM_BIG_ENDIAN_32" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromBigEndian32,
                args,
            }),
            "TO_BIG_ENDIAN_32" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToBigEndian32,
                args,
            }),
            "FROM_BIG_ENDIAN_64" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromBigEndian64,
                args,
            }),
            "TO_BIG_ENDIAN_64" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToBigEndian64,
                args,
            }),
            // New Binary/Encoding functions - IEEE754
            "FROM_IEEE754_32" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromIeee754_32,
                args,
            }),
            "TO_IEEE754_32" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToIeee754_32,
                args,
            }),
            "FROM_IEEE754_64" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::FromIeee754_64,
                args,
            }),
            "TO_IEEE754_64" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ToIeee754_64,
                args,
            }),
            // New Bitwise functions
            "BITWISE_LEFT_SHIFT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BitwiseLeftShift,
                args,
            }),
            "BITWISE_RIGHT_SHIFT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BitwiseRightShift,
                args,
            }),
            "BITWISE_RIGHT_SHIFT_ARITHMETIC" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::BitwiseRightShiftArithmetic,
                args,
            }),
            // New URL functions
            "URL_EXTRACT_FRAGMENT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlExtractFragment,
                args,
            }),
            "URL_EXTRACT_PARAMETER" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::UrlExtractParameter,
                args,
            }),
            // JSON functions
            "JSON_EXTRACT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonExtract,
                args,
            }),
            "JSON_EXTRACT_SCALAR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonExtractScalar,
                args,
            }),
            "JSON_SIZE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonSize,
                args,
            }),
            "JSON_ARRAY_LENGTH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonArrayLength,
                args,
            }),
            "JSON_ARRAY_GET" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonArrayGet,
                args,
            }),
            "JSON_ARRAY_CONTAINS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonArrayContains,
                args,
            }),
            "IS_JSON_SCALAR" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::IsJsonScalar,
                args,
            }),
            "JSON_FORMAT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonFormat,
                args,
            }),
            "JSON_PARSE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonParse,
                args,
            }),
            "JSON_QUERY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonQuery,
                args,
            }),
            "JSON_VALUE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonValue,
                args,
            }),
            "JSON_EXISTS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonExists,
                args,
            }),
            "JSON_OBJECT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonObject,
                args,
            }),
            "JSON_ARRAY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::JsonArray,
                args,
            }),
            // Array functions
            "CARDINALITY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Cardinality,
                args,
            }),
            "ARRAY_LENGTH" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayLength,
                args,
            }),
            "ELEMENT_AT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ElementAt,
                args,
            }),
            "ARRAY_CONTAINS" | "CONTAINS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayContains,
                args,
            }),
            "ARRAY_POSITION" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayPosition,
                args,
            }),
            "ARRAY_DISTINCT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayDistinct,
                args,
            }),
            "ARRAY_INTERSECT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayIntersect,
                args,
            }),
            "ARRAY_UNION" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayUnion,
                args,
            }),
            "ARRAY_EXCEPT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayExcept,
                args,
            }),
            "ARRAY_JOIN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayJoin,
                args,
            }),
            "ARRAY_MAX" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayMax,
                args,
            }),
            "ARRAY_MIN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayMin,
                args,
            }),
            "ARRAY_REMOVE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayRemove,
                args,
            }),
            "ARRAY_SORT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArraySort,
                args,
            }),
            "ARRAYS_OVERLAP" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArraysOverlap,
                args,
            }),
            "ARRAY_CONCAT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayConcat,
                args,
            }),
            "FLATTEN" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Flatten,
                args,
            }),
            "ARRAY_REVERSE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayReverse,
                args,
            }),
            "SEQUENCE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Sequence,
                args,
            }),
            "SHUFFLE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Shuffle,
                args,
            }),
            "SLICE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Slice,
                args,
            }),
            "TRIM_ARRAY" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::TrimArray,
                args,
            }),
            "ARRAY_REPEAT" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayRepeat,
                args,
            }),
            "NGRAMS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Ngrams,
                args,
            }),
            "COMBINATIONS" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Combinations,
                args,
            }),
            "ARRAY_FIRST" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayFirst,
                args,
            }),
            "ARRAY_LAST" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ArrayLast,
                args,
            }),
            "CONTAINS_SEQUENCE" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::ContainsSequence,
                args,
            }),
            "ZIP" => Ok(Expr::ScalarFunc {
                func: ScalarFunction::Zip,
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
            ast::DataType::Float(_) | ast::DataType::Double(_) | ast::DataType::DoublePrecision => {
                Ok(ArrowDataType::Float64)
            }
            ast::DataType::Decimal(info) | ast::DataType::Numeric(info) => match info {
                ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                    Ok(ArrowDataType::Decimal128(*p as u8, *s as i8))
                }
                ast::ExactNumberInfo::Precision(p) => Ok(ArrowDataType::Decimal128(*p as u8, 0)),
                ast::ExactNumberInfo::None => Ok(ArrowDataType::Decimal128(38, 10)),
            },
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
            SqlExpr::Value(ast::ValueWithSpan {
                value: ast::Value::Number(n, _),
                ..
            }) => n
                .parse::<usize>()
                .map_err(|_| QueryError::Parse(format!("Cannot parse as usize: {}", n))),
            _ => Err(QueryError::Parse(format!(
                "Expected numeric literal, got: {:?}",
                expr
            ))),
        }
    }
}

/// Which of a join's two inputs an ON-clause expression belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

/// Resolve which input a join key expression belongs to. Returns `None` when
/// the answer is not unambiguous — the expression is not a bare column, or the
/// name resolves in both inputs (a self join) or in neither.
fn join_key_side(expr: &Expr, left: &PlanSchema, right: &PlanSchema) -> Option<JoinSide> {
    let col = match expr {
        Expr::Column(c) => c,
        _ => return None,
    };
    match (
        left.resolve_column(col).is_some(),
        right.resolve_column(col).is_some(),
    ) {
        (true, false) => Some(JoinSide::Left),
        (false, true) => Some(JoinSide::Right),
        _ => None,
    }
}

/// Orient every ON equi-pair so that `.0` resolves against the join's LEFT
/// input and `.1` against its RIGHT input.
///
/// SQL imposes no order on an equality, so `ON c_custkey = o_custkey` and
/// `ON o_custkey = c_custkey` are the same query. Everything downstream
/// assumes the left-then-right order, though: the physical planner unzips the
/// pairs into build/probe key lists positionally, so a reversed pair made the
/// join evaluate one input's key expression against the other input's batches
/// and fail at runtime with "Column not found". Inner joins were re-oriented
/// as a side effect of JoinReorder, but only when the query actually triggered
/// reordering, and outer joins were never re-oriented at all.
///
/// A pair whose two sides resolve to the SAME input is not an equi-key: it
/// constrains one input on its own and belongs in the join filter. Pairs whose
/// sides cannot be classified (ambiguous names in a self join, non-column
/// expressions) are left exactly as written.
fn normalize_join_on(
    on: Vec<(Expr, Expr)>,
    filter: Option<Expr>,
    left: &PlanSchema,
    right: &PlanSchema,
) -> (Vec<(Expr, Expr)>, Option<Expr>) {
    let mut normalized = Vec::with_capacity(on.len());
    let mut demoted: Vec<Expr> = Vec::new();

    for (l, r) in on {
        match (
            join_key_side(&l, left, right),
            join_key_side(&r, left, right),
        ) {
            (Some(JoinSide::Left), Some(JoinSide::Right)) => normalized.push((l, r)),
            (Some(JoinSide::Right), Some(JoinSide::Left)) => normalized.push((r, l)),
            (Some(a), Some(b)) if a == b => demoted.push(l.eq(r)),
            _ => normalized.push((l, r)),
        }
    }

    let filter = demoted.into_iter().fold(filter, |acc, e| match acc {
        Some(f) => Some(f.and(e)),
        None => Some(e),
    });
    (normalized, filter)
}

/// Collect every column reference inside an expression.
fn collect_expr_columns(e: &Expr, out: &mut Vec<Column>) {
    match e {
        Expr::Column(c) => out.push(c.clone()),
        Expr::BinaryExpr { left, right, .. } => {
            collect_expr_columns(left, out);
            collect_expr_columns(right, out);
        }
        Expr::UnaryExpr { expr, .. } | Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => {
            collect_expr_columns(expr, out)
        }
        Expr::ScalarFunc { args, .. } | Expr::Aggregate { args, .. } => {
            for a in args {
                collect_expr_columns(a, out);
            }
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(o) = operand {
                collect_expr_columns(o, out);
            }
            for (w, t) in when_then {
                collect_expr_columns(w, out);
                collect_expr_columns(t, out);
            }
            if let Some(e) = else_expr {
                collect_expr_columns(e, out);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_columns(expr, out);
            for l in list {
                collect_expr_columns(l, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_columns(expr, out);
            collect_expr_columns(low, out);
            collect_expr_columns(high, out);
        }
        _ => {}
    }
}

/// Make every column an ORDER BY needs visible to the Sort operator.
///
/// `SELECT id FROM t ORDER BY price` is valid SQL, but the plan is
/// `Sort(Project([id], Scan))` — the Sort's input has no `price` column and
/// execution failed with `Column not found: price`. (This bit every query of
/// that shape, not just vector searches.)
///
/// The fix is the textbook one: widen the projection under the Sort with the
/// missing columns, and hand the caller the ORIGINAL output schema so it can
/// trim them back off once sorting (and LIMIT) are done.
///
/// Returns `(possibly-widened plan, Some(original schema) if widened)`.
///
/// Deliberately conservative. Widening happens only when:
///   * the node under the Sort is a plain `Project`, and
///   * every missing column resolves unambiguously in that Project's input, and
///   * the Project's input is not an `Aggregate` — after grouping, a column
///     that was projected away genuinely no longer exists, and inventing it
///     would answer a different question.
/// Anything else is left exactly as it was.
fn extend_projection_for_sort(
    plan: LogicalPlan,
    order_by: &[SortExpr],
) -> Result<(LogicalPlan, Option<PlanSchema>)> {
    let out_schema = plan.schema();

    // Which columns does the sort need that the current output lacks?
    let mut missing: Vec<Column> = Vec::new();
    for sort in order_by {
        // A correlated subquery in ORDER BY resolves columns by rules this
        // widening does not model; leave those plans untouched.
        if sort.expr.contains_subquery() {
            return Ok((plan, None));
        }
        let mut cols = Vec::new();
        collect_expr_columns(&sort.expr, &mut cols);
        for col in cols {
            if out_schema.resolve_column(&col).is_none() && !missing.contains(&col) {
                missing.push(col);
            }
        }
    }
    if missing.is_empty() {
        return Ok((plan, None));
    }

    let LogicalPlan::Project(project) = plan else {
        return Ok((plan, None));
    };
    if matches!(project.input.as_ref(), LogicalPlan::Aggregate(_)) {
        return Ok((LogicalPlan::Project(project), None));
    }

    let input_schema = project.input.schema();
    let mut extra_exprs = Vec::new();
    let mut extra_fields = Vec::new();
    for col in &missing {
        let Some((_, field)) = input_schema.resolve_column(col) else {
            // Not resolvable below either: leave the plan alone so the existing
            // error path reports it rather than silently dropping the sort key.
            return Ok((LogicalPlan::Project(project), None));
        };
        // `ORDER BY <nested column not in the SELECT list>` is the one shape
        extra_exprs.push(Expr::Column(col.clone()));
        extra_fields.push(field.clone());
    }

    // `ORDER BY <nested column not in the SELECT list>` is the one shape that
    // escapes the guard in `bind_order_by`: there the sort key does not resolve
    // against the *projected* schema, so `require_scalar` cannot see its type
    // and silently passes. The input schema resolves it, so this is the first
    // place the type is visible. Without this, `SELECT id FROM t ORDER BY
    // struct_col` sorts by a value with no ordering and returns an arbitrary
    // row order that looks like a real answer.
    //
    // The check is on the sort key EXPRESSION, not on the columns it mentions:
    // `ORDER BY cosine_distance(embedding, [...])` names a vector column but
    // produces a float, and is the single most important query this engine
    // serves. Only the expression's result type decides.
    for sort in order_by {
        crate::planner::vector_types::require_scalar(&sort.expr, &input_schema, "ORDER BY")?;
    }

    let original_schema = project.schema.clone();
    let mut exprs = project.exprs;
    exprs.extend(extra_exprs);
    let mut fields = project.schema.fields().to_vec();
    fields.extend(extra_fields);

    Ok((
        LogicalPlan::Project(ProjectNode {
            input: project.input,
            exprs,
            schema: PlanSchema::new(fields),
        }),
        Some(original_schema),
    ))
}

/// Reduce a bound expression to a constant, if it is one.
///
/// `bind_expr` turns `-0.5` into `Negate(Literal(0.5))` rather than a negative
/// literal, so array literals — which are overwhelmingly written with negative
/// components — must undo that here.
fn const_scalar(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(v) => Some(v.clone()),
        Expr::UnaryExpr {
            op: UnaryOp::Negate,
            expr,
        } => match const_scalar(expr)? {
            ScalarValue::Int8(v) => Some(ScalarValue::Int8(-v)),
            ScalarValue::Int16(v) => Some(ScalarValue::Int16(-v)),
            ScalarValue::Int32(v) => Some(ScalarValue::Int32(-v)),
            ScalarValue::Int64(v) => Some(ScalarValue::Int64(-v)),
            ScalarValue::Float32(v) => Some(ScalarValue::Float32(-v)),
            ScalarValue::Float64(v) => Some(ScalarValue::Float64(-v)),
            ScalarValue::Decimal128(v) => Some(ScalarValue::Decimal128(-v)),
            _ => None,
        },
        _ => None,
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

        let plan = binder
            .bind_sql("SELECT o_orderkey, o_totalprice FROM orders")
            .unwrap();
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

        // `o_totalprice` is not in the SELECT list, so the projection under the
        // Sort is widened to carry it and a trimming Project is added on top.
        fn has_sort(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Sort(_) => true,
                LogicalPlan::Limit(l) => has_sort(&l.input),
                LogicalPlan::Project(p) => has_sort(&p.input),
                _ => false,
            }
        }

        assert!(has_sort(&plan));
        // The user asked for one column and must get exactly one column back.
        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().fields()[0].name, "o_orderkey");
    }

    #[test]
    fn test_order_by_non_projected_column_is_visible_to_sort() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let plan = binder
            .bind_sql("SELECT o_orderkey FROM orders ORDER BY o_totalprice")
            .unwrap();

        // Find the Sort and check its input actually exposes the sort key.
        fn find_sort(plan: &LogicalPlan) -> Option<&SortNode> {
            match plan {
                LogicalPlan::Sort(s) => Some(s),
                LogicalPlan::Limit(l) => find_sort(&l.input),
                LogicalPlan::Project(p) => find_sort(&p.input),
                _ => None,
            }
        }
        let sort = find_sort(&plan).expect("sort node");
        let input_schema = sort.input.schema();
        assert!(
            input_schema
                .resolve_column(&Column::new("o_totalprice"))
                .is_some(),
            "sort input must expose the sort key, got {:?}",
            input_schema
        );
    }

    #[test]
    fn test_order_by_projected_column_is_not_widened() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let plan = binder
            .bind_sql("SELECT o_orderkey FROM orders ORDER BY o_orderkey")
            .unwrap();
        // No widening needed, so no trimming projection on top of the Sort.
        assert!(matches!(plan, LogicalPlan::Sort(_)), "got {:?}", plan);
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

    // ---------- INSERT (native-tables-mutation epic, task 002) ----------

    #[test]
    fn insert_target_name_extracts_the_table_name_without_validating_shape() {
        let stmt = parser::parse_sql("INSERT INTO orders SELECT * FROM orders").unwrap();
        assert_eq!(insert_target_name(&stmt).as_deref(), Some("orders"));

        // Extraction succeeds even for a shape `bind()` will later refuse
        // (mirrors `create_table_target_name`'s own split).
        let overwrite_stmt =
            parser::parse_sql("INSERT OVERWRITE TABLE orders SELECT * FROM orders").unwrap();
        assert_eq!(
            insert_target_name(&overwrite_stmt).as_deref(),
            Some("orders")
        );

        let select_stmt = parser::parse_sql("SELECT * FROM orders").unwrap();
        assert_eq!(insert_target_name(&select_stmt), None);
    }

    #[test]
    fn bind_insert_select_binds_only_the_source_query() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let stmt = parser::parse_sql("INSERT INTO orders SELECT o_orderkey FROM orders").unwrap();
        let plan = binder.bind(&stmt).unwrap();
        // Same shape a plain `SELECT o_orderkey FROM orders` produces --
        // `LogicalPlan` has no DML node, exactly like `CreateTable`.
        assert!(matches!(plan, LogicalPlan::Project(_)), "got {:?}", plan);
    }

    #[test]
    fn bind_insert_values_binds_through_the_same_path_as_select() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        // `INSERT ... VALUES` needs zero extra binder work per task 001's
        // finding: `SetExpr::Values` already has its own arm.
        let stmt =
            parser::parse_sql("INSERT INTO orders VALUES (1, 100, 'O', 42.5, DATE '2024-01-01')")
                .unwrap();
        let plan = binder.bind(&stmt).unwrap();
        assert!(matches!(plan, LogicalPlan::Values(_)), "got {:?}", plan);
    }

    #[test]
    fn bind_insert_with_join_and_group_by_source_works() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);

        let stmt = parser::parse_sql(
            "INSERT INTO orders SELECT o.o_orderkey, COUNT(*) FROM orders o \
             JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o.o_orderkey",
        )
        .unwrap();
        // Must bind successfully -- the same `bind_query()` CTAS already
        // relies on for arbitrary joins/aggregates.
        binder.bind(&stmt).unwrap();
    }

    #[test]
    fn bind_insert_refuses_an_explicit_column_list() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt =
            parser::parse_sql("INSERT INTO orders (o_orderkey) SELECT o_orderkey FROM orders")
                .unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("column list"), "{err}");
    }

    #[test]
    fn bind_insert_refuses_hive_overwrite() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("INSERT OVERWRITE TABLE orders SELECT o_orderkey FROM orders")
            .unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("OVERWRITE"), "{err}");
    }

    #[test]
    fn bind_insert_refuses_on_conflict() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql(
            "INSERT INTO orders SELECT o_orderkey FROM orders ON CONFLICT DO NOTHING",
        )
        .unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("CONFLICT"), "{err}");
    }

    // Note: MySQL's `INSERT INTO t SET a = 1` shape (Insert.assignments)
    // does NOT parse under GenericDialect at all ("Expected: SELECT,
    // VALUES, or a subquery") -- confirmed the same way task 001's spike
    // found MySQL's multi-table DELETE form doesn't parse either. The
    // `assignments` check in `require_supported_insert_shape` stays as
    // defensive code (in case a future dialect populates it) but is not
    // independently testable through real, GenericDialect-parseable SQL
    // text, matching that established precedent.

    #[test]
    fn bind_insert_without_a_source_is_refused() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("INSERT INTO orders DEFAULT VALUES").unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("source"), "{err}");
    }

    // ---------- DELETE (native-tables-mutation epic, task 003) ----------

    #[test]
    fn delete_target_name_extracts_the_table_name_without_validating_shape() {
        let stmt = parser::parse_sql("DELETE FROM orders WHERE o_orderkey = 1").unwrap();
        assert_eq!(delete_target_name(&stmt).as_deref(), Some("orders"));

        let no_where = parser::parse_sql("DELETE FROM orders").unwrap();
        assert_eq!(delete_target_name(&no_where).as_deref(), Some("orders"));

        // Extraction succeeds even for a shape `bind()` will later refuse
        // (mirrors `insert_target_name`'s own split).
        let joined = parser::parse_sql(
            "DELETE FROM orders JOIN customer ON orders.o_custkey = customer.c_custkey",
        )
        .unwrap();
        assert_eq!(delete_target_name(&joined).as_deref(), Some("orders"));

        let select_stmt = parser::parse_sql("SELECT * FROM orders").unwrap();
        assert_eq!(delete_target_name(&select_stmt), None);
    }

    #[test]
    fn bind_delete_with_where_binds_the_predicate() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("DELETE FROM orders WHERE o_orderkey = 1").unwrap();
        let Statement::Delete(delete) = &stmt else {
            panic!("expected a Delete statement");
        };
        let (table_name, predicate) = binder.bind_delete(delete).unwrap();
        assert_eq!(table_name, "orders");
        assert!(
            matches!(predicate, Some(Expr::BinaryExpr { .. })),
            "{predicate:?}"
        );
    }

    #[test]
    fn bind_delete_without_where_binds_no_predicate_meaning_delete_all_rows() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("DELETE FROM orders").unwrap();
        let Statement::Delete(delete) = &stmt else {
            panic!("expected a Delete statement");
        };
        let (table_name, predicate) = binder.bind_delete(delete).unwrap();
        assert_eq!(table_name, "orders");
        assert!(
            predicate.is_none(),
            "no WHERE clause must bind to None (match every row), not an error or a trivial \
             TRUE literal"
        );
    }

    #[test]
    fn bind_delete_with_a_table_alias_binds_a_qualified_where_column() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("DELETE FROM orders o WHERE o.o_orderkey = 1").unwrap();
        let Statement::Delete(delete) = &stmt else {
            panic!("expected a Delete statement");
        };
        let (table_name, predicate) = binder.bind_delete(delete).unwrap();
        assert_eq!(table_name, "orders");
        assert!(predicate.is_some());
    }

    #[test]
    fn bind_delete_against_an_unregistered_table_is_table_not_found() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("DELETE FROM nope WHERE x = 1").unwrap();
        let Statement::Delete(delete) = &stmt else {
            panic!("expected a Delete statement");
        };
        let err = binder.bind_delete(delete).unwrap_err();
        assert!(matches!(err, QueryError::TableNotFound(_)), "{err:?}");
    }

    #[test]
    fn bind_delete_statement_arm_points_at_the_real_entrypoint() {
        // `Binder::bind()` itself cannot express "identify and remove
        // matched rows" as a `LogicalPlan` -- it validates shape then
        // refuses, naming the real entrypoint.
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("DELETE FROM orders WHERE o_orderkey = 1").unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(
            err.to_string().contains("delete_from_native_table"),
            "{err}"
        );
    }

    #[test]
    fn bind_delete_refuses_a_multi_table_from_list() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("DELETE FROM orders, customer WHERE o_orderkey = 1").unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("more than one"), "{err}");
    }

    #[test]
    fn bind_delete_refuses_a_join_in_the_target() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql(
            "DELETE FROM orders JOIN customer ON orders.o_custkey = customer.c_custkey",
        )
        .unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("JOIN"), "{err}");
    }

    #[test]
    fn bind_delete_refuses_using() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql(
            "DELETE FROM orders USING customer WHERE orders.o_custkey = customer.c_custkey",
        )
        .unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("USING"), "{err}");
    }

    #[test]
    fn bind_delete_refuses_returning() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt =
            parser::parse_sql("DELETE FROM orders WHERE o_orderkey = 1 RETURNING o_orderkey")
                .unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("RETURNING"), "{err}");
    }

    #[test]
    fn bind_delete_refuses_order_by_and_limit() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql("DELETE FROM orders ORDER BY o_orderkey LIMIT 5").unwrap();
        let err = binder.bind(&stmt).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        // Whichever check fires first (ORDER BY is checked before LIMIT).
        assert!(err.to_string().contains("ORDER BY"), "{err}");
    }

    #[test]
    fn bind_delete_refuses_a_subquery_in_where_even_though_it_parses_and_binds() {
        let catalog = create_test_catalog();
        let mut binder = Binder::new(&catalog);
        let stmt = parser::parse_sql(
            "DELETE FROM orders WHERE o_custkey IN (SELECT c_custkey FROM customer)",
        )
        .unwrap();
        let Statement::Delete(delete) = &stmt else {
            panic!("expected a Delete statement");
        };
        let err = binder.bind_delete(delete).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(err.to_string().contains("subquery"), "{err}");
    }

    // Note: MySQL's `DELETE t1, t2 FROM t1, t2 WHERE ...` multi-table form
    // (`Delete.tables`, the field BEFORE `FROM`) does NOT parse under
    // GenericDialect at all -- confirmed directly from sqlparser 0.62's
    // `Parser::parse_delete` source: the `tables` list is only ever
    // populated for `BigQueryDialect`/`OracleDialect`/`GenericDialect`
    // when NO leading table list is present, i.e. `tables` is
    // UNCONDITIONALLY `vec![]` for this engine's dialect. The
    // `!delete.tables.is_empty()` check in `require_supported_delete_shape`
    // stays as defensive code (in case a future dialect populates it) but
    // is not independently testable through real, GenericDialect-parseable
    // SQL text -- the identical situation `insert.assignments` is already
    // documented as, above.
}
