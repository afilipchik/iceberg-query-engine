# Table Alias Schema Fix

## Date
2025-01-23

## Bug Description

When the same table was referenced multiple times with different aliases (e.g., `users u` and `users u2`), column resolution failed during execution with "Column not found: u2.id".

## Root Cause

The `MemoryTableExec::from_provider_with_schema()` method was using the logical schema (with qualified column names like `u2.id`) for its `schema` field, but the RecordBatches returned from the provider had unqualified names (like `id`).

During expression evaluation, `find_column_index()` looked for qualified names in the batch's schema, but the batches had unqualified schema fields.

## Fix

Recreate the RecordBatches with the correct (qualified) schema:

```rust
// BEFORE: Batches retain provider's (unqualified) schema
let batches = provider.scan(projection)?;

// AFTER: Batches recreated with logical (qualified) schema
let provider_batches = provider.scan(projection)?;
let output_schema = /* logical schema with qualified names */;

let batches: Result<Vec<RecordBatch>> = provider_batches
    .iter()
    .map(|batch| {
        RecordBatch::try_new(output_schema.clone(), batch.columns().to_vec())
            .map_err(Into::into)
    })
    .collect();
```

## Key Insight

**Arrow RecordBatches don't inherently "own" their schema** - the schema is passed to `try_new()` and stored separately. When we pass `batch.columns()` to a new `try_new()` with a different schema, we create a NEW RecordBatch with the NEW schema but reusing the SAME column arrays.

This allows us to "re-schema" batches without copying data - we're just changing the metadata.

## Table Alias Resolution

### Query Planning (Logical)
- `bind_table_factor()` creates `ScanNode` with `table_name` as the BASE table name (e.g., "users")
- `SchemaField::with_relation()` adds the alias to each column (e.g., `u2`)
- `PlanSchema::to_arrow_field()` uses `qualified_name()` = `{relation}.{name}` (e.g., "u2.id")

### Physical Execution
- `PhysicalPlanner::Scan()` converts `PlanSchema` to `ArrowSchema` with qualified field names
- `MemoryTableExec::from_provider_with_schema()` ensures batches use this qualified schema
- During hash join evaluation, `find_column_index()` matches `Expr::Column(u2.id)` to field "u2.id"

### Critical Point
The `table_name` in `ScanNode` is the BASE table name (used to look up the TableProvider). Multiple scans of the same base table with different aliases will:
1. All use the same `table_name` ("users")
2. Each have different `schema` (qualified with different aliases)
3. Each create separate `MemoryTableExec` operators with different schemas

## DO
- **DO** recreate RecordBatches with the correct schema when the provider's schema differs from the logical schema
- **DO** use `RecordBatch::try_new(schema.clone(), columns.to_vec())` to re-schema without copying data
- **DO** preserve qualified column names through the entire execution pipeline

## DON'T
- **DON'T** assume provider schema matches logical schema when table aliases are involved
- **DON'T** use `batch.schema()` directly when the batch needs a different schema
- **DON'T** forget that `Arc<Schema>` is reference-counted - cloning is cheap

## Related Files
- `src/physical/operators/scan.rs`: `from_provider_with_schema()`
- `src/planner/schema.rs`: `SchemaField::qualified_name()`
- `src/planner/binder.rs`: `bind_table_factor()`
- `src/physical/planner.rs`: `plan_schema_to_arrow()`
