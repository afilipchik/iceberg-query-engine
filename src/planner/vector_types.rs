//! Vector (embedding) column typing rules.
//!
//! Lance stores embeddings as `FixedSizeList<Float32, N>`. The engine carries
//! that type end-to-end as an **opaque value**: it can be scanned, projected,
//! aliased, LIMITed, and passed to the vector-distance functions. It cannot be
//! summed, grouped by, ordered by, or compared with `=` / `<`.
//!
//! # Why opaque rather than "make it work"
//!
//! A 384-float embedding has no total order and no meaningful sum. Arrow will
//! happily *store* one, and several of this engine's operators would happily
//! hash the bytes or fall through to a `{:?}` string, silently answering a
//! question the user did not ask. The rule here is the same one the rest of the
//! codebase follows: fail loudly, naming the column, rather than coerce.
//!
//! Every rejection produced here names the offending column so the error tells
//! the user *which* column of a wide `SELECT *` is the problem.

use crate::error::{QueryError, Result};
use crate::planner::logical_expr::Expr;
use crate::planner::schema::PlanSchema;
use arrow::datatypes::DataType;

/// Distance metric for an index-backed k-NN search.
///
/// Lives with the logical types rather than with the kernels because the
/// optimizer decides the metric (from which SQL function was written) and the
/// storage layer merely obeys it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    /// Euclidean.
    L2,
    /// `1 - cosine similarity`.
    Cosine,
    /// Inner product (Lance stores it internally as `1 - dot`).
    Dot,
}

impl VectorMetric {
    pub fn as_str(&self) -> &'static str {
        match self {
            VectorMetric::L2 => "l2",
            VectorMetric::Cosine => "cosine",
            VectorMetric::Dot => "dot",
        }
    }
}

impl std::fmt::Display for VectorMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// True for the nested types the engine carries opaquely.
///
/// Vectors, lists, structs and maps all share one property that matters to the
/// planner: they have no scalar value, therefore no ordering, no sum and no
/// hashable equality the user would recognize. Every one of them must be
/// refused wherever the SQL demands a scalar, and every one of them is fine to
/// select, project, alias and LIMIT.
pub fn is_opaque_nested(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::FixedSizeList(_, _)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    )
}

/// If `dt` is a float vector column, return `(element type, dimension)`.
///
/// Only `FixedSizeList` qualifies: a variable-length `List` has no fixed
/// dimension, so distance against a fixed query vector is not well defined.
pub fn as_float_vector(dt: &DataType) -> Option<(&DataType, usize)> {
    match dt {
        DataType::FixedSizeList(field, width) => match field.data_type() {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                Some((field.data_type(), *width as usize))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Human-readable name for the column(s) inside `expr` whose type is nested.
///
/// Returns `None` when the expression's own type is scalar, i.e. the expression
/// is legal wherever a scalar is required.
fn nested_operand_name(expr: &Expr, schema: &PlanSchema) -> Option<(String, DataType)> {
    // The expression's *result* type is what matters: `cosine_distance(v, [..])`
    // contains a vector column but produces a float, and is perfectly legal.
    let dt = expr.data_type(schema).ok()?;
    if !is_opaque_nested(&dt) {
        return None;
    }
    // Name the deepest column reference we can find, for a useful message.
    let name = first_column_name(expr).unwrap_or_else(|| expr.output_name());
    Some((name, dt))
}

fn first_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(c.qualified_name()),
        Expr::Alias { expr, .. } => first_column_name(expr),
        Expr::Cast { expr, .. } => first_column_name(expr),
        _ => None,
    }
}

/// Short, user-facing rendering of a nested type.
///
/// Arrow's `Debug` for a struct is ~80 characters of `Field { name: .., dict_id:
/// 0, .. }` per field, which buries the answer in an error message. This prints
/// what the user wrote in their schema.
pub fn describe_type(dt: &DataType) -> String {
    describe(dt)
}

fn describe(dt: &DataType) -> String {
    match dt {
        DataType::FixedSizeList(f, w) => format!("vector({} x {})", w, f.data_type()),
        DataType::List(f) | DataType::LargeList(f) => format!("array of {}", f.data_type()),
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        DataType::Map(_, _) => "map".to_string(),
        other => format!("{:?}", other),
    }
}

/// What the user can actually do with a value of this type, appended to the
/// rejection so the error is a next step rather than a dead end.
fn remedy(dt: &DataType) -> &'static str {
    match dt {
        DataType::FixedSizeList(_, _) => {
            "Vector columns can be selected and passed to l2_distance / \
             cosine_distance / dot_product, but they have no ordering and no \
             sum. Use a distance function to reduce it to a number first."
        }
        DataType::Struct(_) | DataType::Map(_, _) => {
            "Struct and map columns are carried as opaque values: they can be \
             selected, projected and aliased, but the engine cannot order, \
             group or aggregate them, and field access (`col.field`) is not \
             implemented. Project the scalar columns you need instead."
        }
        _ => {
            "Array columns are carried as opaque values: they can be selected, \
             projected and aliased, but they have no ordering and no sum."
        }
    }
}

/// Reject an expression whose value is nested where a scalar is required.
///
/// `context` names the SQL construct ("GROUP BY", "ORDER BY", "SUM", "=") and
/// goes into the message verbatim.
pub fn require_scalar(expr: &Expr, schema: &PlanSchema, context: &str) -> Result<()> {
    if let Some((name, dt)) = nested_operand_name(expr, schema) {
        return Err(QueryError::Type(format!(
            "{} is not supported on column `{}` of type {}. {}",
            context,
            name,
            describe(&dt),
            remedy(&dt)
        )));
    }
    Ok(())
}

/// Reject a plan whose *entire output row* is used as a grouping key when one
/// of its columns is nested.
///
/// `DISTINCT` and set-operation `UNION`/`INTERSECT`/`EXCEPT` are planned as
/// "group by every output column", which never passes through the expression
/// guards above — the columns are implicit, so there is no `Expr` to check.
/// Without this, a struct column reaches the hash aggregate's group-key
/// extractor, which returns a null key for a type it does not know: *every row
/// collapses into one group* before the output builder eventually errors with a
/// message that names a type but not a column. Refuse up front instead, naming
/// the column and the construct.
pub fn require_scalar_row(schema: &PlanSchema, context: &str) -> Result<()> {
    for field in schema.fields() {
        if is_opaque_nested(&field.data_type) {
            return Err(QueryError::Type(format!(
                "{} is not supported on column `{}` of type {}. {}",
                context,
                field.name,
                describe(&field.data_type),
                remedy(&field.data_type)
            )));
        }
    }
    Ok(())
}

/// Reject vector operands of a comparison/arithmetic binary operator.
pub fn require_scalar_operands(
    left: &Expr,
    right: &Expr,
    schema: &PlanSchema,
    op: &str,
) -> Result<()> {
    require_scalar(left, schema, &format!("operator `{}`", op))?;
    require_scalar(right, schema, &format!("operator `{}`", op))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::schema::SchemaField;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn vec_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("id", DataType::Int64),
            SchemaField::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            ),
        ])
    }

    #[test]
    fn vector_type_is_recognized() {
        let dt =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 384);
        assert_eq!(as_float_vector(&dt).map(|(_, w)| w), Some(384));
        assert!(is_opaque_nested(&dt));
        assert!(as_float_vector(&DataType::Float32).is_none());
        // Ints are not a float vector: distance is undefined for them here.
        let ints = DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 8);
        assert!(as_float_vector(&ints).is_none());
        assert!(is_opaque_nested(&ints));
    }

    #[test]
    fn scalar_columns_pass_and_vectors_fail_by_name() {
        let s = vec_schema();
        require_scalar(&Expr::column("id"), &s, "GROUP BY").unwrap();
        let err = require_scalar(&Expr::column("embedding"), &s, "GROUP BY").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("embedding"),
            "message must name column: {}",
            msg
        );
        assert!(msg.contains("GROUP BY"), "{}", msg);
    }
}
