//! Routing and updating must use the same evaluated grouping keys.
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use query_engine::{
    execution::MemoryPool,
    physical::{
        operators::{spillable, MemoryTableExec},
        PhysicalOperator,
    },
    planner::{AggregateFunction, CastMode, Expr, ScalarFunction, ScalarValue},
    ExecutionConfig,
};
use std::{collections::HashSet, sync::Arc};

async fn check(disjoint: bool) {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1; 16384]))],
    )
    .unwrap();
    let input = Arc::new(MemoryTableExec::new("input", schema, vec![batch], None));
    // The oracle never assumes a random sequence or particular group counts.
    // Every possible draw must still produce unique groups and conserve rows.
    let key = Expr::Cast {
        expr: Box::new(Expr::ScalarFunc {
            func: ScalarFunction::Floor,
            args: vec![Expr::ScalarFunc {
                func: ScalarFunction::Random,
                args: vec![],
            }
            .multiply(Expr::literal(ScalarValue::Int64(16)))],
        }),
        data_type: DataType::Int64,
        mode: CastMode::Strict,
    };
    let pool = Arc::new(MemoryPool::new_named("evaluated keys", 64 << 20));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let operator = spillable::SpillableHashAggregateExec::new(
        input,
        vec![key],
        vec![spillable::AggregateExpr {
            func: AggregateFunction::Count,
            input: Expr::column("v"),
            distinct: false,
            second_arg: None,
        }],
        output_schema,
        pool.clone(),
        ExecutionConfig::new().with_memory_limit(64 << 20),
    )
    .with_disjoint_groups(disjoint);
    let output: Vec<RecordBatch> = operator
        .execute(0)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut groups = HashSet::new();
    let mut rows = 0;
    for batch in &output {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(!keys.is_null(i));
            assert!((0..16).contains(&keys.value(i)));
            assert!(
                groups.insert(keys.value(i)),
                "routing and update split one SQL group"
            );
            rows += counts.value(i);
        }
    }
    assert_eq!(rows, 16384);
    drop(output);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn disjoint_routing_preserves_evaluated_volatile_keys() {
    check(true).await;
}

#[tokio::test]
async fn shared_routing_preserves_volatile_group_invariants() {
    check(false).await;
}
