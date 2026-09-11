//! Explicit SQL extrema oracle across ordinary and fused execution.
use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use query_engine::{
    execution::MemoryPool,
    physical::{
        operators::{hash_agg, spillable, HashAggregateExec, MemoryTableExec},
        PhysicalOperator,
    },
    planner::{AggregateFunction, Expr},
    ExecutionConfig,
};
use std::{collections::HashSet, sync::Arc};

async fn check(mode: &str) {
    for split in [false, true] {
        // Opposite NaN positions, infinities, duplicate finite values, all-NULL
        // values and a NULL group. No expected answer comes from this engine.
        let keys = vec![
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(3),
            Some(3),
            None,
            None,
        ];
        let values = vec![
            Some(f64::NAN),
            Some(-7.0),
            Some(-7.0),
            Some(f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(f64::INFINITY),
            None,
            None,
            Some(3.0),
            Some(3.0),
        ];
        let batch = RecordBatch::try_from_iter(vec![
            ("g", Arc::new(Int64Array::from(keys)) as ArrayRef),
            ("v", Arc::new(Float64Array::from(values)) as ArrayRef),
        ])
        .unwrap();
        let batches = if split {
            (0..batch.num_rows()).map(|i| batch.slice(i, 1)).collect()
        } else {
            vec![batch.clone()]
        };
        let input = Arc::new(MemoryTableExec::new("input", batch.schema(), batches, None));
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("min", DataType::Float64, true),
            Field::new("max", DataType::Float64, true),
        ]));
        let pool = Arc::new(MemoryPool::new_named("float extrema", 64 << 20));
        let functions = [AggregateFunction::Min, AggregateFunction::Max];
        let operator: Box<dyn PhysicalOperator> = if mode == "ordinary" {
            Box::new(
                HashAggregateExec::new(
                    input,
                    vec![Expr::column("g")],
                    functions
                        .into_iter()
                        .map(|func| hash_agg::AggregateExpr {
                            func,
                            input: Expr::column("v"),
                            distinct: false,
                            second_arg: None,
                        })
                        .collect(),
                    schema,
                )
                .with_memory_pool(pool.clone()),
            )
        } else {
            Box::new(
                spillable::SpillableHashAggregateExec::new(
                    input,
                    vec![Expr::column("g")],
                    functions
                        .into_iter()
                        .map(|func| spillable::AggregateExpr {
                            func,
                            input: Expr::column("v"),
                            distinct: false,
                            second_arg: None,
                        })
                        .collect(),
                    schema,
                    pool.clone(),
                    ExecutionConfig::new().with_memory_limit(64 << 20),
                )
                .with_disjoint_groups(mode == "disjoint"),
            )
        };
        let output: Vec<RecordBatch> = operator
            .execute(0)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut seen = HashSet::new();
        for batch in &output {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let key = (!keys.is_null(row)).then(|| keys.value(row));
                assert!(seen.insert(key), "duplicate group {key:?}");
                let expected = match key {
                    Some(0 | 1) => [Some(-7.0), Some(f64::NAN)],
                    Some(2) => [Some(f64::NEG_INFINITY), Some(f64::INFINITY)],
                    Some(3) => [None, None],
                    None => [Some(3.0), Some(3.0)],
                    _ => panic!("unexpected group {key:?}"),
                };
                for (i, expected) in expected.into_iter().enumerate() {
                    let array = batch
                        .column(i + 1)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    assert_eq!(array.is_null(row), expected.is_none());
                    if let Some(expected) = expected {
                        let actual = array.value(row);
                        assert!(if expected.is_nan() { actual.is_nan() } else { actual == expected }, "mode={mode} split={split} key={key:?} slot={i} actual={actual} expected={expected}");
                    }
                }
            }
        }
        assert_eq!(seen.len(), 5);
        drop(output);
        drop(operator);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn ordinary_float_extrema() {
    check("ordinary").await;
}
#[tokio::test]
async fn shared_fused_float_extrema() {
    check("shared").await;
}
#[tokio::test]
async fn disjoint_fused_float_extrema() {
    check("disjoint").await;
}
