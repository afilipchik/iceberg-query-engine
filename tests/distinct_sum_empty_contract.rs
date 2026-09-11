use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use query_engine::{
    execution::create_memory_pool,
    physical::{
        operators::{hash_agg, spillable, MemoryTableExec},
        PhysicalOperator,
    },
    planner::{AggregateFunction, Expr},
    ExecutionConfig,
};
use std::{collections::BTreeMap, sync::Arc};
fn fixture() -> (Vec<RecordBatch>, Arc<Schema>, Arc<Schema>) {
    let input = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("i", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("d", DataType::Decimal128(8, 2), true),
    ]));
    let output = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("si", DataType::Int64, true),
        Field::new("sf", DataType::Float64, true),
        Field::new("sd", DataType::Decimal128(38, 2), true),
    ]));
    let batches = (0..64)
        .map(|b| {
            let values: Vec<Option<i64>> = (0..128)
                .map(|g| {
                    if g == 0 || b % 5 == 0 {
                        None
                    } else if b % 2 == 0 {
                        Some(-1)
                    } else {
                        Some(1)
                    }
                })
                .collect();
            RecordBatch::try_new(
                input.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..128)) as ArrayRef,
                    Arc::new(Int64Array::from(values.clone())),
                    Arc::new(Float64Array::from(
                        values
                            .iter()
                            .map(|x| x.map(|v| v as f64))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(
                        Decimal128Array::from(
                            values
                                .iter()
                                .map(|x| x.map(|v| v as i128 * 100))
                                .collect::<Vec<_>>(),
                        )
                        .with_precision_and_scale(8, 2)
                        .unwrap(),
                    ),
                ],
            )
            .unwrap()
        })
        .collect();
    (batches, input, output)
}
fn aggs() -> Vec<hash_agg::AggregateExpr> {
    ["i", "f", "d"]
        .into_iter()
        .map(|s| hash_agg::AggregateExpr {
            func: AggregateFunction::Sum,
            input: Expr::column(s),
            distinct: true,
            second_arg: None,
        })
        .collect()
}
fn verify(batches: &[RecordBatch]) {
    let mut rows = BTreeMap::new();
    for b in batches {
        assert_eq!(b.column(1).data_type(), &DataType::Int64);
        assert_eq!(b.column(2).data_type(), &DataType::Float64);
        assert_eq!(b.column(3).data_type(), &DataType::Decimal128(38, 2));
        let g = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let i = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let f = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let d = b
            .column(3)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        for r in 0..b.num_rows() {
            let key = g.value(r);
            assert!(rows.insert(key, ()).is_none());
            for a in [b.column(1), b.column(2), b.column(3)] {
                assert_eq!(a.is_null(r), key == 0);
            }
            if key != 0 {
                assert_eq!(i.value(r), 0);
                assert_eq!(f.value(r), 0.0);
                assert_eq!(d.value(r), 0);
            }
        }
    }
    assert_eq!(
        rows.keys().copied().collect::<Vec<_>>(),
        (0..128).collect::<Vec<_>>()
    );
}
#[test]
fn ordinary_multibatch_distinct_sum_keeps_all_null_and_valid_zero_separate() {
    let (batches, _, schema) = fixture();
    let result =
        hash_agg::aggregate_batches_external(&batches, &[Expr::column("g")], &aggs(), &schema)
            .unwrap();
    verify(&[result]);
}
#[tokio::test]
async fn spill_and_in_memory_distinct_sum_keep_all_null_and_valid_zero_separate() {
    for spill in [false, true] {
        let (batches, input_schema, output_schema) = fixture();
        let input = Arc::new(MemoryTableExec::new(
            "distinct_fixture",
            input_schema,
            batches,
            None,
        ));
        let directory = tempfile::tempdir().unwrap();
        let budget = if spill { 64 * 1024 } else { 8 * 1024 * 1024 };
        let pool = create_memory_pool(budget);
        let config = ExecutionConfig::new()
            .with_memory_limit(budget)
            .with_spill_path(directory.path().to_owned());
        let aggregates = aggs()
            .into_iter()
            .map(|a| spillable::AggregateExpr {
                func: a.func,
                input: a.input,
                distinct: a.distinct,
                second_arg: a.second_arg,
            })
            .collect();
        let op = spillable::SpillableHashAggregateExec::new(
            input,
            vec![Expr::column("g")],
            aggregates,
            output_schema,
            pool.clone(),
            config,
        );
        let result = op
            .execute(0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        verify(&result);
        if spill {
            assert!(pool.spilled() > 0, "Actual disk spill required")
        } else {
            assert_eq!(pool.spilled(), 0)
        };
        drop(result);
        drop(op);
        assert_eq!(pool.used(), 0);
    }
}
