//! HAVING must not force retention of all input before grouped accumulation.
use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Int64Array},
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
    planner::{AggregateFunction, DecimalValue, Expr, ScalarValue},
    ExecutionConfig,
};
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn having_streams_input_larger_than_transient_domain() {
    let coefficient = 9_007_199_254_740_993i128;
    let batch = RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from(
                (0..32768)
                    .map(|i| {
                        let k = i % 256;
                        (k != 255).then_some(k as i64)
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "v",
            Arc::new(
                Decimal128Array::from(
                    (0..32768)
                        .map(|i| {
                            let k = i % 256;
                            (k % 5 != 0).then_some(coefficient + k as i128)
                        })
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(30, 2)
                .unwrap(),
            ) as ArrayRef,
        ),
    ])
    .unwrap();
    let input = Arc::new(MemoryTableExec::new(
        "generic_input",
        batch.schema(),
        vec![batch; 4],
        None,
    ));
    let budget = 16 * 1024 * 1024;
    let pool = Arc::new(MemoryPool::new_named("having test", budget));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Decimal128(38, 2), true),
    ]));
    let threshold = 512 * (coefficient + 127);
    let op = spillable::SpillableHashAggregateExec::new(
        input,
        vec![Expr::column("k")],
        vec![spillable::AggregateExpr {
            func: AggregateFunction::Sum,
            input: Expr::column("v"),
            distinct: false,
            second_arg: None,
        }],
        schema.clone(),
        pool.clone(),
        ExecutionConfig::new().with_memory_limit(budget),
    )
    .with_post_filter(Some(Expr::column("s").gt(Expr::literal(
        ScalarValue::Decimal128(DecimalValue::new(threshold, 2)),
    ))));
    let output = op
        .execute(0)
        .await
        .expect("HAVING must stream before the transient domain fills")
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut actual = BTreeMap::new();
    for batch in &output {
        assert_eq!(batch.schema(), schema);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(sums.is_valid(row));
            assert!(
                actual
                    .insert(keys.is_valid(row).then(|| keys.value(row)), sums.value(row))
                    .is_none(),
                "duplicate final group"
            );
        }
    }
    let expected = (128..256)
        .filter(|k| k % 5 != 0)
        .map(|k| {
            (
                (k != 255).then_some(k as i64),
                512 * (coefficient + k as i128),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
    let retained = output[0].column(1).slice(0, 1);
    drop(output);
    drop(op);
    assert!(pool.used() > 0);
    drop(retained);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn having_observes_final_groups_after_real_spill() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from((0..4096).collect::<Vec<i64>>())) as ArrayRef,
        ),
        (
            "v",
            Arc::new(Int64Array::from(
                (0..4096)
                    .map(|i| (i % 3 != 0).then_some(1i64))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    let input = Arc::new(MemoryTableExec::new(
        "spill_input",
        batch.schema(),
        vec![batch; 4],
        None,
    ));
    let pool = Arc::new(MemoryPool::new(32 * 1024 * 1024));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Int64, true),
    ]));
    let op = spillable::SpillableHashAggregateExec::new(
        input,
        vec![Expr::column("k")],
        vec![spillable::AggregateExpr {
            func: AggregateFunction::Sum,
            input: Expr::column("v"),
            distinct: false,
            second_arg: None,
        }],
        schema,
        pool.clone(),
        ExecutionConfig::new().with_memory_limit(16 * 1024),
    )
    .with_post_filter(Some(
        Expr::column("s").gt_eq(Expr::literal(ScalarValue::Int64(4))),
    ));
    let output = op
        .execute(0)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(
        pool.spilled() > 0,
        "must exercise actual partial-state spilling"
    );
    let mut actual = BTreeMap::new();
    for batch in &output {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(s.is_valid(row));
            assert!(actual.insert(k.value(row), s.value(row)).is_none());
        }
    }
    assert_eq!(
        actual,
        (0..4096)
            .filter(|k| k % 3 != 0)
            .map(|k| (k, 4))
            .collect::<BTreeMap<_, _>>()
    );
    drop(output);
    drop(op);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn sql_having_aliases_nulls_empty_and_whole_batches() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from(vec![
                None,
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                Some(4),
            ])) as ArrayRef,
        ),
        (
            "v",
            Arc::new(Int64Array::from(vec![
                Some(5),
                Some(1),
                Some(2),
                None,
                None,
                Some(4),
                Some(0),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut ctx = query_engine::ExecutionContext::with_config(
        ExecutionConfig::new().with_memory_limit(16 * 1024 * 1024),
    )
    .with_parallel_partitions(3);
    ctx.register_table(
        "t",
        batch.schema(),
        vec![batch.slice(0, 2), batch.slice(2, 2), batch.slice(4, 3)],
    );
    for (predicate, expected) in [
        (
            "SUM(v)>3 OR SUM(v) IS NULL",
            vec![(None, Some(5)), (Some(2), None), (Some(3), Some(4))],
        ),
        ("total>100", vec![]),
        (
            "COUNT(*)>0",
            vec![
                (None, Some(5)),
                (Some(1), Some(3)),
                (Some(2), None),
                (Some(3), Some(4)),
                (Some(4), Some(0)),
            ],
        ),
    ] {
        let sql = format!(
            "SELECT k,SUM(v) AS total FROM t GROUP BY k HAVING {predicate} ORDER BY k NULLS FIRST"
        );
        let result = ctx.sql(&sql).await.unwrap();
        let actual = result
            .batches
            .iter()
            .flat_map(|b| {
                let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let v = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                k.iter().zip(v.iter()).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{sql}");
    }
    let empty = ctx
        .sql("SELECT k,SUM(v) FROM t WHERE FALSE GROUP BY k HAVING SUM(v)>0")
        .await
        .unwrap();
    assert_eq!(
        empty
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    let scalar = ctx
        .sql("SELECT COUNT(*) AS n FROM t WHERE FALSE HAVING COUNT(*)=0")
        .await
        .unwrap();
    let values = scalar
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some(0)]);
}

#[tokio::test]
async fn fitting_group_state_does_not_spill_at_an_unrelated_quarter_budget_cap() {
    for filtered in [true, false] {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int64Array::from(
                    (0..50_000i64)
                        .map(|i| (i != 123).then_some(i))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "v",
                Arc::new(
                    Decimal128Array::from(
                        (0..50_000i64)
                            .map(|i| (i % 7 != 0).then_some(i128::from(i + 1) * 100))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(30, 2)
                    .unwrap(),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        let input = Arc::new(MemoryTableExec::new(
            "fitting_groups",
            batch.schema(),
            vec![batch; 2],
            None,
        ));
        let budget = 16 * 1024 * 1024;
        let pool = Arc::new(MemoryPool::new_named("fitting aggregate state", budget));
        let directory = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Decimal128(38, 2), true),
        ]));
        let op = spillable::SpillableHashAggregateExec::new(
            input,
            vec![Expr::column("k")],
            vec![spillable::AggregateExpr {
                func: AggregateFunction::Sum,
                input: Expr::column("v"),
                distinct: false,
                second_arg: None,
            }],
            schema.clone(),
            pool.clone(),
            ExecutionConfig::new()
                .with_memory_limit(budget)
                .with_spill_path(directory.path().to_path_buf()),
        );
        let op = if filtered {
            op.with_post_filter(Some(
                Expr::column("s").gt(Expr::literal(ScalarValue::Int64(99_999))),
            ))
        } else {
            op
        };
        let output = op
            .execute(0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut actual = BTreeMap::new();
        for batch in &output {
            assert_eq!(batch.schema(), schema);
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(actual
                    .insert(
                        keys.is_valid(row).then(|| keys.value(row)),
                        sums.is_valid(row).then(|| sums.value(row))
                    )
                    .is_none());
            }
        }
        let expected = (0..50_000i64)
            .filter(|i| !filtered || *i == 49_999)
            .map(|i| {
                (
                    (i != 123).then_some(i),
                    (i % 7 != 0).then_some(i128::from(i + 1) * 200),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected);
        let spilled = pool.spilled();
        drop(output);
        drop(op);
        assert_eq!(
            pool.used(),
            0,
            "no retained allocation after final output release"
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        if filtered {
            assert_eq!(
                spilled, 0,
                "fitting state must not spill because of a separate static count cap"
            );
        }
    }
}

#[tokio::test]
async fn having_preserves_every_group_across_large_output_ranges() {
    let n = 8193;
    let coefficient = (1i128 << 80) + 17;
    let batch = RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from(
                (0..n)
                    .map(|k| (k != n - 1).then_some(k as i64))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "v",
            Arc::new(
                Decimal128Array::from(
                    (0..n)
                        .map(|k| (k % 5 != 0).then_some(coefficient + k as i128))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(30, 2)
                .unwrap(),
            ) as ArrayRef,
        ),
    ])
    .unwrap();
    let input = Arc::new(MemoryTableExec::new(
        "range_input",
        batch.schema(),
        vec![batch; 3],
        None,
    ));
    let budget = 32 * 1024 * 1024;
    let pool = Arc::new(MemoryPool::new_named("large HAVING output ranges", budget));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Decimal128(38, 2), true),
    ]));
    let op = spillable::SpillableHashAggregateExec::new(
        input,
        vec![Expr::column("k")],
        vec![spillable::AggregateExpr {
            func: AggregateFunction::Sum,
            input: Expr::column("v"),
            distinct: false,
            second_arg: None,
        }],
        schema.clone(),
        pool.clone(),
        ExecutionConfig::new().with_memory_limit(budget),
    )
    .with_post_filter(Some(Expr::column("s").gt(Expr::literal(
        ScalarValue::Decimal128(DecimalValue::new(3 * (coefficient + 4000), 2)),
    ))));
    let output = op
        .execute(0)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut actual = BTreeMap::new();
    for batch in &output {
        assert_eq!(batch.schema(), schema);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(sums.is_valid(row));
            assert!(
                actual
                    .insert(keys.is_valid(row).then(|| keys.value(row)), sums.value(row))
                    .is_none(),
                "duplicate published group"
            );
        }
    }
    let expected = (4001..n)
        .filter(|k| k % 5 != 0)
        .map(|k| {
            (
                (k != n - 1).then_some(k as i64),
                3 * (coefficient + k as i128),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
    drop((output, op));
    assert_eq!(pool.used(), 0);
}
