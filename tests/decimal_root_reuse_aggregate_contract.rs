//! Independent grouped decimal oracle across ordinary, morsel and disk paths.
//! These physical fixtures intentionally bypass optimizer rewrites. They model
//! GROUP BY g, SUM(p*(1-d)), SUM(p*(1-d)*(1+t)), COUNT(*) and optionally
//! MIN(DISTINCT g), whose duplicate-insensitive semantics disable the fused
//! route without changing the arithmetic under test.
use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    execution::create_memory_pool,
    physical::{
        morsel_agg::AggregationState,
        operators::{hash_agg, spillable},
        HashAggregateExec, PhysicalOperator, RecordBatchStream,
    },
    planner::{AggregateFunction, BinaryOp, Expr, ScalarValue},
    ExecutionConfig, Result,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Debug)]
struct Fixture {
    schema: SchemaRef,
    parts: Vec<Vec<RecordBatch>>,
    calls: [AtomicUsize; 3],
}
#[async_trait]
impl PhysicalOperator for Fixture {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn name(&self) -> &str {
        "ImmutableDecimalFixture"
    }
    fn output_partitions(&self) -> usize {
        3
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        self.calls[partition].fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::iter(
            self.parts[partition].clone().into_iter().map(Ok),
        )))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Expected {
    discounted: Option<i128>,
    charge: Option<i128>,
    rows: i64,
}
fn add(sum: &mut Option<i128>, value: Option<i128>) {
    if let Some(value) = value {
        *sum = Some(sum.unwrap_or(0).checked_add(value).unwrap());
    }
}
fn fixture() -> (Arc<Fixture>, BTreeMap<Option<i64>, Expected>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("p", DataType::Decimal128(8, 2), true),
        Field::new("d", DataType::Decimal128(8, 2), true),
        Field::new("t", DataType::Decimal128(8, 2), true),
    ]));
    let mut parts = vec![vec![], vec![], vec![]];
    let mut expected = BTreeMap::new();
    for batch_index in 0..48 {
        let mut groups = Vec::new();
        let mut prices = Vec::new();
        let mut discounts = Vec::new();
        let mut taxes = Vec::new();
        for row in 0..128 {
            let id = batch_index * 128 + row;
            let group = id % 256;
            let repetition = (id / 256) / 2; // exact duplicate records in pairs
            let g = (group != 255).then_some(group as i64);
            let p = (group % 17 != 0).then_some((group as i128 - 100) * 100 + repetition as i128);
            let d = (repetition % 11 != 0).then_some((repetition % 5) as i128);
            let t = (repetition % 7 != 0).then_some((repetition % 3) as i128);
            // Independent integer arithmetic, not Arrow or engine evaluation.
            let discounted = p.zip(d).map(|(p, d)| p.checked_mul(100 - d).unwrap());
            let charge = discounted
                .zip(t)
                .map(|(v, t)| v.checked_mul(100 + t).unwrap());
            let acc = expected.entry(g).or_insert(Expected {
                discounted: None,
                charge: None,
                rows: 0,
            });
            add(&mut acc.discounted, discounted);
            add(&mut acc.charge, charge);
            acc.rows += 1;
            groups.push(g);
            prices.push(p);
            discounts.push(d);
            taxes.push(t);
        }
        let decimal = |values| {
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(8, 2)
                    .unwrap(),
            ) as ArrayRef
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(groups)),
                decimal(prices),
                decimal(discounts),
                decimal(taxes),
            ],
        )
        .unwrap();
        parts[batch_index % 3].push(batch);
        // Retain true empty batches between live batches in every partition.
        parts[batch_index % 3].push(RecordBatch::new_empty(schema.clone()));
    }
    (
        Arc::new(Fixture {
            schema,
            parts,
            calls: std::array::from_fn(|_| AtomicUsize::new(0)),
        }),
        expected,
    )
}
fn bin(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}
fn inputs() -> Vec<Expr> {
    let one = || Expr::Literal(ScalarValue::Int8(1));
    let root = bin(
        Expr::column("p"),
        BinaryOp::Multiply,
        bin(one(), BinaryOp::Subtract, Expr::column("d")),
    );
    vec![
        root.clone(),
        bin(
            root,
            BinaryOp::Multiply,
            bin(one(), BinaryOp::Add, Expr::column("t")),
        ),
        one(),
    ]
}
fn output_schema(extra_min: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("g", DataType::Int64, true),
        Field::new("discounted", DataType::Decimal128(38, 4), true),
        Field::new("charge", DataType::Decimal128(38, 6), true),
        Field::new("rows", DataType::Int64, false),
    ];
    if extra_min {
        fields.push(Field::new("minimum", DataType::Int64, true));
    }
    Arc::new(Schema::new(fields))
}
fn aggregates(extra_min: bool) -> Vec<hash_agg::AggregateExpr> {
    let mut result = inputs()
        .into_iter()
        .zip([
            AggregateFunction::Sum,
            AggregateFunction::Sum,
            AggregateFunction::Count,
        ])
        .map(|(input, func)| hash_agg::AggregateExpr {
            func,
            input,
            distinct: false,
            second_arg: None,
        })
        .collect::<Vec<_>>();
    if extra_min {
        result.push(hash_agg::AggregateExpr {
            func: AggregateFunction::Min,
            input: Expr::column("g"),
            distinct: true,
            second_arg: None,
        });
    }
    result
}
fn verify(batches: &[RecordBatch], expected: &BTreeMap<Option<i64>, Expected>, extra_min: bool) {
    let mut seen = BTreeMap::new();
    for batch in batches {
        assert_eq!(batch.schema(), output_schema(extra_min));
        let g = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let a = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let b = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let n = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let key = (!g.is_null(i)).then(|| g.value(i));
            assert!(!n.is_null(i));
            let value = Expected {
                discounted: (!a.is_null(i)).then(|| a.value(i)),
                charge: (!b.is_null(i)).then(|| b.value(i)),
                rows: n.value(i),
            };
            assert!(seen.insert(key, value).is_none(), "duplicate output group");
            if extra_min {
                let m = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!((!m.is_null(i)).then(|| m.value(i)), key);
            }
        }
    }
    assert_eq!(&seen, expected);
    assert!(seen
        .values()
        .any(|v| v.discounted.is_none() && v.charge.is_none()));
    assert!(seen.contains_key(&None));
}
#[test]
fn common_morsel_partition_states_merge_independent_decimal_oracle() {
    let (input, expected) = fixture();
    let types = vec![
        DataType::Decimal128(18, 4),
        DataType::Decimal128(28, 6),
        DataType::Int8,
    ];
    let funcs = vec![
        AggregateFunction::Sum,
        AggregateFunction::Sum,
        AggregateFunction::Count,
    ];
    let mut merged = AggregationState::new(funcs.clone(), types.clone());
    for partition in &input.parts {
        let mut local = AggregationState::new(funcs.clone(), types.clone());
        for batch in partition {
            local
                .process_batch(batch, &[Expr::column("g")], &inputs())
                .unwrap();
        }
        merged.merge(&local).unwrap();
    }
    verify(
        &[merged.build_output(&output_schema(false)).unwrap()],
        &expected,
        false,
    );
}
#[tokio::test]
async fn ordinary_partitioned_aggregate_matches_independent_decimal_oracle() {
    for coalesce in [false, true] {
        let (mut input, expected) = fixture();
        if coalesce {
            let fixture = Arc::get_mut(&mut input).unwrap();
            for part in &mut fixture.parts {
                *part = vec![arrow::compute::concat_batches(&fixture.schema, part.iter()).unwrap()];
            }
        }
        let pool = create_memory_pool(32 * 1024 * 1024);
        let agg = HashAggregateExec::new(
            input.clone(),
            vec![Expr::column("g")],
            aggregates(false),
            output_schema(false),
        )
        .with_memory_pool(pool.clone());
        let output = agg
            .execute(0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        verify(&output, &expected, false);
        assert!(pool.used() > 0, "ordinary finalizer lost query ownership");
        assert!(input.calls.iter().all(|c| c.load(Ordering::SeqCst) == 1));
        drop(output);
        drop(agg);
        assert_eq!(pool.used(), 0);
    }
}
#[tokio::test]
async fn fused_and_forced_spill_match_independent_decimal_oracle() {
    for (spill, fallback, refuse_output) in [
        (false, false, false),
        (false, true, false),
        (true, true, false),
        (true, true, true),
    ] {
        let (input, expected) = fixture();
        let directory = tempfile::tempdir().unwrap();
        let budget = if spill { 128 * 1024 } else { 32 * 1024 * 1024 };
        // Keep the operator's spill threshold fixed. A separate query pool
        // admits retained output in the value-oracle case; the original128KiB
        // query must refuse rather than charge that output to the process pool.
        let query_budget = if spill && !refuse_output {
            1024 * 1024
        } else {
            budget
        };
        let pool = create_memory_pool(query_budget);
        let config = ExecutionConfig::new()
            .with_memory_limit(budget)
            .with_spill_path(directory.path().to_owned());
        let aggs = aggregates(fallback)
            .into_iter()
            .map(|a| spillable::AggregateExpr {
                func: a.func,
                input: a.input,
                distinct: a.distinct,
                second_arg: a.second_arg,
            })
            .collect();
        let agg = spillable::SpillableHashAggregateExec::new(
            input.clone(),
            vec![Expr::column("g")],
            aggs,
            output_schema(fallback),
            pool.clone(),
            config,
        );
        let execution = agg.execute(0).await;
        if refuse_output {
            let error = match execution {
                Err(error) => error,
                Ok(_) => panic!("retained spill output escaped the query budget"),
            };
            assert!(
                error.to_string().contains("Memory limit exceeded"),
                "{error}"
            );
            assert!(pool.spilled() > 0, "refusal fixture must actually spill");
            assert_eq!(pool.used(), 0, "error leaked output reservations");
            assert_eq!(
                std::fs::read_dir(directory.path()).unwrap().count(),
                0,
                "error leaked spill files"
            );
            continue;
        }
        let output = execution.unwrap().try_collect::<Vec<_>>().await.unwrap();
        verify(&output, &expected, fallback);
        // Both fused and materialized spill finalizers must retain the query's
        // output leases, including while the consumer keeps only a slice.
        assert!(pool.used() > 0, "decimal output escaped the query pool");
        let retained = output[0].slice(0, output[0].num_rows().min(1));
        assert!(
            input.calls.iter().all(|c| c.load(Ordering::SeqCst) == 1),
            "unexpected omitted/reexecuted input partition"
        );
        if spill {
            assert!(pool.spilled() > 0, "fixture must execute real disk spill");
        } else {
            assert_eq!(pool.spilled(), 0);
        }
        drop(output);
        drop(agg);
        assert!(pool.used() > 0, "slice lost decimal output ownership");
        drop(retained);
        assert_eq!(pool.used(), 0);
    }
}
