use super::*;
use crate::execution::MemoryPool;
use arrow::array::Decimal128Array;
use std::collections::BTreeMap;

#[test]
fn retained_partition_outputs_share_query_pool_and_release_on_error() {
    for distinct in [false, true] {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Decimal128(30, 2), true),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(9_007_199_254_740_993i128),
                        Some(9_007_199_254_740_993),
                        None,
                        Some(-123),
                    ])
                    .with_precision_and_scale(30, 2)
                    .unwrap(),
                ),
            ],
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Decimal128(38, 2), true),
            Field::new("m", DataType::Int64, true),
        ]));
        let aggregates = vec![
            AggregateExpr {
                func: AggregateFunction::Sum,
                input: Expr::column("v"),
                distinct: false,
                second_arg: None,
            },
            AggregateExpr {
                func: AggregateFunction::Min,
                input: Expr::column("k"),
                distinct,
                second_arg: None,
            },
        ];
        let finalize = |pool: &MemoryPool| {
            aggregate_batches_external_with_pool(
                &[batch.clone()],
                &[Expr::column("k")],
                &aggregates,
                &schema,
                pool,
            )
        };
        let calibration = MemoryPool::new(64 * 1024);
        let output = finalize(&calibration).unwrap();
        let charge = calibration.used();
        assert!(charge >= 3 * 16);
        let keys = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = output
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(sums.data_type(), &DataType::Decimal128(38, 2));
        let actual: BTreeMap<_, _> = keys.iter().zip(sums.iter()).collect();
        assert_eq!(
            actual,
            BTreeMap::from([
                (Some(1), Some(18_014_398_509_481_986)),
                (Some(2), None),
                (None, Some(-123)),
            ])
        );
        drop(output);
        assert_eq!(calibration.used(), 0);

        let parent = MemoryPool::new_named("process control", charge * 100);
        let query = MemoryPool::new_child(&parent, "partition query", charge * 2);
        let first = finalize(&query).unwrap();
        let second = finalize(&query).unwrap();
        assert_eq!(query.used(), charge * 2);
        let error = finalize(&query).unwrap_err().to_string();
        assert!(error.contains("partition query"), "{error}");
        assert_eq!(query.used(), charge * 2);
        assert_eq!(parent.used(), charge * 2);
        let retained = first.slice(0, 1);
        drop(first);
        drop(second);
        assert_eq!(query.used(), charge);
        drop(retained);
        assert_eq!(query.used(), 0);
        assert_eq!(parent.used(), 0);
        drop(finalize(&query).unwrap());
        assert_eq!(query.used(), 0);
    }
}
