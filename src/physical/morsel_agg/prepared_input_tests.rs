use super::*;
use crate::planner::ScalarFunction;
use arrow::array::Decimal128Array;
use arrow::datatypes::{Field, Schema};
use std::collections::BTreeMap;

#[test]
fn slices_preserve_evaluated_draws_and_exact_partial_values() {
    let pool = MemoryPool::new(8 << 20);
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(38, 2), true),
    ]));
    let large = (1i128 << 80) + 7;
    let keys = [Some(0), Some(0), None, Some(1), Some(0), None];
    let values = [Some(large), Some(-1), None, None, Some(2), Some(large)];
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(
                Decimal128Array::from(values.to_vec())
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    let prepared = prepare_aggregate_batch(
        &batch,
        &[Expr::column("g")],
        &[
            Expr::column("d"),
            Expr::ScalarFunc {
                func: ScalarFunction::Random,
                args: vec![],
            },
            Expr::column("d"),
        ],
    )
    .unwrap();
    let draws = prepared
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut expected = BTreeMap::<Option<i64>, (Option<i128>, f64, i64)>::new();
    for i in 0..keys.len() {
        let entry = expected.entry(keys[i]).or_insert((None, f64::INFINITY, 0));
        if let Some(v) = values[i] {
            entry.0 = Some(entry.0.unwrap_or(0) + v);
            entry.2 += 1;
        }
        entry.1 = entry.1.min(draws.value(i));
    }
    drop(batch);
    let slices = [
        prepared.slice(0, 1),
        prepared.slice(1, 3),
        prepared.slice(4, 2),
    ];
    drop(prepared);
    let mut state = AggregationState::new_with_pool(
        vec![
            AggregateFunction::Sum,
            AggregateFunction::Min,
            AggregateFunction::Count,
        ],
        vec![
            DataType::Decimal128(38, 2),
            DataType::Float64,
            DataType::Decimal128(38, 2),
        ],
        &pool,
    );
    for slice in &slices {
        state.process_evaluated_batch(slice, 1).unwrap();
    }
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("sum", DataType::Decimal128(38, 2), true),
        Field::new("min", DataType::Float64, true),
        Field::new("count", DataType::Int64, false),
    ]));
    let output = state.build_output_with_pool(&output_schema, &pool).unwrap();
    let g = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = output
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let min = output
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let count = output
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(output.num_rows(), expected.len());
    for i in 0..output.num_rows() {
        let key = (!g.is_null(i)).then(|| g.value(i));
        let (s, m, c) = expected.remove(&key).unwrap();
        assert_eq!((!sum.is_null(i)).then(|| sum.value(i)), s);
        assert_eq!(
            min.value(i).to_bits(),
            m.to_bits(),
            "a volatile draw was re-evaluated"
        );
        assert_eq!(count.value(i), c);
    }
    assert!(expected.is_empty());
    drop(state);
    drop(output);
    drop(slices);
    assert_eq!(pool.used(), 0);
}
