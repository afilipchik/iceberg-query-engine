//! Pure output construction; filtering and publication belong to the caller.
use super::group_rows::GroupRows;
use crate::Result;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

pub(super) const TARGET_ROWS: usize = 1024;

pub(super) fn build_next(
    groups: &GroupRows,
    schema: &SchemaRef,
    start: usize,
    quantum: &mut usize,
) -> Result<RecordBatch> {
    if start >= groups.len() || *quantum == 0 || *quantum > TARGET_ROWS {
        return Err(crate::QueryError::Execution(
            "aggregate output quantum: invalid cursor or target".into(),
        ));
    }
    let mut rows = (groups.len() - start).min(*quantum);
    loop {
        // This constructor is pure with respect to completed aggregate state.
        // All temporary owners drop on Err before a smaller range is attempted.
        match groups.build_output_range(schema, start, start + rows) {
            Err(error) if error.is_memory_limit() && rows > 1 => {
                rows /= 2;
                *quantum = rows;
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction as A, DecimalValue, ScalarValue as V},
    };
    use arrow::{
        array::{Array, Decimal128Array, Float64Array, Int64Array},
        datatypes::{DataType, Field, Schema},
    };
    use std::{collections::HashMap, sync::Arc};

    fn fixture(pool: &MemoryPool, n: usize) -> (GroupRows, SchemaRef) {
        let layout = super::super::group_rows::GroupLayout::bind(
            pool,
            &[DataType::Int64],
            &[
                (A::Count, DataType::Int64, false),
                (A::Sum, DataType::Decimal128(38, 2), false),
                (A::Sum, DataType::Float64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        for k in 0..n {
            key.encode(&[if k == n - 1 {
                V::Null
            } else {
                V::Int64(k as i64)
            }])
            .unwrap();
            for weight in [1, 2] {
                groups
                    .prepare_update(
                        &key,
                        &mut row,
                        &[
                            V::Int64(1),
                            if k == n - 2 {
                                V::Null
                            } else {
                                V::Decimal128(DecimalValue::new(
                                    ((1i128 << 80) + k as i128) * weight,
                                    2,
                                ))
                            },
                            V::Float64(((k as f64 / 8.0) * weight as f64).into()),
                        ],
                    )
                    .unwrap()
                    .commit();
            }
        }
        let metadata = HashMap::from([("owner".into(), "admitted range".into())]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("k", DataType::Int64, true).with_metadata(metadata.clone()),
                Field::new("c", DataType::Int64, false),
                Field::new("d", DataType::Decimal128(38, 2), true),
                Field::new("f", DataType::Float64, true),
            ],
            metadata,
        ));
        (groups, schema)
    }
    #[test]
    fn complete_ranges_preserve_exact_values_short_tail_and_retained_slices() {
        let pool = MemoryPool::new_named("output quantum values", 16 * 1024 * 1024);
        let (groups, schema) = fixture(&pool, 2053);
        let mut quantum = TARGET_ROWS;
        let mut cursor = 0;
        let mut batches = Vec::new();
        while cursor < groups.len() {
            let batch = build_next(&groups, &schema, cursor, &mut quantum).unwrap();
            assert_eq!(batch.schema(), schema);
            cursor += batch.num_rows();
            batches.push(batch);
        }
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![1024, 1024, 5]
        );
        assert_eq!(
            quantum, TARGET_ROWS,
            "a short tail must not reduce the next target"
        );
        let mut seen = vec![false; 2053];
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let decimal = batch
                .column(2)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            let float = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let k = if keys.is_null(row) {
                    2052
                } else {
                    keys.value(row) as usize
                };
                assert!(!seen[k]);
                seen[k] = true;
                assert_eq!(count.value(row), 2);
                if k == 2051 {
                    assert!(decimal.is_null(row))
                } else {
                    assert_eq!(decimal.value(row), ((1i128 << 80) + k as i128) * 3)
                }
                assert_eq!(float.value(row).to_bits(), (k as f64 * 3.0 / 8.0).to_bits());
            }
        }
        assert!(seen.into_iter().all(|x| x));
        let extracted = batches[0].column(2).slice(1, 2);
        drop((batches, groups, schema));
        assert!(pool.used() > 0);
        assert_eq!(extracted.len(), 2);
        drop(extracted);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn actual_admission_pressure_shrinks_without_reapplying_groups() {
        let pool = MemoryPool::new_named("output quantum pressure", 4 * 1024 * 1024);
        let (groups, schema) = fixture(&pool, 128);
        let baseline = pool.used();
        let mut witnesses = 0;
        for headroom in (2048..16384).step_by(1024) {
            let hold = pool.allocate(pool.available() - headroom).unwrap();
            let used = pool.used();
            // NULL-bearing rows may need additional validity metadata: prove
            // that every individual row fits, not merely the first all-valid row.
            let mut one_fits = true;
            for row in 0..groups.len() {
                let one = groups.build_output_range(&schema, row, row + 1);
                one_fits &= one.is_ok();
                drop(one);
                assert_eq!(pool.used(), used);
            }
            let large = groups.build_output_range(&schema, 0, 64);
            let large_denied = large.as_ref().is_err_and(|e| e.is_memory_limit());
            drop(large);
            assert_eq!(pool.used(), used);
            if one_fits && large_denied {
                witnesses += 1;
                let mut quantum = TARGET_ROWS;
                let mut cursor = 0;
                while cursor < groups.len() {
                    let batch = build_next(&groups, &schema, cursor, &mut quantum).unwrap();
                    assert!(batch.num_rows() > 0 && batch.num_rows() <= quantum);
                    let keys = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        if cursor + i == 127 {
                            assert!(keys.is_null(i))
                        } else {
                            assert_eq!(keys.value(i), (cursor + i) as i64)
                        }
                    }
                    cursor += batch.num_rows();
                    drop(batch);
                    assert_eq!(pool.used(), used);
                }
                assert!(quantum < 64);
            }
            drop(hold);
            assert_eq!(pool.used(), baseline);
        }
        assert!(
            witnesses > 0,
            "exercise an actual allocation denial where one row fits"
        );
        let hold = pool.allocate(pool.available()).unwrap();
        let mut quantum = TARGET_ROWS;
        assert!(build_next(&groups, &schema, 0, &mut quantum)
            .unwrap_err()
            .is_memory_limit());
        drop(hold);
        assert_eq!(pool.used(), baseline);
        let wrong = Arc::new(Schema::empty());
        let mut unchanged = TARGET_ROWS;
        assert!(!build_next(&groups, &wrong, 0, &mut unchanged)
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(
            unchanged, TARGET_ROWS,
            "schema failure must not retry smaller ranges"
        );
        assert!(build_next(&groups, &schema, groups.len(), &mut unchanged).is_err());
        let mut zero = 0;
        assert!(build_next(&groups, &schema, 0, &mut zero).is_err());
        drop((groups, schema));
        assert_eq!(pool.used(), 0);
    }
}
