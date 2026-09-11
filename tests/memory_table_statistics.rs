use arrow::array::*;
use arrow::datatypes::{DataType, Field, Int8Type, Schema};
use query_engine::physical::operators::{MemoryTable, TableProvider};
use query_engine::{planner::LogicalPlan, ExecutionContext};
use std::sync::Arc;

fn table(arrays: Vec<ArrayRef>, names: &[&str]) -> MemoryTable {
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .zip(names)
            .map(|(a, n)| Field::new(*n, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    MemoryTable::new(
        schema.clone(),
        vec![RecordBatch::try_new(schema, arrays).unwrap()],
    )
}

#[test]
fn ranges_merge_batches_duplicates_and_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Key", DataType::Int64, true),
        Field::new("day", DataType::Date32, true),
    ]));
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(4), None, Some(4)])),
                Arc::new(Date32Array::from(vec![Some(-1), Some(4), None])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(7), Some(5)])),
                Arc::new(Date32Array::from(vec![Some(8), Some(0)])),
            ],
        )
        .unwrap(),
    ];
    let t = MemoryTable::new(schema, batches);
    for t in [t.clone(), t] {
        for _ in 0..2 {
            let s = t.statistics().unwrap();
            assert_eq!(s.row_count, 5);
            let c = &s.column_stats["key"];
            assert_eq!(
                (c.min_i64, c.max_i64, c.null_count, c.ndv_est),
                (Some(4), Some(7), Some(1), Some(4))
            );
            let c = &s.column_stats["day"];
            assert_eq!(
                (c.min_i64, c.max_i64, c.null_count, c.ndv_est),
                (Some(-1), Some(8), Some(1), Some(4))
            );
        }
    }
}

#[test]
fn unsupported_and_wide_domains_do_not_wrap_or_hide_logical_nulls() {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::new(Int64Array::from(vec![Some(9), None])),
    )
    .unwrap();
    let t = table(
        vec![
            Arc::new(dictionary),
            Arc::new(UInt64Array::from(vec![Some(0), Some(u64::MAX), None])),
            Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
            Arc::new(Date64Array::from(vec![Some(0), Some(86400000), None])),
        ],
        &["dict", "unsigned", "signed", "millis"],
    );
    let s = t.statistics().unwrap();
    for name in ["dict", "unsigned", "signed", "millis"] {
        let c = &s.column_stats[name];
        assert_eq!((c.min_i64, c.max_i64, c.ndv_est), (None, None, None));
        assert_eq!(c.null_count, Some(if name == "dict" { 2 } else { 1 }));
    }
}

#[test]
fn small_widths_empty_all_null_and_ambiguous_names() {
    let t = table(
        vec![
            Arc::new(Int8Array::from(vec![-2, 4])),
            Arc::new(Int16Array::from(vec![-20, 30])),
            Arc::new(Int32Array::from(vec![-200, 300])),
            Arc::new(UInt8Array::from(vec![0, 255])),
            Arc::new(UInt16Array::from(vec![0, u16::MAX])),
            Arc::new(UInt32Array::from(vec![0, u32::MAX])),
            Arc::new(UInt64Array::from(vec![0, i64::MAX as u64])),
        ],
        &["i8", "i16", "i32", "u8", "u16", "u32", "u64"],
    );
    for c in t.statistics().unwrap().column_stats.values() {
        assert_eq!(c.ndv_est, Some(2));
        assert_eq!(c.null_count, Some(0));
    }
    let t = table(vec![Arc::new(Int64Array::from(vec![None, None]))], &["x"]);
    let s = t.statistics().unwrap();
    let c = &s.column_stats["x"];
    assert_eq!(
        (c.min_i64, c.max_i64, c.ndv_est, c.null_count),
        (None, None, None, Some(2))
    );
    let t = MemoryTable::new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
        vec![],
    );
    assert_eq!(t.statistics().unwrap().row_count, 0);
    let t = table(
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
        ],
        &["x", "X"],
    );
    assert!(t.statistics().unwrap().column_stats.is_empty());
}

#[test]
fn low_cardinality_join_edge_is_not_costed_as_unique() {
    let mut ctx = ExecutionContext::new();
    for (name, n) in [("people", 10000usize), ("vendors", 2000)] {
        let t = table(
            vec![
                Arc::new(Int64Array::from_iter_values((0..n).map(|i| i as i64))),
                Arc::new(Int64Array::from_iter_values((0..n).map(|i| (i % 5) as i64))),
            ],
            &["id", "zone"],
        );
        assert_eq!(
            t.statistics().unwrap().column_stats["zone"].ndv_est,
            Some(5)
        );
        ctx.register_table_provider(name, Arc::new(t));
    }
    let t = table(
        vec![
            Arc::new(Int64Array::from_iter_values(0..20)),
            Arc::new(Int64Array::from_iter_values(0..20)),
        ],
        &["person", "vendor"],
    );
    ctx.register_table_provider("links", Arc::new(t));
    let plan=ctx.optimized_plan("SELECT p.id, v.id FROM people p JOIN vendors v ON p.zone=v.zone JOIN links l ON p.id=l.person AND v.id=l.vendor").unwrap();
    fn scans(p: &LogicalPlan) -> Vec<String> {
        if let LogicalPlan::Scan(s) = p {
            return vec![s.table_name.clone()];
        }
        p.children().iter().flat_map(|c| scans(c)).collect()
    }
    fn check(p: &LogicalPlan) {
        if let LogicalPlan::Join(_) = p {
            let names = scans(p);
            assert!(
                !(names.len() == 2
                    && names.contains(&"people".into())
                    && names.contains(&"vendors".into())),
                "low-NDV many-to-many edge selected before selective key join: {p}"
            );
        }
        for c in p.children() {
            check(&c);
        }
    }
    check(&plan);
}
