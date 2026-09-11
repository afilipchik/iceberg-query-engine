//! Explicit optimized component experiment; not a query performance gate.
use super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use std::{hint::black_box, time::Instant};

#[test]
#[ignore = "optimized key-preparation component measurement; run explicitly with --release"]
fn measure_retained_key_preparation() {
    assert!(!cfg!(debug_assertions), "measurement requires --release");
    for (rows, cardinality, width) in [
        (128, 6, 1),
        (8192, 6, 1),
        (8192, 8192, 1),
        (8192, 6, 128),
        (8192, 8192, 128),
    ] {
        let strings: Vec<Option<String>> = (0..rows)
            .map(|row| {
                (row % 17 != 0).then(|| format!("{:0width$}", row % cardinality, width = width))
            })
            .collect();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(strings)),
            Arc::new(Int64Array::from_iter((0..rows).map(|row| {
                (row % 13 != 0).then_some((row % cardinality) as i64)
            }))),
        ];
        let pool = MemoryPool::new_named("retained-key component", 16 * 1024 * 1024);
        let source = pool
            .allocate(arrays.iter().map(|a| a.get_array_memory_size()).sum())
            .unwrap();
        let layout = KeyLayout::bind(&pool, &[DataType::Utf8, DataType::Int64])
            .unwrap()
            .unwrap();
        layout.validate_arrays(&arrays).unwrap();
        // Both sides produce the same two consumer checksums. Retaining hashes
        // removes the second hash computation as well as the second encoding.
        let run = |retained: bool| {
            let mut result = (0u64, 0u64);
            let start = Instant::now();
            for _ in 0..32 {
                let mut scratch = KeyWorkspace::new(layout.clone()).unwrap();
                if retained {
                    let mut keys = KeyRows::new(layout.clone()).unwrap();
                    let mut hashes = ReservedVec::with_capacity(&pool, rows).unwrap();
                    for row in 0..rows {
                        scratch.encode_arrays(black_box(&arrays), row).unwrap();
                        let hash = scratch.key().unwrap().hash64();
                        keys.append(&scratch).unwrap();
                        hashes.extend_reserved(1, std::iter::once(hash)).unwrap();
                        result.0 = result.0.wrapping_add(black_box(hash));
                    }
                    for row in 0..rows {
                        let key = keys.key(row).unwrap();
                        let hash = hashes.as_slice()[row];
                        result.1 = result
                            .1
                            .wrapping_add(black_box(hash ^ key.bytes().len() as u64));
                    }
                } else {
                    for row in 0..rows {
                        scratch.encode_arrays(black_box(&arrays), row).unwrap();
                        result.0 = result
                            .0
                            .wrapping_add(black_box(scratch.key().unwrap().hash64()));
                    }
                    for row in 0..rows {
                        scratch.encode_arrays(black_box(&arrays), row).unwrap();
                        let key = scratch.key().unwrap();
                        result.1 = result
                            .1
                            .wrapping_add(black_box(key.hash64() ^ key.bytes().len() as u64));
                    }
                }
            }
            (start.elapsed().as_nanos(), result)
        };
        let expected = run(false).1;
        assert_eq!(run(true).1, expected);
        for block in 0..8 {
            for retained in if block % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let (ns, result) = run(retained);
                assert_eq!(result, expected);
                println!(
                    "{}",
                    serde_json::json!({
                        "rows":rows, "cardinality":cardinality, "string_width":width,
                        "block":block, "retained":retained, "iterations":32, "ns":ns,
                        "scope":"key preparation only; no routing, state updates or spill",
                    })
                );
            }
        }
        drop((layout, source, arrays));
        assert_eq!(pool.used(), 0);
    }
}
