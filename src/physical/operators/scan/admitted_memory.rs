//! Admitted copies from immutable registered memory batches. Source residency
//! retains its existing registration/preload contract; no decoder is launched.
use super::*;
use crate::execution::{reserved_vec::ReservedVec, MemoryReservation, SharedMemoryPool};
use crate::physical::{plan::admit_stream, PreparedAdmittedInput};
use crate::storage::admitted_selection;
fn invalid(message: &str) -> crate::QueryError {
    crate::QueryError::Execution(format!("admitted memory scan: {message}"))
}
struct Source {
    batches: Arc<Vec<RecordBatch>>,
    columns: ReservedVec<usize>,
    schema: SchemaRef,
    _metadata: MemoryReservation,
    frame_bytes: usize,
}
pub(super) fn prepare(
    input: &MemoryTableExec,
    pool: SharedMemoryPool,
) -> Result<Option<PreparedAdmittedInput>> {
    if input.schema.fields().is_empty()
        || input
            .schema
            .fields()
            .iter()
            .any(|f| !admitted_selection::supported(f.data_type()))
    {
        return Ok(None);
    }
    for batch in input.batches.iter() {
        for (column, field) in input.schema.fields().iter().enumerate() {
            let index = input.projection.as_ref().map_or(column, |p| p[column]);
            let Some(array) = batch.columns().get(index) else {
                return Err(invalid("projection outside source domain"));
            };
            let mut actual = array.data_type();
            while let arrow::datatypes::DataType::Dictionary(_, value) = actual {
                actual = value.as_ref();
            }
            if actual != field.data_type() {
                return Ok(None);
            }
            // A logical nonnullable field cannot be silently changed by decoding
            // nullable dictionary values. Leave uncertain layouts to the ordinary
            // schema-adapting path before creating any streams.
            if !field.is_nullable()
                && (array.null_count() != 0
                    || matches!(
                        array.data_type(),
                        arrow::datatypes::DataType::Dictionary(_, _)
                    ))
            {
                return Ok(None);
            }
        }
    }
    let partitions = input.output_partitions();
    // Reserve input progress space before downstream state grows. Output
    // lifetimes are not queue credits: collecting consumers may retain batches
    // until EOF, so admission must refuse rather than await their release.
    let live_frames = partitions
        .max(1)
        .checked_mul(5)
        .ok_or_else(|| invalid("frame extent overflow"))?;
    let budget = pool.available() / 8;
    let frame_bytes = budget / 2 / live_frames;
    let minimum = input
        .schema
        .fields()
        .len()
        .checked_mul(4096 + 1024 + 32)
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| invalid("minimum frame overflow"))?;
    if frame_bytes < minimum {
        return Ok(None);
    }
    let input_pool = Arc::new(crate::execution::MemoryPool::child_with_progress_credit(
        &pool,
        "memory scan working space",
        budget,
    )?);
    let metadata = pool.allocate(512)?;
    let batches = input.batches.clone();
    let mut columns = ReservedVec::with_capacity(&pool, input.schema.fields().len())?;
    columns.extend_reserved(
        input.schema.fields().len(),
        (0..input.schema.fields().len()).map(|i| input.projection.as_ref().map_or(i, |p| p[i])),
    )?;
    let source = Arc::new(Source {
        batches,
        columns,
        schema: input.schema.clone(),
        _metadata: metadata,
        frame_bytes,
    });
    let partitions = input.output_partitions();
    let mut streams = ReservedVec::with_capacity(&pool, partitions)?;
    for partition in 0..partitions {
        let state = (source.clone(), input_pool.clone(), partition, 0usize);
        streams.extend_reserved(
            1,
            [admit_stream(
                stream::try_unfold(
                    state,
                    move |(source, pool, mut batch_index, mut offset)| async move {
                        loop {
                            let Some(batch) = source.batches.get(batch_index) else {
                                return Ok(None);
                            };
                            if offset == batch.num_rows() {
                                batch_index += partitions;
                                offset = 0;
                                continue;
                            }
                            // Charge actual output bytes, never a maximum-sized
                            // retained frame or an output-lifetime semaphore. A
                            // byte cap also bounds construction across concurrent
                            // producers while allowing useful source batch sizes.
                            let frame_pool = crate::execution::MemoryPool::new_child(
                                &pool,
                                "memory scan output",
                                source.frame_bytes,
                            );
                            let mut count = batch.num_rows() - offset;
                            loop {
                                let copy = || -> Result<RecordBatch> {
                                    admitted_selection::copy_range(
                                        batch,
                                        offset,
                                        count,
                                        source.columns.as_slice(),
                                        source.schema.clone(),
                                        &frame_pool,
                                    )
                                };
                                match copy() {
                                    Err(e) if e.is_memory_limit() && count > 1 => {
                                        count = count.div_ceil(2)
                                    }
                                    Err(e) => return Err(e),
                                    Ok(output) => {
                                        return Ok(Some((
                                            output,
                                            (source, pool, batch_index, offset + count),
                                        )))
                                    }
                                }
                            }
                        }
                    },
                ),
                &pool,
            )?],
        )?;
    }
    Ok(Some(PreparedAdmittedInput {
        pool: input_pool,
        streams,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use futures::TryStreamExt;

    #[tokio::test]
    async fn retained_outputs_do_not_block_source_completion() {
        let batches: Vec<_> = (0..128i64)
            .map(|value| {
                RecordBatch::try_from_iter([(
                    "id",
                    Arc::new(Int64Array::from(vec![value])) as ArrayRef,
                )])
                .unwrap()
            })
            .collect();
        let input = MemoryTableExec::new("resident", batches[0].schema(), batches, None);
        let pool = Arc::new(crate::execution::MemoryPool::new(64 * 1024 * 1024));
        let prepared = prepare(&input, pool.clone()).unwrap().unwrap();
        let outputs = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut outputs = Vec::new();
            for mut stream in prepared.streams.into_owned_iter() {
                while let Some(batch) = stream.try_next().await.unwrap() {
                    outputs.push(batch);
                }
            }
            outputs
        })
        .await
        .expect("retaining output must not make EOF depend on releasing that output");
        let mut values: Vec<_> = outputs
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect();
        values.sort_unstable();
        assert_eq!(values, (0..128).collect::<Vec<_>>());
        drop(prepared.pool);
        drop(input);
        assert!(
            pool.used() > 0,
            "output buffers must retain their admission"
        );
        drop(outputs);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn exhausted_input_budget_refuses_without_waiting_for_retained_output() {
        let batch =
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![42])) as ArrayRef)])
                .unwrap();
        let input = MemoryTableExec::new("resident", batch.schema(), vec![batch], None);
        let pool = Arc::new(crate::execution::MemoryPool::new(64 * 1024 * 1024));
        let prepared = prepare(&input, pool.clone()).unwrap().unwrap();
        let held = prepared.pool.allocate(prepared.pool.available()).unwrap();
        let mut streams = prepared.streams.into_owned_iter();
        let mut stream = streams.next().unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), stream.try_next())
            .await
            .expect("memory refusal must not wait for a consumer")
            .unwrap_err();
        assert!(error.is_memory_limit());
        drop(stream);
        drop(streams);
        drop(held);
        drop(prepared.pool);
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod quantum_tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use futures::TryStreamExt;

    #[tokio::test]
    async fn retained_outputs_can_grow_beyond_input_progress_credit() {
        let rows = 600_000i64;
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
        )])
        .unwrap();
        let input = MemoryTableExec::new("resident", batch.schema(), vec![batch], None);
        let pool = Arc::new(crate::execution::MemoryPool::new(32 * 1024 * 1024));
        // Account for the resident source independently of copied output.
        let resident = pool.allocate(rows as usize * 8).unwrap();
        let prepared = prepare(&input, pool.clone()).unwrap().unwrap();
        let mut outputs = Vec::new();
        for mut stream in prepared.streams.into_owned_iter() {
            while let Some(batch) = stream.try_next().await.unwrap_or_else(|error| {
                panic!(
                    "retained output refused with {} parent bytes free: {error}",
                    pool.available()
                )
            }) {
                outputs.push(batch);
            }
        }
        let mut seen = 0i64;
        for batch in &outputs {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for value in values.iter() {
                assert_eq!(value, Some(seen));
                seen += 1;
            }
        }
        assert_eq!(seen, rows);
        drop(prepared.pool);
        drop(input);
        drop(resident);
        assert!(pool.used() >= rows as usize * 8);
        drop(outputs);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn byte_budget_preserves_a_fitting_source_batch_and_splits_under_pressure() {
        for (limit, fits) in [(256 * 1024 * 1024, true), (8 * 1024 * 1024, false)] {
            let batch = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int64Array::from_iter_values(0..65536)) as ArrayRef,
            )])
            .unwrap();
            let input = MemoryTableExec::new("resident", batch.schema(), vec![batch], None);
            let pool = Arc::new(crate::execution::MemoryPool::new(limit));
            let prepared = prepare(&input, pool.clone()).unwrap().unwrap();
            let mut streams = prepared.streams.into_owned_iter();
            let mut stream = streams.next().unwrap();
            let mut seen = 0i64;
            let mut count = 0;
            while let Some(batch) = stream.try_next().await.unwrap() {
                count += 1;
                if fits {
                    assert_eq!(batch.num_rows(), 65536, "fitting input was fragmented");
                }
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for value in values.iter() {
                    assert_eq!(value, Some(seen));
                    seen += 1;
                }
            }
            assert_eq!(seen, 65536);
            if !fits {
                assert!(count > 1);
            }
            drop(stream);
            drop(streams);
            drop(prepared.pool);
            assert_eq!(pool.used(), 0);
        }
    }
}

#[cfg(test)]
mod contiguous_tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use futures::TryStreamExt;
    #[tokio::test]
    async fn contiguous_source_fits_without_row_index_staging() {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from_iter_values(0..65536)) as ArrayRef,
        )])
        .unwrap();
        let input = MemoryTableExec::new("resident", batch.schema(), vec![batch], None);
        // The per-frame byte allowance fits values and metadata, but not a
        // row-index tuple for every value. An identity copy needs no such list.
        let pool = Arc::new(crate::execution::MemoryPool::new(64 * 1024 * 1024));
        let prepared = prepare(&input, pool.clone()).unwrap().unwrap();
        let mut streams = prepared.streams.into_owned_iter();
        let mut stream = streams.next().unwrap();
        let output = stream.try_next().await.unwrap().unwrap();
        assert_eq!(
            output.num_rows(),
            65536,
            "contiguous copy spent its budget on row indices"
        );
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(values
            .values()
            .iter()
            .enumerate()
            .all(|(i, v)| *v == i as i64));
        assert!(stream.try_next().await.unwrap().is_none());
        let retained = output.slice(3, 17);
        drop(output);
        drop(stream);
        drop(streams);
        drop(prepared.pool);
        drop(input);
        assert!(pool.used() > 0);
        drop(retained);
        assert_eq!(pool.used(), 0);
    }
}
