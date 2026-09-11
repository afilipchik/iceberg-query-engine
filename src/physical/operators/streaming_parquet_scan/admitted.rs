//! Live pressure-path adapter. Footer/schema preparation remains outside decoder
//! admission; no claim is made that this alone accounts for total query RSS.
use super::*;
use crate::{
    execution::{reserved_vec::ReservedVec, ReservedBufferBuilder, SharedMemoryPool},
    physical::compiled_expr::CompiledPredicate,
    storage::{
        admitted_batch, admitted_flat_column::AdmittedFlatColumn, admitted_gather,
        admitted_row_group,
    },
};
use arrow::{
    array::{Array, BooleanArray, Int32Array, Int64Array},
    buffer::BooleanBuffer,
};
use std::fs::File;

const VALUE_BYTES: usize = 64 * 1024;
fn unsupported(message: &str) -> QueryError {
    QueryError::NotImplemented(format!("admitted Parquet scan: {message}"))
}

struct Reader {
    input: admitted_batch::AdmittedBatchReader<AdmittedFlatColumn<File>>,
    predicate: Option<CompiledPredicate>,
    runtime: ReservedVec<(usize, Arc<RuntimeFilterPayload>)>,
    output_positions: ReservedVec<usize>,
    pending_output: Option<(RecordBatch, usize)>,
    // Drop input/schema/program owners before releasing construction admission.
    _schema_reservation: crate::execution::MemoryReservation,
}

impl Reader {
    fn open(work: &RowGroupWork, state: &State) -> Result<Self> {
        // The plan retains the plain view alongside the legacy dictionary view;
        // do not reconstruct Arrow metadata or parse embedded schema per group.
        let metadata = work.snapshot.plain_metadata();
        let field_count = metadata.schema().fields().len();
        // Schema::project clones the metadata map and Field Arcs. Reserve before
        // that construction and retain the lease with the reader's schema owner.
        // Original plan/footer storage remains a distinct ownership contract.
        let schema_bytes = metadata
            .schema()
            .metadata()
            .iter()
            .try_fold(
                field_count
                    .checked_mul(4096)
                    .and_then(|n| n.checked_add(512))
                    .ok_or_else(|| unsupported("schema admission extent overflow"))?,
                |bytes, (key, value)| {
                    bytes
                        .checked_add(512)?
                        .checked_add(key.capacity().checked_mul(2)?)?
                        .checked_add(value.capacity().checked_mul(2)?)
                },
            )
            .ok_or_else(|| unsupported("schema admission extent overflow"))?;
        let schema_reservation = state.pool.allocate(schema_bytes)?;
        let mut indices = ReservedVec::with_capacity(&state.pool, field_count)?;
        let outputs: &[usize];
        let all;
        if let Some(projection) = state.projection.as_ref() {
            outputs = projection;
        } else {
            let mut owned = ReservedVec::with_capacity(&state.pool, field_count)?;
            owned.extend_reserved(field_count, 0..field_count)?;
            all = owned;
            outputs = all.as_slice();
        }
        let mut add = |index| -> Result<()> {
            if index >= field_count {
                return Err(unsupported("column outside schema"));
            }
            if !indices.as_slice().contains(&index) {
                indices.extend_reserved(1, [index])?;
            }
            Ok(())
        };
        for &index in outputs {
            add(index)?;
        }
        if let Some((_, columns)) = state.filter.as_ref() {
            for &index in columns {
                add(index)?;
            }
        }
        let config = state.runtime.lock();
        let mut runtime = ReservedVec::with_capacity(&state.pool, config.len())?;
        for (index, slot) in config.iter() {
            if let Some(payload) = slot.lock().clone() {
                add(*index)?;
                runtime.extend_reserved(1, [(*index, payload)])?;
            }
        }
        drop(config);
        let read_schema = Arc::new(metadata.schema().project(indices.as_slice())?);
        let mut output_positions = ReservedVec::with_capacity(&state.pool, outputs.len())?;
        for index in outputs {
            output_positions.extend_reserved(
                1,
                [indices.as_slice().iter().position(|i| i == index).unwrap()],
            )?;
        }
        for (index, _) in runtime.as_mut_slice() {
            *index = indices.as_slice().iter().position(|i| i == index).unwrap();
            if !matches!(
                read_schema.field(*index).data_type(),
                arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64
            ) {
                return Err(unsupported("runtime key requires a signed integer column"));
            }
        }
        let predicate = match state.filter.as_ref() {
            Some((expr, _)) => Some(
                CompiledPredicate::compile_reserved(expr, &read_schema, &state.pool)?
                    .ok_or_else(|| unsupported("static predicate has no admitted evaluator"))?,
            ),
            None => None,
        };
        let file = work.snapshot.open_file()?;
        let input = admitted_row_group::open(
            &file,
            &metadata,
            work.row_group_idx,
            indices.as_slice(),
            read_schema,
            &state.pool,
        )?;
        Ok(Self {
            input,
            predicate,
            runtime,
            output_positions,
            pending_output: None,
            _schema_reservation: schema_reservation,
        })
    }

    fn next(
        &mut self,
        output: &SchemaRef,
        pool: &SharedMemoryPool,
        max_rows: usize,
    ) -> Result<Option<RecordBatch>> {
        if self.pending_output.is_none() {
            self.pending_output = self
                .next_chunk(output, pool, max_rows)?
                .map(|batch| (batch, 0));
        }
        let Some((batch, offset)) = self.pending_output.as_ref() else {
            return Ok(None);
        };
        if *offset == 0
            && (batch.num_rows() == max_rows
                || (self.predicate.is_none() && self.runtime.as_slice().is_empty()))
        {
            return Ok(self.pending_output.take().map(|(batch, _)| batch));
        }
        // Packing is optional construction after a complete admitted chunk. A
        // construction-only refusal may hand that whole chunk off unchanged.
        // It must never replay input or return an already-consumed prefix.
        let mut accumulator = match crate::storage::admitted_coalesce::BatchAccumulator::new(
            output.clone(),
            max_rows,
            VALUE_BYTES,
            pool,
        ) {
            Ok(accumulator) => accumulator,
            Err(error) if error.is_memory_limit() && *offset == 0 => {
                return Ok(self.pending_output.take().map(|(batch, _)| batch));
            }
            Err(error) => return Err(error),
        };
        loop {
            let (batch, offset) = self
                .pending_output
                .as_mut()
                .expect("pending admitted chunk");
            let consumed = accumulator.append(batch, *offset)?;
            *offset += consumed;
            if consumed == 0 {
                if accumulator.rows() > 0 {
                    return accumulator.finish().map(Some);
                }
                if *offset == 0 {
                    // A single already-admitted long UTF8 value may exceed the
                    // packing byte target. Hand it off without copying or retry.
                    drop(accumulator);
                    return Ok(self.pending_output.take().map(|(batch, _)| batch));
                }
                return Err(unsupported(
                    "remaining selected value exceeds packing capacity",
                ));
            }
            if *offset == batch.num_rows() {
                self.pending_output = None;
            }
            if accumulator.full() {
                return accumulator.finish().map(Some);
            }
            if self.pending_output.is_none() {
                // Source/decoder errors are terminal, even when a prefix exists.
                // No error here is converted into replay or a different decoder.
                match self.next_chunk(output, pool, max_rows)? {
                    Some(batch) => self.pending_output = Some((batch, 0)),
                    None => return accumulator.finish().map(Some),
                }
            }
        }
    }

    fn next_chunk(
        &mut self,
        output: &SchemaRef,
        pool: &SharedMemoryPool,
        max_rows: usize,
    ) -> Result<Option<RecordBatch>> {
        loop {
            let Some(batch) = self.input.next(max_rows, VALUE_BYTES, pool)? else {
                return Ok(None);
            };
            if self.predicate.is_some() || !self.runtime.as_slice().is_empty() {
                let static_mask = self
                    .predicate
                    .as_ref()
                    .map(|predicate| {
                        predicate
                            .evaluate_admitted(&batch, pool)?
                            .ok_or_else(|| unsupported("predicate runtime type mismatch"))
                    })
                    .transpose()?;
                let mask = if self.runtime.as_slice().is_empty() {
                    // The gather already implements WHERE's valid-and-true rule.
                    // A static-only nullable bitmap needs no normalization copy.
                    static_mask.expect("a predicate or runtime filter exists")
                } else {
                    let rows = batch.num_rows();
                    let mut bits =
                        ReservedBufferBuilder::<u8>::with_capacity(pool, rows.div_ceil(8))?;
                    bits.extend_reserved(rows.div_ceil(8), std::iter::repeat(0))?;
                    for row in 0..rows {
                        let mut keep = static_mask
                            .as_ref()
                            .is_none_or(|mask| mask.is_valid(row) && mask.value(row));
                        for (column, payload) in self.runtime.as_slice() {
                            if !keep {
                                break;
                            }
                            let array = batch.column(*column);
                            if array.is_null(row) {
                                keep = false;
                                continue;
                            }
                            let value = if let Some(values) =
                                array.as_any().downcast_ref::<Int64Array>()
                            {
                                values.value(row)
                            } else if let Some(values) = array.as_any().downcast_ref::<Int32Array>()
                            {
                                i64::from(values.value(row))
                            } else {
                                return Err(unsupported("runtime key type changed"));
                            };
                            keep = payload.contains(value);
                        }
                        if keep {
                            bits.as_mut_slice()[row / 8] |= 1 << (row % 8);
                        }
                    }
                    BooleanArray::new(BooleanBuffer::new(bits.finish(), 0, rows), None)
                };
                let filtered = admitted_gather::filter_projected(
                    &batch,
                    &mask,
                    output.clone(),
                    self.output_positions.as_slice(),
                    pool,
                )?;
                if filtered.num_rows() == 0 {
                    continue;
                }
                return Ok(Some(filtered));
            }
            if batch.num_rows() == 0 {
                continue;
            }
            let mut columns =
                ReservedVec::with_capacity(pool, self.output_positions.as_slice().len())?;
            for &position in self.output_positions.as_slice() {
                columns.extend_reserved(1, [batch.column(position).clone()])?;
            }
            return admitted_batch::finish(output.clone(), batch.num_rows(), columns, pool)
                .map(Some);
        }
    }
}

struct State {
    // Preserve the planner/configuration row target across optional preparation.
    // This is an output target, never proof that page or expression memory fits.
    batch_rows: usize,
    work: Arc<Vec<RowGroupWork>>,
    next_work: usize,
    output: SchemaRef,
    projection: Arc<Option<Vec<usize>>>,
    filter: Arc<Option<(Expr, Vec<usize>)>>,
    runtime: RuntimeFilterConfig,
    pool: SharedMemoryPool,
    reader: Option<Reader>,
}

fn state(scan: &StreamingParquetScanExec, partition: usize, pool: SharedMemoryPool) -> State {
    State {
        batch_rows: scan.batch_size,
        work: scan.partitioned_work[partition].clone(),
        next_work: 0,
        output: scan.schema.clone(),
        projection: scan.projection.clone(),
        filter: scan.filter_spec.clone(),
        runtime: scan.runtime_filter.clone(),
        pool,
        reader: None,
    }
}

fn stream(state: State) -> impl futures::Stream<Item = Result<RecordBatch>> + Send {
    futures::stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(reader) = &mut state.reader {
                if let Some(batch) = reader.next(&state.output, &state.pool, state.batch_rows)? {
                    return Ok(Some((batch, state)));
                }
                state.reader = None;
            }
            let Some(work) = state.work.get(state.next_work).cloned() else {
                return Ok(None);
            };
            state.next_work += 1;
            state.reader = Some(Reader::open(&work, &state)?);
        }
    })
}

pub(super) fn execute(scan: &StreamingParquetScanExec, partition: usize) -> RecordBatchStream {
    Box::pin(stream(state(scan, partition, scan.memory_pool.clone())))
}

pub(super) fn prepare(
    scan: &StreamingParquetScanExec,
    pool: SharedMemoryPool,
) -> Result<Option<crate::physical::PreparedAdmittedInput>> {
    // This optional descriptor is built without pulling any output or reading
    // encoded pages. A fixed-width scan already has a certified copied route;
    // release all failed preparation owners before declining to that route.
    // Other failures and every error after selection remain terminal.
    match prepare_inner(scan, pool) {
        Err(error) if scan.fixed_output.is_some() && error.is_memory_limit() => Ok(None),
        result => result,
    }
}
fn prepare_inner(
    scan: &StreamingParquetScanExec,
    pool: SharedMemoryPool,
) -> Result<Option<crate::physical::PreparedAdmittedInput>> {
    if !scan.ipc_dirs.is_empty() {
        return Ok(None);
    }
    // A compact-copy certificate describes a separate execution route; it does
    // not exclude this decoder, which admits its own buffers and scratch.
    // Reader construction validates metadata and compiles predicates, but neither
    // reads encoded pages nor emits output. Check every group before selecting
    // this capability. The outer factory only handles optional fixed-route
    // metadata refusal; I/O failures are never fallback signals.
    for partition in 0..scan.output_partitions() {
        let candidate = state(scan, partition, pool.clone());
        for work in candidate.work.iter() {
            match Reader::open(work, &candidate) {
                Ok(reader) => drop(reader),
                Err(QueryError::NotImplemented(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
        }
    }
    let mut streams = ReservedVec::with_capacity(&pool, scan.output_partitions())?;
    for partition in 0..scan.output_partitions() {
        let output =
            crate::physical::admit_stream(stream(state(scan, partition, pool.clone())), &pool)?;
        streams.extend_reserved(1, [output])?;
    }
    Ok(Some(crate::physical::PreparedAdmittedInput {
        pool,
        streams,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{BinaryOp, ScalarValue};
    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
    };
    use futures::TryStreamExt;
    use parquet::{
        basic::Compression,
        file::properties::{WriterProperties, WriterVersion},
    };

    fn fixture(path: &std::path::Path, version: WriterVersion, dictionary: bool) -> SchemaRef {
        fixture_with_codec(path, version, dictionary, Compression::SNAPPY)
    }

    fn fixture_with_codec(
        path: &std::path::Path,
        version: WriterVersion,
        dictionary: bool,
        codec: Compression,
    ) -> SchemaRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("runtime_key", DataType::Int32, true),
            Field::new("tag", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..40)),
                Arc::new(StringArray::from(
                    (0..40)
                        .map(|i| (i % 4 != 0).then(|| format!("duplicate-{}", i % 5)))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    (0..40)
                        .map(|i| (i % 7 != 0).then_some(i % 3))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..40)
                        .map(|i| (i % 11 != 0).then_some(if i % 3 == 0 { "drop" } else { "keep" }))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_writer_version(version)
            .set_dictionary_enabled(dictionary)
            .set_compression(codec)
            .set_encoding(parquet::basic::Encoding::PLAIN)
            .set_max_row_group_size(3)
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(path).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        schema
    }

    #[tokio::test]
    async fn live_pressure_projection_filters_partitions_and_owners_are_exact() {
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            for (dictionary, codec) in [
                (false, Compression::SNAPPY),
                (true, Compression::SNAPPY),
                (false, Compression::ZSTD(Default::default())),
                (true, Compression::ZSTD(Default::default())),
            ] {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("data.parquet");
                let schema = fixture_with_codec(&path, version, dictionary, codec);
                let predicate = Expr::BinaryExpr {
                    left: Box::new(Expr::BinaryExpr {
                        left: Box::new(Expr::column("tag")),
                        op: BinaryOp::Like,
                        right: Box::new(Expr::Literal(ScalarValue::Utf8("keep%".into()))),
                    }),
                    op: BinaryOp::And,
                    right: Box::new(Expr::BinaryExpr {
                        left: Box::new(Expr::column("id")),
                        op: BinaryOp::Gt,
                        right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
                    }),
                };
                let pool = crate::execution::create_memory_pool(16 * 1024 * 1024);
                let scan = StreamingParquetScanExec::try_new_with_batch_size(
                    "live",
                    &[path],
                    schema.clone(),
                    Some(vec![1, 0, 1]),
                    Some(&predicate),
                    &schema,
                    1,
                    true,
                )
                .unwrap()
                .with_memory_pressure(true)
                .with_memory_pool(pool.clone());
                for (column, keys) in [
                    (2, vec![1, 2]),
                    (0, (0..40).filter(|i| i % 2 == 0).collect()),
                ] {
                    let payload = Arc::new(RuntimeFilterPayload::Set(keys.into_iter().collect()));
                    scan.runtime_filter
                        .lock()
                        .push((column, Arc::new(parking_lot::Mutex::new(Some(payload)))));
                }
                let mut held = Vec::new();
                let mut actual = Vec::new();
                for partition in 0..scan.output_partitions() {
                    let mut stream = scan.execute(partition).await.unwrap();
                    while let Some(batch) = stream.try_next().await.unwrap() {
                        assert_eq!(batch.schema(), scan.schema());
                        assert_eq!(batch.column(0).to_data(), batch.column(2).to_data());
                        let ids = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let values = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        actual.extend((0..batch.num_rows()).map(|i| {
                            (
                                ids.value(i),
                                values.is_valid(i).then(|| values.value(i).to_owned()),
                            )
                        }));
                        held.push(batch);
                    }
                }
                actual.sort();
                let expected: Vec<_> = (0..40)
                    .filter(|i| *i > 2 && i % 2 == 0 && i % 3 != 0 && i % 7 != 0 && i % 11 != 0)
                    .map(|i| (i, (i % 4 != 0).then(|| format!("duplicate-{}", i % 5))))
                    .collect();
                assert_eq!(actual, expected);
                assert!(pool.used() > 0, "returned buffers must retain admission");
                drop(held);
                drop(scan);
                assert_eq!(pool.used(), 0);
            }
        }
    }

    #[tokio::test]
    async fn duckdb_padded_blocks_preserve_exact_nullable_rows() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/duckdb-1.4.4-padded-runs.parquet");
        let schema = crate::storage::metadata_cache::cached_metadata(&path)
            .unwrap()
            .schema()
            .clone();
        let pool = crate::execution::create_memory_pool(2 * 1024 * 1024);
        let scan = StreamingParquetScanExec::try_new_with_batch_size(
            "duck_fixture",
            &[path],
            schema.clone(),
            None,
            None,
            &schema,
            1,
            true,
        )
        .unwrap()
        .with_memory_pressure(true)
        .with_memory_pool(pool.clone());
        let mut actual = Vec::new();
        for partition in 0..scan.output_partitions() {
            let mut stream = scan.execute(partition).await.unwrap();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                actual.extend((0..batch.num_rows()).map(|i| {
                    (
                        ids.value(i),
                        values.is_valid(i).then(|| values.value(i).to_owned()),
                    )
                }));
            }
        }
        actual.sort();
        let expected: Vec<_> = (0..37)
            .map(|i| (i, (i % 5 != 0).then(|| format!("group-{}", i % 3))))
            .collect();
        assert_eq!(actual, expected);
        drop(scan);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn schema_metadata_clone_is_admitted_before_reader_construction() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.parquet");
        let schema = Arc::new(
            Schema::new(vec![Field::new("text", DataType::Utf8, false)]).with_metadata(
                std::collections::HashMap::from([("large".into(), "x".repeat(65536))]),
            ),
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema.clone(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let pool = crate::execution::create_memory_pool(32768);
        let scan = StreamingParquetScanExec::try_new_with_batch_size(
            "metadata",
            &[path],
            schema.clone(),
            None,
            None,
            &schema,
            1,
            true,
        )
        .unwrap()
        .with_memory_pressure(true)
        .with_memory_pool(pool.clone());
        assert_eq!(
            scan.partitioned_work[0][0]
                .snapshot
                .plain_metadata()
                .schema()
                .metadata()["large"]
                .len(),
            65536
        );
        let mut stream = scan.execute(0).await.unwrap();
        assert!(stream.try_next().await.unwrap_err().is_memory_limit());
        assert!(stream.try_next().await.unwrap().is_none());
        drop(stream);
        drop(scan);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn live_pressure_denial_terminates_without_legacy_replay() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        let schema = fixture(&path, WriterVersion::PARQUET_1_0, true);
        let pool = crate::execution::create_memory_pool(128);
        let scan = StreamingParquetScanExec::try_new_with_batch_size(
            "denied",
            &[path],
            schema.clone(),
            None,
            None,
            &schema,
            1,
            true,
        )
        .unwrap()
        .with_memory_pressure(true)
        .with_memory_pool(pool.clone());
        let mut stream = scan.execute(0).await.unwrap();
        assert!(stream.try_next().await.unwrap_err().is_memory_limit());
        assert!(stream.try_next().await.unwrap().is_none());
        drop(stream);
        drop(scan);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn live_pressure_unsupported_codec_refuses_without_legacy_replay() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        let schema = fixture_with_codec(
            &path,
            WriterVersion::PARQUET_1_0,
            true,
            Compression::GZIP(Default::default()),
        );
        let pool = crate::execution::create_memory_pool(16 * 1024 * 1024);
        let scan = StreamingParquetScanExec::try_new_with_batch_size(
            "unsupported",
            &[path],
            schema.clone(),
            None,
            None,
            &schema,
            1,
            true,
        )
        .unwrap()
        .with_memory_pressure(true)
        .with_memory_pool(pool.clone());
        let mut stream = scan.execute(0).await.unwrap();
        let error = stream.try_next().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("codec or encoding requires another reader"),
            "{error}"
        );
        assert!(stream.try_next().await.unwrap().is_none());
        drop(stream);
        drop(scan);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn fixed_width_admitted_scan_preserves_decimal_nulls_and_partitions() {
        use arrow::array::{Date32Array, Decimal128Array};
        for (version, dictionary) in [
            (WriterVersion::PARQUET_1_0, false),
            (WriterVersion::PARQUET_2_0, true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixed.parquet");
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("amount", DataType::Decimal128(15, 2), true),
                Field::new("day", DataType::Date32, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values((0..48).map(|i| i % 4))),
                    Arc::new(
                        Decimal128Array::from(
                            (0..48)
                                .map(|i| (i % 7 != 0).then_some(i as i128 * 101 - 2000))
                                .collect::<Vec<_>>(),
                        )
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                    ),
                    Arc::new(Date32Array::from(
                        (0..48)
                            .map(|i| (i % 5 != 0).then_some(i - 20))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let props = WriterProperties::builder()
                .set_writer_version(version)
                .set_dictionary_enabled(dictionary)
                .set_compression(Compression::ZSTD(Default::default()))
                .set_max_row_group_row_count(Some(5))
                .build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                File::create(&path).unwrap(),
                schema.clone(),
                Some(props),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let workers = rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap();
            let pool = crate::execution::create_memory_pool(8 * 1024 * 1024);
            let scan = workers
                .install(|| {
                    StreamingParquetScanExec::try_new_with_batch_size(
                        "fixed",
                        &[path],
                        schema.clone(),
                        None,
                        None,
                        &schema,
                        3,
                        true,
                    )
                    .unwrap()
                })
                .with_memory_pool(pool.clone());
            assert_eq!(scan.output_partitions(), 3);
            assert!(scan.fixed_output.is_some());
            let prepared = scan
                .prepare_admitted_queue_input(pool.clone())
                .await
                .unwrap()
                .expect(
                    "a copied-output certificate must not exclude separately admitted decoding",
                );
            assert_eq!(prepared.streams.as_slice().len(), 3);
            let mut actual = vec![];
            let mut held = vec![];
            for mut stream in prepared.streams.into_owned_iter() {
                while let Some(batch) = stream.try_next().await.unwrap() {
                    let id = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let amount = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap();
                    let day = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap();
                    assert_eq!(amount.data_type(), &DataType::Decimal128(15, 2));
                    for row in 0..batch.num_rows() {
                        actual.push((
                            id.value(row),
                            (!amount.is_null(row)).then(|| amount.value(row)),
                            (!day.is_null(row)).then(|| day.value(row)),
                        ));
                    }
                    held.extend(batch.columns().iter().cloned());
                }
            }
            let mut expected = (0..48)
                .map(|i| {
                    (
                        i as i64 % 4,
                        (i % 7 != 0).then_some(i as i128 * 101 - 2000),
                        (i % 5 != 0).then_some(i - 20),
                    )
                })
                .collect::<Vec<_>>();
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
            drop(prepared.pool);
            drop(scan);
            assert!(
                pool.used() > 0,
                "detached output arrays must retain admission"
            );
            drop(held);
            assert_eq!(pool.used(), 0);
        }
    }

    #[tokio::test]
    async fn fixed_scan_preparation_and_late_errors_preserve_fallback_boundary() {
        for codec in [
            Compression::UNCOMPRESSED,
            Compression::GZIP(Default::default()),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixed.parquet");
            let batch = RecordBatch::try_from_iter(vec![(
                "id",
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])) as arrow::array::ArrayRef,
            )])
            .unwrap();
            let schema = batch.schema();
            let props = WriterProperties::builder().set_compression(codec).build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                File::create(&path).unwrap(),
                schema.clone(),
                Some(props),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let pool = crate::execution::create_memory_pool(1024 * 1024);
            let scan = StreamingParquetScanExec::try_new_with_batch_size(
                "fixed",
                &[path.clone()],
                schema.clone(),
                None,
                None,
                &schema,
                3,
                true,
            )
            .unwrap()
            .with_memory_pool(pool.clone());
            assert!(scan.fixed_output.is_some());
            if matches!(codec, Compression::GZIP(_)) {
                assert!(scan
                    .prepare_admitted_queue_input(pool.clone())
                    .await
                    .unwrap()
                    .is_none());
                assert_eq!(pool.used(), 0);
                let mut ordinary = scan.execute(0).await.unwrap();
                assert_eq!(ordinary.try_next().await.unwrap().unwrap().num_rows(), 3);
                assert!(ordinary.try_next().await.unwrap().is_none());
                continue;
            }
            let tiny = crate::execution::create_memory_pool(128);
            assert!(scan
                .prepare_admitted_queue_input(tiny.clone())
                .await
                .unwrap()
                .is_none());
            assert!(scan.pool_independent_queue_copy_bound().is_some());
            assert_eq!(tiny.used(), 0);
            let mut pressured = scan
                .prepare_admitted_queue_input(pool.clone())
                .await
                .unwrap()
                .unwrap();
            let pressure = pool.allocate(pool.available()).unwrap();
            let output = &mut pressured.streams.as_mut_slice()[0];
            assert!(output.try_next().await.unwrap_err().is_memory_limit());
            drop(pressure);
            assert!(output.try_next().await.unwrap().is_none());
            drop(pressured);
            assert_eq!(pool.used(), 0);
            let mut prepared = scan
                .prepare_admitted_queue_input(pool.clone())
                .await
                .unwrap()
                .unwrap();
            // Preparation reads metadata, not pages. Once selected, an I/O failure
            // terminates its exact stream even if the path becomes readable later.
            let saved = directory.path().join("saved.parquet");
            std::fs::rename(&path, &saved).unwrap();
            let output = &mut prepared.streams.as_mut_slice()[0];
            assert!(output.try_next().await.is_err());
            std::fs::rename(&saved, &path).unwrap();
            assert!(output.try_next().await.unwrap().is_none());
            drop(prepared);
            drop(scan);
            assert_eq!(pool.used(), 0);
        }
    }
}

#[cfg(test)]
#[path = "admitted_quantum_tests.rs"]
mod quantum_tests;

#[cfg(test)]
#[path = "admitted_filter_quantum_tests.rs"]
mod filter_quantum_tests;
