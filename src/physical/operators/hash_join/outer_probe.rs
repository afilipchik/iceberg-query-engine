//! Pull-driven outer joins: one probe batch, bounded candidate/output chunks,
//! and round-local post-filter match state. No partition-wide output vector.
use super::*;
use crate::execution::{reserved_vec::ReservedVec, MemoryReservation, SharedMemoryPool};
use crate::storage::admitted_selection::{self as outer_gather, Row};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("bounded outer join: {message}"))
}
#[derive(Debug)]
pub(super) struct Round {
    matched: ReservedVec<ReservedVec<AtomicBool>>,
    claimed: ReservedVec<AtomicBool>,
    completed: AtomicUsize,
    active: AtomicUsize,
    failed: AtomicBool,
    _metadata: MemoryReservation,
}
impl Round {
    fn new(cache: &BuildSideCache, partitions: usize, pool: &SharedMemoryPool) -> Result<Self> {
        let metadata = pool.allocate(512)?;
        let mut matched = ReservedVec::with_capacity(pool, cache.batches.len())?;
        for batch in &cache.batches {
            let mut bits = ReservedVec::with_capacity(pool, batch.num_rows())?;
            bits.extend_reserved(
                batch.num_rows(),
                (0..batch.num_rows()).map(|_| AtomicBool::new(false)),
            )?;
            matched.extend_reserved(1, [bits])?;
        }
        let mut claimed = ReservedVec::with_capacity(pool, partitions)?;
        claimed.extend_reserved(partitions, (0..partitions).map(|_| AtomicBool::new(false)))?;
        Ok(Self {
            matched,
            claimed,
            completed: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            _metadata: metadata,
        })
    }
}
struct Ticket {
    round: Arc<Round>,
    complete: bool,
}
impl Ticket {
    fn claim(
        registry: &Mutex<Option<Arc<Round>>>,
        cache: &BuildSideCache,
        partition: usize,
        partitions: usize,
        pool: &SharedMemoryPool,
    ) -> Result<Self> {
        let mut registry = registry
            .lock()
            .map_err(|_| invalid("round registry poisoned"))?;
        let replace = match registry.as_ref() {
            None => true,
            Some(round) => {
                let claimed = round
                    .claimed
                    .as_slice()
                    .get(partition)
                    .ok_or_else(|| invalid("partition outside round"))?;
                if !claimed.load(Ordering::Acquire) {
                    false
                } else if round
                    .claimed
                    .as_slice()
                    .iter()
                    .all(|v| v.load(Ordering::Acquire))
                    || round.active.load(Ordering::Acquire) == 0
                {
                    true
                } else {
                    return Err(invalid("overlapping partial execution rounds"));
                }
            }
        };
        if replace {
            *registry = Some(Arc::new(Round::new(cache, partitions, pool)?));
        }
        let round = registry.as_ref().unwrap().clone();
        round.claimed.as_slice()[partition].store(true, Ordering::Release);
        round.active.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            round,
            complete: false,
        })
    }
    fn finish(&mut self) -> Result<bool> {
        if self.round.failed.load(Ordering::Acquire) {
            return Err(invalid("a probe partition failed or was cancelled"));
        }
        self.complete = true;
        Ok(self.round.completed.fetch_add(1, Ordering::AcqRel) + 1
            == self.round.claimed.as_slice().len())
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        if !self.complete {
            self.round.failed.store(true, Ordering::Release);
        }
        self.round.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) fn eligible(join: &HashJoinExec) -> bool {
    matches!(
        join.join_type,
        JoinType::Left | JoinType::Right | JoinType::Full
    ) && admitted_eligible(join)
}

pub(super) fn admitted_eligible(join: &HashJoinExec) -> bool {
    if !matches!(
        join.join_type,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
    ) || join.on.is_empty()
        || join.schema.fields().is_empty()
        || join
            .schema
            .fields()
            .iter()
            .any(|f| !outer_gather::supported(f.data_type()))
        || (join.filter.is_some() && join.retained.is_some())
        || join.filter.as_ref().is_some_and(|f| f.contains_subquery())
    {
        return false;
    }
    // Evaluated computed/variable-width key ownership needs its own factory
    // proof. Decline before consuming anything; keep the guarded legacy route.
    let left_schema = crate::planner::PlanSchema::from_qualified_arrow(join.left.schema().as_ref());
    let right_schema =
        crate::planner::PlanSchema::from_qualified_arrow(join.right.schema().as_ref());
    join.on.iter().all(|(left, right)| {
        [(left, &left_schema), (right, &right_schema)]
            .iter()
            .all(|(e, schema)| {
                if !matches!(e, Expr::Column(_)) {
                    return false;
                }
                let Ok(mut dt) = e.data_type(schema) else {
                    return false;
                };
                while let arrow::datatypes::DataType::Dictionary(_, value) = dt {
                    dt = *value;
                }
                matches!(
                    dt,
                    arrow::datatypes::DataType::Int8
                        | arrow::datatypes::DataType::Int16
                        | arrow::datatypes::DataType::Int32
                        | arrow::datatypes::DataType::Int64
                        | arrow::datatypes::DataType::UInt8
                        | arrow::datatypes::DataType::UInt16
                        | arrow::datatypes::DataType::UInt32
                        | arrow::datatypes::DataType::UInt64
                        | arrow::datatypes::DataType::Float32
                        | arrow::datatypes::DataType::Float64
                        | arrow::datatypes::DataType::Date32
                        | arrow::datatypes::DataType::Date64
                        | arrow::datatypes::DataType::Decimal128(_, _)
                )
            })
    })
}

pub(super) fn stream(
    join: &HashJoinExec,
    cache: Arc<BuildSideCache>,
    input: RecordBatchStream,
    keys: Vec<Expr>,
    swapped: bool,
    partition: usize,
    admitted_input: bool,
    pool: SharedMemoryPool,
) -> Result<RecordBatchStream> {
    let candidate_rows =
        (pool.max() / join.output_partitions().max(1) / 8 / (2 * std::mem::size_of::<Row>()))
            .clamp(1, INNER_CANDIDATE_ROWS);
    crate::physical::check_partition(join, partition)?;
    let inner = join.join_type == JoinType::Inner;
    // Inner results have no unmatched-build phase and need no shared match map.
    let ticket = if inner {
        None
    } else {
        Some(Ticket::claim(
            &join.outer_round,
            &cache,
            partition,
            join.output_partitions(),
            &pool,
        )?)
    };
    let left_columns = join.left.schema().fields().len();
    let build_columns = if swapped {
        join.right.schema().fields().len()
    } else {
        left_columns
    };
    let probe_columns = if swapped {
        left_columns
    } else {
        join.right.schema().fields().len()
    };
    let probe_keep: Vec<usize> = (0..probe_columns)
        .filter(|i| {
            join.retained
                .as_ref()
                .is_none_or(|m| m[if swapped { *i } else { left_columns + *i }])
        })
        .collect();
    let build_count = cache.batches.first().map_or_else(
        || {
            (0..build_columns)
                .filter(|i| {
                    join.retained
                        .as_ref()
                        .is_none_or(|m| m[if swapped { left_columns + *i } else { *i }])
                })
                .count()
        },
        RecordBatch::num_columns,
    );
    let (build_fields, probe_fields) = if swapped {
        (
            &join.schema.fields()[probe_keep.len()..],
            &join.schema.fields()[..probe_keep.len()],
        )
    } else {
        (
            &join.schema.fields()[..build_count],
            &join.schema.fields()[build_count..],
        )
    };
    let build_schema = Arc::new(Schema::new(build_fields.to_vec()));
    let probe_schema = Arc::new(Schema::new(probe_fields.to_vec()));
    let mut build_map = ReservedVec::with_capacity(&pool, build_count)?;
    build_map.extend_reserved(build_count, 0..build_count)?;
    let mut probe_map = ReservedVec::with_capacity(&pool, probe_keep.len())?;
    probe_map.extend_reserved(probe_keep.len(), probe_keep)?;
    let state = Outer {
        profile: JoinProfile::new(
            if inner {
                "admitted_inner_probe"
            } else {
                "outer_probe"
            },
            &keys,
        ),
        cache,
        input,
        ticket,
        pool: pool.clone(),
        keys_expr: keys,
        swapped,
        preserve_probe: join.join_type == JoinType::Full
            || (join.join_type == JoinType::Left && swapped)
            || (join.join_type == JoinType::Right && !swapped),
        preserve_build: join.join_type == JoinType::Full
            || (join.join_type == JoinType::Left && !swapped)
            || (join.join_type == JoinType::Right && swapped),
        schema: join.schema.clone(),
        combined_schema: join.combined_schema.clone(),
        filter: join.filter.clone(),
        build_schema,
        probe_schema,
        build_map,
        probe_map,
        batch: None,
        keys: ReservedVec::with_capacity(&pool, 0)?,
        hashes: ReservedVec::with_capacity(&pool, 0)?,
        input_lease: None,
        admitted_input,
        candidate_rows,
        matched: ReservedVec::with_capacity(&pool, 0)?,
        row: 0,
        entry: u32::MAX,
        started: false,
        unmatched_probe: 0,
        input_done: false,
        emit_build: false,
        build_batch: 0,
        build_row: 0,
        pending_build: ReservedVec::with_capacity(&pool, candidate_rows)?,
        pending_probe: ReservedVec::with_capacity(&pool, candidate_rows)?,
        pending_offset: 0,
    };
    crate::physical::plan::admit_stream(
        stream::try_unfold(state, |mut state| async move {
            match state.next().await? {
                Some(batch) => Ok(Some((batch, state))),
                None => Ok(None),
            }
        }),
        &pool,
    )
}
struct Outer {
    profile: Option<JoinProfile>,
    cache: Arc<BuildSideCache>,
    input: RecordBatchStream,
    ticket: Option<Ticket>,
    pool: SharedMemoryPool,
    keys_expr: Vec<Expr>,
    swapped: bool,
    preserve_probe: bool,
    preserve_build: bool,
    schema: SchemaRef,
    combined_schema: SchemaRef,
    filter: Option<Expr>,
    build_schema: SchemaRef,
    probe_schema: SchemaRef,
    build_map: ReservedVec<usize>,
    probe_map: ReservedVec<usize>,
    batch: Option<RecordBatch>,
    keys: ReservedVec<ArrayRef>,
    hashes: ReservedVec<u64>,
    input_lease: Option<MemoryReservation>,
    admitted_input: bool,
    candidate_rows: usize,
    matched: ReservedVec<bool>,
    row: usize,
    entry: u32,
    started: bool,
    unmatched_probe: usize,
    input_done: bool,
    emit_build: bool,
    build_batch: usize,
    build_row: usize,
    pending_build: ReservedVec<Row>,
    pending_probe: ReservedVec<Row>,
    pending_offset: usize,
}
impl Outer {
    fn reset_batch(&mut self) {
        self.batch = None;
        self.keys.truncate(0);
        self.hashes.truncate(0);
        self.input_lease = None;
        self.matched.truncate(0);
    }
    fn gather_pending(&mut self, shrink: bool) -> Result<RecordBatch> {
        let remaining = self.pending_build.as_slice().len() - self.pending_offset;
        let mut count = remaining;
        loop {
            let range = self.pending_offset..self.pending_offset + count;
            let gather = || -> Result<RecordBatch> {
                let build = outer_gather::gather(
                    &self.cache.batches,
                    &self.pending_build.as_slice()[range.clone()],
                    self.build_map.as_slice(),
                    self.build_schema.clone(),
                    &self.pool,
                )?;
                let probe = outer_gather::gather(
                    self.batch.as_ref().map(std::slice::from_ref).unwrap_or(&[]),
                    &self.pending_probe.as_slice()[range.clone()],
                    self.probe_map.as_slice(),
                    self.probe_schema.clone(),
                    &self.pool,
                )?;
                let mut columns =
                    ReservedVec::with_capacity(&self.pool, self.schema.fields().len())?;
                let (left, right) = if self.swapped {
                    (&probe, &build)
                } else {
                    (&build, &probe)
                };
                columns.extend_reserved(left.num_columns(), left.columns().iter().cloned())?;
                columns.extend_reserved(right.num_columns(), right.columns().iter().cloned())?;
                crate::storage::admitted_batch::finish(
                    self.schema.clone(),
                    count,
                    columns,
                    &self.pool,
                )
            };
            match gather() {
                Err(e) if e.is_memory_limit() && shrink && count > 1 => count = count.div_ceil(2),
                Err(e) => return Err(e),
                Ok(batch) => {
                    self.pending_offset += count;
                    return Ok(batch);
                }
            }
        }
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.pending_offset < self.pending_build.as_slice().len() {
                JoinProfile::phase(&mut self.profile, 4);
                let batch = self.gather_pending(true)?;
                if let Some(profile) = &mut self.profile {
                    profile.output_rows += batch.num_rows();
                    profile.output_batches += 1;
                }
                JoinProfile::phase(&mut self.profile, 6);
                return Ok(Some(batch));
            }
            self.pending_build.truncate(0);
            self.pending_probe.truncate(0);
            self.pending_offset = 0;
            if self.input_done {
                JoinProfile::phase(&mut self.profile, 10);
                if self.emit_build {
                    while self.build_batch < self.cache.batches.len()
                        && self.pending_build.as_slice().len() < self.candidate_rows
                    {
                        let rows = self.cache.batches[self.build_batch].num_rows();
                        if self.build_row == rows {
                            self.build_batch += 1;
                            self.build_row = 0;
                            continue;
                        }
                        let row = self.build_row;
                        self.build_row += 1;
                        if !self
                            .ticket
                            .as_ref()
                            .ok_or_else(|| invalid("missing outer execution ticket"))?
                            .round
                            .matched
                            .as_slice()[self.build_batch]
                            .as_slice()[row]
                            .load(Ordering::Acquire)
                        {
                            self.pending_build
                                .extend_reserved(1, [Some((self.build_batch, row))])?;
                            self.pending_probe.extend_reserved(1, [None])?;
                        }
                        if self.build_row % INNER_PROBE_WORK_STEPS == 0 {
                            JoinProfile::phase(&mut self.profile, 5);
                            tokio::task::yield_now().await;
                            JoinProfile::phase(&mut self.profile, 10);
                        }
                    }
                    if !self.pending_build.as_slice().is_empty() {
                        continue;
                    }
                }
                if let Some(profile) = &mut self.profile {
                    profile.completed = true;
                }
                return Ok(None);
            }
            if self.batch.is_none() {
                JoinProfile::phase(&mut self.profile, 0);
                let Some(batch) = self.input.try_next().await? else {
                    self.input_done = true;
                    self.emit_build = match self.ticket.as_mut() {
                        Some(ticket) => ticket.finish()? && self.preserve_build,
                        None => false,
                    };
                    continue;
                };
                if !self.admitted_input {
                    let retained =
                        crate::execution::retained_batch::retained_batch_bytes(&batch, &self.pool)?;
                    self.input_lease = Some(self.pool.allocate(retained)?);
                }
                JoinProfile::phase(&mut self.profile, 1);
                self.keys = ReservedVec::with_capacity(&self.pool, self.keys_expr.len())?;
                for expression in &self.keys_expr {
                    let array = evaluate_expr(&batch, expression)?;
                    self.keys
                        .extend_reserved(1, [outer_gather::decode_key(array, &self.pool)?])?;
                }
                let Some(table) = self.cache.vectorized_ht.as_ref() else {
                    if self.ticket.is_some() {
                        self.matched = ReservedVec::with_capacity(&self.pool, batch.num_rows())?;
                        self.matched
                            .extend_reserved(batch.num_rows(), std::iter::repeat(false))?;
                    }
                    self.row = batch.num_rows();
                    self.unmatched_probe = 0;
                    if let Some(p) = &mut self.profile {
                        p.input_rows += batch.num_rows();
                        p.input_batches += 1;
                    }
                    self.batch = Some(batch);
                    continue;
                };
                if table.build_key_arrays.first().is_some_and(|build| {
                    build.len() != self.keys.as_slice().len()
                        || build
                            .iter()
                            .zip(self.keys.as_slice())
                            .any(|(l, r)| l.data_type() != r.data_type())
                }) {
                    return Err(invalid("key domains differ"));
                }
                if self.keys.as_slice().iter().any(|a| {
                    !matches!(
                        a.data_type(),
                        arrow::datatypes::DataType::Int8
                            | arrow::datatypes::DataType::Int16
                            | arrow::datatypes::DataType::Int32
                            | arrow::datatypes::DataType::Int64
                            | arrow::datatypes::DataType::UInt8
                            | arrow::datatypes::DataType::UInt16
                            | arrow::datatypes::DataType::UInt32
                            | arrow::datatypes::DataType::UInt64
                            | arrow::datatypes::DataType::Float32
                            | arrow::datatypes::DataType::Float64
                            | arrow::datatypes::DataType::Date32
                            | arrow::datatypes::DataType::Date64
                            | arrow::datatypes::DataType::Decimal128(_, _)
                    )
                }) {
                    return Err(invalid("unsupported key ownership domain"));
                }
                let hash_rows = if table.direct.is_some() {
                    0
                } else {
                    batch.num_rows()
                };
                self.hashes = ReservedVec::with_capacity(&self.pool, hash_rows)?;
                self.hashes
                    .extend_reserved(hash_rows, std::iter::repeat(0x517cc1b727220a95u64))?;
                if hash_rows > 0 {
                    for array in self.keys.as_slice() {
                        vectorized_hash::hash_array_into(array, self.hashes.as_mut_slice());
                    }
                }
                if self.ticket.is_some() {
                    self.matched = ReservedVec::with_capacity(&self.pool, batch.num_rows())?;
                    self.matched
                        .extend_reserved(batch.num_rows(), std::iter::repeat(false))?;
                }
                self.row = 0;
                self.entry = u32::MAX;
                self.started = false;
                self.unmatched_probe = 0;
                if let Some(p) = &mut self.profile {
                    p.input_rows += batch.num_rows();
                    p.input_batches += 1;
                }
                self.batch = Some(batch);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row == batch.num_rows() {
                JoinProfile::phase(&mut self.profile, 2);
                while self.preserve_probe
                    && self.unmatched_probe < batch.num_rows()
                    && self.pending_build.as_slice().len() < self.candidate_rows
                {
                    let row = self.unmatched_probe;
                    self.unmatched_probe += 1;
                    if !self.matched.as_slice()[row] {
                        self.pending_build.extend_reserved(1, [None])?;
                        self.pending_probe.extend_reserved(1, [Some((0, row))])?;
                    }
                    if self.unmatched_probe % INNER_PROBE_WORK_STEPS == 0 {
                        JoinProfile::phase(&mut self.profile, 5);
                        tokio::task::yield_now().await;
                        JoinProfile::phase(&mut self.profile, 2);
                    }
                }
                if !self.pending_build.as_slice().is_empty() {
                    continue;
                }
                self.reset_batch();
                continue;
            }
            JoinProfile::phase(&mut self.profile, 2);
            let table = self.cache.vectorized_ht.as_ref().unwrap();
            let mut work = 0;
            while self.row < batch.num_rows()
                && self.pending_build.as_slice().len() < self.candidate_rows
            {
                if work == INNER_PROBE_WORK_STEPS {
                    JoinProfile::phase(&mut self.profile, 5);
                    tokio::task::yield_now().await;
                    JoinProfile::phase(&mut self.profile, 2);
                    work = 0;
                }
                work += 1;
                if !self.started {
                    self.started = true;
                    self.entry = if vectorized_hash::has_null(self.keys.as_slice(), self.row) {
                        u32::MAX
                    } else if let Some((min, max)) = table.direct {
                        let keys = self.keys.as_slice()[0]
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| invalid("direct key domain differs"))?;
                        let key = keys.value(self.row);
                        if key < min || key > max {
                            u32::MAX
                        } else {
                            table.heads[(key - min) as usize]
                        }
                    } else {
                        table.heads[self.hashes.as_slice()[self.row] as usize & table.mask]
                    };
                }
                if self.entry == u32::MAX {
                    self.row += 1;
                    self.started = false;
                    continue;
                }
                let (b, r) = table.entries[self.entry as usize];
                self.entry = table.next[self.entry as usize];
                if table.direct.is_some()
                    || vectorized_hash::compare_row(
                        &table.build_key_arrays[b as usize],
                        r as usize,
                        self.keys.as_slice(),
                        self.row,
                    )
                {
                    self.pending_build
                        .extend_reserved(1, [Some((b as usize, r as usize))])?;
                    self.pending_probe
                        .extend_reserved(1, [Some((0, self.row))])?;
                }
            }
            if let Some(p) = &mut self.profile {
                p.candidates += self.pending_build.as_slice().len();
            }
            JoinProfile::phase(&mut self.profile, 3);
            // Filtering precedes both match bits and unmatched emission. The
            // gathered candidate batch is admission-owned and bounded in rows.
            if self.filter.is_some() && !self.pending_build.as_slice().is_empty() {
                let output = self.gather_pending(false)?;
                let filter = self.filter.as_ref().unwrap();
                // The retained mask may remove columns from the public output.
                // The factory currently declines that combination with a filter.
                let combined =
                    RecordBatch::try_new(self.combined_schema.clone(), output.columns().to_vec())?;
                let mask =
                    crate::execution::expression_memory::with_expression_pool(&self.pool, || {
                        evaluate_expr(&combined, filter)
                    })?;
                let mask = mask
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| invalid("ON predicate is not Boolean"))?;
                if mask.len() != self.pending_build.as_slice().len() {
                    return Err(invalid("filter output length differs"));
                }
                let mut kept = 0;
                for row in 0..mask.len() {
                    if mask.is_valid(row) && mask.value(row) {
                        self.pending_build.as_mut_slice()[kept] =
                            self.pending_build.as_slice()[row];
                        self.pending_probe.as_mut_slice()[kept] =
                            self.pending_probe.as_slice()[row];
                        kept += 1;
                    }
                }
                self.pending_build.truncate(kept);
                self.pending_probe.truncate(kept);
                self.pending_offset = 0;
            }
            if let Some(ticket) = &self.ticket {
                for (build, probe) in self
                    .pending_build
                    .as_slice()
                    .iter()
                    .zip(self.pending_probe.as_slice())
                {
                    let (b, r) = build.unwrap();
                    let (_, p) = probe.unwrap();
                    ticket.round.matched.as_slice()[b].as_slice()[r].store(true, Ordering::Release);
                    self.matched.as_mut_slice()[p] = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    #[derive(Debug)]
    struct IndependentInput {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        pulls: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl PhysicalOperator for IndependentInput {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
            vec![]
        }
        fn name(&self) -> &str {
            "IndependentOuterInput"
        }
        fn output_partitions(&self) -> usize {
            self.batches.len()
        }
        fn pool_independent_queue_copy_bound(
            &self,
        ) -> Option<crate::physical::queue_layout::QueueCopyBound> {
            crate::physical::queue_layout::QueueCopyBound::from_batches(&self.schema, &self.batches)
        }
        async fn prepare_admitted_queue_input(
            &self,
            pool: SharedMemoryPool,
        ) -> Result<Option<crate::physical::PreparedAdmittedInput>> {
            let mut streams = ReservedVec::with_capacity(&pool, self.batches.len())?;
            for batch in &self.batches {
                let batch = batch.clone();
                let owned_pool = pool.clone();
                let pulls = self.pulls.clone();
                streams.extend_reserved(
                    1,
                    [crate::physical::plan::admit_stream(
                        stream::once(async move {
                            pulls.fetch_add(1, Ordering::SeqCst);
                            let mut rows =
                                ReservedVec::with_capacity(&owned_pool, batch.num_rows())?;
                            rows.extend_reserved(
                                batch.num_rows(),
                                (0..batch.num_rows()).map(|r| Some((0, r))),
                            )?;
                            outer_gather::gather(
                                std::slice::from_ref(&batch),
                                rows.as_slice(),
                                &[0],
                                batch.schema(),
                                &owned_pool,
                            )
                        }),
                        &pool,
                    )?],
                )?;
            }
            Ok(Some(crate::physical::PreparedAdmittedInput {
                pool,
                streams,
            }))
        }
        async fn execute(&self, p: usize) -> Result<RecordBatchStream> {
            crate::physical::check_partition(self, p)?;
            let b = self.batches[p].clone();
            let pulls = self.pulls.clone();
            Ok(Box::pin(stream::once(async move {
                pulls.fetch_add(1, Ordering::SeqCst);
                Ok(b)
            })))
        }
    }
    #[tokio::test]
    async fn admitted_inner_composes_computed_probe_without_outer_match_state() {
        use crate::physical::operators::ProjectExec;
        use crate::planner::ScalarValue;
        let make = |name: &str, rows: Vec<Vec<Option<i64>>>| {
            let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
            Arc::new(IndependentInput {
                schema: schema.clone(),
                batches: rows
                    .into_iter()
                    .map(|values| {
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int64Array::from(values))],
                        )
                        .unwrap()
                    })
                    .collect(),
                pulls: Arc::new(AtomicUsize::new(0)),
            })
        };
        let left = make("l", vec![vec![Some(1), Some(1), Some(2), None]]);
        let right = make(
            "r",
            vec![vec![Some(1), None], vec![Some(9)], vec![Some(2), Some(1)]],
        );
        let pool = Arc::new(crate::execution::MemoryPool::new(4 * 1024 * 1024));
        let probe = Arc::new(
            ProjectExec::try_new(
                right.clone(),
                vec![Expr::column("r")
                    .add(Expr::literal(ScalarValue::Int64(0)))
                    .alias("rk")],
            )
            .unwrap()
            .with_memory_pool(pool.clone()),
        );
        let join = HashJoinExec::new(
            left,
            probe,
            vec![(Expr::column("l"), Expr::column("rk"))],
            JoinType::Inner,
        )
        .with_memory_pool(pool.clone());
        let prepared = join
            .prepare_admitted_queue_input(pool.clone())
            .await
            .unwrap()
            .expect("inner join must retain the admitted computed probe");
        assert_eq!(prepared.streams.as_slice().len(), 3);
        assert_eq!(right.pulls.load(Ordering::SeqCst), 0);
        assert!(
            join.outer_round.lock().unwrap().is_none(),
            "inner joins must not allocate unmatched-build bookkeeping"
        );
        let mut streams = futures::stream::SelectAll::new();
        streams.extend(prepared.streams.into_owned_iter());
        let mut batches = vec![];
        while let Some(batch) = streams.try_next().await.unwrap() {
            batches.push(batch);
        }
        assert_eq!(right.pulls.load(Ordering::SeqCst), 3);
        let mut actual = vec![];
        for batch in &batches {
            let l = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let r = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual.extend(l.iter().zip(r.iter()));
        }
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (Some(1), Some(1)),
                (Some(1), Some(1)),
                (Some(1), Some(1)),
                (Some(1), Some(1)),
                (Some(2), Some(2))
            ]
        );
        drop((streams, join));
        assert!(pool.used() > 0);
        drop(batches);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn admitted_inner_build_orientations_empty_sides_and_multibatch_keys() {
        for swapped in [false, true] {
            for empty in [0, 1, 2] {
                let make = |name: &str, mut rows: Vec<Vec<Option<i64>>>, clear: bool| {
                    if clear {
                        for row in &mut rows {
                            row.clear();
                        }
                    }
                    let schema =
                        Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
                    Arc::new(IndependentInput {
                        schema: schema.clone(),
                        batches: rows
                            .into_iter()
                            .map(|values| {
                                RecordBatch::try_new(
                                    schema.clone(),
                                    vec![Arc::new(Int64Array::from(values))],
                                )
                                .unwrap()
                            })
                            .collect(),
                        pulls: Arc::new(AtomicUsize::new(0)),
                    })
                };
                let left = make(
                    "l",
                    vec![vec![Some(1), Some(1), None], vec![Some(2)]],
                    empty == 1,
                );
                let right = make(
                    "r",
                    vec![vec![Some(1), None], vec![Some(9)], vec![Some(2), Some(1)]],
                    empty == 2,
                );
                let probe = if swapped { left.clone() } else { right.clone() };
                let pool = Arc::new(crate::execution::MemoryPool::new(4 * 1024 * 1024));
                let join = HashJoinExec::new(
                    left,
                    right,
                    vec![(Expr::column("l"), Expr::column("r"))],
                    JoinType::Inner,
                )
                .with_memory_pool(pool.clone())
                .with_build_right(swapped);
                let prepared = join
                    .prepare_admitted_queue_input(pool.clone())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(probe.pulls.load(Ordering::SeqCst), 0);
                assert_eq!(
                    prepared.streams.as_slice().len(),
                    if swapped { 2 } else { 3 }
                );
                let mut streams = futures::stream::SelectAll::new();
                streams.extend(prepared.streams.into_owned_iter());
                let mut rows = vec![];
                while let Some(batch) = streams.try_next().await.unwrap() {
                    let l = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let r = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    rows.extend(l.iter().zip(r.iter()));
                }
                rows.sort();
                let expected = if empty == 0 {
                    vec![
                        (Some(1), Some(1)),
                        (Some(1), Some(1)),
                        (Some(1), Some(1)),
                        (Some(1), Some(1)),
                        (Some(2), Some(2)),
                    ]
                } else {
                    vec![]
                };
                assert_eq!(rows, expected, "swapped={swapped},empty={empty}");
                assert!(join.outer_round.lock().unwrap().is_none());
                drop((streams, join));
                assert_eq!(pool.used(), 0);
            }
        }
    }

    #[tokio::test]
    async fn admitted_factory_is_lazy_and_all_partitions_keep_exact_output() {
        let make = |name, values: Vec<Vec<i64>>| {
            let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
            Arc::new(IndependentInput {
                schema: schema.clone(),
                batches: values
                    .into_iter()
                    .map(|v| {
                        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(v))])
                            .unwrap()
                    })
                    .collect(),
                pulls: Arc::new(AtomicUsize::new(0)),
            })
        };
        let left = make("l", vec![vec![1, 2, 3]]);
        let right = make("r", vec![vec![1, 1], vec![9], vec![2]]);
        let pool = Arc::new(crate::execution::MemoryPool::new(4 * 1024 * 1024));
        let join = HashJoinExec::new(
            left,
            right.clone(),
            vec![(Expr::column("l"), Expr::column("r"))],
            JoinType::Full,
        )
        .with_memory_pool(pool.clone());
        let prepared = join
            .prepare_admitted_queue_input(pool.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prepared.streams.as_slice().len(), 3);
        assert!(Arc::ptr_eq(&prepared.pool, &pool));
        assert_eq!(right.pulls.load(Ordering::SeqCst), 0);
        let mut tasks = tokio::task::JoinSet::new();
        for mut stream in prepared.streams.into_owned_iter() {
            tasks.spawn(async move {
                let mut rows = vec![];
                while let Some(b) = stream.try_next().await.unwrap() {
                    assert!(b.num_rows() <= INNER_CANDIDATE_ROWS);
                    let l = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    let r = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                    rows.extend(l.iter().zip(r.iter()));
                }
                rows
            });
        }
        let mut rows = vec![];
        while let Some(result) = tasks.join_next().await {
            rows.extend(result.unwrap());
        }
        rows.sort();
        let mut expected = vec![
            (Some(1), Some(1)),
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(3), None),
            (None, Some(9)),
        ];
        expected.sort();
        assert_eq!(rows, expected);
        drop(join);
        drop(prepared.pool);
        assert_eq!(pool.used(), 0);
    }
    #[tokio::test]
    async fn memory_filter_project_outer_pipeline_is_transitively_admitted() {
        use crate::physical::operators::{FilterExec, MemoryTableExec, ProjectExec};
        use crate::planner::ScalarValue;
        use arrow::array::StringArray;
        let pool = Arc::new(crate::execution::MemoryPool::new(16 * 1024 * 1024));
        let left_batch = RecordBatch::try_from_iter([(
            "l",
            Arc::new(Int64Array::from_iter_values(0..13)) as ArrayRef,
        )])
        .unwrap();
        let left = Arc::new(MemoryTableExec::new(
            "l",
            left_batch.schema(),
            vec![left_batch],
            None,
        ));
        let batches: Vec<_> = (0..4)
            .map(|part| {
                let ids = part * 10000..(part + 1) * 10000;
                RecordBatch::try_from_iter(vec![
                    (
                        "r",
                        Arc::new(Int64Array::from_iter_values(ids.clone().map(|i| i % 11)))
                            as ArrayRef,
                    ),
                    (
                        "rv",
                        Arc::new(Int64Array::from_iter_values(ids.clone())) as ArrayRef,
                    ),
                    (
                        "text",
                        Arc::new(StringArray::from_iter(ids.map(|i| {
                            if i % 7 == 0 {
                                None
                            } else if i % 5 == 0 {
                                Some("alpha middle beta")
                            } else {
                                Some("ordinary")
                            }
                        }))) as ArrayRef,
                    ),
                ])
                .unwrap()
            })
            .collect();
        let memory = Arc::new(MemoryTableExec::new(
            "r",
            batches[0].schema(),
            batches,
            None,
        ));
        let filter = Arc::new(FilterExec::new(
            memory,
            Expr::BinaryExpr {
                left: Box::new(Expr::column("text")),
                op: crate::planner::BinaryOp::NotLike,
                right: Box::new(Expr::Literal(ScalarValue::Utf8("%alpha%beta%".into()))),
            },
        ));
        let project = Arc::new(
            ProjectExec::try_new(filter, vec![Expr::column("r"), Expr::column("rv")]).unwrap(),
        );
        let join = HashJoinExec::new(
            left,
            project,
            vec![(Expr::column("l"), Expr::column("r"))],
            JoinType::Left,
        )
        .with_memory_pool(pool.clone());
        let prepared = join
            .prepare_admitted_queue_input(pool.clone())
            .await
            .unwrap()
            .expect("the complete pipeline must be admitted");
        assert_eq!(prepared.streams.as_slice().len(), 4);
        assert!(
            pool.used() >= pool.max() / 8,
            "input working space must be prepaid"
        );
        assert!(prepared.pool.is_within(&pool));
        let mut tasks = tokio::task::JoinSet::new();
        for mut input in prepared.streams.into_owned_iter() {
            tasks.spawn(async move {
                let mut rows = vec![];
                while let Some(batch) = input.try_next().await.unwrap() {
                    assert!(batch.num_rows() <= INNER_CANDIDATE_ROWS);
                    let arrays: Vec<_> = batch
                        .columns()
                        .iter()
                        .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap())
                        .collect();
                    for row in 0..batch.num_rows() {
                        rows.push(
                            arrays
                                .iter()
                                .map(|a| (!a.is_null(row)).then(|| a.value(row)))
                                .collect::<Vec<_>>(),
                        );
                    }
                }
                rows
            });
        }
        let mut rows = vec![];
        while let Some(result) = tasks.join_next().await {
            rows.extend(result.unwrap());
        }
        let mut expected: Vec<_> = (0..40000)
            .filter(|i| i % 5 != 0 && i % 7 != 0)
            .map(|i| vec![Some(i % 11), Some(i % 11), Some(i)])
            .collect();
        expected.extend([vec![Some(11), None, None], vec![Some(12), None, None]]);
        rows.sort();
        expected.sort();
        assert_eq!(rows, expected);
        drop(join);
        drop(prepared.pool);
        assert_eq!(pool.used(), 0);
    }
}
