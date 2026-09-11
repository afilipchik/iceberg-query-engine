//! Narrow closed, pinned MemoryTable membership. This is not a general subquery cache.
use super::closed_subquery::{prove_closed_int64_membership, ClosedInt64Membership};
use super::int64_membership::{Int64Membership, Int64MembershipBuilder};
use super::{MemoryTable, TableProvider};
use crate::error::{QueryError, Result};
use crate::execution::{ExecutionConfig, MemoryReservation, SharedMemoryPool};
use crate::physical::queue_layout::PreparedOutputLayouts;
use crate::physical::PhysicalPlanner;
use crate::physical::{
    PhysicalOperator, PreparedOutputBound, PreparedQueueInput, RecordBatchStream,
};
use crate::planner::{Expr, LogicalPlan};
use arrow::array::{Array, BooleanArray, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;

#[derive(Debug)]
enum Outcome {
    Ready(Int64Membership),
    Failed(Arc<QueryError>),
}

pub(crate) struct InitializedMembership {
    proof: ClosedInt64Membership,
    provider: Arc<dyn TableProvider>,
    config: ExecutionConfig,
    pool: SharedMemoryPool,
    state: OnceCell<Outcome>,
    generic: super::subquery::SubqueryExecutor,
    // Last: state and its keys are destroyed before control admission is released.
    _control: MemoryReservation,
}

impl std::fmt::Debug for InitializedMembership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitializedMembership")
            .field("proof", &self.proof)
            .finish_non_exhaustive()
    }
}

fn eligible_type(data_type: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    matches!(data_type, DataType::Int64)
        || matches!(data_type, DataType::Dictionary(key, value) if key.is_integer() && value.as_ref() == &DataType::Int64)
}

/// Requested retained bytes for the owned proof's column name and qualifier.
fn with_identifier_bytes(base: usize, column: &crate::planner::Column) -> Option<usize> {
    base.checked_add(column.name.len())?
        .checked_add(column.relation.as_ref().map_or(0, String::len))
}

fn scan_name(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::Scan(node) => Some(&node.table_name),
        LogicalPlan::Filter(node) => scan_name(&node.input),
        LogicalPlan::Project(node) => scan_name(&node.input),
        _ => None,
    }
}

fn membership_mask(
    state: &Int64Membership,
    lhs: &dyn Array,
    negated: bool,
) -> Result<BooleanArray> {
    if state.is_empty() {
        return Ok((0..lhs.len()).map(|_| Some(negated)).collect());
    }
    if let Some(values) = lhs.as_any().downcast_ref::<Int64Array>() {
        return Ok(values.iter().map(|v| state.lookup(v, negated)).collect());
    }
    // Logical NULLs include both null dictionary keys and null dictionary values.
    // No decode allocation, RHS rebuilding, or future query-pool admission.
    macro_rules! dictionary {
        ($key:ty) => {
            if let Some(array) = lhs
                .as_any()
                .downcast_ref::<arrow::array::DictionaryArray<$key>>()
            {
                if let Some(values) = array.values().as_any().downcast_ref::<Int64Array>() {
                    return Ok((0..array.len())
                        .map(|row| {
                            let value = array
                                .key(row)
                                .and_then(|key| (!values.is_null(key)).then(|| values.value(key)));
                            state.lookup(value, negated)
                        })
                        .collect());
                }
            }
        };
    }
    dictionary!(arrow::datatypes::Int8Type);
    dictionary!(arrow::datatypes::Int16Type);
    dictionary!(arrow::datatypes::Int32Type);
    dictionary!(arrow::datatypes::Int64Type);
    dictionary!(arrow::datatypes::UInt8Type);
    dictionary!(arrow::datatypes::UInt16Type);
    dictionary!(arrow::datatypes::UInt32Type);
    dictionary!(arrow::datatypes::UInt64Type);
    // Only a lying known layout could reach this from a certified stream.
    Err(QueryError::Internal(
        "initialized membership physical layout certificate violated".into(),
    ))
}

impl InitializedMembership {
    pub(crate) fn try_new(
        predicate: &Expr,
        outer: &SchemaRef,
        tables: &HashMap<String, Arc<dyn TableProvider>>,
        pool: &SharedMemoryPool,
        config: &ExecutionConfig,
    ) -> Result<Option<Arc<Self>>> {
        let Expr::InSubquery { subquery, .. } = predicate else {
            return Ok(None);
        };
        let Some(name) = scan_name(subquery) else {
            return Ok(None);
        };
        let Some(provider) = tables.get(name) else {
            return Ok(None);
        };
        let Some(memory) = provider.as_any().downcast_ref::<MemoryTable>() else {
            return Ok(None);
        };
        if !memory.has_exact_physical_schema() {
            return Ok(None);
        }
        let Some(proof) = prove_closed_int64_membership(predicate, outer, |requested| {
            (requested == name).then(|| provider.schema())
        }) else {
            return Ok(None);
        };
        // Requested-layout allowance: owner + Arc header/alignment + small future/error
        // control state. Not exact allocator/RSS accounting. Existing plan/provider
        // payloads remain shared source ownership; dynamic error strings are not bounded.
        let bytes = std::mem::size_of::<Self>()
            .checked_add(4096)
            .and_then(|v| {
                config
                    .spill_path
                    .as_os_str()
                    .len()
                    .checked_mul(2)
                    .and_then(|path| v.checked_add(path))
            })
            .and_then(|v| v.checked_add(name.len()))
            .and_then(|v| with_identifier_bytes(v, &proof.left_column))
            .ok_or_else(|| QueryError::Execution("membership control layout overflow".into()))?;
        let control = pool.allocate(bytes)?;
        // Private generic executor: only the captured provider is registered, and
        // this executor is never exposed to later registry mutation. Its legacy
        // cache/materialization remains uncertified and is never a known envelope.
        let mut pinned = HashMap::new();
        pinned.try_reserve(1).map_err(|e| {
            QueryError::Execution(format!("membership pinned registry allocation failed: {e}"))
        })?;
        let mut pinned_name = String::new();
        pinned_name.try_reserve_exact(name.len()).map_err(|e| {
            QueryError::Execution(format!("membership pinned name allocation failed: {e}"))
        })?;
        pinned_name.push_str(name);
        pinned.insert(pinned_name, provider.clone());
        let generic = super::subquery::SubqueryExecutor::from_tables_with_config(
            pinned,
            pool.clone(),
            config.clone(),
        );
        Ok(Some(Arc::new(Self {
            proof,
            provider: provider.clone(),
            config: config.clone(),
            pool: pool.clone(),
            state: OnceCell::new(),
            generic,
            _control: control,
        })))
    }

    async fn build(&self) -> Result<Int64Membership> {
        let mut builder = Int64MembershipBuilder::new(&self.pool)?;
        let input = {
            let mut planner = PhysicalPlanner::with_config(self.pool.clone(), self.config.clone());
            let name = scan_name(&self.proof.subquery)
                .ok_or_else(|| QueryError::Internal("lost closed membership scan".into()))?;
            // Only this pinned concrete provider is visible. Never consult SubqueryExecutor.
            planner.register_table(name, self.provider.clone());
            planner.create_physical_plan(&self.proof.subquery)?
        };
        for partition in 0..input.output_partitions() {
            let mut stream = input.execute(partition).await?;
            while let Some(batch) = stream.try_next().await? {
                if batch.num_columns() != 1 {
                    return Err(QueryError::Type(
                        "initialized membership RHS requires one Int64 column".into(),
                    ));
                }
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        QueryError::Type(
                            "initialized membership RHS requires physical Int64".into(),
                        )
                    })?;
                // Bounded cooperative work, without cloning the full RHS or retaining
                // a detached producer. Empty batches preserve empty-RHS semantics.
                for start in (0..array.len()).step_by(4096) {
                    builder =
                        builder.extend(&array.slice(start, (array.len() - start).min(4096)))?;
                    tokio::task::yield_now().await;
                }
            }
        }
        Ok(builder.finish())
    }

    async fn initialize(&self) -> &Outcome {
        self.state
            .get_or_init(|| async {
                match self.build().await {
                    Ok(value) => Outcome::Ready(value),
                    Err(error) => Outcome::Failed(Arc::new(error)),
                }
            })
            .await
    }

    pub(crate) async fn evaluate(
        &self,
        batch: &RecordBatch,
        predicate: &Expr,
    ) -> Result<RecordBatch> {
        let Expr::InSubquery { expr, .. } = predicate else {
            return Err(QueryError::Internal(
                "initialized membership predicate changed".into(),
            ));
        };
        // Same direct-column resolver as ordinary evaluation, before observing RHS
        // errors; a delivered zero-row batch still evaluates RHS.
        let lhs = super::filter::evaluate_expr(batch, expr)?;
        if !eligible_type(lhs.data_type()) {
            // Preserve any already published original failure, and empty-RHS
            // identity; otherwise use the pinned legacy evaluator in ordinary
            // execution. This is never called by a certified prepared stream.
            match self.state.get() {
                Some(Outcome::Failed(error)) => return Err(QueryError::Shared(error.clone())),
                Some(Outcome::Ready(state)) if state.is_empty() => {
                    let mask = membership_mask(state, lhs.as_ref(), self.proof.negated)?;
                    let columns = batch
                        .columns()
                        .iter()
                        .map(|column| arrow::compute::filter(column, &mask).map_err(Into::into))
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(RecordBatch::try_new(batch.schema(), columns)?);
                }
                _ => return self.evaluate_generic(batch, predicate),
            }
        }
        let state = match self.initialize().await {
            Outcome::Ready(state) => state,
            Outcome::Failed(error) => return Err(QueryError::Shared(error.clone())),
        };
        // Empty RHS identity precedes any physical decode/common-type check,
        // exactly as the ordinary evaluator. A cached RHS error precedes them too.
        let mask = membership_mask(state, lhs.as_ref(), self.proof.negated)?;
        let columns = batch
            .columns()
            .iter()
            .map(|column| arrow::compute::filter(column, &mask).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }

    pub(crate) fn eligible_output(&self, output: &PreparedOutputBound) -> bool {
        matches!(output, PreparedOutputBound::Layouts(layouts) if layouts.int64_membership_column(self.proof.left_index, &self.proof.left_column))
    }

    pub(crate) fn evaluate_generic(
        &self,
        batch: &RecordBatch,
        predicate: &Expr,
    ) -> Result<RecordBatch> {
        let Expr::InSubquery { expr, .. } = predicate else {
            return Err(QueryError::Internal("membership predicate changed".into()));
        };
        let lhs = super::filter::evaluate_expr(batch, expr)?;
        match self.state.get() {
            Some(Outcome::Failed(error)) => return Err(QueryError::Shared(error.clone())),
            Some(Outcome::Ready(state)) if state.is_empty() => {
                let mask = membership_mask(state, lhs.as_ref(), self.proof.negated)?;
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| arrow::compute::filter(column, &mask).map_err(Into::into))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(RecordBatch::try_new(batch.schema(), columns)?);
            }
            _ => {}
        }
        // Existing evaluator evaluates the direct LHS first, then RHS, then
        // common type/coercion (except empty RHS), preserving legacy semantics.
        // Repeating the proven direct-column resolution is side-effect free.
        super::filter::evaluate_filter_with_subquery(
            batch,
            predicate,
            &batch.schema(),
            &self.generic,
        )
    }

    pub(crate) async fn evaluate_certified(
        &self,
        batch: &RecordBatch,
        predicate: &Expr,
    ) -> Result<RecordBatch> {
        let Expr::InSubquery { expr, .. } = predicate else {
            return Err(QueryError::Internal("membership predicate changed".into()));
        };
        let lhs = super::filter::evaluate_expr(batch, expr)?;
        match self.initialize().await {
            Outcome::Failed(error) => return Err(QueryError::Shared(error.clone())),
            Outcome::Ready(state) if state.is_empty() => {
                return self.evaluate(batch, predicate).await
            }
            _ => {}
        }
        if !eligible_type(lhs.data_type()) {
            // A dishonest producer must fail rather than invoke an uncertified
            // provider/materialization path under the known parallel envelope.
            return Err(QueryError::Internal(
                "initialized membership physical layout certificate violated".into(),
            ));
        }
        self.evaluate(batch, predicate).await
    }

    pub(crate) async fn prepare_child(
        &self,
        input: &Arc<dyn PhysicalOperator>,
    ) -> Result<Option<PreparedQueueInput>> {
        // Finish child initialization before RHS and outer envelope admission.
        let mut prepared = if let Some(prepared) = input.prepare_queue_input().await? {
            prepared
        } else {
            let gather = input.pool_independent_gather_copy_bound();
            let queue = input.pool_independent_queue_copy_bound();
            if gather.is_none() && queue.is_none() {
                return Ok(None);
            }
            let layouts = gather
                .and_then(PreparedOutputLayouts::from_gather)
                .or_else(|| queue.and_then(PreparedOutputLayouts::from_bound));
            let mut streams: Vec<RecordBatchStream> = Vec::new();
            streams
                .try_reserve_exact(input.output_partitions())
                .map_err(|e| {
                    QueryError::Execution(format!("membership input stream allocation failed: {e}"))
                })?;
            for partition in 0..input.output_partitions() {
                streams.push(input.execute(partition).await?);
            }
            PreparedQueueInput {
                streams,
                output: layouts
                    .map(PreparedOutputBound::Layouts)
                    .unwrap_or(PreparedOutputBound::Unknown),
            }
        };
        // Store original RHS error; only actual predicate evaluation delivers it.
        // Unknown descriptors keep these same streams and use serial admission.
        if self.eligible_output(&prepared.output) {
            self.initialize().await;
        } else {
            // Same streams, uncertified legacy evaluator, serial actual admission.
            // Never label this as a pool-independent prepared capability.
            prepared.output = PreparedOutputBound::Unknown;
        }
        Ok(Some(prepared))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::FilterExec;
    use crate::planner::{PlanSchema, ScanNode, SchemaField};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn batch(name: &str, values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }
    fn fixture(
        values: Vec<Option<i64>>,
        negated: bool,
    ) -> (Expr, SchemaRef, HashMap<String, Arc<dyn TableProvider>>) {
        let rhs = batch("k", values);
        let table: Arc<dyn TableProvider> = Arc::new(MemoryTable::new(rhs.schema(), vec![rhs]));
        let predicate = Expr::InSubquery {
            expr: Box::new(Expr::column("x")),
            subquery: Arc::new(LogicalPlan::Scan(ScanNode {
                table_name: "rhs".into(),
                schema: PlanSchema::new(vec![
                    SchemaField::new("k", DataType::Int64).with_relation("rhs")
                ]),
                projection: None,
                filter: None,
            })),
            negated,
        };
        (
            predicate,
            batch("x", vec![]).schema(),
            HashMap::from([("rhs".into(), table)]),
        )
    }
    #[test]
    fn long_retained_identifiers_change_real_admission_and_refuse_cleanly() {
        fn create(
            name: &str,
            relation: &str,
            pool: &SharedMemoryPool,
        ) -> Result<Option<Arc<InitializedMembership>>> {
            let (mut predicate, _, tables) = fixture(vec![Some(2)], false);
            let Expr::InSubquery { expr, .. } = &mut predicate else {
                panic!()
            };
            *expr = Box::new(Expr::qualified_column(relation, name));
            let schema = Arc::new(Schema::new(vec![Field::new(
                format!("{relation}.{name}"),
                DataType::Int64,
                true,
            )]));
            InitializedMembership::try_new(
                &predicate,
                &schema,
                &tables,
                pool,
                &ExecutionConfig::default(),
            )
        }
        let pool = crate::execution::create_memory_pool(1 << 20);
        let baseline = create("x", "r", &pool).unwrap().unwrap();
        let base_bytes = pool.used();
        drop(baseline);
        assert_eq!(pool.used(), 0);
        let long_name = "n".repeat(65_537);
        let long_relation = "r".repeat(131_073);
        // Independent differences in retained UTF-8 payload, not the helper's
        // formula: replace each one-byte identifier with this many extra bytes.
        for (name, relation, expected_delta) in [
            (long_name.as_str(), "r", 65_536usize),
            ("x", long_relation.as_str(), 131_072usize),
            (long_name.as_str(), long_relation.as_str(), 196_608usize),
        ] {
            let owner = create(name, relation, &pool).unwrap().unwrap();
            assert_eq!(pool.used() - base_bytes, expected_delta);
            assert!(
                owner.state.get().is_none(),
                "planning admission must not run RHS"
            );
            drop(owner);
            assert_eq!(pool.used(), 0);
        }
        let tight = crate::execution::create_memory_pool(base_bytes + 196_608 - 1);
        let error = create(&long_name, &long_relation, &tight)
            .err()
            .expect("one byte below retained identifier charge must refuse");
        assert!(error.is_memory_limit(), "{error}");
        assert!(error.to_string().contains("Memory limit exceeded in '"));
        assert_eq!(tight.used(), 0, "failed construction releases admission");
        let exact = crate::execution::create_memory_pool(base_bytes + 196_608);
        let owner = create(&long_name, &long_relation, &exact).unwrap().unwrap();
        assert_eq!(exact.used(), exact.max());
        drop(owner);
        assert_eq!(exact.used(), 0);
    }

    #[test]
    fn retained_identifier_admission_checks_name_and_relation_overflow() {
        let name_only = crate::planner::Column::new("n");
        assert_eq!(with_identifier_bytes(usize::MAX, &name_only), None);
        let relation_only = crate::planner::Column::new_qualified("r", "");
        assert_eq!(with_identifier_bytes(usize::MAX, &relation_only), None);
        let both = crate::planner::Column::new_qualified("r", "n");
        assert_eq!(with_identifier_bytes(usize::MAX - 1, &both), None);
        assert_eq!(
            with_identifier_bytes(usize::MAX - 2, &both),
            Some(usize::MAX)
        );
    }

    fn owner(
        p: &Expr,
        s: &SchemaRef,
        t: &HashMap<String, Arc<dyn TableProvider>>,
        pool: &SharedMemoryPool,
    ) -> Arc<InitializedMembership> {
        InitializedMembership::try_new(p, s, t, pool, &ExecutionConfig::default())
            .unwrap()
            .expect("positive pinned proof")
    }
    fn values(batch: &RecordBatch) -> Vec<Option<i64>> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn dictionary_membership_checks_key_and_value_nulls_without_decode() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;
        let pool = crate::execution::create_memory_pool(1 << 20);
        let state = Int64MembershipBuilder::new(&pool)
            .unwrap()
            .extend(&Int64Array::from(vec![Some(2), None]))
            .unwrap()
            .finish();
        let encoded = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1), Some(2), Some(0)]),
            Arc::new(Int64Array::from(vec![Some(2), None, Some(3)])),
        )
        .unwrap();
        assert_eq!(
            membership_mask(&state, &encoded, false)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(true), None, None, None, Some(true)]
        );
        assert_eq!(
            membership_mask(&state, &encoded, true)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(false), None, None, None, Some(false)]
        );
    }

    #[tokio::test]
    async fn rhs_error_and_empty_identity_precede_physical_lhs_coercion() {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let (p, schema, tables) = fixture(vec![Some(2)], false);
        let state = owner(&p, &schema, &tables, &pool);
        let held = pool.allocate(pool.available()).unwrap();
        let float = RecordBatch::try_from_iter([(
            "x",
            Arc::new(arrow::array::Float64Array::from(vec![Some(2.0), None])) as Arc<dyn Array>,
        )])
        .unwrap();
        assert!(matches!(state.initialize().await, Outcome::Failed(_)));
        let error = state.evaluate(&float, &p).await.unwrap_err();
        assert_eq!(error.kind(), "Execution");
        assert!(error.to_string().contains("Memory limit exceeded in '"));
        drop(held);
        let (empty, schema, tables) = fixture(vec![], true);
        let empty_state = owner(&empty, &schema, &tables, &pool);
        assert_eq!(
            empty_state
                .evaluate(&float, &empty)
                .await
                .unwrap()
                .num_rows(),
            2
        );
    }

    #[tokio::test]
    async fn pinned_rhs_singleflight_duplicate_null_and_empty_truth() {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let (p, s, mut tables) = fixture(vec![Some(i64::MIN), Some(2), Some(2), None], false);
        let state = owner(&p, &s, &tables, &pool);
        // Mutating the registry cannot change the captured concrete provider.
        tables.insert(
            "rhs".into(),
            Arc::new(MemoryTable::new(batch("k", vec![]).schema(), vec![])),
        );
        let (a, b) = tokio::join!(state.initialize(), state.initialize());
        assert!(std::ptr::eq(a, b));
        let used = pool.used();
        let lhs = batch("x", vec![None, Some(2), Some(3), Some(i64::MIN), Some(2)]);
        assert_eq!(
            values(&state.evaluate(&lhs, &p).await.unwrap()),
            vec![Some(2), Some(i64::MIN), Some(2)]
        );
        assert_eq!(pool.used(), used);
        let (not, s, t) = fixture(vec![Some(2), None], true);
        let other = owner(&not, &s, &t, &pool);
        assert_eq!(other.evaluate(&lhs, &not).await.unwrap().num_rows(), 0);
        let (empty, s, t) = fixture(vec![], true);
        let empty_owner = owner(&empty, &s, &t, &pool);
        assert_eq!(
            values(&empty_owner.evaluate(&lhs, &empty).await.unwrap()),
            values(&lhs)
        );
        drop((state, other, empty_owner));
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn cancelled_initialization_discards_partial_membership_then_retries() {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let (p, s, t) = fixture((0..20000).map(Some).collect(), false);
        let state = owner(&p, &s, &t, &pool);
        let control = pool.used();
        {
            let future = state.initialize();
            futures::pin_mut!(future);
            assert!(
                futures::poll!(future.as_mut()).is_pending(),
                "first bounded chunk yields"
            );
            assert!(pool.used() > control);
        }
        assert!(state.state.get().is_none());
        assert_eq!(pool.used(), control);
        assert!(matches!(state.initialize().await, Outcome::Ready(_)));
        assert_eq!(
            values(
                &state
                    .evaluate(&batch("x", vec![Some(123), Some(30000)]), &p)
                    .await
                    .unwrap()
            ),
            vec![Some(123)]
        );
        drop(state);
        assert_eq!(pool.used(), 0);
    }

    #[derive(Debug)]
    struct Child {
        batch: RecordBatch,
        unknown: bool,
        no_batches: bool,
        prepare_mode: u8,
        declared_schema: Option<SchemaRef>,
        calls: Arc<AtomicUsize>,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    struct Ack(Arc<AtomicUsize>);
    impl Drop for Ack {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Child {
        fn stream(&self) -> RecordBatchStream {
            let ack = Ack(self.drops.clone());
            let polls = self.polls.clone();
            let batch = self.batch.clone();
            let empty = self.no_batches;
            Box::pin(
                stream::once(async move {
                    let _ack = ack;
                    polls.fetch_add(1, Ordering::SeqCst);
                    Ok(if empty { None } else { Some(batch) })
                })
                .try_filter_map(|batch| futures::future::ready(Ok(batch))),
            )
        }
    }
    #[async_trait]
    impl PhysicalOperator for Child {
        fn name(&self) -> &str {
            "membership-test-child"
        }
        fn schema(&self) -> SchemaRef {
            self.declared_schema
                .clone()
                .unwrap_or_else(|| self.batch.schema())
        }
        fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
            vec![]
        }
        fn output_partitions(&self) -> usize {
            3
        }
        async fn execute(&self, p: usize) -> Result<RecordBatchStream> {
            crate::physical::check_partition(self, p)?;
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.stream())
        }
        async fn prepare_queue_input(&self) -> Result<Option<PreparedQueueInput>> {
            if !self.unknown {
                return Ok(None);
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            let streams = (0..3).map(|_| self.stream()).collect();
            if self.prepare_mode == 2 {
                futures::future::pending::<()>().await;
            }
            if self.prepare_mode == 1 {
                return Err(QueryError::Execution("child initialization failed".into()));
            }
            let output = if self.prepare_mode == 3 {
                PreparedOutputBound::Layouts(
                    PreparedOutputLayouts::from_bound(
                        crate::physical::queue_layout::QueueCopyBound::from_batches(
                            &self.batch.schema(),
                            &[self.batch.clone()],
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                )
            } else {
                PreparedOutputBound::Unknown
            };
            Ok(Some(PreparedQueueInput { streams, output }))
        }
    }
    fn child(no_batches: bool, unknown: bool) -> Arc<Child> {
        Arc::new(Child {
            batch: batch("x", vec![]),
            unknown,
            no_batches,
            prepare_mode: 0,
            declared_schema: None,
            calls: Arc::new(AtomicUsize::new(0)),
            polls: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
        })
    }

    #[tokio::test]
    async fn unknown_child_keeps_streams_unpulled_and_drop_releases_them() {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let (p, s, t) = fixture(vec![Some(2)], false);
        let state = owner(&p, &s, &t, &pool);
        let input = child(false, true);
        let filter = FilterExec::new(input.clone(), p).with_initialized_membership(state.clone());
        let prepared = filter.prepare_queue_input().await.unwrap().unwrap();
        assert!(matches!(prepared.output, PreparedOutputBound::Unknown));
        assert_eq!(input.calls.load(Ordering::SeqCst), 1);
        assert_eq!(input.polls.load(Ordering::SeqCst), 0);
        assert!(
            state.state.get().is_none(),
            "Unknown uses serial pinned legacy path"
        );
        drop(prepared);
        assert_eq!(input.drops.load(Ordering::SeqCst), 3);
        drop((filter, state));
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn unknown_and_rejected_float_layout_preserve_generic_values_and_pinned_snapshot() {
        for mode in [0, 3] {
            let pool = crate::execution::create_memory_pool(1 << 20);
            let (p, schema, mut tables) = fixture(vec![Some(2), Some(2)], false);
            let state = owner(&p, &schema, &tables, &pool);
            tables.insert(
                "rhs".into(),
                Arc::new(MemoryTable::new(batch("k", vec![]).schema(), vec![])),
            );
            let mut source = child(false, true);
            let mutable = Arc::get_mut(&mut source).unwrap();
            mutable.prepare_mode = mode;
            mutable.declared_schema = Some(schema);
            mutable.batch = RecordBatch::try_from_iter([(
                "x",
                Arc::new(arrow::array::Float64Array::from(vec![
                    Some(2.0),
                    Some(2.5),
                    None,
                    Some(2.0),
                ])) as Arc<dyn Array>,
            )])
            .unwrap();
            let filter =
                FilterExec::new(source.clone(), p).with_initialized_membership(state.clone());
            let prepared = filter.prepare_queue_input().await.unwrap().unwrap();
            assert!(matches!(prepared.output, PreparedOutputBound::Unknown));
            assert_eq!(source.calls.load(Ordering::SeqCst), 1);
            assert_eq!(source.polls.load(Ordering::SeqCst), 0);
            assert!(state.state.get().is_none());
            let mut found = Vec::new();
            for mut stream in prepared.streams {
                while let Some(batch) = stream.try_next().await.unwrap() {
                    found.extend(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .unwrap()
                            .iter(),
                    );
                }
            }
            assert_eq!(found, vec![Some(2.0); 6]);
            assert_eq!(source.calls.load(Ordering::SeqCst), 1);
            assert_eq!(source.polls.load(Ordering::SeqCst), 3);
            assert!(
                state.state.get().is_none(),
                "legacy fallback must not masquerade as initialized state"
            );
            drop((filter, state));
            assert_eq!(pool.used(), 0);
        }
    }

    #[tokio::test]
    async fn ordinary_float_fallback_preserves_numeric_coercion_without_type_refusal() {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let (p, s, t) = fixture(vec![Some(2)], false);
        let state = owner(&p, &s, &t, &pool);
        let actual = RecordBatch::try_from_iter([(
            "x",
            Arc::new(arrow::array::Float64Array::from(vec![
                Some(2.0),
                Some(2.5),
                None,
            ])) as Arc<dyn Array>,
        )])
        .unwrap();
        let output = state.evaluate(&actual, &p).await.unwrap();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2.0)]
        );
        assert!(state.state.get().is_none());
    }

    #[tokio::test]
    async fn known_integer_layout_initializes_and_keeps_parallel_bound() {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let (p, s, t) = fixture(vec![Some(2)], false);
        let state = owner(&p, &s, &t, &pool);
        let mut source = child(false, true);
        let source_mut = Arc::get_mut(&mut source).unwrap();
        source_mut.prepare_mode = 3;
        source_mut.batch = batch("x", vec![Some(2), Some(3), None]);
        let filter = FilterExec::new(source.clone(), p).with_initialized_membership(state.clone());
        let prepared = filter.prepare_queue_input().await.unwrap().unwrap();
        assert!(prepared.output.max_bytes().is_some());
        assert!(matches!(state.state.get(), Some(Outcome::Ready(_))));
        assert_eq!(source.polls.load(Ordering::SeqCst), 0);
        let mut found = Vec::new();
        for mut stream in prepared.streams {
            while let Some(batch) = stream.try_next().await.unwrap() {
                found.extend(values(&batch));
            }
        }
        assert_eq!(found, vec![Some(2); 3]);
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deferred_refusal_empty_batch_is_evaluation_no_batch_is_not() {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let (p, s, t) = fixture(vec![Some(2)], false);
        let state = owner(&p, &s, &t, &pool);
        let held = pool.allocate((1 << 20) - pool.used()).unwrap();
        let absent = FilterExec::new(child(true, false), p.clone())
            .with_initialized_membership(state.clone());
        assert!(absent
            .execute(0)
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .is_none());
        assert!(state.state.get().is_none());
        let emitted = FilterExec::new(child(false, false), p.clone())
            .with_initialized_membership(state.clone());
        let error = emitted
            .execute(0)
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), "Execution");
        assert!(error.to_string().contains("Memory limit exceeded in '"));
        drop(held); // Stored error is not retried after resources become available.
        assert!(matches!(state.state.get(), Some(Outcome::Failed(_))));
        let missing = batch("wrong", vec![]);
        assert!(matches!(
            state.evaluate(&missing, &p).await.unwrap_err().root(),
            QueryError::ColumnNotFound(_)
        ));
        drop((absent, emitted, state));
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn child_failure_and_cancel_drop_owned_streams_without_starting_rhs() {
        for mode in [1, 2] {
            let pool = crate::execution::create_memory_pool(1 << 20);
            let (p, schema, tables) = fixture(vec![Some(2)], false);
            let state = owner(&p, &schema, &tables, &pool);
            let mut source = child(false, true);
            Arc::get_mut(&mut source).unwrap().prepare_mode = mode;
            let input: Arc<dyn PhysicalOperator> = source.clone();
            if mode == 1 {
                let error = state
                    .prepare_child(&input)
                    .await
                    .err()
                    .expect("child failure");
                assert!(error.to_string().contains("child initialization failed"));
            } else {
                let future = state.prepare_child(&input);
                futures::pin_mut!(future);
                assert!(futures::poll!(future.as_mut()).is_pending());
            }
            assert!(state.state.get().is_none());
            assert_eq!(source.drops.load(Ordering::SeqCst), 3);
            assert_eq!(source.polls.load(Ordering::SeqCst), 0);
            drop(state);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn mismatched_actual_physical_schema_declines_before_admission() {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let (p, s, mut t) = fixture(vec![Some(2)], false);
        let actual = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::UInt64, true)])),
            vec![Arc::new(arrow::array::UInt64Array::from(vec![2]))],
        )
        .unwrap();
        t.insert(
            "rhs".into(),
            Arc::new(MemoryTable::new(batch("k", vec![]).schema(), vec![actual])),
        );
        assert!(
            InitializedMembership::try_new(&p, &s, &t, &pool, &ExecutionConfig::default())
                .unwrap()
                .is_none()
        );
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod real_composition_tests {
    use super::*;
    use crate::physical::operators::spillable::AggregateExpr;
    use crate::physical::operators::{
        FilterExec, MemoryTableExec, SpillableHashAggregateExec, SpillableHashJoinExec,
    };
    use crate::physical::queue_layout::GatherCopyBound;
    use crate::planner::{AggregateFunction, JoinType, PlanSchema, ScanNode, SchemaField};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Probe {
        batch: RecordBatch,
        pool: SharedMemoryPool,
        owner: Arc<InitializedMembership>,
        calls: AtomicUsize,
        polls: Arc<AtomicUsize>,
        gate: Arc<tokio::sync::Semaphore>,
    }
    #[async_trait]
    impl PhysicalOperator for Probe {
        fn name(&self) -> &str {
            "initialized-membership-overlap"
        }
        fn schema(&self) -> SchemaRef {
            self.batch.schema()
        }
        fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
            vec![]
        }
        fn output_partitions(&self) -> usize {
            3
        }
        fn resident_gather_copy_bound(&self) -> Option<GatherCopyBound> {
            GatherCopyBound::from_batches(&self.batch.schema(), &[self.batch.clone()])
        }
        async fn execute(&self, p: usize) -> Result<RecordBatchStream> {
            crate::physical::check_partition(self, p)?;
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                self.owner.state.get().is_none(),
                "child initialization precedes RHS membership"
            );
            // Pin the actual shared root while initializing. No outer queue
            // envelope can be retained yet; all remaining headroom is borrowable.
            assert!(
                self.pool.used() <= self.owner._control.size() + 1024,
                "only control metadata and this tiny build index may precede initialization"
            );
            let held = self.pool.allocate(self.pool.available())?;
            drop(held);
            let batch = self.batch.clone();
            let polls = self.polls.clone();
            let gate = self.gate.clone();
            Ok(Box::pin(stream::once(async move {
                let ordinal = polls.fetch_add(1, Ordering::SeqCst);
                if ordinal == 1 {
                    gate.add_permits(3);
                }
                gate.acquire().await.unwrap().forget();
                Ok(batch)
            })))
        }
    }
    fn batch(name: &str, values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }
    #[test]
    fn real_aggregate_filter_inner_prepares_rhs_then_overlaps_probes_exactly_once() {
        let rayon = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rayon.install(|| {
            runtime.block_on(async {
                let pool = crate::execution::create_memory_pool(8 << 20);
                let rhs = batch("k", vec![Some(2), Some(2), None]);
                let tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::from([(
                    "rhs".into(),
                    Arc::new(MemoryTable::new(rhs.schema(), vec![rhs])) as Arc<dyn TableProvider>,
                )]);
                let predicate = Expr::InSubquery {
                    expr: Box::new(Expr::column("pk")),
                    subquery: Arc::new(LogicalPlan::Scan(ScanNode {
                        table_name: "rhs".into(),
                        schema: PlanSchema::new(vec![
                            SchemaField::new("k", DataType::Int64).with_relation("rhs")
                        ]),
                        projection: None,
                        filter: None,
                    })),
                    negated: false,
                };
                let probe_batch = batch("pk", vec![Some(2), Some(2), None, Some(3)]);
                let build_batch = batch("bk", vec![Some(2), Some(2), Some(3), None]);
                let schema = Arc::new(Schema::new(vec![
                    Field::new("bk", DataType::Int64, true),
                    Field::new("pk", DataType::Int64, true),
                ]));
                let config = ExecutionConfig::new().with_memory_limit(pool.max());
                let owner =
                    InitializedMembership::try_new(&predicate, &schema, &tables, &pool, &config)
                        .unwrap()
                        .unwrap();
                let probe = Arc::new(Probe {
                    batch: probe_batch,
                    pool: pool.clone(),
                    owner: owner.clone(),
                    calls: AtomicUsize::new(0),
                    polls: Arc::new(AtomicUsize::new(0)),
                    gate: Arc::new(tokio::sync::Semaphore::new(0)),
                });
                let build = Arc::new(MemoryTableExec::new(
                    "build",
                    build_batch.schema(),
                    vec![build_batch],
                    None,
                ));
                let join = Arc::new(SpillableHashJoinExec::new(
                    build,
                    probe.clone(),
                    vec![(Expr::column("bk"), Expr::column("pk"))],
                    JoinType::Inner,
                    pool.clone(),
                    config.clone(),
                ));
                let filter = Arc::new(
                    FilterExec::new(join, predicate).with_initialized_membership(owner.clone()),
                );
                assert!(
                    filter.pool_independent_queue_copy_bound().is_none(),
                    "no unsafe static subquery certificate"
                );
                let aggregate = SpillableHashAggregateExec::new(
                    filter,
                    vec![],
                    vec![AggregateExpr {
                        func: AggregateFunction::Count,
                        input: Expr::column("pk"),
                        distinct: false,
                        second_arg: None,
                    }],
                    batch("n", vec![]).schema(),
                    pool.clone(),
                    config,
                );
                let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    let mut result = Vec::new();
                    for p in 0..aggregate.output_partitions() {
                        let mut stream = aggregate.execute(p).await.unwrap();
                        while let Some(batch) = stream.try_next().await.unwrap() {
                            result.extend(
                                batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .iter(),
                            );
                        }
                    }
                    result
                })
                .await
                .expect("prepared wrapper must allow two probe polls to overlap");
                // 2 build duplicates × 2 matching probe duplicates × 3 partitions.
                assert_eq!(result, vec![Some(12)]);
                assert_eq!(probe.calls.load(Ordering::SeqCst), 3);
                assert_eq!(probe.polls.load(Ordering::SeqCst), 3);
                assert!(matches!(owner.state.get(), Some(Outcome::Ready(_))));
                assert!(pool.reserved_peak() <= pool.max());
                drop((aggregate, probe, owner));
                assert_eq!(pool.used(), 0);
            })
        });
    }
}
