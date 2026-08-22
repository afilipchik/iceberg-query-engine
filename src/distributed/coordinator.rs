//! Scatter–gather: the initiator fans a query out, each node answers over its
//! own splits, the initiator merges.
//!
//! # Shape of the exchange
//!
//! ```text
//!            POST /sql?distributed=1        (any node may receive this)
//!                     |
//!            plan_distributed()  -- reject loudly if unsupported
//!                     |
//!            enumerate splits  ->  assign_lpt(bytes)  ->  imbalance
//!                     |
//!        +------------+------------+----------------+
//!        |                         |                |
//!   POST /fragment            POST /fragment     (self: in-process,
//!   shard 1 of 3              shard 2 of 3        same executor)
//!        |                         |                |
//!        +------------+------------+----------------+
//!                     |
//!            merge (SUM of counts, SUM/MIN/MAX, sum/count for AVG)
//! ```
//!
//! # No central assignment service
//!
//! The initiator sends `(shard_index, shard_count, digest)`. Each worker
//! *recomputes* the whole assignment from its own copy of the table metadata
//! and takes slice `shard_index`. It then compares its digest with the
//! initiator's. If the two disagree — a node with stale data, a half-finished
//! copy, a different mount — the fragment FAILS. That check is the reason a
//! node is allowed to compute its own share instead of being told row by row:
//! divergence becomes an error rather than an answer over the wrong rows.
//!
//! # Failure
//!
//! Any fragment failing fails the query, with the node named. There is no
//! partial-result mode and no "best effort" flag, because a partial answer that
//! looks like a complete one is the single most expensive bug a query engine
//! can ship.
//!
//! # What this does NOT claim
//!
//! Running N nodes on one box does not multiply memory bandwidth, disk, or page
//! cache. The wall-time numbers this produces measure *coordination*, not
//! scaling. See `spread` in [`Distribution`] and the note on it.

use crate::distributed::gather::{plan_gather, GatherPlan};
use crate::distributed::plan::{plan_distributed, DistributedPlan, MergeShape, PARTIAL_TABLE};
use crate::distributed::splits::{assign_lpt, Assignment, SplitSet};
use crate::error::{QueryError, Result};
use crate::execution::{ExecutionContext, QueryResult};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A fragment request, as it goes over the wire.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FragmentRequest {
    /// The partial SQL. Already rewritten by the initiator; a worker never
    /// re-derives it, so there is exactly one place the rewrite can happen.
    pub sql: String,
    /// Table whose splits are being divided.
    pub table: String,
    pub shard_index: usize,
    pub shard_count: usize,
    /// The initiator's [`SplitSet::digest`]. A mismatch fails the fragment.
    pub splits_digest: u64,
}

/// What one node contributed.
#[derive(Clone, Debug, serde::Serialize)]
pub struct NodeContribution {
    pub node_id: u64,
    pub address: String,
    /// Table this contribution scanned. One query produces contributions for
    /// several tables under [`MergeShape::Gather`]; exactly one otherwise.
    pub table: String,
    pub shard_index: usize,
    /// Bytes the assignment gave this node. The balance metric is computed
    /// from these.
    pub assigned_bytes: u64,
    pub assigned_rows: i64,
    pub assigned_splits: usize,
    /// Rows the node returned (post-filter, post-partial-aggregate).
    pub result_rows: usize,
    /// Wall time on that node, measured on that node.
    pub elapsed_ms: f64,
    /// True when the initiator executed this shard in-process.
    pub local: bool,
}

/// Everything about how the work was divided, reported with the answer.
///
/// Exposed on `/sql` responses (header `x-qe-distribution`) so balance is an
/// operational fact rather than a claim in a test.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Distribution {
    pub table: String,
    pub shape: MergeShape,
    pub shard_count: usize,
    pub total_splits: usize,
    pub total_bytes: u64,
    pub target_split_bytes: u64,
    pub splits_digest: u64,
    /// `max_node_bytes / mean_node_bytes` over the participating nodes. The
    /// acceptance gate for TPC-H `lineitem` is <= 1.10 at 3 and at 8 nodes.
    pub imbalance: f64,
    /// `max_node_ms / mean_node_ms`. This is the number that actually matters
    /// for latency, and it is NOT the same as `imbalance`: equal bytes is not
    /// equal time when nodes differ in speed or filters differ in selectivity.
    /// Equalizing it needs work stealing, which is M4.
    pub wall_time_spread: f64,
    pub nodes: Vec<NodeContribution>,
    pub partial_sql: String,
    pub final_sql: Option<String>,
}

/// One member the initiator may send work to.
#[derive(Clone, Debug)]
pub struct Participant {
    pub node_id: u64,
    pub address: String,
    /// True for the initiator itself.
    pub is_self: bool,
}

/// How a fragment reaches a peer. Injected so the tests can drive the whole
/// coordinator without a socket, and so the server can supply the real one.
#[async_trait::async_trait]
pub trait FragmentTransport: Send + Sync {
    /// Execute `req` on `address` and return (Arrow IPC bytes, rows, node ms).
    async fn send(&self, address: &str, req: &FragmentRequest) -> Result<(Vec<u8>, usize, f64)>;
}

/// The result of a distributed query: the answer plus how it was produced.
#[derive(Debug)]
pub struct DistributedResult {
    pub result: QueryResult,
    pub distribution: Distribution,
}

/// Enumerate the splits of `table` as this node sees them.
///
/// The provider decides what a split IS: row ranges inside Parquet row groups
/// (Parquet and Iceberg tables), whole fragments (Lance). What every provider
/// must guarantee is the same canonical enumeration on every node — the
/// digest interlock depends on it.
pub fn splits_of(ctx: &ExecutionContext, table: &str, nodes: usize) -> Result<SplitSet> {
    let provider = ctx
        .table_provider(table)
        .ok_or_else(|| QueryError::TableNotFound(table.to_string()))?;
    provider.distributed_splits(table, nodes).ok_or_else(|| {
        QueryError::NotImplemented(format!(
            "table `{table}`'s provider cannot be divided into distributed splits; \
             Parquet, Iceberg and Lance tables can"
        ))
    })?
}

/// Build a context in which `table` is replaced by this node's shard of it.
///
/// Everything else about the context is the node's normal configuration, and
/// the plan the worker runs is an ordinary local plan. That is what keeps M2
/// clear of the readiness blockers: nothing below the scan changes.
pub fn shard_context(
    base: &ExecutionContext,
    table: &str,
    set: &SplitSet,
    assignment: &Assignment,
    shard_index: usize,
) -> Result<(ExecutionContext, ShardStats)> {
    let provider = base
        .table_provider(table)
        .ok_or_else(|| QueryError::TableNotFound(table.to_string()))?;
    let owned: Vec<_> = assignment
        .per_node
        .get(shard_index)
        .ok_or_else(|| {
            QueryError::Execution(format!(
                "shard index {shard_index} is out of range for a {}-way assignment",
                assignment.nodes
            ))
        })?
        .iter()
        .map(|&i| set.splits[i].clone())
        .collect();

    let stats = ShardStats {
        bytes: owned.iter().map(|s| s.bytes).sum(),
        rows: owned.iter().map(|s| s.num_rows).sum(),
        splits: owned.len(),
    };

    let sharded = provider.shard_by_splits(&owned).ok_or_else(|| {
        QueryError::Execution(format!(
            "table `{table}`'s provider enumerated splits but cannot shard by them; \
                 distributed_splits and shard_by_splits must be implemented together"
        ))
    })??;
    // The fragment sees the FULL catalog — every table the node serves — with
    // only the sharded table overridden. This is what lets a partial query
    // join its shard against replicated dimension tables (the ClickHouse
    // sharded-fact / replicated-dims model; distributed-pushdown epic).
    let mut config = base.config().clone();
    // Never GPU-offload inside a shard context — the device column cache is
    // keyed by table name, and this context's tables are SHARDS.
    config.gpu_offload = false;
    let mut ctx = ExecutionContext::with_config(config);
    for name in base.table_names() {
        if name != table {
            if let Some(p) = base.table_provider(&name) {
                ctx.register_table_provider(&name, p);
            }
        }
    }
    ctx.register_table_provider(table, sharded);
    Ok((ctx, stats))
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ShardStats {
    pub bytes: u64,
    pub rows: i64,
    pub splits: usize,
}

/// Execute one fragment locally. This is the ONLY place a shard is executed —
/// the initiator's own shard and every peer's take the same path, so there is
/// no "local mode" that can drift from the remote one.
pub async fn execute_fragment(
    base: &ExecutionContext,
    req: &FragmentRequest,
) -> Result<(QueryResult, ShardStats)> {
    let set = splits_of(base, &req.table, req.shard_count)?;
    let digest = set.digest();
    if digest != req.splits_digest {
        return Err(QueryError::Execution(format!(
            "split digest mismatch on table `{}`: initiator has {:#x}, this node computes {:#x} \
             ({} splits, {} rows, {} bytes here). The nodes do not agree on what data exists, \
             so any answer would be over the wrong rows. Refusing.",
            req.table,
            req.splits_digest,
            digest,
            set.len(),
            set.total_rows,
            set.total_bytes
        )));
    }
    let assignment = assign_lpt(&set, req.shard_count);
    let (ctx, stats) = shard_context(base, &req.table, &set, &assignment, req.shard_index)?;
    let result = ctx.sql(&req.sql).await?;
    Ok((result, stats))
}

/// Fan `sql` out over the shards of `table` and collect every shard's batches.
///
/// This is the one fan-out in the module: the scatter path runs it once with a
/// partial query, the gather path once per referenced table with a column
/// scan. The initiator's own shard executes in-process while the remote ones
/// fly; any shard failing fails the whole call, with the node named.
async fn scatter_sql_over_table(
    base: &ExecutionContext,
    sql: &str,
    table: &str,
    set: &SplitSet,
    participants: &[Participant],
    transport: &dyn FragmentTransport,
) -> Result<(Vec<RecordBatch>, Vec<NodeContribution>)> {
    let digest = set.digest();
    let assignment = assign_lpt(set, participants.len());

    // A node with no splits is skipped rather than sent an empty fragment. For
    // a global aggregate an empty shard would contribute an identity row, which
    // merges harmlessly — but sending work that is known to be empty wastes a
    // round trip and muddies the wall-time spread.
    let active: Vec<usize> = (0..participants.len())
        .filter(|&i| assignment.node_splits[i] > 0)
        .collect();

    let mut contributions: Vec<NodeContribution> = Vec::with_capacity(active.len());
    let mut batches: Vec<RecordBatch> = Vec::new();

    if active.is_empty() {
        // The table has no row groups at all. Answer it locally over an empty
        // shard so an aggregate still emits its identity row (COUNT = 0), which
        // is what a single-node run returns.
        let (ctx, _) = shard_context(base, table, set, &assignment, 0)?;
        let started = Instant::now();
        let r = ctx.sql(sql).await?;
        contributions.push(NodeContribution {
            node_id: participants[0].node_id,
            address: participants[0].address.clone(),
            table: table.to_string(),
            shard_index: 0,
            assigned_bytes: 0,
            assigned_rows: 0,
            assigned_splits: 0,
            result_rows: r.row_count,
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
            local: true,
        });
        batches.extend(r.batches);
        return Ok((batches, contributions));
    }

    let req_for = |shard_index: usize| FragmentRequest {
        sql: sql.to_string(),
        table: table.to_string(),
        shard_index,
        shard_count: participants.len(),
        splits_digest: digest,
    };

    // Remote fragments concurrently; the local one on this thread while
    // they fly.
    let remote: Vec<_> = active
        .iter()
        .copied()
        .filter(|&i| !participants[i].is_self)
        .map(|i| {
            let req = req_for(i);
            let addr = participants[i].address.clone();
            async move {
                let started = Instant::now();
                let out = transport.send(&addr, &req).await;
                (i, out, started.elapsed())
            }
        })
        .collect();
    let remote = futures::future::join_all(remote);

    let local_index = active.iter().copied().find(|&i| participants[i].is_self);
    let local = async {
        match local_index {
            None => Ok::<_, QueryError>(None),
            Some(i) => {
                let started = Instant::now();
                let (r, stats) = execute_fragment(base, &req_for(i)).await?;
                Ok(Some((i, r, stats, started.elapsed())))
            }
        }
    };

    let (remote_out, local_out) = futures::future::join(remote, local).await;
    let local_out: Option<_> = local_out?;

    if let Some((i, r, stats, elapsed)) = local_out {
        contributions.push(NodeContribution {
            node_id: participants[i].node_id,
            address: participants[i].address.clone(),
            table: table.to_string(),
            shard_index: i,
            assigned_bytes: stats.bytes,
            assigned_rows: stats.rows,
            assigned_splits: stats.splits,
            result_rows: r.row_count,
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            local: true,
        });
        batches.extend(r.batches);
    }

    for (i, out, elapsed) in remote_out {
        let (bytes, rows, node_ms) = out.map_err(|e| {
            QueryError::Execution(format!(
                "distributed query failed: node {} ({}) did not complete shard {} of {} \
                 (table `{table}`): {e}",
                participants[i].node_id,
                participants[i].address,
                i,
                participants.len()
            ))
        })?;
        let decoded = decode_ipc(&bytes).map_err(|e| {
            QueryError::Execution(format!(
                "node {} ({}) returned an undecodable fragment result: {e}",
                participants[i].node_id, participants[i].address
            ))
        })?;
        contributions.push(NodeContribution {
            node_id: participants[i].node_id,
            address: participants[i].address.clone(),
            table: table.to_string(),
            shard_index: i,
            assigned_bytes: assignment.node_bytes[i],
            assigned_rows: assignment.node_rows[i],
            assigned_splits: assignment.node_splits[i],
            result_rows: rows,
            elapsed_ms: if node_ms > 0.0 {
                node_ms
            } else {
                elapsed.as_secs_f64() * 1000.0
            },
            local: false,
        });
        batches.extend(decoded);
    }

    contributions.sort_by_key(|c| c.shard_index);
    Ok((batches, contributions))
}

/// `max/mean` of a per-node reduction over contributions — the balance metrics
/// for a query that may have scanned several tables on each node.
fn per_node_spread(
    contributions: &[NodeContribution],
    value: impl Fn(&NodeContribution) -> f64,
) -> f64 {
    let mut per_node: std::collections::BTreeMap<&str, f64> = Default::default();
    for c in contributions {
        *per_node.entry(c.address.as_str()).or_default() += value(c);
    }
    let n = per_node.len();
    if n == 0 {
        return 1.0;
    }
    let sum: f64 = per_node.values().sum();
    let max = per_node.values().fold(0.0f64, |a, &b| a.max(b));
    let mean = sum / n as f64;
    if mean > 0.0 {
        max / mean
    } else {
        1.0
    }
}

/// Run `sql` across `participants` on the exact scatter–gather path, with the
/// initiator executing its own shard in-process and `transport` reaching the
/// rest. Refuses (`NotImplemented`) any shape whose partial/final split is not
/// exact — [`execute_any_distributed`] adds the general fallback.
pub async fn execute_distributed(
    base: &ExecutionContext,
    sql: &str,
    participants: &[Participant],
    transport: &dyn FragmentTransport,
) -> Result<DistributedResult> {
    let plan = plan_distributed(base, sql)?;
    if participants.is_empty() {
        return Err(QueryError::Execution(
            "no cluster members are up; cannot execute a distributed query".into(),
        ));
    }

    let set = splits_of(base, &plan.table, participants.len())?;
    let digest = set.digest();
    let assignment = assign_lpt(&set, participants.len());

    let (batches, contributions) = scatter_sql_over_table(
        base,
        &plan.partial_sql,
        &plan.table,
        &set,
        participants,
        transport,
    )
    .await?;

    let result = merge(base, &plan, batches).await?;

    Ok(DistributedResult {
        result,
        distribution: Distribution {
            table: plan.table.clone(),
            shape: plan.shape,
            shard_count: participants.len(),
            total_splits: set.len(),
            total_bytes: set.total_bytes,
            target_split_bytes: set.target_split_bytes,
            splits_digest: digest,
            imbalance: assignment.imbalance(),
            wall_time_spread: per_node_spread(&contributions, |c| c.elapsed_ms),
            nodes: contributions,
            partial_sql: plan.partial_sql.clone(),
            final_sql: plan.final_sql.clone(),
        },
    })
}

/// Run `sql` by gathering sharded scans of every referenced table, then
/// executing the original statement on the initiator. See
/// [`crate::distributed::gather`] for the design and its limits.
pub async fn execute_gathered(
    base: &ExecutionContext,
    plan: &GatherPlan,
    participants: &[Participant],
    transport: &dyn FragmentTransport,
) -> Result<DistributedResult> {
    if participants.is_empty() {
        return Err(QueryError::Execution(
            "no cluster members are up; cannot execute a distributed query".into(),
        ));
    }

    // Enumerate every table's splits BEFORE any fragment is sent, both for the
    // memory bound and so a non-shardable table refuses the query up front.
    let mut sets: Vec<SplitSet> = Vec::with_capacity(plan.tables.len());
    for t in &plan.tables {
        sets.push(splits_of(base, &t.name, participants.len())?);
    }

    // The gathered columns are materialized in initiator memory. Refuse
    // anything that could not possibly fit rather than find out by dying:
    // compressed on-disk bytes are the (conservative: pruning is not counted)
    // stand-in for what will arrive. Half the budget, because the query
    // itself — a join build side, a sort — still has to run over the result.
    let moved: u64 = sets.iter().map(|s| s.total_bytes).sum();
    let budget = (base.config().memory_limit / 2) as u64;
    if moved > budget {
        return Err(QueryError::NotImplemented(format!(
            "gather would move up to {moved} compressed bytes of {} into initiator memory, \
             over the {budget}-byte bound (half of --memory-limit). Cross-node partitioned \
             execution of this shape is M3 (shuffle); until then, raise the memory limit or \
             restrict the query to an exactly-mergeable aggregate",
            plan.tables
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        )));
    }

    // Gather every table concurrently; each table fans over all shards.
    let gathers = futures::future::join_all(plan.tables.iter().zip(&sets).map(|(t, set)| {
        scatter_sql_over_table(base, &t.gather_sql, &t.name, set, participants, transport)
    }))
    .await;

    let mut gctx = ExecutionContext::with_config(base.config().clone());
    let mut contributions: Vec<NodeContribution> = Vec::new();
    for (t, out) in plan.tables.iter().zip(gathers) {
        let (batches, contrib) = out?;
        let batches = unify(batches)?;
        let schema = match batches.first() {
            Some(b) => b.schema(),
            // Zero rows gathered: the registered table still needs the shape
            // the statement will bind against — the provider schema, narrowed
            // to the gathered columns.
            None => {
                let full = base
                    .table_provider(&t.name)
                    .ok_or_else(|| QueryError::TableNotFound(t.name.clone()))?
                    .schema();
                match &t.columns {
                    None => full,
                    Some(cols) => Arc::new(arrow::datatypes::Schema::new(
                        full.fields()
                            .iter()
                            .filter(|f| cols.contains(f.name()))
                            .map(|f| f.as_ref().clone())
                            .collect::<Vec<_>>(),
                    )),
                }
            }
        };
        gctx.register_table(&t.name, schema, batches);
        contributions.extend(contrib);
    }

    // The original statement, over the gathered tables, with the ordinary
    // local engine. Nothing was rewritten, so nothing can disagree with what a
    // single process would answer.
    let result = gctx.sql(&plan.sql).await?;

    let total_splits = sets.iter().map(|s| s.len()).sum();
    let total_bytes = sets.iter().map(|s| s.total_bytes).sum();
    let target_split_bytes = sets.iter().map(|s| s.target_split_bytes).max().unwrap_or(0);
    let digest = sets
        .iter()
        .fold(0u64, |acc, s| acc.rotate_left(1) ^ s.digest());

    Ok(DistributedResult {
        result,
        distribution: Distribution {
            table: plan
                .tables
                .iter()
                .map(|t| t.name.clone())
                .collect::<Vec<_>>()
                .join(","),
            shape: MergeShape::Gather,
            shard_count: participants.len(),
            total_splits,
            total_bytes,
            target_split_bytes,
            splits_digest: digest,
            imbalance: per_node_spread(&contributions, |c| c.assigned_bytes as f64),
            wall_time_spread: per_node_spread(&contributions, |c| c.elapsed_ms),
            nodes: contributions,
            partial_sql: plan
                .tables
                .iter()
                .map(|t| t.gather_sql.clone())
                .collect::<Vec<_>>()
                .join("; "),
            final_sql: Some(plan.sql.clone()),
        },
    })
}

/// The distributed entry point: the exact scatter–gather path when the shape
/// allows it, the gather path for everything else. A non-`NotImplemented`
/// planning error (unknown table, unknown column) propagates untouched — it
/// is the statement that is wrong, not the distribution.
pub async fn execute_any_distributed(
    base: &ExecutionContext,
    sql: &str,
    participants: &[Participant],
    transport: &dyn FragmentTransport,
) -> Result<DistributedResult> {
    match plan_distributed(base, sql) {
        Ok(_) => execute_distributed(base, sql, participants, transport).await,
        Err(QueryError::NotImplemented(_)) => {
            let plan = plan_gather(base, sql)?;
            execute_gathered(base, &plan, participants, transport).await
        }
        Err(other) => Err(other),
    }
}

/// Combine the shards' answers.
async fn merge(
    base: &ExecutionContext,
    plan: &DistributedPlan,
    batches: Vec<RecordBatch>,
) -> Result<QueryResult> {
    let batches = unify(batches)?;
    match plan.shape {
        MergeShape::Concat => {
            let schema = batches
                .first()
                .map(|b| b.schema())
                .ok_or_else(|| QueryError::Execution("no shard returned a schema".into()))?;
            // Empty shards ship schema-only placeholders (decode_ipc); drop
            // them so an all-empty answer matches the single-process engine,
            // which returns no batches for a rowless plain select.
            let batches: Vec<RecordBatch> =
                batches.into_iter().filter(|b| b.num_rows() > 0).collect();
            let row_count = batches.iter().map(|b| b.num_rows()).sum();
            Ok(QueryResult {
                schema,
                batches,
                row_count,
                metrics: Default::default(),
            })
        }
        MergeShape::Gather => {
            // A DistributedPlan is never built with this shape; gather runs
            // through execute_gathered, which does not merge.
            return Err(QueryError::Execution(
                "merge() called with MergeShape::Gather — a scatter plan cannot carry it".into(),
            ));
        }
        MergeShape::TwoPhase | MergeShape::TopN => {
            let final_sql = plan
                .final_sql
                .as_ref()
                .expect("TwoPhase/TopN always carry a merge query");
            let schema = batches
                .first()
                .map(|b| b.schema())
                .ok_or_else(|| QueryError::Execution("no shard returned a schema".into()))?;
            let mut ctx = ExecutionContext::with_config(base.config().clone());
            ctx.register_table(PARTIAL_TABLE, schema, batches);
            // The merge is an ordinary local query over an in-memory table, so
            // it gets the engine's real aggregation — including its NULL and
            // type semantics — rather than a hand-rolled reduction that would
            // have to reimplement them and could disagree.
            let mut result = ctx.sql(final_sql).await?;
            // TopN finals are plain selects; the single-process engine emits
            // NO batches for a rowless plain select, while an aggregate's
            // empty answer keeps its zero-row batch. Match it exactly — the
            // acceptance gate diffs CSV byte for byte.
            if matches!(plan.shape, MergeShape::TopN) && result.row_count == 0 {
                result.batches.clear();
            }
            Ok(result)
        }
    }
}

/// Make every batch share one schema so they can be stacked.
///
/// Nullability is widened to `true` everywhere: a shard whose filter matched
/// nothing legitimately produces all-null aggregate columns while another
/// shard's are non-null, and the two must still concatenate. A difference in
/// field NAME or TYPE is not widened — it means the shards ran different
/// queries, and that is an error, loudly.
fn unify(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(batches);
    };
    let unified = Arc::new(arrow::datatypes::Schema::new(
        first
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ));

    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let s = b.schema();
        if s.fields().len() != unified.fields().len() {
            return Err(QueryError::Execution(format!(
                "shards returned different column counts ({} vs {}); refusing to merge",
                s.fields().len(),
                unified.fields().len()
            )));
        }
        for (a, e) in s.fields().iter().zip(unified.fields().iter()) {
            if a.name() != e.name() || a.data_type() != e.data_type() {
                return Err(QueryError::Execution(format!(
                    "shards returned incompatible columns: `{}: {:?}` vs `{}: {:?}`; refusing to merge",
                    a.name(),
                    a.data_type(),
                    e.name(),
                    e.data_type()
                )));
            }
        }
        out.push(RecordBatch::try_new(unified.clone(), b.columns().to_vec())?);
    }
    Ok(out)
}

/// Decode an Arrow IPC stream into batches.
///
/// An empty shard answers with a schema-only stream; that schema must
/// survive as a zero-row batch, or the merge stage cannot even register the
/// partial table (Q20-shaped TopN over a selective filter hits this).
pub fn decode_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let mut out = Vec::new();
    for b in reader {
        out.push(b?);
    }
    if out.is_empty() {
        out.push(RecordBatch::new_empty(schema));
    }
    Ok(out)
}

/// Encode batches as an Arrow IPC stream.
pub fn encode_ipc(
    schema: &arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| schema.clone());
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)?;
        for b in batches {
            w.write(b)?;
        }
        w.finish()?;
    }
    Ok(buf)
}

/// The default fragment timeout. Generous: a shard of a multi-gigabyte table is
/// a real query, not a health check. It exists so a wedged peer fails the query
/// instead of hanging it forever.
pub const DEFAULT_FRAGMENT_TIMEOUT: Duration = Duration::from_secs(600);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::splits::Split;

    fn ctx() -> ExecutionContext {
        let mut c = ExecutionContext::new();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/data/tpch-1mb");
        for t in ["lineitem", "orders", "nation"] {
            c.register_parquet(t, format!("{dir}/{t}.parquet"))
                .unwrap_or_else(|e| panic!("cannot load {t}: {e}"));
        }
        c
    }

    /// Transport that runs the peer's fragment in-process against the same
    /// data. It exercises the coordinator, the digest check, the IPC round
    /// trip and the merge — everything except the socket, which
    /// `tests/distributed_cluster.rs` covers with real ones.
    struct InProcess {
        peer: Arc<ExecutionContext>,
    }

    #[async_trait::async_trait]
    impl FragmentTransport for InProcess {
        async fn send(
            &self,
            _address: &str,
            req: &FragmentRequest,
        ) -> Result<(Vec<u8>, usize, f64)> {
            let (r, _) = execute_fragment(&self.peer, req).await?;
            let bytes = encode_ipc(&r.schema, &r.batches)?;
            Ok((bytes, r.row_count, 0.0))
        }
    }

    struct Dead;

    #[async_trait::async_trait]
    impl FragmentTransport for Dead {
        async fn send(
            &self,
            address: &str,
            _req: &FragmentRequest,
        ) -> Result<(Vec<u8>, usize, f64)> {
            Err(QueryError::Execution(format!(
                "connection refused: {address}"
            )))
        }
    }

    fn participants(n: usize) -> Vec<Participant> {
        (0..n)
            .map(|i| Participant {
                node_id: i as u64,
                address: format!("127.0.0.1:{}", 17700 + i),
                is_self: i == 0,
            })
            .collect()
    }

    async fn distributed(sql: &str, n: usize) -> Result<DistributedResult> {
        let base = ctx();
        let peer = Arc::new(ctx());
        execute_distributed(&base, sql, &participants(n), &InProcess { peer }).await
    }

    fn csv(r: &QueryResult) -> String {
        let mut buf = Vec::new();
        {
            let mut w = arrow::csv::WriterBuilder::new()
                .with_header(true)
                .build(&mut buf);
            for b in &r.batches {
                w.write(b).unwrap();
            }
        }
        String::from_utf8(buf).unwrap()
    }

    async fn single_node(sql: &str) -> String {
        csv(&ctx().sql(sql).await.unwrap())
    }

    /// Cell-by-cell comparison with the SAME numeric tolerance the project's
    /// DuckDB-validated suite uses (`tests/duckdb_validated.rs`): exact for
    /// integers, strings and NULLs, 1e-6 relative for floating point.
    ///
    /// The tolerance is not slack, it is arithmetic. `SUM` over `f64` is not
    /// associative, so adding a column in three shard-sized pieces and adding
    /// it in one pass differ in the last bits — by ~1e-16 relative here. That
    /// is true of any parallel or distributed sum, including DuckDB's own
    /// across thread counts, and demanding bit-identity would mean demanding
    /// single-threaded execution.
    /// Rows are compared as a SET, after the header. `GROUP BY` without
    /// `ORDER BY` has unspecified row order in SQL, and it genuinely differs
    /// here: the merge hashes a few hundred partial rows where the single-node
    /// run hashes six thousand base rows. Requiring the same order would be
    /// asserting an implementation detail neither engine promises — and
    /// `ORDER BY` is rejected outright for distributed queries precisely so no
    /// caller can mistake this for a guarantee.
    fn sorted_rows(csv: &str) -> Vec<String> {
        let mut lines: Vec<String> = csv.lines().map(|s| s.to_string()).collect();
        if lines.len() > 1 {
            lines[1..].sort();
        }
        lines
    }

    fn assert_cells_match(got: &str, expected: &str, context: &str) {
        let gs = sorted_rows(got);
        let es = sorted_rows(expected);
        let g: Vec<&str> = gs.iter().map(|s| s.as_str()).collect();
        let e: Vec<&str> = es.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            g.len(),
            e.len(),
            "{context}: row count differs\n{got}\n{expected}"
        );
        for (row, (gl, el)) in g.iter().zip(e.iter()).enumerate() {
            let gc: Vec<&str> = gl.split(',').collect();
            let ec: Vec<&str> = el.split(',').collect();
            assert_eq!(
                gc.len(),
                ec.len(),
                "{context}: column count differs on row {row}"
            );
            for (col, (a, b)) in gc.iter().zip(ec.iter()).enumerate() {
                if a == b {
                    continue;
                }
                match (a.parse::<f64>(), b.parse::<f64>()) {
                    (Ok(x), Ok(y)) => {
                        let tol = (1e-6 * x.abs().max(y.abs())).max(1e-9);
                        assert!(
                            (x - y).abs() <= tol,
                            "{context}: row {row} col {col}: {a} vs {b}"
                        );
                    }
                    _ => panic!("{context}: row {row} col {col}: `{a}` vs `{b}`"),
                }
            }
        }
    }

    /// The core claim: a distributed answer equals the single-node answer,
    /// cell for cell, at every cluster size.
    #[tokio::test]
    async fn distributed_answers_match_the_single_node_answer() {
        let queries = [
            "SELECT COUNT(*) AS n FROM lineitem",
            "SELECT SUM(l_quantity) AS s, MIN(l_quantity) AS lo, MAX(l_quantity) AS hi FROM lineitem",
            "SELECT AVG(l_extendedprice) AS a FROM lineitem",
            "SELECT COUNT(*) AS n FROM lineitem WHERE l_shipdate < '1994-01-01'",
            "SELECT COUNT(*) AS n FROM lineitem WHERE l_orderkey < 0",
            "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sq, \
             SUM(l_extendedprice) AS sp, AVG(l_discount) AS ad, COUNT(*) AS c \
             FROM lineitem GROUP BY l_returnflag, l_linestatus",
            "SELECT COUNT(*) AS n, SUM(o_totalprice) AS t FROM orders WHERE o_orderstatus = 'F'",
        ];
        for sql in queries {
            let expected = single_node(sql).await;
            for n in [1usize, 2, 3, 5] {
                let got = distributed(sql, n).await.unwrap_or_else(|e| {
                    panic!("`{sql}` at {n} nodes failed: {e}");
                });
                assert_cells_match(
                    &csv(&got.result),
                    &expected,
                    &format!("`{sql}` at {n} nodes"),
                );
            }
        }
    }

    /// The specific trap: `AVG` must not be an average of averages. With one
    /// row group in the fixture the shards are wildly uneven, which is exactly
    /// the case where the naive merge is wrong.
    #[tokio::test]
    async fn avg_is_exact_even_when_shards_are_uneven() {
        let sql = "SELECT l_returnflag, AVG(l_quantity) AS a, COUNT(*) AS c \
                   FROM lineitem GROUP BY l_returnflag";
        let expected = single_node(sql).await;
        for n in [2usize, 3, 7] {
            assert_cells_match(
                &csv(&distributed(sql, n).await.unwrap().result),
                &expected,
                &format!("AVG at {n} nodes"),
            );
        }
    }

    #[tokio::test]
    async fn a_pass_through_query_returns_every_row_exactly_once() {
        let sql = "SELECT l_orderkey, l_quantity FROM lineitem WHERE l_quantity > 45";
        let expected_rows = ctx().sql(sql).await.unwrap().row_count;
        for n in [1usize, 3] {
            let got = distributed(sql, n).await.unwrap();
            assert_eq!(got.result.row_count, expected_rows);
            assert_eq!(got.distribution.shape, MergeShape::Concat);
        }
    }

    #[tokio::test]
    async fn a_dead_node_fails_the_query_and_names_it() {
        let base = ctx();
        // Three participants, one of which is remote and refuses connections.
        let err = execute_distributed(
            &base,
            "SELECT COUNT(*) AS n FROM lineitem",
            &participants(3),
            &Dead,
        )
        .await;
        match err {
            Ok(r) => panic!(
                "a dead peer must fail the query, got {} rows",
                r.result.row_count
            ),
            Err(e) => {
                let m = e.to_string();
                assert!(m.contains("did not complete shard"), "{m}");
                assert!(
                    m.contains("127.0.0.1:177"),
                    "the failing node must be named: {m}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_digest_mismatch_refuses_to_answer() {
        let base = ctx();
        let set = splits_of(&base, "lineitem", 2).unwrap();
        let req = FragmentRequest {
            sql: "SELECT COUNT(*) AS n FROM lineitem".into(),
            table: "lineitem".into(),
            shard_index: 0,
            shard_count: 2,
            splits_digest: set.digest() ^ 0xdead_beef,
        };
        let err = execute_fragment(&base, &req).await.unwrap_err().to_string();
        assert!(err.contains("split digest mismatch"), "{err}");
        assert!(err.contains("Refusing"), "{err}");
    }

    #[tokio::test]
    async fn the_distribution_report_adds_up() {
        let d = distributed("SELECT COUNT(*) AS n FROM lineitem", 3)
            .await
            .unwrap()
            .distribution;
        assert_eq!(d.table, "lineitem");
        assert_eq!(d.shard_count, 3);
        let assigned: u64 = d.nodes.iter().map(|n| n.assigned_bytes).sum();
        assert_eq!(assigned, d.total_bytes, "every byte must be accounted for");
        assert!(d.imbalance >= 1.0);
        assert!(d.nodes.iter().any(|n| n.local), "the initiator works too");
    }

    #[tokio::test]
    async fn unsupported_shapes_never_reach_the_wire() {
        let base = ctx();
        for sql in [
            // Joins and ORDER BY scatter since the distributed-pushdown
            // epic; what must never reach the wire is what cannot be
            // decomposed at all.
            "SELECT COUNT(DISTINCT l_orderkey) FROM lineitem",
            "SELECT STDDEV(l_quantity) FROM lineitem",
            "SELECT COUNT(*) FROM lineitem a, lineitem b WHERE a.l_orderkey = b.l_orderkey",
        ] {
            // `Dead` would error on any fan-out, so reaching NotImplemented
            // proves the rejection happened before the first request.
            let e = execute_distributed(&base, sql, &participants(3), &Dead)
                .await
                .unwrap_err();
            assert!(
                matches!(e, QueryError::NotImplemented(_)),
                "`{sql}` produced {e:?}"
            );
        }
    }

    #[test]
    fn unify_refuses_to_stack_incompatible_shards() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let a = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["1"]))],
        )
        .unwrap();
        let err = unify(vec![a, b]).unwrap_err().to_string();
        assert!(err.contains("incompatible columns"), "{err}");
    }

    #[test]
    fn a_shard_context_sees_only_its_own_splits() {
        let base = ctx();
        let set = splits_of(&base, "lineitem", 3).unwrap();
        let assignment = assign_lpt(&set, 3);
        let mut total = 0i64;
        for i in 0..3 {
            let (_, stats) = shard_context(&base, "lineitem", &set, &assignment, i).unwrap();
            total += stats.rows;
        }
        assert_eq!(total, set.total_rows);
    }

    #[test]
    fn splits_of_rejects_a_non_parquet_table() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let mut c = ExecutionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        c.register_table(
            "mem",
            schema.clone(),
            vec![
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap(),
            ],
        );
        let e = splits_of(&c, "mem", 3).unwrap_err();
        assert!(matches!(e, QueryError::NotImplemented(_)), "{e:?}");
        assert!(
            e.to_string().contains("cannot be divided"),
            "the refusal must say the provider is not shardable: {e}"
        );
    }

    #[test]
    fn a_split_that_does_not_exist_locally_is_an_error_not_silence() {
        let base = ctx();
        let provider = base.table_provider("lineitem").unwrap();
        let files = provider.parquet_files().unwrap();
        let bogus = crate::distributed::ShardedParquetTable::new(
            provider.schema(),
            vec![Split {
                table: "lineitem".into(),
                path: files[0].clone(),
                file: "lineitem.parquet".into(),
                row_group: 999,
                row_offset: 0,
                num_rows: 1,
                bytes: 1,
            }],
            None,
        );
        use crate::physical::operators::TableProvider as _;
        let e = bogus.scan(None).unwrap_err().to_string();
        assert!(e.contains("does not match the assignment"), "{e}");
    }
}
