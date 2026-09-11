//! GPU-resident aggregate offload (`--features gpu`).
//!
//! Architecture decided by the pricing bench (`examples/gpu_price_bench.rs`):
//! warm fused aggregates on the RTX-class device run 28-33x the 32-thread CPU
//! path, while a cold PCIe upload LOSES to just computing on the CPU. So the
//! rule is absolute: **the GPU only ever runs over columns already resident
//! in VRAM.** The first query that could use the GPU takes the normal CPU
//! path unchanged and enqueues background uploads; later queries find the
//! columns resident and fly. There is no path where the GPU makes a query
//! slower.
//!
//! Scope (v1, deliberately narrow):
//! - Shape: `Aggregate(Filter?(Scan(table)))` — after the optimizer has
//!   pushed the WHERE into the scan. The scanned table's provider must have
//!   a stable identity (`TableProvider::identity()`, used as the resident-
//!   cache key by `GpuAggPlan::pid()`) — today that means parquet-backed
//!   tables (the trait's default derives identity from `parquet_files()`);
//!   any future provider with its own stable identity (e.g. a native
//!   table's manifest version) is eligible the same way, without this
//!   module needing to special-case provider types by name.
//! - Aggregates: COUNT(*) / COUNT(col) / SUM / MIN / MAX / AVG over Float64
//!   columns, plus the two TPC-H fused forms `a*(1-b)` and `a*(1-b)*(1+c)`.
//! - Filters: conjunctions of single-column numeric comparisons (the same
//!   family compiled_expr fuses on the CPU).
//! - GROUP BY: none, or string/dictionary keys with <= 48 distinct
//!   combinations (Q1's 6). Key codes are computed once on the CPU and
//!   cached on the device alongside the columns.
//! - Numeric columns are cached as f64 (Date32/Int32 convert losslessly;
//!   Int64 columns are refused as aggregate inputs — f64 sums over big
//!   integers would not be exact).
//!
//! All CUDA state lives on ONE worker thread (context, module, buffers);
//! the engine talks to it over channels. Kernels are CUDA C compiled at
//! runtime by NVRTC (no toolkit needed; libnvrtc ships in the repo .venv).
//! `QE_GPU=0` disables routing even in a gpu build.
//!
//! Float sums reduce in a different order than the CPU — differences live in
//! the last bits, the same 1e-6 tolerance class as the distributed two-phase
//! path, and the validation compares with that tolerance.
//!
//! ## VRAM budget + LRU eviction (native-tables-tiering task 001)
//!
//! Every resident entry (a column's `CudaSlice<f64>`, a group-codes
//! `CudaSlice<u8>`) is byte-accounted and tagged with a monotonic
//! last-used tick (`GpuCache`, owned solely by the worker thread — no new
//! concurrency surface). Before an upload is committed, `GpuCache::reserve`
//! evicts the globally least-recently-used resident entries (columns and
//! codes compete in the same LRU order) until the upload fits inside
//! `QE_GPU_CACHE_MB` (default 24576 MiB — see `cache_budget_bytes`), or
//! until nothing is left to evict (a single entry larger than the whole
//! budget is still allowed to land — a soft target, never a hard refusal).
//!
//! This same mechanism is also the fix for a real, empirically-confirmed
//! leak: a native table's `identity()` is `table_id ++ version`
//! (`native_table.rs`), so every INSERT/DELETE/UPDATE changes the cache key
//! (`GpuAggPlan::pid()`) and the OLD version's columns become permanently
//! unreachable — nothing ever touches them again, so under pure LRU they
//! are always the coldest entries and are evicted first once budget
//! pressure appears. No native-table-specific code is needed: eviction is
//! a single, generic, provider-agnostic policy that happens to also solve
//! the mutation-leak as a direct consequence of "least recently used."
//! Confirmed by measurement (`examples/gpu_cache_tiering_check.rs`): before
//! this mechanism, 15 mutation cycles against a GPU-queried native table
//! grew resident VRAM by +224 MiB (1864 -> 2088 MiB) while the table's own
//! row count oscillated within 0.05% of constant; after, VRAM stays
//! bounded near the configured budget indefinitely.
//!
//! Evicting a column/codes entry clears the corresponding `resident`/
//! `codes` bookkeeping in `GpuEngine` AND the `queued` dedup key for that
//! entry (previously left stuck forever on every upload, evicted or not —
//! a latent bug this task also fixes: without clearing it, a column that
//! becomes not-resident again could never be re-queued for upload,
//! silently pinning it in permanent CPU-fallback). A query that finds its
//! column evicted takes the exact same "not yet resident" path a
//! never-uploaded column already took before this task — no new
//! re-upload logic, just returning a column to a state the engine already
//! handled correctly.
//!
//! ## Failure isolation + observability (native-tables-tiering task 002)
//!
//! Before this task, ANY upload failure — for ANY column, ANY table, for
//! ANY reason — called `mark_unhealthy()`, which set a single
//! process-wide `healthy: AtomicBool` to `false`. Both `request()` and
//! `ready()` checked it and short-circuited ("do nothing" / "never
//! ready") once set, permanently, for the rest of the process's
//! lifetime, with no reset path anywhere in the file. One column on one
//! table failing to upload (a transient VRAM pressure spike, a driver
//! hiccup) silently disabled GPU offload for every OTHER column, every
//! OTHER table, every future query, forever — the opposite of graceful
//! degradation, and never exercised by any pre-existing test.
//!
//! **Fix: delete the global gate, don't replace it with a new one.**
//! Task 001 already gives every upload attempt a `(pid, column)`- or
//! `codes_key`-scoped identity in `resident`/`codes`/`queued` — a failed
//! upload for column X was ALREADY incapable of touching column Y's own
//! entries in those maps. The only thing standing in the way of correct
//! per-column isolation was the single global `healthy` flag layered on
//! top, gating BOTH `request()` (so a poisoned engine stopped even
//! TRYING new uploads) and `ready()` (so a poisoned engine stopped
//! serving ALREADY-uploaded, perfectly resident columns from the GPU,
//! too). Removing `healthy` entirely — the field, its two gate checks,
//! `mark_unhealthy()`, and its one call site — is therefore the complete
//! fix: per-column isolation was already structurally present, just
//! overridden by a coarser, unconditional gate.
//!
//! **Decision: a failed upload is RETRIED on a later query, never
//! permanently blacklisted — for that column, or the process.** Chosen
//! over a per-`(pid, column)` blacklist for three concrete reasons:
//!
//! 1. **Symmetry with existing behavior.** `load_column_f64` returning
//!    `Ok(None)` (nulls, an unsupported type, an Int64 value outside
//!    2^52) was ALREADY handled this way before this task — no
//!    blacklist; `unmark_queued` clears the dedup key unconditionally, so
//!    the exact same doomed upload is retried on the very next query that
//!    needs it, and that has never been a problem. Treating a hard
//!    upload ERROR (a CUDA/driver failure) any differently would mean
//!    maintaining two different failure-handling mechanisms for what is,
//!    from a caller's perspective, the identical outcome: "this column
//!    isn't resident yet."
//! 2. **A blacklist would fight task 001's own eviction mechanism.** The
//!    named example failure modes ("a momentary VRAM pressure spike, a
//!    driver hiccup") are transient by construction — task 001 built LRU
//!    eviction specifically so VRAM pressure resolves itself over time.
//!    A permanent per-key blacklist would keep a column stuck on the CPU
//!    path forever even after eviction frees the exact room its upload
//!    needed — actively working against the mechanism task 001 just
//!    built. Retry lets a later, successful attempt (once room exists
//!    again) simply happen, with zero new code: `unmark_queued` already
//!    clears the dedup key after every job, success or failure.
//! 3. **Minimal, surgical diff.** No new state is needed at all — the fix
//!    is SUBTRACTIVE (delete `healthy` and its call sites), not additive
//!    (a new `HashSet` of blacklisted keys, with its own insert/check/
//!    never-cleared-on-mutation lifecycle to get right). Smaller surface,
//!    less to get wrong, zero risk of regressing task 001's own
//!    eviction/budget logic (confirmed untouched by this task).
//!
//! **Accepted cost, named plainly**: a column whose upload fails for a
//! genuinely PERMANENT reason (e.g. a value outside `load_column_f64`'s
//! accepted range, which cannot change without a new table version/pid)
//! is retried on every single future query that needs it, forever costing
//! one column scan + one failed device call per query — never incorrect
//! (always falls back to a correct CPU answer), just not free. Matches
//! this program's own "always correct, even if that means not fast"
//! standing culture. A future task could add a short backoff/cooldown
//! between retry attempts for a specific key if this cost is ever
//! measured to matter in practice; not attempted here — out of scope, and
//! not asked for by this task's own acceptance criteria, which name only
//! "retry" or "permanent blacklist" as the two options.
//!
//! **Observability**: `GpuEngine` gained two new counters alongside task
//! 001's `eviction_count` — `upload_failures` (every upload/build-codes
//! attempt that did NOT result in a resident entry: a hard error, a
//! provider scan error, "not cacheable," or a row-count mismatch — see
//! each field's own doc for the exact breakdown) and `run_fallbacks` (a
//! fully-`ready()` plan whose device `run()` itself still failed, forcing
//! `GpuAggExec` to fall back to the CPU operator for that one query).
//! `GpuEngine::snapshot()` bundles every observable metric (resident
//! columns, VRAM used, budget, evictions, upload failures, run
//! fallbacks) into one `GpuCacheSnapshot`; `QE_GPU_DEBUG` (unset by
//! default, matching `QE_SPILL_DEBUG`'s established convention exactly —
//! checked fresh via `std::env::var(...).is_ok()` on every relevant call,
//! never cached in a `OnceLock`) traces every upload/build-codes/run
//! outcome plus a cache snapshot to stderr, prefixed `[gpu-trace]`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::{QueryError, Result};
use crate::physical::operators::TableProvider;
use crate::physical::plan::{PhysicalOperator, RecordBatchStream};
use crate::planner::{AggregateFunction, BinaryOp, Expr, ScalarValue};

// ---------------------------------------------------------------------------
// Plan-side description (built by the physical planner, cheap, no CUDA)
// ---------------------------------------------------------------------------

/// One predicate: `col <op> lit` (BETWEEN becomes ge+le).
#[derive(Clone, Debug)]
pub struct GpuPred {
    pub col: String,
    /// 0 <, 1 <=, 2 >, 3 >=, 4 ==, 5 !=
    pub op: i32,
    pub value: f64,
}

/// The value an aggregate consumes.
#[derive(Clone, Debug, PartialEq)]
pub enum GpuInput {
    /// COUNT(*) — always 1.
    One,
    Col(String),
    /// a * b
    Mul(String, String),
    /// a * (1 - b)
    MulOneMinus(String, String),
    /// a * (1 - b) * (1 + c)
    MulOneMinusOnePlus(String, String, String),
}

/// One output aggregate. AVG is expanded to Sum+Count by the planner and
/// divided when the output batch is built.
#[derive(Clone, Debug, PartialEq)]
pub enum GpuAgg {
    Sum(GpuInput),
    Min(GpuInput),
    Max(GpuInput),
    Count(GpuInput),
    Avg(GpuInput),
}

/// Everything the wrapper needs to try the GPU, or to arrange for it to be
/// possible next time.
#[derive(Clone)]
pub struct GpuAggPlan {
    pub table: String,
    pub provider: Arc<dyn TableProvider>,
    pub preds: Vec<GpuPred>,
    pub aggs: Vec<GpuAgg>,
    /// Group-key columns (strings); empty = flat.
    pub group_cols: Vec<String>,
    /// Output schema of the aggregate node (group cols then aggregates).
    pub schema: SchemaRef,
}

/// Every numeric column the kernel reads.
impl GpuAggPlan {
    fn needed_columns(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut push = |c: &str| {
            if !out.iter().any(|x| x == c) {
                out.push(c.to_string());
            }
        };
        for p in &self.preds {
            push(&p.col);
        }
        for a in &self.aggs {
            let input = match a {
                GpuAgg::Sum(i)
                | GpuAgg::Min(i)
                | GpuAgg::Max(i)
                | GpuAgg::Count(i)
                | GpuAgg::Avg(i) => i,
            };
            match input {
                GpuInput::One => {}
                GpuInput::Col(c) => push(c),
                GpuInput::Mul(a, b) => {
                    push(a);
                    push(b);
                }
                GpuInput::MulOneMinus(a, b) => {
                    push(a);
                    push(b);
                }
                GpuInput::MulOneMinusOnePlus(a, b, c) => {
                    push(a);
                    push(b);
                    push(c);
                }
            }
        }
        out
    }

    /// Exact cache identity: immutable memory providers retain a strong Arc
    /// and compare allocation identity; versioned providers compare complete
    /// identity bytes and file vectors. Hashes are diagnostics only. Mixed
    /// routing still declines MemoryTable; explicit resident planning admits it.
    fn pid(&self) -> ProviderKey {
        ProviderKey::new(&self.provider)
    }
    fn codes_key(&self) -> Option<CodeKey> {
        (!self.group_cols.is_empty()).then(|| CodeKey {
            provider: self.pid(),
            columns: self.group_cols.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Planner-side recognition
// ---------------------------------------------------------------------------

/// Try to describe `Aggregate(node)` as a GPU plan. `None` = not routable
/// (the normal operator runs alone).
pub fn plan_gpu_agg(
    node: &crate::planner::AggregateNode,
    tables: &HashMap<String, Arc<dyn TableProvider>>,
) -> Option<GpuAggPlan> {
    if !gpu_enabled() {
        return None;
    }
    plan_gpu_agg_impl(node, tables, false)
}
pub(crate) fn plan_gpu_agg_resident(
    node: &crate::planner::AggregateNode,
    tables: &HashMap<String, Arc<dyn TableProvider>>,
) -> Option<GpuAggPlan> {
    if !gpu_enabled() {
        return None;
    }
    plan_gpu_agg_impl(node, tables, true)
}
fn plan_gpu_agg_impl(
    node: &crate::planner::AggregateNode,
    tables: &HashMap<String, Arc<dyn TableProvider>>,
    resident: bool,
) -> Option<GpuAggPlan> {
    // Input shape: a Scan under any chain of Filters and pure-column
    // Projects (projection pushdown inserts those). Filters contribute
    // predicates; Projects must be plain column selections.
    let mut preds: Vec<GpuPred> = Vec::new();
    let mut cur = node.input.as_ref();
    let scan = loop {
        match cur {
            crate::planner::LogicalPlan::Scan(s) => break s,
            crate::planner::LogicalPlan::Filter(f) => {
                collect_preds(&f.predicate, &mut preds)?;
                cur = f.input.as_ref();
            }
            crate::planner::LogicalPlan::Project(p) => {
                if !p.exprs.iter().all(|e| matches!(e, Expr::Column(_))) {
                    return None;
                }
                cur = p.input.as_ref();
            }
            _ => return None,
        }
    };
    if let Some(f) = &scan.filter {
        collect_preds(f, &mut preds)?;
    }
    let provider = tables.get(&scan.table_name)?.clone();
    // Cache keys retain exact provider equality. Immutable MemoryTable is
    // positively admitted only by explicit resident planning; mixed behavior
    // and unsupported-provider declines stay unchanged.
    if !ProviderKey::new(&provider).supported()
        || (!resident
            && provider
                .as_any()
                .is::<crate::physical::operators::MemoryTable>())
    {
        return None;
    }

    // Group keys: none, or plain string columns.
    let mut group_cols = Vec::new();
    for g in &node.group_by {
        match g {
            Expr::Column(c) => group_cols.push(c.name.clone()),
            _ => return None,
        }
    }

    // Aggregates.
    let mut aggs = Vec::new();
    for a in &node.aggregates {
        let Expr::Aggregate {
            func,
            args,
            distinct,
        } = a
        else {
            return None;
        };
        if *distinct {
            return None;
        }
        let input = match args.first() {
            None | Some(Expr::Wildcard) => GpuInput::One,
            Some(e) => gpu_input(e)?,
        };
        aggs.push(match func {
            AggregateFunction::Count => GpuAgg::Count(input),
            AggregateFunction::Sum => GpuAgg::Sum(input),
            AggregateFunction::Min => GpuAgg::Min(input),
            AggregateFunction::Max => GpuAgg::Max(input),
            AggregateFunction::Avg => GpuAgg::Avg(input),
            _ => return None,
        });
    }
    if aggs.is_empty() {
        return None;
    }

    let plan = GpuAggPlan {
        table: scan.table_name.clone(),
        provider,
        preds,
        aggs,
        group_cols,
        schema: crate::physical::planner::plan_schema_to_arrow(&node.schema),
    };
    if !supported_aggregate_domain(&plan.aggs, &plan.schema) {
        return None;
    }
    // A plan that touches no numeric column (bare COUNT(*)) has no length
    // source on the device — and the CPU answers it from metadata anyway.
    if plan.needed_columns().is_empty() {
        return None;
    }
    Some(plan)
}

fn gpu_input(e: &Expr) -> Option<GpuInput> {
    // col
    if let Expr::Column(c) = e {
        return Some(GpuInput::Col(c.name.clone()));
    }
    // a * (1 - b)   |   a * (1 - b) * (1 + c)
    if let Expr::BinaryExpr { left, op, right } = e {
        if *op == BinaryOp::Multiply {
            // (a * (1-b)) * (1+c)
            if let (Some(GpuInput::MulOneMinus(a, b)), Some(c)) =
                (gpu_input_mul_one_minus(left), one_plus_col(right))
            {
                return Some(GpuInput::MulOneMinusOnePlus(a, b, c));
            }
            if let (Some(a), Some(b)) = (col_name(left), one_minus_col(right)) {
                return Some(GpuInput::MulOneMinus(a, b));
            }
            if let (Some(a), Some(b)) = (col_name(left), col_name(right)) {
                return Some(GpuInput::Mul(a, b));
            }
        }
    }
    None
}

fn gpu_input_mul_one_minus(e: &Expr) -> Option<GpuInput> {
    if let Expr::BinaryExpr { left, op, right } = e {
        if *op == BinaryOp::Multiply {
            if let (Some(a), Some(b)) = (col_name(left), one_minus_col(right)) {
                return Some(GpuInput::MulOneMinus(a, b));
            }
        }
    }
    None
}

fn col_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Column(c) => Some(c.name.clone()),
        _ => None,
    }
}

fn lit_num(e: &Expr) -> Option<f64> {
    match e {
        Expr::Literal(ScalarValue::Float64(v)) => Some((*v).into()),
        Expr::Literal(ScalarValue::Int64(v)) => Some(*v as f64),
        Expr::Literal(ScalarValue::Int32(v)) => Some(*v as f64),
        Expr::Literal(ScalarValue::Date32(v)) => Some(*v as f64),
        _ => None,
    }
}

fn one_minus_col(e: &Expr) -> Option<String> {
    if let Expr::BinaryExpr { left, op, right } = e {
        if *op == BinaryOp::Subtract && lit_num(left) == Some(1.0) {
            return col_name(right);
        }
    }
    None
}

fn one_plus_col(e: &Expr) -> Option<String> {
    if let Expr::BinaryExpr { left, op, right } = e {
        if *op == BinaryOp::Add {
            if lit_num(left) == Some(1.0) {
                return col_name(right);
            }
            if let Some(c) = col_name(left) {
                if lit_num(right) == Some(1.0) {
                    return Some(c);
                }
            }
        }
    }
    None
}

/// Conjunction of single-column comparisons; anything else refuses.
fn collect_preds(e: &Expr, out: &mut Vec<GpuPred>) -> Option<()> {
    match e {
        Expr::BinaryExpr { left, op, right } => match op {
            BinaryOp::And => {
                collect_preds(left, out)?;
                collect_preds(right, out)
            }
            BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::Eq
            | BinaryOp::NotEq => {
                let (col, v, op_code) = match (col_name(left), lit_num(right)) {
                    (Some(c), Some(v)) => (c, v, op_to_code(*op)),
                    _ => match (lit_num(left), col_name(right)) {
                        // literal <op> col — mirror the operator.
                        (Some(v), Some(c)) => (c, v, mirror_op(op_to_code(*op))),
                        _ => return None,
                    },
                };
                out.push(GpuPred {
                    col,
                    op: op_code,
                    value: v,
                });
                Some(())
            }
            _ => None,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            let c = col_name(expr)?;
            out.push(GpuPred {
                col: c.clone(),
                op: 3, // >=
                value: lit_num(low)?,
            });
            out.push(GpuPred {
                col: c,
                op: 1, // <=
                value: lit_num(high)?,
            });
            Some(())
        }
        _ => None,
    }
}

fn op_to_code(op: BinaryOp) -> i32 {
    match op {
        BinaryOp::Lt => 0,
        BinaryOp::LtEq => 1,
        BinaryOp::Gt => 2,
        BinaryOp::GtEq => 3,
        BinaryOp::Eq => 4,
        BinaryOp::NotEq => 5,
        _ => unreachable!(),
    }
}

fn mirror_op(code: i32) -> i32 {
    match code {
        0 => 2,
        1 => 3,
        2 => 0,
        3 => 1,
        x => x,
    }
}

fn gpu_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("QE_GPU").map(|v| v != "0").unwrap_or(true))
}

/// Default `QE_GPU_CACHE_MB`: 24576 MiB (24 GiB). Matches the number the
/// `gpu-acceleration` design doc/PRD/epic already proposed in prose (never
/// implemented until this task) — sized to leave several GB of this box's
/// 32GB RTX 5090 headroom for the CUDA context, kernel launch workspace
/// and driver overhead, while comfortably holding every column a
/// realistic single TPC-H-scale table needs resident at once.
const DEFAULT_GPU_CACHE_MB: usize = 24576;

/// Pure parsing logic for `QE_GPU_CACHE_MB`, factored out for the same
/// reason `execution::context::parse_merge_concurrency` is: unit-testable
/// without mutating the real process environment (`cargo test` runs many
/// tests from one binary concurrently; a test that called
/// `std::env::set_var` here would race every other test reading the same
/// key). Absent, unparseable, or zero falls back to the default — never a
/// panic, never a zero-byte budget that would evict everything forever.
fn parse_cache_budget_mb(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&mb| mb > 0)
        .unwrap_or(DEFAULT_GPU_CACHE_MB)
}

/// The resident-cache VRAM budget, in bytes. Deliberately re-read from the
/// environment on every call rather than cached in a `OnceLock` (contrast
/// `gpu_enabled()` just above): the only caller is `GpuCache::reserve`,
/// invoked at most once per column/codes upload — never per-row, never for
/// an already-resident hit — so the cost of an env var read is negligible,
/// and staying reconfigurable without a process restart is real value
/// (this module's own test suite depends on it to exercise eviction
/// deterministically with a tiny budget, and it lets an operator retune
/// the cap without restarting a long-lived single-process session (`repl`,
/// `benchmark-parquet`, ...) — `serve`/distributed contexts never reach
/// this code at all (`gpu_offload` stays `false` there; see the module doc
/// above and `physical::planner`'s `LogicalPlan::Aggregate` arm).
fn cache_budget_bytes() -> usize {
    parse_cache_budget_mb(std::env::var("QE_GPU_CACHE_MB").ok().as_deref())
        .saturating_mul(1024 * 1024)
}

// ---------------------------------------------------------------------------
// The engine: one worker thread owns every CUDA object
// ---------------------------------------------------------------------------

const MAX_BINS: usize = 96;
const BLOCKS: u32 = 512;
const THREADS: u32 = 256;

enum Job {
    PrepareResident {
        id: u64,
        plan: Arc<GpuAggPlan>,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
        reply: tokio::sync::oneshot::Sender<ResidentResult<ResidentAck>>,
    },
    RunResident {
        id: u64,
        reply: tokio::sync::oneshot::Sender<ResidentResult<ResidentRun>>,
    },
    ReleaseResident {
        id: u64,
    },
    Upload {
        pid: ProviderKey,
        col: String,
        provider: Arc<dyn TableProvider>,
    },
    BuildCodes {
        key: CodeKey,
        pid: ProviderKey,
        cols: Vec<String>,
        provider: Arc<dyn TableProvider>,
    },
    Run {
        spec: RunSpec,
        reply: tokio::sync::oneshot::Sender<Result<RecordBatch>>,
    },
}

struct RunSpec {
    columns: Vec<String>,
    pid: ProviderKey,
    preds: Vec<GpuPred>,
    aggs: Vec<GpuAgg>,
    codes_key: Option<CodeKey>,
    schema: SchemaRef,
}

pub struct GpuEngine {
    sender: std::sync::mpsc::Sender<Job>,
    /// (provider identity, col) resident in VRAM.
    resident: Mutex<HashSet<(ProviderKey, String)>>,
    /// codes_key -> number of groups (resident code buffers).
    codes: Mutex<HashMap<CodeKey, usize>>,
    /// Upload requests already queued (dedup).
    queued: Mutex<HashSet<QueuedKey>>,
    /// Mirrors the worker thread's `GpuCache::total_bytes` (task 001's real
    /// byte accounting) for cheap, lock-free external reads — tests,
    /// diagnostics, and task 002's observability work. The worker thread's
    /// own accounting is the source of truth used for actual eviction
    /// decisions; this is a same-step mirror, not a second independent
    /// count.
    resident_bytes: AtomicUsize,
    /// Total evictions performed since process start (task 001's LRU
    /// policy). Monotonically increasing; never reset.
    eviction_count: AtomicU64,
    /// Total upload/build-codes attempts that did NOT result in a resident
    /// entry, since process start (task 002; never reset). Counts every
    /// non-success outcome uniformly: a hard CUDA/driver error (e.g. VRAM
    /// exhaustion), a provider scan error, "not cacheable" (nulls, an
    /// unsupported type, an Int64 value outside 2^52), or a row-count
    /// mismatch against a pid's other resident columns — see the module
    /// doc's "Failure isolation" section for why these are NOT
    /// distinguished by a separate blacklist. Deliberately does NOT count
    /// the ordinary "not yet uploaded" cold-start path (`GpuAggExec::
    /// execute` finding `ready() == false` simply because a column hasn't
    /// been requested yet) — that is expected warm-up, not a failure, and
    /// counting it would drown the signal this metric exists to surface.
    /// Incrementing this NEVER prevents any other column's upload from
    /// being attempted or succeeding — isolated per-column by construction
    /// (see `resident`/`codes`/`queued`, all keyed per column/codes-key).
    upload_failures: AtomicU64,
    /// Total times `GpuAggExec::execute` found its plan fully `ready()`
    /// (every needed column/codes buffer already resident) but the actual
    /// device `run()` call itself returned an error, forcing a fallback to
    /// the CPU operator for that one query (task 002; never reset).
    /// Distinct from `upload_failures`: the data WAS successfully
    /// resident: only the kernel launch/execution itself failed.
    run_fallbacks: AtomicU64,
}

impl GpuEngine {
    /// The process-wide engine, or `None` when there is no usable device.
    pub fn get() -> Option<&'static GpuEngine> {
        static ENGINE: OnceLock<Option<GpuEngine>> = OnceLock::new();
        ENGINE
            .get_or_init(|| {
                let (tx, rx) = std::sync::mpsc::channel::<Job>();
                let (ready_tx, ready_rx) = std::sync::mpsc::channel::<bool>();
                std::thread::Builder::new()
                    .name("qe-gpu".into())
                    .spawn(move || worker(rx, ready_tx))
                    .ok()?;
                match ready_rx.recv() {
                    Ok(true) => Some(GpuEngine {
                        sender: tx,
                        resident: Mutex::new(HashSet::new()),
                        codes: Mutex::new(HashMap::new()),
                        queued: Mutex::new(HashSet::new()),
                        resident_bytes: AtomicUsize::new(0),
                        eviction_count: AtomicU64::new(0),
                        upload_failures: AtomicU64::new(0),
                        run_fallbacks: AtomicU64::new(0),
                    }),
                    _ => {
                        tracing::info!("gpu: no usable CUDA device/nvrtc; offload disabled");
                        None
                    }
                }
            })
            .as_ref()
    }

    fn is_resident(&self, pid: ProviderKey, col: &str) -> bool {
        self.resident
            .lock()
            .unwrap()
            .contains(&(pid, col.to_string()))
    }

    fn codes_groups(&self, key: &CodeKey) -> Option<usize> {
        self.codes.lock().unwrap().get(key).copied()
    }

    /// Queue whatever this plan needs that is not yet resident. No
    /// process-wide health gate (task 002): a column/codes buffer that
    /// previously failed to upload is always eligible to be retried here —
    /// see the module doc's "Failure isolation" section for why retry,
    /// not a permanent blacklist, is this task's chosen design.
    pub fn request(&self, plan: &GpuAggPlan) {
        if !plan.pid().supported() {
            return;
        }
        let mut queued = self.queued.lock().unwrap();
        let pid = plan.pid();
        for col in plan.needed_columns() {
            let k = QueuedKey::Column(pid.clone(), col.clone());
            if !self.is_resident(pid.clone(), &col) && queued.insert(k.clone()) {
                if self
                    .sender
                    .send(Job::Upload {
                        pid: pid.clone(),
                        col,
                        provider: plan.provider.clone(),
                    })
                    .is_err()
                {
                    queued.remove(&k);
                }
            }
        }
        if let Some(key) = plan.codes_key() {
            if self.codes_groups(&key).is_none() && queued.insert(QueuedKey::Codes(key.clone())) {
                if self
                    .sender
                    .send(Job::BuildCodes {
                        key: key.clone(),
                        pid: plan.pid(),
                        cols: plan.group_cols.clone(),
                        provider: plan.provider.clone(),
                    })
                    .is_err()
                {
                    queued.remove(&QueuedKey::Codes(key));
                }
            }
        }
    }

    /// Is everything resident so a run would succeed right now? No
    /// process-wide health gate (task 002) — see `request`'s doc and the
    /// module doc's "Failure isolation" section. A column that failed to
    /// upload simply stays (or returns to) not-resident, so this correctly
    /// reports `false` for THAT plan without affecting any other plan's
    /// columns, which live under independent `resident`/`codes` keys.
    pub fn ready(&self, plan: &GpuAggPlan) -> bool {
        if !plan.pid().supported() {
            return false;
        }
        let pid = plan.pid();
        let cols_ok = plan
            .needed_columns()
            .iter()
            .all(|c| self.is_resident(pid.clone(), c));
        let (codes_ok, ngroups) = match plan.codes_key() {
            None => (true, 1),
            Some(k) => match self.codes_groups(&k) {
                Some(g) => (true, g),
                None => (false, 0),
            },
        };
        // Hidden presence-count bin per group + one bin per agg slot.
        let slots = expanded_slots(&plan.aggs) + 1;
        cols_ok && codes_ok && ngroups * slots <= MAX_BINS
    }

    /// Run the aggregate on the device. Only call when [`Self::ready`].
    pub async fn run(&self, plan: &GpuAggPlan) -> Result<RecordBatch> {
        if !plan.pid().supported() {
            return Err(residency_error("unsupported provider cache identity"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let spec = RunSpec {
            columns: plan.needed_columns(),
            pid: plan.pid(),
            preds: plan.preds.clone(),
            aggs: plan.aggs.clone(),
            codes_key: plan.codes_key(),
            schema: plan.schema.clone(),
        };
        self.sender
            .send(Job::Run { spec, reply: tx })
            .map_err(|_| QueryError::Execution("gpu worker gone".into()))?;
        rx.await
            .map_err(|_| QueryError::Execution("gpu worker dropped the job".into()))?
    }

    fn mark_resident(pid: ProviderKey, col: &str, bytes: usize, replaced: usize) {
        if let Some(e) = GpuEngine::get() {
            e.resident.lock().unwrap().insert((pid, col.to_string()));
            e.adjust_resident_bytes(replaced, bytes);
        }
    }

    fn mark_codes(key: &CodeKey, groups: usize, bytes: usize, replaced: usize) {
        if let Some(e) = GpuEngine::get() {
            e.codes.lock().unwrap().insert(key.clone(), groups);
            e.adjust_resident_bytes(replaced, bytes);
        }
    }

    /// Undo `mark_resident`: called by `GpuCache::reserve` when LRU
    /// eviction drops a column buffer. Clears `resident` so `is_resident`/
    /// `ready` correctly report it gone, and decrements the byte mirror.
    fn mark_evicted_column(pid: ProviderKey, col: &str, bytes: usize) {
        if let Some(e) = GpuEngine::get() {
            e.resident.lock().unwrap().remove(&(pid, col.to_string()));
            e.resident_bytes.fetch_sub(bytes, Ordering::Relaxed);
            e.eviction_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Undo `mark_codes`: called by `GpuCache::reserve` when LRU eviction
    /// drops a group-codes buffer.
    fn mark_evicted_codes(key: &CodeKey, bytes: usize) {
        if let Some(e) = GpuEngine::get() {
            e.codes.lock().unwrap().remove(key);
            e.resident_bytes.fetch_sub(bytes, Ordering::Relaxed);
            e.eviction_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Clear an upload/build-codes dedup key once the worker has finished
    /// processing that job (success OR failure) — without this, `request`'s
    /// `queued.insert(k)` dedup (see below) would permanently believe an
    /// upload is still in flight for any column that is ever evicted,
    /// silently blocking every future re-upload attempt for it. Previously
    /// missing entirely (`queued` was insert-only, harmless only because
    /// nothing was ever evicted): fixed as part of this task, not a
    /// pre-existing behavior being preserved.
    fn unmark_queued(key: &QueuedKey) {
        if let Some(e) = GpuEngine::get() {
            e.queued.lock().unwrap().remove(key);
        }
    }

    /// Task 002: record that an upload/build-codes attempt did NOT result
    /// in a resident entry — see the `upload_failures` field doc for
    /// exactly what counts. Deliberately per-metric only, never per-key:
    /// this NEVER touches `resident`, `codes`, or `queued`, so it cannot
    /// affect any OTHER column's ability to be requested/uploaded/served —
    /// the failing column simply stays not-resident and is retried the
    /// next time a query needs it (see the module doc's "Failure
    /// isolation" section for why retry, not a blacklist, was chosen).
    fn mark_upload_failed() {
        if let Some(e) = GpuEngine::get() {
            e.upload_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Task 002: record a `Job::Run` whose plan was fully resident/ready
    /// but whose actual device execution failed, forcing `GpuAggExec::
    /// execute` to fall back to the CPU operator for that one query.
    fn mark_run_fallback() {
        if let Some(e) = GpuEngine::get() {
            e.run_fallbacks.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Current VRAM bytes held by the resident-column/codes cache. See the
    /// `resident_bytes` field doc for why this mirrors, rather than owns,
    /// the worker thread's own accounting.
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes.load(Ordering::Relaxed)
    }

    /// Total evictions performed since process start.
    pub fn eviction_count(&self) -> u64 {
        self.eviction_count.load(Ordering::Relaxed)
    }

    /// Number of distinct `(provider identity, column)` pairs currently
    /// resident (does not count group-codes buffers).
    pub fn resident_column_count(&self) -> usize {
        self.resident.lock().unwrap().len()
    }

    /// The configured VRAM cache budget in bytes (`QE_GPU_CACHE_MB`,
    /// default [`DEFAULT_GPU_CACHE_MB`] MiB) — re-read from the
    /// environment on every call; see `cache_budget_bytes`.
    pub fn budget_bytes() -> usize {
        cache_budget_bytes()
    }

    /// Total upload/build-codes attempts that did NOT result in a resident
    /// entry, since process start. See the `upload_failures` field doc for
    /// exactly what counts (task 002).
    pub fn upload_failures(&self) -> u64 {
        self.upload_failures.load(Ordering::Relaxed)
    }

    /// Total times a fully-`ready()` GPU plan's device `run()` itself
    /// failed, forcing a fallback to the CPU operator for that one query.
    /// See the `run_fallbacks` field doc (task 002).
    pub fn run_fallbacks(&self) -> u64 {
        self.run_fallbacks.load(Ordering::Relaxed)
    }

    /// A point-in-time snapshot of every cache-state metric this task
    /// (native-tables-tiering task 002) exposes — resident columns, VRAM
    /// used, budget, eviction count, upload failures, run fallbacks. The
    /// single source both `QE_GPU_DEBUG`'s trace lines and any external
    /// caller (tests, a future admin surface) should read, so the two can
    /// never silently disagree. Every field is a cheap, lock-free atomic
    /// read except `resident_columns` (a `Mutex<HashSet>` length, the same
    /// pre-existing cost `resident_column_count` already has).
    pub fn snapshot(&self) -> GpuCacheSnapshot {
        GpuCacheSnapshot {
            resident_columns: self.resident_column_count(),
            resident_bytes: self.resident_bytes(),
            budget_bytes: Self::budget_bytes(),
            eviction_count: self.eviction_count(),
            upload_failures: self.upload_failures(),
            run_fallbacks: self.run_fallbacks(),
        }
    }
}

/// See `GpuEngine::snapshot`. Plain data, cheap to copy, `Display`-able so
/// `QE_GPU_DEBUG` tracing and any caller share one rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GpuCacheSnapshot {
    pub resident_columns: usize,
    pub resident_bytes: usize,
    pub budget_bytes: usize,
    pub eviction_count: u64,
    pub upload_failures: u64,
    pub run_fallbacks: u64,
}

impl std::fmt::Display for GpuCacheSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "resident_columns={} resident_bytes={} budget_bytes={} eviction_count={} \
             upload_failures={} run_fallbacks={}",
            self.resident_columns,
            self.resident_bytes,
            self.budget_bytes,
            self.eviction_count,
            self.upload_failures,
            self.run_fallbacks
        )
    }
}

/// `QE_GPU_DEBUG`: this module's cache-state/failure-isolation trace,
/// matching `QE_SPILL_DEBUG`'s established convention exactly (checked
/// fresh via `std::env::var(...).is_ok()` on every call, never cached in a
/// `OnceLock` — see `cache_budget_bytes`'s own doc for why that is the
/// right call for a diagnostic switch a long-lived process or test might
/// toggle; call frequency here is at most once per upload/build-codes/run
/// job and once per query's GPU-routing decision, never per-row, so the
/// cost is negligible). Zero cost when unset beyond one env lookup.
fn gpu_debug_enabled() -> bool {
    std::env::var("QE_GPU_DEBUG").is_ok()
}

/// SUM/MIN/MAX/COUNT take one slot; AVG takes two (sum + count).
fn expanded_slots(aggs: &[GpuAgg]) -> usize {
    aggs.iter()
        .map(|a| if matches!(a, GpuAgg::Avg(_)) { 2 } else { 1 })
        .sum()
}

// ---------------------------------------------------------------------------
// Worker thread
// ---------------------------------------------------------------------------

const KERNEL_SRC: &str = r#"
extern "C" __global__ void fused_agg(
    const double* const* __restrict__ cols,
    const unsigned char* __restrict__ codes, // null => flat
    long long n,
    int npred,
    const int* __restrict__ pred_col,
    const int* __restrict__ pred_op,
    const double* __restrict__ pred_val,
    int nslot,
    const int* __restrict__ slot_kind, // 0 sum,1 min,2 max,3 count
    const int* __restrict__ slot_input, // 0 one,1 col,2 a*(1-b),3 a*(1-b)*(1+c),4 a*b
    const int* __restrict__ slot_c0,
    const int* __restrict__ slot_c1,
    const int* __restrict__ slot_c2,
    int ngroups,
    double* __restrict__ out) // [gridDim][ngroups][nslot]
{
    const int BINS = 96;
    double local[BINS];
    int total = ngroups * nslot;
    for (int k = 0; k < total; k++) {
        int kind = slot_kind[k % nslot];
        local[k] = (kind == 1) ? (1.0/0.0) : (kind == 2) ? (-1.0/0.0) : 0.0;
    }

    for (long long i = (long long)blockIdx.x * blockDim.x + threadIdx.x; i < n;
         i += (long long)gridDim.x * blockDim.x) {
        bool keep = true;
        for (int p = 0; p < npred; p++) {
            double v = cols[pred_col[p]][i];
            double t = pred_val[p];
            int op = pred_op[p];
            bool ok = op == 0 ? (v < t)
                    : op == 1 ? (v <= t)
                    : op == 2 ? (v > t)
                    : op == 3 ? (v >= t)
                    : op == 4 ? (v == t)
                    : (v != t);
            if (!ok) { keep = false; break; }
        }
        if (!keep) continue;
        int g = codes ? (int)codes[i] : 0;
        int base = g * nslot;
        for (int s = 0; s < nslot; s++) {
            int inp = slot_input[s];
            double v;
            if (inp == 0) v = 1.0;
            else if (inp == 1) v = cols[slot_c0[s]][i];
            else if (inp == 2) v = cols[slot_c0[s]][i] * (1.0 - cols[slot_c1[s]][i]);
            else if (inp == 4) v = cols[slot_c0[s]][i] * cols[slot_c1[s]][i];
            else v = cols[slot_c0[s]][i] * (1.0 - cols[slot_c1[s]][i])
                     * (1.0 + cols[slot_c2[s]][i]);
            int kind = slot_kind[s];
            int k = base + s;
            if (kind == 0 || kind == 3) local[k] += (kind == 3) ? 1.0 : v;
            else if (kind == 1) local[k] = fmin(local[k], v);
            else local[k] = fmax(local[k], v);
        }
    }

    // Block reduction through shared memory, one bin at a time.
    __shared__ double sh[256];
    for (int k = 0; k < total; k++) {
        sh[threadIdx.x] = local[k];
        __syncthreads();
        int kind = slot_kind[k % nslot];
        for (int s = blockDim.x / 2; s > 0; s >>= 1) {
            if (threadIdx.x < s) {
                double a = sh[threadIdx.x], b = sh[threadIdx.x + s];
                sh[threadIdx.x] = kind == 1 ? fmin(a, b) : kind == 2 ? fmax(a, b) : a + b;
            }
            __syncthreads();
        }
        if (threadIdx.x == 0)
            out[((long long)blockIdx.x * ngroups * nslot) + k] = sh[0];
        __syncthreads();
    }
}
"#;

// ---------------------------------------------------------------------------
// Worker-owned VRAM cache: real byte accounting + LRU eviction (task 001)
// ---------------------------------------------------------------------------

/// One VRAM-resident column buffer: the device allocation, its byte size
/// (real accounting, not a row/group count), and a monotonic last-used
/// tick for LRU ordering.
struct ColumnEntry {
    buf: cudarc::driver::CudaSlice<f64>,
    bytes: usize,
    last_used: u64,
}

/// One VRAM-resident group-codes buffer. `labels` (the per-code string
/// rows) live in HOST memory, not VRAM, and are deliberately NOT counted
/// in `bytes` — only the device-resident `buf` consumes budget.
struct CodesEntry {
    buf: cudarc::driver::CudaSlice<u8>,
    labels: Vec<Vec<String>>,
    ngroups: usize,
    bytes: usize,
    last_used: u64,
}

/// Everything the worker thread's single-consumer loop owns: the two
/// VRAM-resident maps (replacing the old insert-only `columns`/`code_bufs`
/// HashMaps), the row-count-mismatch guard (`rows`), and a tick clock +
/// running byte total for LRU eviction against `QE_GPU_CACHE_MB`. Lives
/// entirely on the worker thread, touched only from job-processing code —
/// no lock needed, matching the epic's own "no new concurrency surface"
/// architecture decision.
struct GpuCache {
    columns: HashMap<(ProviderKey, String), ColumnEntry>,
    code_bufs: HashMap<CodeKey, CodesEntry>,
    /// pid -> expected row count, used to detect and skip a column whose
    /// row count disagrees with a pid's other already-resident columns.
    rows: HashMap<ProviderKey, usize>,
    total_bytes: usize,
    clock: u64,
}

impl GpuCache {
    fn new() -> Self {
        GpuCache {
            columns: HashMap::new(),
            code_bufs: HashMap::new(),
            rows: HashMap::new(),
            total_bytes: 0,
            clock: 0,
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Evict globally least-recently-used entries — columns and codes
    /// compete in the same LRU order — until there is room for `need` more
    /// bytes under `QE_GPU_CACHE_MB`, or nothing is left to evict. Never
    /// refuses the caller's own upload outright: a single buffer larger
    /// than the whole budget is still allowed to land once everything else
    /// has been evicted (a soft target, not a hard cap — see
    /// `cache_budget_bytes`'s doc for why).
    fn reserve(&mut self, need: usize) {
        let budget = cache_budget_bytes();
        while self.total_bytes + need > budget {
            let oldest_col = self
                .columns
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, e)| (k.clone(), e.last_used, e.bytes));
            let oldest_codes = self
                .code_bufs
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, e)| (k.clone(), e.last_used, e.bytes));
            let evict_column = match (&oldest_col, &oldest_codes) {
                (Some(c), Some(g)) => c.1 <= g.1,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break, // nothing resident left to evict
            };
            if evict_column {
                let (key, _, bytes) = oldest_col.expect("checked Some above");
                self.columns.remove(&key);
                self.total_bytes = self.total_bytes.saturating_sub(bytes);
                let (pid, col) = key;
                GpuEngine::mark_evicted_column(pid.clone(), &col, bytes);
                self.forget_rows_if_unused(&pid);
                tracing::info!(
                    "gpu: evicted column {col} (pid={pid:x}, {} MB) — over QE_GPU_CACHE_MB budget",
                    bytes / 1_000_000
                );
            } else {
                let (key, _, bytes) = oldest_codes.expect("checked Some above");
                self.code_bufs.remove(&key);
                self.total_bytes = self.total_bytes.saturating_sub(bytes);
                GpuEngine::mark_evicted_codes(&key, bytes);
                self.forget_rows_if_unused(&key.provider);
                tracing::info!(
                    "gpu: evicted group codes {key} ({} MB) — over QE_GPU_CACHE_MB budget",
                    bytes / 1_000_000
                );
            }
        }
    }

    fn insert_column(
        &mut self,
        pid: ProviderKey,
        col: String,
        buf: cudarc::driver::CudaSlice<f64>,
        bytes: usize,
    ) -> Result<usize> {
        let key = (pid, col);
        let replaced = self.columns.get(&key).map_or(0, |entry| entry.bytes);
        let total = replacement_total(self.total_bytes, replaced, bytes)?;
        let last_used = self.next_tick();
        self.columns.insert(
            key,
            ColumnEntry {
                buf,
                bytes,
                last_used,
            },
        );
        self.total_bytes = total;
        Ok(replaced)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_codes(
        &mut self,
        key: CodeKey,
        buf: cudarc::driver::CudaSlice<u8>,
        labels: Vec<Vec<String>>,
        ngroups: usize,
        bytes: usize,
    ) -> Result<usize> {
        let replaced = self.code_bufs.get(&key).map_or(0, |entry| entry.bytes);
        let total = replacement_total(self.total_bytes, replaced, bytes)?;
        let last_used = self.next_tick();
        self.code_bufs.insert(
            key,
            CodesEntry {
                buf,
                labels,
                ngroups,
                bytes,
                last_used,
            },
        );
        self.total_bytes = total;
        Ok(replaced)
    }

    /// Bump a resident column's LRU tick on use (a cache hit inside
    /// `run_on_device`). A no-op if the column is not resident (should not
    /// happen — `GpuEngine::ready` already checked — but never panics).
    fn touch_column(&mut self, pid: ProviderKey, col: &str) {
        let tick = self.next_tick();
        if let Some(e) = self.columns.get_mut(&(pid, col.to_string())) {
            e.last_used = tick;
        }
    }

    /// Bump a resident codes buffer's LRU tick on use.
    fn touch_codes(&mut self, key: &CodeKey) {
        let tick = self.next_tick();
        if let Some(e) = self.code_bufs.get_mut(key) {
            e.last_used = tick;
        }
    }

    /// Exact provider association, with no diagnostic-string parsing.
    /// Release row metadata after its final column/code allocation is removed.
    fn forget_rows_if_unused(&mut self, pid: &ProviderKey) {
        if !self.pid_in_use(pid.clone()) {
            self.rows.remove(pid);
        }
    }
    fn pid_in_use(&self, pid: ProviderKey) -> bool {
        self.columns.keys().any(|(p, _)| *p == pid)
            || self.code_bufs.keys().any(|k| k.provider == pid)
    }
}

#[cfg(feature = "gpu")]
fn worker(rx: std::sync::mpsc::Receiver<Job>, ready: std::sync::mpsc::Sender<bool>) {
    use cudarc::driver::{CudaContext, LaunchConfig, PushKernelArg};

    let init = (|| -> std::result::Result<_, Box<dyn std::error::Error>> {
        let ctx = CudaContext::new(0)?;
        let stream = ctx.default_stream();
        let ptx = cudarc::nvrtc::compile_ptx(KERNEL_SRC)?;
        let module = ctx.load_module(ptx)?;
        let func = module.load_function("fused_agg")?;
        Ok((ctx, stream, module, func))
    })();
    let (_ctx, stream, _module, func) = match init {
        Ok(x) => {
            let _ = ready.send(true);
            x
        }
        Err(e) => {
            tracing::info!("gpu: init failed ({e}); offload disabled");
            let _ = ready.send(false);
            return;
        }
    };

    // Device-side state, owned here: real byte accounting + LRU eviction
    // (task 001) replaces the old insert-only HashMaps.
    let _mirror_cleanup = WorkerMirrorCleanup;
    let mut cache = GpuCache::new();

    let mut resident: Option<ResidentSession> = None;
    while let Ok(job) = rx.recv() {
        if resident.as_ref().is_some_and(|s| !s.active()) {
            resident = None;
        }
        match job {
            Job::PrepareResident {
                id,
                plan,
                cancelled,
                reply,
            } => {
                if resident.as_ref().is_some_and(|s| s.active()) {
                    let _ = reply.send(Err(resident_failure(
                        ResidentFailureKind::Conflict,
                        "prepare",
                        "resident session active",
                    )));
                    continue;
                }
                let outcome = (|| -> ResidentResult<ResidentAck> {
                    let check = || {
                        if cancelled.load(Ordering::Acquire) || reply.is_closed() {
                            Err(resident_failure(
                                ResidentFailureKind::Cancelled,
                                "prepare",
                                "caller dropped",
                            ))
                        } else {
                            Ok(())
                        }
                    };
                    check()?;
                    if !plan.pid().supported() {
                        return Err(resident_failure(
                            ResidentFailureKind::Unsupported,
                            "provider",
                            "missing exact cache identity",
                        ));
                    }
                    if !supported_aggregate_domain(&plan.aggs, &plan.schema) {
                        return Err(resident_failure(
                            ResidentFailureKind::Unsupported,
                            "aggregate domain",
                            "SUM requires Float64 output; MIN/MAX requires direct column input; output schema must contain every aggregate",
                        ));
                    }
                    for col in plan.needed_columns() {
                        check()?;
                        if !cache.columns.contains_key(&(plan.pid(), col.clone())) {
                            if let Err(e) =
                                upload_typed(&stream, &mut cache, plan.pid(), &col, &plan.provider)
                            {
                                GpuEngine::mark_upload_failed();
                                return Err(e);
                            }
                        }
                    }
                    if let Some(key) = plan.codes_key() {
                        check()?;
                        if !cache.code_bufs.contains_key(&key) {
                            if let Err(e) = codes_typed(
                                &stream,
                                &mut cache,
                                plan.pid(),
                                &key,
                                &plan.group_cols,
                                &plan.provider,
                            ) {
                                GpuEngine::mark_upload_failed();
                                return Err(e);
                            }
                        }
                    }
                    check()?;
                    stream.synchronize().map_err(|e| {
                        resident_failure(ResidentFailureKind::Upload, "synchronize", e)
                    })?;
                    check()?;
                    let ack = verify_resident(&cache, &plan, id)?;
                    if ack
                        .column_bytes
                        .checked_add(ack.codes_bytes)
                        .is_none_or(|n| n > cache_budget_bytes())
                    {
                        return Err(resident_failure(
                            ResidentFailureKind::Capacity,
                            "prepare",
                            "dependencies exceed cache budget",
                        ));
                    }
                    Ok(ack)
                })();
                match outcome {
                    Ok(ack) => {
                        resident = Some(ResidentSession {
                            id,
                            cancelled,
                            plan,
                            ack: ack.clone(),
                            attempted: 0,
                            completed: 0,
                        });
                        if reply.send(Ok(ack)).is_err() {
                            resident = None;
                        }
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            }
            Job::ReleaseResident { id } => {
                retire_resident(&mut resident, id);
            }
            Job::RunResident { id, reply } => {
                let outcome = (|| -> ResidentResult<ResidentRun> {
                    let s = resident_for_run(&mut resident, id)?;
                    if reply.is_closed() {
                        return Err(resident_failure(
                            ResidentFailureKind::Cancelled,
                            "run",
                            "caller dropped",
                        ));
                    }
                    verify_resident(&cache, &s.plan, id)?;
                    s.attempted = s.attempted.checked_add(1).ok_or_else(|| {
                        resident_failure(ResidentFailureKind::Overflow, "run", "counter")
                    })?;
                    let batch = run_on_device(&stream, &func, &mut cache, &resident_spec(&s.plan))
                        .map_err(|e| resident_failure(ResidentFailureKind::Upload, "run", e))?;
                    stream.synchronize().map_err(|e| {
                        resident_failure(ResidentFailureKind::Upload, "run synchronization", e)
                    })?;
                    s.completed = s.completed.checked_add(1).ok_or_else(|| {
                        resident_failure(ResidentFailureKind::Overflow, "run", "counter")
                    })?;
                    Ok(ResidentRun {
                        batch,
                        counters: ResidentRunCounters {
                            attempted: s.attempted,
                            completed: s.completed,
                        },
                    })
                })();
                let failed = outcome.is_err();
                let _ = reply.send(outcome);
                if failed {
                    retire_resident(&mut resident, id);
                }
            }
            Job::Upload { pid, col, provider } => {
                if resident.as_ref().is_some_and(|s| s.active()) {
                    GpuEngine::mark_upload_failed();
                    tracing::warn!("gpu: upload rejected while resident session active");
                } else if let Err(e) =
                    upload_typed(&stream, &mut cache, pid.clone(), &col, &provider)
                {
                    GpuEngine::mark_upload_failed();
                    tracing::warn!("gpu: upload failed: {:?}", e);
                }
                GpuEngine::unmark_queued(&QueuedKey::Column(pid.clone(), col.clone()));
            }
            Job::BuildCodes {
                key,
                pid,
                cols,
                provider,
            } => {
                if resident.as_ref().is_some_and(|s| s.active()) {
                    GpuEngine::mark_upload_failed();
                    tracing::warn!("gpu: codes rejected while resident session active");
                } else if let Err(e) =
                    codes_typed(&stream, &mut cache, pid.clone(), &key, &cols, &provider)
                {
                    GpuEngine::mark_upload_failed();
                    tracing::warn!("gpu: codes failed: {:?}", e);
                }
                GpuEngine::unmark_queued(&QueuedKey::Codes(key.clone()));
            }
            Job::Run { spec, reply } => {
                if resident.as_ref().is_some_and(|s| s.active()) {
                    let _ = reply.send(Err(QueryError::Execution(
                        "gpu: mixed run rejected during resident session".into(),
                    )));
                    continue;
                }
                let result = run_on_device(&stream, &func, &mut cache, &spec);
                let _ = reply.send(result);
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn run_on_device(
        stream: &Arc<cudarc::driver::CudaStream>,
        func: &cudarc::driver::CudaFunction,
        cache: &mut GpuCache,
        spec: &RunSpec,
    ) -> Result<RecordBatch> {
        if !supported_aggregate_domain(&spec.aggs, &spec.schema) {
            return Err(residency_error("unsupported aggregate domain: SUM requires Float64 output; MIN/MAX requires direct column input; output schema must contain every aggregate"));
        }
        use cudarc::driver::{DevicePtr, LaunchConfig, PushKernelArg};
        let gpu_err = |e: cudarc::driver::DriverError| {
            QueryError::Execution(format!("gpu launch failed: {e}"))
        };

        // Touch phase first (task 001's LRU): every column/codes buffer this
        // run actually uses counts as "just used," in a separate pass so the
        // borrow below can hand out plain immutable refs without conflicting
        // with these `&mut self` calls.
        for c in &spec.columns {
            cache.touch_column(spec.pid.clone(), c);
        }
        if let Some(k) = &spec.codes_key {
            cache.touch_codes(k);
        }

        // Column pointer table, in spec.columns order.
        let mut ptrs: Vec<u64> = Vec::with_capacity(spec.columns.len());
        let mut n = usize::MAX;
        for c in &spec.columns {
            let buf = &cache
                .columns
                .get(&(spec.pid.clone(), c.clone()))
                .ok_or_else(|| QueryError::Execution(format!("gpu: {c} not resident")))?
                .buf;
            n = n.min(buf.len());
            let (p, _record) = buf.device_ptr(stream);
            ptrs.push(p as u64);
        }
        let col_index =
            |name: &str| -> i32 { spec.columns.iter().position(|c| c == name).unwrap() as i32 };

        let (codes_arg, labels, ngroups): (Option<&cudarc::driver::CudaSlice<u8>>, _, usize) =
            match &spec.codes_key {
                None => (None, None, 1),
                Some(k) => {
                    let entry = cache
                        .code_bufs
                        .get(k)
                        .ok_or_else(|| QueryError::Execution("gpu: codes not resident".into()))?;
                    (Some(&entry.buf), Some(&entry.labels), entry.ngroups)
                }
            };

        // Expand aggregates to slots (+ hidden per-group presence count).
        let mut slot_kind: Vec<i32> = Vec::new();
        let mut slot_input: Vec<i32> = Vec::new();
        let mut slot_c: Vec<[i32; 3]> = Vec::new();
        fn push_slot(
            kind: i32,
            input: &GpuInput,
            col_index: &dyn Fn(&str) -> i32,
            slot_kind: &mut Vec<i32>,
            slot_input: &mut Vec<i32>,
            slot_c: &mut Vec<[i32; 3]>,
        ) {
            let (icode, c) = match input {
                GpuInput::One => (0, [0, 0, 0]),
                GpuInput::Col(a) => (1, [col_index(a), 0, 0]),
                GpuInput::MulOneMinus(a, b) => (2, [col_index(a), col_index(b), 0]),
                GpuInput::MulOneMinusOnePlus(a, b, cc) => {
                    (3, [col_index(a), col_index(b), col_index(cc)])
                }
                GpuInput::Mul(a, b) => (4, [col_index(a), col_index(b), 0]),
            };
            slot_kind.push(kind);
            slot_input.push(icode);
            slot_c.push(c);
        }
        // Per output aggregate: its slot(s).
        let mut out_slots: Vec<(usize, Option<usize>)> = Vec::new(); // (main, count for avg)
        for a in &spec.aggs {
            match a {
                GpuAgg::Sum(i) => {
                    push_slot(
                        0,
                        i,
                        &col_index,
                        &mut slot_kind,
                        &mut slot_input,
                        &mut slot_c,
                    );
                    out_slots.push((slot_kind.len() - 1, None));
                }
                GpuAgg::Min(i) => {
                    push_slot(
                        1,
                        i,
                        &col_index,
                        &mut slot_kind,
                        &mut slot_input,
                        &mut slot_c,
                    );
                    out_slots.push((slot_kind.len() - 1, None));
                }
                GpuAgg::Max(i) => {
                    push_slot(
                        2,
                        i,
                        &col_index,
                        &mut slot_kind,
                        &mut slot_input,
                        &mut slot_c,
                    );
                    out_slots.push((slot_kind.len() - 1, None));
                }
                GpuAgg::Count(i) => {
                    push_slot(
                        3,
                        i,
                        &col_index,
                        &mut slot_kind,
                        &mut slot_input,
                        &mut slot_c,
                    );
                    out_slots.push((slot_kind.len() - 1, None));
                }
                GpuAgg::Avg(i) => {
                    push_slot(
                        0,
                        i,
                        &col_index,
                        &mut slot_kind,
                        &mut slot_input,
                        &mut slot_c,
                    );
                    let s = slot_kind.len() - 1;
                    push_slot(
                        3,
                        i,
                        &col_index,
                        &mut slot_kind,
                        &mut slot_input,
                        &mut slot_c,
                    );
                    out_slots.push((s, Some(slot_kind.len() - 1)));
                }
            }
        }
        // Hidden presence counter.
        push_slot(
            3,
            &GpuInput::One,
            &col_index,
            &mut slot_kind,
            &mut slot_input,
            &mut slot_c,
        );
        let presence = slot_kind.len() - 1;
        let nslot = slot_kind.len();
        if ngroups * nslot > MAX_BINS {
            return Err(QueryError::Execution("gpu: too many bins".into()));
        }

        let d_ptrs = stream.memcpy_stod(&ptrs).map_err(gpu_err)?;
        let d_pred_col = stream
            .memcpy_stod(
                &spec
                    .preds
                    .iter()
                    .map(|p| col_index(&p.col))
                    .collect::<Vec<_>>(),
            )
            .map_err(gpu_err)?;
        let d_pred_op = stream
            .memcpy_stod(&spec.preds.iter().map(|p| p.op).collect::<Vec<_>>())
            .map_err(gpu_err)?;
        let d_pred_val = stream
            .memcpy_stod(&spec.preds.iter().map(|p| p.value).collect::<Vec<_>>())
            .map_err(gpu_err)?;
        let d_kind = stream.memcpy_stod(&slot_kind).map_err(gpu_err)?;
        let d_input = stream.memcpy_stod(&slot_input).map_err(gpu_err)?;
        let d_c0 = stream
            .memcpy_stod(&slot_c.iter().map(|c| c[0]).collect::<Vec<_>>())
            .map_err(gpu_err)?;
        let d_c1 = stream
            .memcpy_stod(&slot_c.iter().map(|c| c[1]).collect::<Vec<_>>())
            .map_err(gpu_err)?;
        let d_c2 = stream
            .memcpy_stod(&slot_c.iter().map(|c| c[2]).collect::<Vec<_>>())
            .map_err(gpu_err)?;
        let mut d_out = stream
            .alloc_zeros::<f64>(BLOCKS as usize * ngroups * nslot)
            .map_err(gpu_err)?;

        let cfg = LaunchConfig {
            grid_dim: (BLOCKS, 1, 1),
            block_dim: (THREADS, 1, 1),
            shared_mem_bytes: 0,
        };
        let nn = n as i64;
        let npred = spec.preds.len() as i32;
        let nslot_i = nslot as i32;
        let ngroups_i = ngroups as i32;
        {
            let mut b = stream.launch_builder(func);
            b.arg(&d_ptrs);
            match codes_arg {
                Some(codes) => {
                    b.arg(codes);
                }
                None => {
                    b.arg(&0u64);
                }
            }
            b.arg(&nn)
                .arg(&npred)
                .arg(&d_pred_col)
                .arg(&d_pred_op)
                .arg(&d_pred_val)
                .arg(&nslot_i)
                .arg(&d_kind)
                .arg(&d_input)
                .arg(&d_c0)
                .arg(&d_c1)
                .arg(&d_c2)
                .arg(&ngroups_i)
                .arg(&mut d_out);
            unsafe { b.launch(cfg) }.map_err(gpu_err)?;
        }
        let partials: Vec<f64> = stream.memcpy_dtov(&d_out).map_err(gpu_err)?;

        // Merge block partials on the host.
        let mut merged = vec![0f64; ngroups * nslot];
        for k in 0..(ngroups * nslot) {
            let kind = slot_kind[k % nslot];
            let mut acc = match kind {
                1 => f64::INFINITY,
                2 => f64::NEG_INFINITY,
                _ => 0.0,
            };
            for blk in 0..BLOCKS as usize {
                let v = partials[blk * ngroups * nslot + k];
                acc = match kind {
                    1 => acc.min(v),
                    2 => acc.max(v),
                    _ => acc + v,
                };
            }
            merged[k] = acc;
        }

        // A scalar aggregate always has one row; empty grouped aggregates
        // have none. Presence controls nullable scalar values below.
        let present: Vec<usize> = (0..ngroups)
            .filter(|g| spec.codes_key.is_none() || merged[g * nslot + presence] > 0.0)
            .collect();

        // Build the output batch: group columns then aggregates.
        let mut arrays: Vec<ArrayRef> = Vec::new();
        let fields = spec.schema.fields();
        let ngroup_cols = fields.len() - spec.aggs.len();
        for gc in 0..ngroup_cols {
            let vals: Vec<&str> = present
                .iter()
                .map(|g| labels.expect("grouped")[*g][gc].as_str())
                .collect();
            arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
        }
        for (ai, (main, avg_cnt)) in out_slots.iter().enumerate() {
            let field = &fields[ngroup_cols + ai];
            let vals: Vec<Option<f64>> = present
                .iter()
                .map(|g| {
                    if merged[g * nslot + presence] == 0.0 {
                        return if matches!(spec.aggs[ai], GpuAgg::Count(_)) {
                            Some(0.0)
                        } else {
                            None
                        };
                    }
                    let v = merged[g * nslot + main];
                    Some(match avg_cnt {
                        Some(cs) => {
                            let c = merged[g * nslot + cs];
                            if c > 0.0 {
                                v / c
                            } else {
                                f64::NAN
                            }
                        }
                        None => v,
                    })
                })
                .collect();
            let arr: ArrayRef = match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(
                    vals.iter().map(|v| v.map(|v| v as i64)).collect::<Vec<_>>(),
                )),
                _ => Arc::new(Float64Array::from(vals)),
            };
            // Cast to the exact declared type when needed.
            let arr = if arr.data_type() != field.data_type() {
                arrow::compute::cast(&arr, field.data_type())?
            } else {
                arr
            };
            arrays.push(arr);
        }
        RecordBatch::try_new(spec.schema.clone(), arrays).map_err(Into::into)
    }
}

/// Read one column fully, in scan order, as f64. `None` when not cacheable
/// (nulls, unsupported type, lossy i64).
fn load_column_f64(provider: &Arc<dyn TableProvider>, col: &str) -> Result<Option<Vec<f64>>> {
    let schema = provider.schema();
    let Some((idx, field)) = schema.column_with_name(col) else {
        return Ok(None);
    };
    let batches = provider.scan(Some(&[idx]))?;
    let mut out: Vec<f64> = Vec::new();
    for b in &batches {
        let a = b.column(0);
        if a.null_count() > 0 {
            return Ok(None);
        }
        match field.data_type() {
            DataType::Float64 => {
                let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
                if a.values().iter().any(|v| !v.is_finite()) {
                    return Ok(None);
                }
                out.extend_from_slice(a.values());
            }
            DataType::Int32 => {
                let a = arrow::compute::cast(a, &DataType::Float64)?;
                let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
                out.extend_from_slice(a.values());
            }
            DataType::Date32 => {
                // arrow refuses Date32->Float64; the day number IS the value.
                let a = a
                    .as_any()
                    .downcast_ref::<arrow::array::Date32Array>()
                    .unwrap();
                out.extend(a.values().iter().map(|v| *v as f64));
            }
            DataType::Int64 => {
                let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
                for v in a.values() {
                    if v.unsigned_abs() > (1u64 << 52) {
                        return Ok(None);
                    }
                    out.push(*v as f64);
                }
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(out))
}

/// Combined group codes (u8) + per-code label rows, or `None` when the keys
/// are not strings or exceed the bin budget.
#[allow(clippy::type_complexity)]
fn build_codes(
    provider: &Arc<dyn TableProvider>,
    cols: &[String],
) -> Result<Option<(Vec<u8>, Vec<Vec<String>>)>> {
    let schema = provider.schema();
    let mut idxs = Vec::new();
    for c in cols {
        let Some((i, f)) = schema.column_with_name(c) else {
            return Ok(None);
        };
        match f.data_type() {
            DataType::Utf8 | DataType::Dictionary(_, _) => idxs.push(i),
            _ => return Ok(None),
        }
    }
    let batches = provider.scan(Some(&idxs))?;
    let mut codes: Vec<u8> = Vec::new();
    let mut map: HashMap<Vec<String>, u8> = HashMap::new();
    let mut labels: Vec<Vec<String>> = Vec::new();
    for b in &batches {
        // Normalize dictionaries to plain strings.
        let cols_plain: Vec<StringArray> = (0..idxs.len())
            .map(|ci| {
                let a = b.column(ci);
                let a = if matches!(a.data_type(), DataType::Dictionary(_, _)) {
                    arrow::compute::cast(a, &DataType::Utf8).unwrap()
                } else {
                    a.clone()
                };
                a.as_any().downcast_ref::<StringArray>().unwrap().clone()
            })
            .collect();
        for row in 0..b.num_rows() {
            let key: Vec<String> = cols_plain
                .iter()
                .map(|a| a.value(row).to_string())
                .collect();
            let code = match map.get(&key) {
                Some(c) => *c,
                None => {
                    if labels.len() >= MAX_BINS {
                        return Ok(None);
                    }
                    let c = labels.len() as u8;
                    map.insert(key.clone(), c);
                    labels.push(key);
                    c
                }
            };
            codes.push(code);
        }
    }
    Ok(Some((codes, labels)))
}

// ---------------------------------------------------------------------------
// The wrapper operator
// ---------------------------------------------------------------------------

/// Wraps the normal aggregate operator; runs on the GPU when everything is
/// resident, otherwise requests uploads and delegates. Never slower.
pub struct GpuAggExec {
    plan: Arc<GpuAggPlan>,
    inner: Arc<dyn PhysicalOperator>,
    /// GPU-or-CPU, decided ONCE per operator instance (= per query): an
    /// upload finishing mid-query must not desynchronize partitions.
    decision: OnceLock<bool>,
    resident_request: Option<Arc<ResidentRequest>>,
}

impl GpuAggExec {
    pub(crate) fn with_resident_request(mut self, request: Arc<ResidentRequest>) -> Self {
        self.resident_request = Some(request);
        self
    }

    pub fn new(plan: GpuAggPlan, inner: Arc<dyn PhysicalOperator>) -> Self {
        Self {
            plan: Arc::new(plan),
            inner,
            decision: OnceLock::new(),
            resident_request: None,
        }
    }
}

impl std::fmt::Debug for GpuAggExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GpuAggExec [{} aggs on {}]",
            self.plan.aggs.len(),
            self.plan.table
        )
    }
}

#[async_trait::async_trait]
impl PhysicalOperator for GpuAggExec {
    fn name(&self) -> &str {
        "GpuAggExec"
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.inner.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;
        if let Some(request) = &self.resident_request {
            if partition != 0 || self.inner.output_partitions() != 1 {
                return Err(residency_error("multi-output delegate"));
            }
            let batch = request.run().await?;
            return Ok(Box::pin(futures::stream::iter(vec![Ok(batch)])));
        }
        // GPU aggregation emits one complete result on partition zero. A
        // multi-output CPU delegate must retain all of its partitions, even
        // if cache readiness would otherwise choose the device path.
        if self.inner.output_partitions() != 1 {
            return self.inner.execute(partition).await;
        }
        let trace = gpu_debug_enabled();
        let use_gpu = *self.decision.get_or_init(|| match GpuEngine::get() {
            Some(engine) => {
                let ready = engine.ready(&self.plan);
                if !ready {
                    engine.request(&self.plan);
                    if trace {
                        eprintln!(
                            "[gpu-trace] not ready table={} pid={:x} -- requesting upload(s), \
                             this query runs on CPU snapshot=[{}]",
                            self.plan.table,
                            self.plan.pid(),
                            engine.snapshot()
                        );
                    }
                }
                ready
            }
            None => false,
        });
        if use_gpu {
            if partition == 0 {
                let engine = GpuEngine::get().expect("decided");
                match engine.run(&self.plan).await {
                    Ok(batch) => {
                        tracing::debug!("gpu: served {} on device", self.plan.table);
                        if trace {
                            eprintln!(
                                "[gpu-trace] run OK table={} pid={:x} snapshot=[{}]",
                                self.plan.table,
                                self.plan.pid(),
                                engine.snapshot()
                            );
                        }
                        return Ok(Box::pin(futures::stream::iter(vec![Ok(batch)])));
                    }
                    Err(e) => {
                        // The plan WAS fully resident/ready (unlike an
                        // upload failure, this is an execution-time
                        // failure) -- task 002: falls back to the CPU
                        // operator for THIS query only, isolated exactly
                        // like an upload failure (see module doc).
                        tracing::warn!("gpu: run failed, falling back: {e}");
                        GpuEngine::mark_run_fallback();
                        if trace {
                            eprintln!(
                                "[gpu-trace] run FAILED table={} pid={:x} err={e} \
                                 -- falling back to CPU for this query only snapshot=[{}]",
                                self.plan.table,
                                self.plan.pid(),
                                engine.snapshot()
                            );
                        }
                        return self.inner.execute(partition).await;
                    }
                }
            }
            // GPU answers on partition 0 alone.
            return Ok(Box::pin(futures::stream::empty()));
        }
        self.inner.execute(partition).await
    }

    fn output_partitions(&self) -> usize {
        self.inner.output_partitions()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `parse_cache_budget_mb` is pure (no env access) precisely so it can
    // be unit-tested hermetically — see its own doc comment for why
    // `cache_budget_bytes` (the env-touching wrapper) is NOT tested here:
    // `cargo test` runs every `#[test]` in this crate's lib target from one
    // process, by default on parallel threads, and mutating a real
    // process-global env var from a unit test would race any other test
    // that happens to read the same key (the exact hazard
    // `execution::context::parse_merge_concurrency`'s own doc names).

    #[test]
    fn cache_budget_defaults_when_unset() {
        assert_eq!(parse_cache_budget_mb(None), DEFAULT_GPU_CACHE_MB);
    }

    #[test]
    fn cache_budget_parses_a_valid_value() {
        assert_eq!(parse_cache_budget_mb(Some("1024")), 1024);
        assert_eq!(parse_cache_budget_mb(Some("  512  ")), 512);
    }

    #[test]
    fn cache_budget_falls_back_on_garbage_or_zero() {
        assert_eq!(
            parse_cache_budget_mb(Some("not a number")),
            DEFAULT_GPU_CACHE_MB
        );
        assert_eq!(parse_cache_budget_mb(Some("0")), DEFAULT_GPU_CACHE_MB);
        assert_eq!(parse_cache_budget_mb(Some("-5")), DEFAULT_GPU_CACHE_MB);
        assert_eq!(parse_cache_budget_mb(Some("")), DEFAULT_GPU_CACHE_MB);
    }

    #[test]
    fn cache_new_starts_empty() {
        let cache = GpuCache::new();
        assert_eq!(cache.total_bytes, 0);
        assert!(cache.columns.is_empty());
        assert!(cache.code_bufs.is_empty());
        assert!(cache.rows.is_empty());
    }

    // Task 002: `GpuCacheSnapshot`/its `Display` impl are plain data with no
    // CUDA/env dependency, so — unlike the failure-isolation mechanism
    // itself, which needs real hardware (`tests/gpu_failure_isolation_tests
    // .rs`) — they can be unit-tested hermetically right here.
    #[test]
    fn snapshot_display_includes_every_field() {
        let snap = GpuCacheSnapshot {
            resident_columns: 3,
            resident_bytes: 1024,
            budget_bytes: 2048,
            eviction_count: 5,
            upload_failures: 7,
            run_fallbacks: 2,
        };
        let s = snap.to_string();
        assert!(s.contains("resident_columns=3"));
        assert!(s.contains("resident_bytes=1024"));
        assert!(s.contains("budget_bytes=2048"));
        assert!(s.contains("eviction_count=5"));
        assert!(s.contains("upload_failures=7"));
        assert!(s.contains("run_fallbacks=2"));
    }
}

#[cfg(test)]
mod gpu_partition_contract_tests {
    use super::*;
    use futures::TryStreamExt;

    #[derive(Debug)]
    struct Source {
        partitions: usize,
        calls: Mutex<Vec<usize>>,
    }
    impl Source {
        fn output_schema(&self) -> SchemaRef {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("partition", DataType::Int64, false),
            ]))
        }
    }
    impl TableProvider for Source {
        fn schema(&self) -> SchemaRef {
            self.output_schema()
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn scan(&self, _: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
            panic!("partition fallback must not request GPU population")
        }
    }
    #[async_trait::async_trait]
    impl PhysicalOperator for Source {
        fn schema(&self) -> SchemaRef {
            self.output_schema()
        }
        fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
            vec![]
        }
        fn name(&self) -> &str {
            "GpuPartitionSentinel"
        }
        fn output_partitions(&self) -> usize {
            self.partitions
        }
        async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
            // Intentionally permissive sentinel: the wrapper must reject an
            // invalid partition before delegation, independent of its child.
            self.calls.lock().unwrap().push(partition);
            let batch = RecordBatch::try_new(
                self.output_schema(),
                vec![Arc::new(Int64Array::from(vec![partition as i64]))],
            )?;
            Ok(Box::pin(futures::stream::iter(vec![Ok(batch)])))
        }
    }
    fn wrapper(partitions: usize, ready: bool) -> (GpuAggExec, Arc<Source>) {
        let source = Arc::new(Source {
            partitions,
            calls: Mutex::new(Vec::new()),
        });
        let op = GpuAggExec::new(
            GpuAggPlan {
                table: "partition_sentinel".into(),
                provider: source.clone(),
                preds: vec![],
                aggs: vec![GpuAgg::Count(GpuInput::Col("partition".into()))],
                group_cols: vec![],
                schema: source.output_schema(),
            },
            source.clone(),
        );
        // Model the cached readiness decision without initializing CUDA or
        // changing process-global state. Nonzero partitions never call run.
        op.decision.set(ready).unwrap();
        (op, source)
    }

    #[tokio::test]
    async fn ready_gpu_wrapper_preserves_nonzero_cpu_partitions() {
        let (op, source) = wrapper(3, true);
        for partition in [2, 1] {
            let batches: Vec<_> = op
                .execute(partition)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let values: Vec<_> = batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(values, vec![partition as i64]);
        }
        assert_eq!(*source.calls.lock().unwrap(), vec![2, 1]);
    }

    #[tokio::test]
    async fn ready_multi_output_partition_zero_does_not_enter_cuda() {
        let (op, source) = wrapper(3, true);
        let batches: Vec<_> = op.execute(0).await.unwrap().try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert_eq!(*source.calls.lock().unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn gpu_wrapper_rejects_invalid_and_zero_declared_partitions() {
        for (partitions, ready, requested) in [(0, false, 0), (1, false, 1), (3, true, 3)] {
            let (op, source) = wrapper(partitions, ready);
            let result = op.execute(requested).await;
            assert!(
                matches!(result, Err(QueryError::Internal(message)) if message.contains("GpuAggExec") && message.contains("out of range"))
            );
            assert!(source.calls.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn cpu_choice_preserves_all_declared_partitions() {
        let (op, source) = wrapper(3, false);
        let mut values = Vec::new();
        for partition in 0..op.output_partitions() {
            let mut stream = op.execute(partition).await.unwrap();
            while let Some(batch) = stream.try_next().await.unwrap() {
                values.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                );
            }
        }
        assert_eq!(values, vec![0, 1, 2]);
        assert_eq!(*source.calls.lock().unwrap(), vec![0, 1, 2]);
    }
}

/// Worker substrate only: no benchmark/context opt-in is wired yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResidentFailureKind {
    Unsupported,
    RowMismatch,
    Capacity,
    Upload,
    WorkerGone,
    Cancelled,
    Timeout,
    Conflict,
    Session,
    Overflow,
}
#[derive(Debug)]
pub(crate) struct ResidentFailure {
    pub kind: ResidentFailureKind,
    pub dependency: String,
    pub detail: String,
}
type ResidentResult<T> = std::result::Result<T, ResidentFailure>;
fn resident_failure(
    kind: ResidentFailureKind,
    dependency: impl Into<String>,
    detail: impl ToString,
) -> ResidentFailure {
    ResidentFailure {
        kind,
        dependency: dependency.into(),
        detail: detail.to_string(),
    }
}
#[derive(Debug, Clone)]
pub(crate) struct ResidentAck {
    pub session_id: u64,
    pub rows: usize,
    pub groups: usize,
    pub column_bytes: usize,
    pub codes_bytes: usize,
    pub columns: Vec<String>,
    pub codes_key: Option<String>,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResidentRunCounters {
    pub attempted: u64,
    pub completed: u64,
}
pub(crate) struct ResidentRun {
    pub batch: RecordBatch,
    pub counters: ResidentRunCounters,
}
/// Dropping either a pending prepare or an acknowledged lease cancels its session.
/// Active CUDA work is not preempted; it retains worker-owned buffers to completion.
pub(crate) struct ResidentLease {
    id: u64,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    sender: std::sync::mpsc::Sender<Job>,
}
impl Drop for ResidentLease {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.sender.send(Job::ReleaseResident { id: self.id });
    }
}
impl ResidentLease {
    pub(crate) async fn run(&self) -> ResidentResult<ResidentRun> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(Job::RunResident { id: self.id, reply })
            .map_err(|e| resident_failure(ResidentFailureKind::WorkerGone, "run", e))?;
        rx.await
            .map_err(|e| resident_failure(ResidentFailureKind::WorkerGone, "run", e))?
    }
}
struct ResidentSession {
    id: u64,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    plan: Arc<GpuAggPlan>,
    ack: ResidentAck,
    attempted: u64,
    completed: u64,
}
// Both explicit Release and failed Run retire only the owning session.
// A late message from an older lease cannot unlock another active session.
fn retire_resident(state: &mut Option<ResidentSession>, id: u64) {
    if state.as_ref().is_some_and(|session| session.id == id) {
        *state = None;
    }
}
fn resident_for_run(
    state: &mut Option<ResidentSession>,
    id: u64,
) -> ResidentResult<&mut ResidentSession> {
    state
        .as_mut()
        .filter(|session| session.id == id && session.active())
        .ok_or_else(|| {
            resident_failure(
                ResidentFailureKind::Session,
                "run",
                "unknown/cancelled session",
            )
        })
}
impl ResidentSession {
    fn active(&self) -> bool {
        !self.cancelled.load(Ordering::Acquire)
    }
}
impl GpuEngine {
    pub(crate) async fn prepare_resident(
        &self,
        plan: Arc<GpuAggPlan>,
        timeout: std::time::Duration,
    ) -> ResidentResult<(ResidentLease, ResidentAck)> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| {
                resident_failure(ResidentFailureKind::Overflow, "session", "IDs exhausted")
            })?;
        let lease = ResidentLease {
            id,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sender: self.sender.clone(),
        };
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(Job::PrepareResident {
                id,
                plan,
                cancelled: lease.cancelled.clone(),
                reply,
            })
            .map_err(|e| resident_failure(ResidentFailureKind::WorkerGone, "prepare", e))?;
        let ack = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| {
                resident_failure(ResidentFailureKind::Timeout, "prepare", "deadline exceeded")
            })?
            .map_err(|e| resident_failure(ResidentFailureKind::WorkerGone, "prepare", e))??;
        Ok((lease, ack))
    }
}
fn resident_spec(plan: &GpuAggPlan) -> RunSpec {
    RunSpec {
        columns: plan.needed_columns(),
        pid: plan.pid(),
        preds: plan.preds.clone(),
        aggs: plan.aggs.clone(),
        codes_key: plan.codes_key(),
        schema: plan.schema.clone(),
    }
}
fn verify_resident(cache: &GpuCache, plan: &GpuAggPlan, id: u64) -> ResidentResult<ResidentAck> {
    if !supported_aggregate_domain(&plan.aggs, &plan.schema) {
        return Err(resident_failure(
            ResidentFailureKind::Unsupported,
            "aggregate domain",
            "SUM requires Float64 output; MIN/MAX requires direct column input; output schema must contain every aggregate",
        ));
    }
    let columns = plan.needed_columns();
    if columns.is_empty() {
        return Err(resident_failure(
            ResidentFailureKind::Unsupported,
            "columns",
            "no numeric length source",
        ));
    }
    let mut rows = None;
    let mut bytes = 0usize;
    for col in &columns {
        let e = cache
            .columns
            .get(&(plan.pid(), col.clone()))
            .ok_or_else(|| {
                resident_failure(
                    ResidentFailureKind::Capacity,
                    col,
                    "dependency not retained",
                )
            })?;
        if rows.is_some_and(|n| n != e.buf.len()) {
            return Err(resident_failure(
                ResidentFailureKind::RowMismatch,
                col,
                "different row count",
            ));
        }
        rows = Some(e.buf.len());
        bytes = bytes
            .checked_add(e.bytes)
            .ok_or_else(|| resident_failure(ResidentFailureKind::Overflow, col, "bytes"))?;
    }
    let key = plan.codes_key();
    let (groups, codes_bytes) = if let Some(key) = &key {
        let e = cache.code_bufs.get(key).ok_or_else(|| {
            resident_failure(ResidentFailureKind::Capacity, key, "codes not retained")
        })?;
        if Some(e.buf.len()) != rows {
            return Err(resident_failure(
                ResidentFailureKind::RowMismatch,
                key,
                "codes row count",
            ));
        }
        (e.ngroups, e.bytes)
    } else {
        (1, 0)
    };
    let slots = plan
        .aggs
        .iter()
        .try_fold(1usize, |n, a| {
            n.checked_add(if matches!(a, GpuAgg::Avg(_)) { 2 } else { 1 })
        })
        .ok_or_else(|| resident_failure(ResidentFailureKind::Overflow, "bins", "slots"))?;
    if groups.checked_mul(slots).is_none_or(|n| n > MAX_BINS) {
        return Err(resident_failure(
            ResidentFailureKind::Unsupported,
            "bins",
            "group/bin limit",
        ));
    }
    Ok(ResidentAck {
        session_id: id,
        rows: rows.unwrap(),
        groups,
        column_bytes: bytes,
        codes_bytes,
        columns,
        codes_key: key.map(|key| key.to_string()),
    })
}
fn upload_typed(
    stream: &Arc<cudarc::driver::CudaStream>,
    cache: &mut GpuCache,
    pid: ProviderKey,
    col: &str,
    provider: &Arc<dyn TableProvider>,
) -> ResidentResult<()> {
    // A queued request can become obsolete before the worker consumes it.
    // Exact keys prove this immutable/versioned dependency already exists.
    if cache.columns.contains_key(&(pid.clone(), col.to_owned())) {
        cache.touch_column(pid, col);
        return Ok(());
    }
    let values = load_column_f64(provider, col)
        .map_err(|e| resident_failure(ResidentFailureKind::Upload, col, e))?
        .ok_or_else(|| {
            resident_failure(ResidentFailureKind::Unsupported, col, "null/type/range")
        })?;
    if cache
        .rows
        .get(&pid)
        .is_some_and(|expected| *expected != values.len())
    {
        return Err(resident_failure(
            ResidentFailureKind::RowMismatch,
            col,
            "row count",
        ));
    }
    let bytes = values
        .len()
        .checked_mul(std::mem::size_of::<f64>())
        .ok_or_else(|| resident_failure(ResidentFailureKind::Overflow, col, "upload bytes"))?;
    cache.reserve(bytes);
    let buf = stream
        .memcpy_stod(&values)
        .map_err(|e| resident_failure(ResidentFailureKind::Upload, col, e))?;
    let replaced = cache
        .insert_column(pid.clone(), col.to_owned(), buf, bytes)
        .map_err(|error| resident_failure(ResidentFailureKind::Overflow, col, error))?;
    cache.rows.insert(pid.clone(), values.len());
    GpuEngine::mark_resident(pid.clone(), col, bytes, replaced);
    if gpu_debug_enabled() {
        eprintln!("[gpu-trace] upload OK pid={pid:x} col={col} bytes={bytes}");
    }
    Ok(())
}
fn codes_typed(
    stream: &Arc<cudarc::driver::CudaStream>,
    cache: &mut GpuCache,
    pid: ProviderKey,
    key: &CodeKey,
    cols: &[String],
    provider: &Arc<dyn TableProvider>,
) -> ResidentResult<()> {
    if cache.code_bufs.contains_key(key) {
        cache.touch_codes(key);
        return Ok(());
    }
    let (codes, labels) = build_codes(provider, cols)
        .map_err(|e| resident_failure(ResidentFailureKind::Upload, key.to_string(), e))?
        .ok_or_else(|| {
            resident_failure(
                ResidentFailureKind::Unsupported,
                key,
                "group domain/bin limit",
            )
        })?;
    if cache
        .rows
        .get(&pid)
        .is_some_and(|expected| *expected != codes.len())
    {
        return Err(resident_failure(
            ResidentFailureKind::RowMismatch,
            key,
            "row count",
        ));
    }
    let groups = labels.len();
    let bytes = codes.len();
    cache.reserve(bytes);
    let buf = stream
        .memcpy_stod(&codes)
        .map_err(|e| resident_failure(ResidentFailureKind::Upload, key.to_string(), e))?;
    let replaced = cache
        .insert_codes(key.to_owned(), buf, labels, groups, bytes)
        .map_err(|error| resident_failure(ResidentFailureKind::Overflow, key, error))?;
    cache.rows.insert(pid.clone(), codes.len());
    GpuEngine::mark_codes(key, groups, bytes, replaced);
    if gpu_debug_enabled() {
        eprintln!("[gpu-trace] codes OK key={key} groups={groups} bytes={bytes}");
    }
    Ok(())
}

#[cfg(test)]
mod resident_substrate_tests {
    use super::*;
    fn engine() -> (GpuEngine, std::sync::mpsc::Receiver<Job>) {
        let (sender, rx) = std::sync::mpsc::channel();
        (
            GpuEngine {
                sender,
                resident: Mutex::new(HashSet::new()),
                codes: Mutex::new(HashMap::new()),
                queued: Mutex::new(HashSet::new()),
                resident_bytes: AtomicUsize::new(0),
                eviction_count: AtomicU64::new(0),
                upload_failures: AtomicU64::new(0),
                run_fallbacks: AtomicU64::new(0),
            },
            rx,
        )
    }
    fn plan() -> Arc<GpuAggPlan> {
        let batch = RecordBatch::try_from_iter([(
            "v",
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        )])
        .unwrap();
        Arc::new(GpuAggPlan {
            table: "fixture".into(),
            provider: Arc::new(crate::physical::operators::MemoryTable::new(
                batch.schema(),
                vec![batch.clone()],
            )),
            preds: vec![],
            aggs: vec![GpuAgg::Sum(GpuInput::Col("v".into()))],
            group_cols: vec![],
            schema: batch.schema(),
        })
    }
    #[tokio::test]
    async fn acknowledged_prepare_waits_for_worker_and_lease_drop_releases() {
        let (engine, rx) = engine();
        let future = engine.prepare_resident(plan(), std::time::Duration::from_secs(10));
        tokio::pin!(future);
        assert!(futures::poll!(&mut future).is_pending());
        let Job::PrepareResident {
            id,
            cancelled,
            reply,
            ..
        } = rx.try_recv().unwrap()
        else {
            panic!("prepare expected")
        };
        // A worker has received the request, but numeric/codes completion has
        // not yet acknowledged: mere enqueue/readiness observation is insufficient.
        assert!(futures::poll!(&mut future).is_pending());
        reply
            .send(Ok(ResidentAck {
                session_id: id,
                rows: 1,
                groups: 1,
                column_bytes: 8,
                codes_bytes: 0,
                columns: vec!["v".into()],
                codes_key: None,
            }))
            .unwrap();
        let (lease, ack) = future.await.unwrap();
        assert_eq!(ack.session_id, id);
        assert!(!cancelled.load(Ordering::Acquire));
        drop(lease);
        assert!(cancelled.load(Ordering::Acquire));
        assert!(
            matches!(rx.try_recv().unwrap(),Job::ReleaseResident {id:released} if released==id)
        );
    }
    #[tokio::test]
    async fn dropped_prepare_future_cancels_already_enqueued_work() {
        let (engine, rx) = engine();
        let mut future =
            Box::pin(engine.prepare_resident(plan(), std::time::Duration::from_secs(10)));
        assert!(futures::poll!(&mut future).is_pending());
        let Job::PrepareResident {
            id,
            cancelled,
            reply,
            ..
        } = rx.try_recv().unwrap()
        else {
            panic!("prepare expected")
        };
        drop(future);
        assert!(cancelled.load(Ordering::Acquire));
        assert!(reply.is_closed());
        assert!(
            matches!(rx.try_recv().unwrap(),Job::ReleaseResident {id:released} if released==id)
        );
    }
    #[tokio::test]
    async fn worker_shutdown_is_terminal_and_preparation_owner_released() {
        let (engine, rx) = engine();
        let mut future =
            Box::pin(engine.prepare_resident(plan(), std::time::Duration::from_secs(10)));
        assert!(futures::poll!(&mut future).is_pending());
        let Job::PrepareResident {
            cancelled, reply, ..
        } = rx.try_recv().unwrap()
        else {
            panic!("prepare expected")
        };
        drop(reply);
        let error = match future.await {
            Err(e) => e,
            Ok(_) => panic!("worker loss must fail"),
        };
        assert_eq!(error.kind, ResidentFailureKind::WorkerGone);
        assert!(cancelled.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn resident_run_uses_session_only_and_propagates_failure_without_cpu_path() {
        let (sender, rx) = std::sync::mpsc::channel();
        let lease = ResidentLease {
            id: 17,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sender,
        };
        let mut future = Box::pin(lease.run());
        assert!(futures::poll!(&mut future).is_pending());
        let Job::RunResident { id, reply } = rx.try_recv().unwrap() else {
            panic!("resident dispatch expected")
        };
        assert_eq!(id, 17);
        assert!(reply
            .send(Err(resident_failure(
                ResidentFailureKind::Session,
                "run",
                "expired"
            )))
            .is_ok());
        let error = match future.await {
            Err(e) => e,
            Ok(_) => panic!("expired session must fail"),
        };
        assert_eq!(error.kind, ResidentFailureKind::Session);
    }
    fn session(id: u64) -> ResidentSession {
        ResidentSession {
            id,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            plan: plan(),
            ack: ResidentAck {
                session_id: id,
                rows: 1,
                groups: 1,
                column_bytes: 8,
                codes_bytes: 0,
                columns: vec!["v".into()],
                codes_key: None,
            },
            attempted: 2,
            completed: 2,
        }
    }
    #[test]
    fn stale_run_failure_and_release_do_not_retire_current_session() {
        let mut state = Some(session(22));
        let error = match resident_for_run(&mut state, 21) {
            Err(e) => e,
            Ok(_) => panic!("stale run must fail"),
        };
        assert_eq!(error.kind, ResidentFailureKind::Session);
        // The same transition the worker applies after RunResident failure.
        retire_resident(&mut state, 21);
        assert_eq!(state.as_ref().unwrap().id, 22);
        assert!(state.as_ref().unwrap().active());
        assert_eq!(state.as_ref().unwrap().attempted, 2);
        // Explicit stale Release uses this same transition.
        retire_resident(&mut state, 21);
        assert_eq!(resident_for_run(&mut state, 22).unwrap().completed, 2);
        retire_resident(&mut state, 22);
        assert!(state.is_none());
    }
    #[test]
    fn own_cancelled_run_is_rejected_and_retired_without_touching_other_tokens() {
        let mut state = Some(session(22));
        state
            .as_ref()
            .unwrap()
            .cancelled
            .store(true, Ordering::Release);
        assert!(resident_for_run(&mut state, 22).is_err());
        retire_resident(&mut state, 21);
        assert!(state.is_some());
        retire_resident(&mut state, 22);
        assert!(state.is_none());
    }
    #[test]
    fn minimum_int64_upload_domain_is_explicitly_unsupported() {
        let batch = RecordBatch::try_from_iter([(
            "v",
            Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef,
        )])
        .unwrap();
        let provider: Arc<dyn TableProvider> = Arc::new(
            crate::physical::operators::MemoryTable::new(batch.schema(), vec![batch]),
        );
        assert!(load_column_f64(&provider, "v").unwrap().is_none());
        let batch = RecordBatch::try_from_iter([(
            "v",
            Arc::new(Int64Array::from(vec![-(1i64 << 52), 0, 1i64 << 52])) as ArrayRef,
        )])
        .unwrap();
        let provider: Arc<dyn TableProvider> = Arc::new(
            crate::physical::operators::MemoryTable::new(batch.schema(), vec![batch]),
        );
        assert_eq!(
            load_column_f64(&provider, "v").unwrap().unwrap(),
            vec![-4503599627370496.0, 0.0, 4503599627370496.0]
        );
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GpuResidentPreparation {
    pub session_id: u64,
    pub preparation_ms: f64,
    pub rows: usize,
    pub groups: usize,
    pub column_bytes: usize,
    pub codes_bytes: usize,
    pub columns: Vec<String>,
    pub codes_key: Option<String>,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct GpuResidentEvidence {
    pub session_id: u64,
    pub matched_operators: usize,
    pub attempted_device_runs: usize,
    pub completed_device_runs: usize,
    pub failures: usize,
    pub failure_reason: Option<String>,
}
pub struct GpuResidentQueryOutcome {
    pub result: Result<crate::execution::QueryResult>,
    pub evidence: GpuResidentEvidence,
}
pub struct PreparedGpuSession {
    pub(crate) lease: Arc<ResidentLease>,
    pub(crate) plan: Arc<GpuAggPlan>,
    pub(crate) sql: String,
    pub(crate) busy: Arc<std::sync::atomic::AtomicBool>,
    metadata: GpuResidentPreparation,
}
impl PreparedGpuSession {
    pub fn metadata(&self) -> &GpuResidentPreparation {
        &self.metadata
    }
    pub(crate) fn new(
        lease: ResidentLease,
        ack: ResidentAck,
        plan: Arc<GpuAggPlan>,
        sql: String,
        ms: f64,
    ) -> Self {
        Self {
            lease: Arc::new(lease),
            plan,
            sql,
            busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            metadata: GpuResidentPreparation {
                session_id: ack.session_id,
                preparation_ms: ms,
                rows: ack.rows,
                groups: ack.groups,
                column_bytes: ack.column_bytes,
                codes_bytes: ack.codes_bytes,
                columns: ack.columns,
                codes_key: ack.codes_key,
            },
        }
    }
    pub(crate) fn begin(&self) -> Result<Arc<ResidentRequest>> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| residency_error("session already executing"))?;
        Ok(Arc::new(ResidentRequest {
            lease: self.lease.clone(),
            plan: self.plan.clone(),
            sql: self.sql.clone(),
            busy: self.busy.clone(),
            matched: AtomicUsize::new(0),
            attempted: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
        }))
    }
}
pub(crate) struct ResidentRequest {
    lease: Arc<ResidentLease>,
    plan: Arc<GpuAggPlan>,
    pub(crate) sql: String,
    busy: Arc<std::sync::atomic::AtomicBool>,
    matched: AtomicUsize,
    attempted: AtomicUsize,
    completed: AtomicUsize,
}
impl Drop for ResidentRequest {
    fn drop(&mut self) {
        self.busy.store(false, Ordering::Release);
    }
}
impl ResidentRequest {
    pub(crate) fn bind(&self, plan: &GpuAggPlan, partitions: usize) -> Result<()> {
        if partitions != 1 || !same_resident_plan(&self.plan, plan) {
            return Err(residency_error(
                "replanned GPU operator/provider does not match prepared session",
            ));
        }
        if self.matched.fetch_add(1, Ordering::SeqCst) != 0 {
            return Err(residency_error("multiple GPU operators"));
        }
        Ok(())
    }
    pub(crate) fn check_planned(&self) -> Result<()> {
        if self.matched.load(Ordering::SeqCst) != 1 {
            Err(residency_error("exactly one matched GPU operator required"))
        } else {
            Ok(())
        }
    }
    pub(crate) fn check_completed(&self) -> Result<()> {
        self.check_planned()?;
        if self.attempted.load(Ordering::SeqCst) != 1 || self.completed.load(Ordering::SeqCst) != 1
        {
            Err(residency_error("expected one successful device execution"))
        } else {
            Ok(())
        }
    }
    pub(crate) fn evidence(&self, error: Option<String>) -> GpuResidentEvidence {
        GpuResidentEvidence {
            session_id: self.lease.id,
            matched_operators: self.matched.load(Ordering::SeqCst),
            attempted_device_runs: self.attempted.load(Ordering::SeqCst),
            completed_device_runs: self.completed.load(Ordering::SeqCst),
            failures: usize::from(error.is_some()),
            failure_reason: error,
        }
    }
    async fn run(&self) -> Result<RecordBatch> {
        if self.attempted.fetch_add(1, Ordering::SeqCst) != 0 {
            return Err(residency_error("duplicate device dispatch"));
        }
        let output = self.lease.run().await.map_err(|e| {
            residency_error(&format!(
                "resident worker {:?}: {}: {}",
                e.kind, e.dependency, e.detail
            ))
        })?;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(output.batch)
    }
}
pub(crate) fn residency_error(message: &str) -> QueryError {
    QueryError::Execution(format!("GPU residency required: {message}"))
}
fn same_resident_plan(a: &GpuAggPlan, b: &GpuAggPlan) -> bool {
    Arc::ptr_eq(&a.provider, &b.provider)
        && a.table == b.table
        && a.schema == b.schema
        && a.group_cols == b.group_cols
        && a.aggs == b.aggs
        && a.preds.len() == b.preds.len()
        && a.preds
            .iter()
            .zip(&b.preds)
            .all(|(a, b)| a.col == b.col && a.op == b.op && a.value.to_bits() == b.value.to_bits())
}
/// Positive, deliberately narrow expression proof. Unknown forms decline.
fn resident_expr(e: &Expr) -> bool {
    match e {
        Expr::Column(_) | Expr::Literal(_) => true,
        Expr::BinaryExpr { left, right, .. } => resident_expr(left) && resident_expr(right),
        Expr::UnaryExpr { expr, .. } | Expr::Alias { expr, .. } | Expr::Cast { expr, .. } => {
            resident_expr(expr)
        }
        Expr::Aggregate {
            func,
            args,
            distinct,
        } => {
            (!distinct
                && matches!(func, AggregateFunction::Count)
                && matches!(args.as_slice(), [Expr::Wildcard]))
                || args.iter().all(resident_expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => resident_expr(expr) && resident_expr(low) && resident_expr(high),
        _ => false,
    }
}
pub(crate) fn resident_logical(
    plan: &crate::planner::LogicalPlan,
    tables: &HashMap<String, Arc<dyn TableProvider>>,
) -> Result<()> {
    use crate::planner::LogicalPlan;
    let valid = match plan {
        LogicalPlan::Scan(n) => {
            tables
                .get(&n.table_name)
                .is_some_and(|p| p.as_any().is::<crate::physical::operators::MemoryTable>())
                && n.filter.as_ref().is_none_or(resident_expr)
        }
        LogicalPlan::Filter(n) => resident_expr(&n.predicate),
        LogicalPlan::Project(n) => n.exprs.iter().all(resident_expr),
        LogicalPlan::Aggregate(n) => n.group_by.iter().chain(&n.aggregates).all(resident_expr),
        LogicalPlan::Sort(n) => n.order_by.iter().all(|s| resident_expr(&s.expr)),
        LogicalPlan::Limit(_) => true,
        _ => false,
    };
    if !valid {
        return Err(residency_error("requires closed Scan/Filter/Project/Aggregate/Sort/Limit over pinned MemoryTable; unsupported expression/provider"));
    }
    for child in plan.children() {
        resident_logical(child, tables)?;
    }
    Ok(())
}
pub(crate) fn resident_statement(stmt: &sqlparser::ast::Statement) -> Result<()> {
    match stmt {
        sqlparser::ast::Statement::Query(query) if query.with.is_none() => Ok(()),
        _ => Err(residency_error("only SELECT without CTEs is supported")),
    }
}

#[cfg(test)]
mod resident_engine_contract_tests {
    use super::*;
    fn session() -> (PreparedGpuSession, std::sync::mpsc::Receiver<Job>) {
        let batch = RecordBatch::try_from_iter([(
            "v",
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
        )])
        .unwrap();
        let plan = Arc::new(GpuAggPlan {
            table: "t".into(),
            provider: Arc::new(crate::physical::operators::MemoryTable::new(
                batch.schema(),
                vec![batch.clone()],
            )),
            preds: vec![GpuPred {
                col: "v".into(),
                op: 2,
                value: 0.0,
            }],
            aggs: vec![GpuAgg::Sum(GpuInput::Col("v".into()))],
            group_cols: vec![],
            schema: batch.schema(),
        });
        let (sender, rx) = std::sync::mpsc::channel();
        let lease = ResidentLease {
            id: 42,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sender,
        };
        let ack = ResidentAck {
            session_id: 42,
            rows: 2,
            groups: 1,
            column_bytes: 16,
            codes_bytes: 0,
            columns: vec!["v".into()],
            codes_key: None,
        };
        (
            PreparedGpuSession::new(
                lease,
                ack,
                plan,
                "SELECT SUM(v) FROM t WHERE v > 0".into(),
                1.0,
            ),
            rx,
        )
    }
    #[derive(Debug)]
    struct NeverCpu(SchemaRef);
    #[async_trait::async_trait]
    impl PhysicalOperator for NeverCpu {
        fn name(&self) -> &str {
            "NeverCpu"
        }
        fn schema(&self) -> SchemaRef {
            self.0.clone()
        }
        fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
            vec![]
        }
        async fn execute(&self, _: usize) -> Result<RecordBatchStream> {
            panic!("resident mode must never execute CPU delegate")
        }
    }
    #[tokio::test]
    async fn required_operator_dispatch_consumes_acknowledged_output_with_exact_evidence() {
        use futures::TryStreamExt;
        let (s, rx) = session();
        let request = s.begin().unwrap();
        request.bind(&s.plan, 1).unwrap();
        let operator =
            GpuAggExec::new((*s.plan).clone(), Arc::new(NeverCpu(s.plan.schema.clone())))
                .with_resident_request(request.clone());
        let mut pending = Box::pin(operator.execute(0));
        assert!(futures::poll!(&mut pending).is_pending());
        let Job::RunResident { reply, .. } = rx.try_recv().unwrap() else {
            panic!("required worker message")
        };
        let expected = RecordBatch::try_from_iter([(
            "v",
            Arc::new(Float64Array::from(vec![3.0])) as ArrayRef,
        )])
        .unwrap();
        assert!(reply
            .send(Ok(ResidentRun {
                batch: expected,
                counters: ResidentRunCounters {
                    attempted: 1,
                    completed: 1
                }
            }))
            .is_ok());
        let mut stream = pending.await.unwrap();
        let batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            3.0
        );
        assert!(stream.try_next().await.unwrap().is_none());
        request.check_completed().unwrap();
        let e = request.evidence(None);
        assert_eq!(
            (
                e.matched_operators,
                e.attempted_device_runs,
                e.completed_device_runs,
                e.failures
            ),
            (1, 1, 1, 0)
        );
    }
    #[test]
    fn immutable_layout_rejects_nullable_grouping_before_upload() {
        let (s, _) = session();
        resident_memory_layout(&s.plan).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("v", Arc::new(Float64Array::from(vec![1.0])) as ArrayRef),
            (
                "g",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut plan = (*s.plan).clone();
        plan.provider = Arc::new(crate::physical::operators::MemoryTable::new(
            batch.schema(),
            vec![batch],
        ));
        plan.group_cols = vec!["g".into()];
        assert!(resident_memory_layout(&plan).is_err());
    }

    #[test]
    fn typed_plan_match_requires_same_provider_float_bits_aggregate_and_schema() {
        let (s, _) = session();
        let mut p = (*s.plan).clone();
        assert!(same_resident_plan(&s.plan, &p));
        p.preds[0].value = -0.0;
        assert!(!same_resident_plan(&s.plan, &p));
        p = (*s.plan).clone();
        p.aggs = vec![GpuAgg::Count(GpuInput::Col("v".into()))];
        assert!(!same_resident_plan(&s.plan, &p));
        let (other, _) = session();
        assert!(!same_resident_plan(&s.plan, &other.plan));
        p = (*s.plan).clone();
        p.preds[0].value = f64::from_bits(0x7ff8000000000001);
        let mut q = p.clone();
        assert!(same_resident_plan(&p, &q));
        q.preds[0].value = f64::from_bits(0x7ff8000000000002);
        assert!(!same_resident_plan(&p, &q));
    }
    #[test]
    fn required_request_rejects_zero_multiple_and_multi_partition_gpu_execution() {
        let (s, _) = session();
        let r = s.begin().unwrap();
        assert!(r.check_planned().is_err());
        assert!(r.bind(&s.plan, 2).is_err());
        assert_eq!(r.evidence(None).matched_operators, 0);
        r.bind(&s.plan, 1).unwrap();
        assert!(r.check_completed().is_err());
        assert!(r.bind(&s.plan, 1).is_err());
        assert!(s.begin().is_err());
        drop(r);
        assert!(s.begin().is_ok());
    }
    #[tokio::test]
    async fn failed_worker_dispatch_has_evidence_and_never_cpu_success() {
        let (s, rx) = session();
        let r = s.begin().unwrap();
        r.bind(&s.plan, 1).unwrap();
        let mut future = Box::pin(r.run());
        assert!(futures::poll!(&mut future).is_pending());
        let Job::RunResident { reply, .. } = rx.try_recv().unwrap() else {
            panic!("resident run required")
        };
        assert!(reply
            .send(Err(resident_failure(
                ResidentFailureKind::Session,
                "run",
                "missing residency"
            )))
            .is_ok());
        assert!(future.await.is_err());
        let e = r.evidence(Some("worker refusal".into()));
        assert_eq!(
            (e.attempted_device_runs, e.completed_device_runs, e.failures),
            (1, 0, 1)
        );
        assert!(r.check_completed().is_err());
    }
    #[tokio::test]
    async fn public_request_rejects_sql_mismatch_with_failure_evidence() {
        let (s, rx) = session();
        let ctx = crate::ExecutionContext::new();
        let out = ctx.sql_gpu_resident("SELECT 1", &s).await;
        assert!(out.result.is_err());
        assert_eq!(out.evidence.session_id, 42);
        assert_eq!(out.evidence.completed_device_runs, 0);
        assert_eq!(out.evidence.failures, 1);
        assert!(rx.try_recv().is_err());
    }
    #[tokio::test]
    async fn preflight_rejects_cte_subqueries_and_zero_gpu_before_device_access() {
        let mut ctx = crate::ExecutionContext::new();
        ctx.enable_gpu_offload();
        ctx.register_batch(
            "t",
            RecordBatch::try_from_iter([(
                "v",
                Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
            )])
            .unwrap(),
        );
        for sql in [
            "WITH x AS (SELECT v FROM t) SELECT SUM(v) FROM x",
            "SELECT SUM(v) FROM t WHERE v IN (SELECT v FROM t)",
            "SELECT v FROM t",
        ] {
            let error = match ctx
                .prepare_gpu_resident(sql, std::time::Duration::from_secs(1))
                .await
            {
                Err(e) => e,
                Ok(_) => panic!("unsupported request must fail preflight"),
            };
            assert!(
                error.to_string().contains("GPU residency required"),
                "{error}"
            );
            assert!(
                !error.to_string().contains("device unavailable"),
                "preflight must precede device access"
            );
        }
    }
}

/// Validate immutable physical arrays before the legacy upload helpers access
/// typed buffers. This first public slice deliberately declines dictionary
/// grouping (including logical NULL ambiguity), rather than decoding speculatively.
pub(crate) fn resident_memory_layout(plan: &GpuAggPlan) -> Result<()> {
    if !plan
        .provider
        .as_any()
        .is::<crate::physical::operators::MemoryTable>()
    {
        return Err(residency_error("provider must be immutable MemoryTable"));
    }
    let schema = plan.provider.schema();
    let columns = plan.needed_columns();
    for name in columns.iter().chain(&plan.group_cols) {
        let (index, field) = schema
            .column_with_name(name)
            .ok_or_else(|| residency_error("missing dependency column"))?;
        for batch in plan.provider.scan(Some(&[index]))? {
            if batch.num_columns() != 1 {
                return Err(residency_error("invalid dependency projection"));
            }
            let a = batch.column(0);
            if a.data_type() != field.data_type() || a.null_count() != 0 {
                return Err(residency_error("physical dependency type/NULL unsupported"));
            }
            let valid = if plan.group_cols.contains(name) {
                a.as_any().is::<StringArray>()
            } else {
                match a.data_type() {
                    DataType::Float64 => a
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .is_some_and(|values| values.values().iter().all(|v| v.is_finite())),
                    DataType::Int32 => a.as_any().is::<arrow::array::Int32Array>(),
                    DataType::Date32 => a.as_any().is::<arrow::array::Date32Array>(),
                    DataType::Int64 => a
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .is_some_and(|v| v.values().iter().all(|n| n.unsigned_abs() <= 1u64 << 52)),
                    _ => false,
                }
            };
            if !valid {
                return Err(residency_error("unsupported physical dependency domain"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod resident_hardware_lifecycle_contract {
    use super::*;
    const SQL: &str = "SELECT g, SUM(v) AS total FROM resident_fixture GROUP BY g ORDER BY g";
    fn context(scale: f64) -> crate::ExecutionContext {
        let mut context = crate::ExecutionContext::with_memory_limit(64 * 1024 * 1024);
        context.enable_gpu_offload();
        let batch = RecordBatch::try_from_iter([
            (
                "g",
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])) as ArrayRef,
            ),
            (
                "v",
                Arc::new(Float64Array::from(vec![
                    scale,
                    2.0 * scale,
                    4.0 * scale,
                    8.0 * scale,
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        context.register_batch("resident_fixture", batch);
        context
    }
    fn exact_output(outcome: GpuResidentQueryOutcome, session: u64, expected: [f64; 2]) {
        let evidence = outcome.evidence;
        assert_eq!(evidence.session_id, session);
        assert_eq!(
            (
                evidence.matched_operators,
                evidence.attempted_device_runs,
                evidence.completed_device_runs,
                evidence.failures
            ),
            (1, 1, 1, 0)
        );
        assert!(evidence.failure_reason.is_none());
        let result = outcome
            .result
            .expect("required execution must succeed on actual device");
        let mut rows = Vec::new();
        for batch in result.batches {
            assert_eq!(batch.num_columns(), 2);
            let groups = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("plain Utf8 grouped output");
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("exact Float64 result type");
            assert_eq!(groups.null_count(), 0);
            assert_eq!(sums.null_count(), 0);
            for i in 0..batch.num_rows() {
                rows.push((groups.value(i).to_owned(), sums.value(i)));
            }
        }
        assert_eq!(
            rows,
            vec![("a".into(), expected[0]), ("b".into(), expected[1])]
        );
    }
    #[tokio::test]
    #[ignore = "requires real CUDA device; run alone under capped wrapper with GPU enabled"]
    async fn resident_hardware_ack_conflict_release_and_stale_messages_preserve_new_session() {
        let engine = GpuEngine::get()
            .expect("explicit hardware test requires usable CUDA; never skip/fallback");
        let a = context(1.0);
        let b = context(16.0);
        let timeout = std::time::Duration::from_secs(30);
        // Preparation itself requests numeric columns AND group codes and
        // synchronizes their completion. No SQL warmup or fixed sleep.
        let session_a = a
            .prepare_gpu_resident(SQL, timeout)
            .await
            .expect("fresh MemoryTable A must prepare numeric and grouped dependencies");
        let a_id = session_a.metadata().session_id;
        assert_eq!(session_a.metadata().rows, 4);
        assert_eq!(session_a.metadata().groups, 2);
        assert_eq!(session_a.metadata().columns, vec!["v"]);
        assert_eq!(session_a.metadata().column_bytes, 32);
        assert_eq!(session_a.metadata().codes_bytes, 4);
        assert!(session_a.metadata().codes_key.is_some());
        let conflict = match b.prepare_gpu_resident(SQL, timeout).await {
            Err(error) => error,
            Ok(_) => panic!("B preparation must conflict with active A"),
        };
        assert!(conflict.to_string().contains("Conflict"), "{conflict}");
        exact_output(a.sql_gpu_resident(SQL, &session_a).await, a_id, [5.0, 10.0]);
        drop(session_a); // enqueues release before the following preparation
        let session_b = b
            .prepare_gpu_resident(SQL, timeout)
            .await
            .expect("B must prepare after A lease release");
        let b_id = session_b.metadata().session_id;
        assert_ne!(a_id, b_id);
        assert_eq!(session_b.metadata().column_bytes, 32);
        assert_eq!(session_b.metadata().codes_bytes, 4);
        let (reply, rx) = tokio::sync::oneshot::channel();
        engine
            .sender
            .send(Job::RunResident { id: a_id, reply })
            .expect("actual worker alive");
        let stale = match tokio::time::timeout(timeout, rx)
            .await
            .expect("stale run response deadline")
            .expect("worker response")
        {
            Err(error) => error,
            Ok(_) => panic!("stale A run must not execute"),
        };
        assert_eq!(stale.kind, ResidentFailureKind::Session);
        engine
            .sender
            .send(Job::ReleaseResident { id: a_id })
            .expect("actual worker alive");
        // FIFO processing guarantees both stale messages were handled before B
        // dispatch. They must not invalidate B or unlock its resident cache.
        exact_output(
            b.sql_gpu_resident(SQL, &session_b).await,
            b_id,
            [80.0, 160.0],
        );
        drop(session_b);
    }
}

/// Exact cache equality. Numeric formatting is diagnostic ONLY.
#[derive(Clone)]
enum ProviderKey {
    Memory(Arc<dyn TableProvider>),
    Version {
        identity: Vec<u8>,
        files: Option<Vec<std::path::PathBuf>>,
    },
    Unsupported(Arc<dyn TableProvider>),
}
impl ProviderKey {
    fn new(provider: &Arc<dyn TableProvider>) -> Self {
        if provider
            .as_any()
            .is::<crate::physical::operators::MemoryTable>()
        {
            Self::Memory(provider.clone())
        } else if let Some(identity) = provider.identity() {
            Self::Version {
                identity,
                files: provider.parquet_files(),
            }
        } else {
            Self::Unsupported(provider.clone())
        }
    }
    fn supported(&self) -> bool {
        !matches!(self, Self::Unsupported(_))
    }
}
impl PartialEq for ProviderKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Memory(a), Self::Memory(b)) | (Self::Unsupported(a), Self::Unsupported(b)) => {
                Arc::ptr_eq(a, b)
            }
            (
                Self::Version {
                    identity: a,
                    files: af,
                },
                Self::Version {
                    identity: b,
                    files: bf,
                },
            ) => a == b && af == bf,
            _ => false,
        }
    }
}
impl Eq for ProviderKey {}
impl std::hash::Hash for ProviderKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::hash::Hash;
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Memory(p) | Self::Unsupported(p) => {
                (Arc::as_ptr(p) as *const () as usize).hash(state)
            }
            Self::Version { identity, files } => {
                identity.hash(state);
                files.hash(state);
            }
        }
    }
}
impl std::fmt::LowerHex for ProviderKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut h);
        write!(f, "{:x}", h.finish())
    }
}
impl std::fmt::Debug for ProviderKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProviderKey({self:x})")
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CodeKey {
    provider: ProviderKey,
    columns: Vec<String>,
}
impl std::fmt::Display for CodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}:{:?}", self.provider, self.columns)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum QueuedKey {
    Column(ProviderKey, String),
    Codes(CodeKey),
}

impl From<&CodeKey> for String {
    fn from(key: &CodeKey) -> Self {
        key.to_string()
    }
}

// Worker exit/panic drops actual buffers, then releases mirrored strong keys.
struct WorkerMirrorCleanup;
impl Drop for WorkerMirrorCleanup {
    fn drop(&mut self) {
        if let Some(engine) = GpuEngine::get() {
            engine.clear_residency_metadata();
        }
    }
}
impl GpuEngine {
    fn clear_residency_metadata(&self) {
        self.resident
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.codes.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.queued
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.resident_bytes.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod exact_gpu_cache_identity_tests {
    use super::*;
    fn table(value: f64) -> Arc<dyn TableProvider> {
        let batch = RecordBatch::try_from_iter([(
            "v",
            Arc::new(Float64Array::from(vec![value])) as ArrayRef,
        )])
        .unwrap();
        Arc::new(crate::physical::operators::MemoryTable::new(
            batch.schema(),
            vec![batch],
        ))
    }
    #[test]
    fn equal_schema_memory_tables_have_distinct_exact_value_keys() {
        let a = table(1.0);
        let b = table(2.0);
        assert_eq!(a.schema(), b.schema());
        let mut values = HashMap::new();
        values.insert((ProviderKey::new(&a), "v".to_string()), vec![1.0]);
        values.insert((ProviderKey::new(&b), "v".to_string()), vec![2.0]);
        assert_eq!(values.len(), 2);
        assert_eq!(values[&(ProviderKey::new(&a), "v".into())], vec![1.0]);
        assert_eq!(values[&(ProviderKey::new(&b), "v".into())], vec![2.0]);
    }
    #[test]
    fn codes_and_queue_keys_do_not_alias_delimiter_collisions() {
        let p = ProviderKey::new(&table(1.0));
        let a = CodeKey {
            provider: p.clone(),
            columns: vec!["a\u{1}b".into(), "c".into()],
        };
        let b = CodeKey {
            provider: p.clone(),
            columns: vec!["a".into(), "b\u{1}c".into()],
        };
        assert_eq!(a.columns.join("\u{1}"), b.columns.join("\u{1}"));
        assert_ne!(a, b);
        let keys = HashSet::from([
            QueuedKey::Codes(a.clone()),
            QueuedKey::Codes(b),
            QueuedKey::Column(p, a.to_string()),
        ]);
        assert_eq!(keys.len(), 3);
    }
    #[test]
    fn exact_version_bytes_and_full_file_lists_are_part_of_equality() {
        let a = ProviderKey::Version {
            identity: vec![1, 2],
            files: Some(vec!["one.parquet".into()]),
        };
        let b = ProviderKey::Version {
            identity: vec![1, 2],
            files: Some(vec!["two.parquet".into()]),
        };
        let c = ProviderKey::Version {
            identity: vec![1, 3],
            files: Some(vec!["one.parquet".into()]),
        };
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(HashSet::from([a, b, c]).len(), 3);
    }
    #[test]
    fn strong_owner_releases_after_rows_mirrors_queued_jobs_and_keys_are_removed() {
        let provider = table(1.0);
        let weak = Arc::downgrade(&provider);
        let key = ProviderKey::new(&provider);
        let codes = CodeKey {
            provider: key.clone(),
            columns: vec!["g".into()],
        };
        let (sender, rx) = std::sync::mpsc::channel();
        let engine = GpuEngine {
            sender,
            resident: Mutex::new(HashSet::from([(key.clone(), "v".into())])),
            codes: Mutex::new(HashMap::from([(codes.clone(), 1)])),
            queued: Mutex::new(HashSet::from([
                QueuedKey::Column(key.clone(), "v".into()),
                QueuedKey::Codes(codes.clone()),
            ])),
            resident_bytes: AtomicUsize::new(9),
            eviction_count: AtomicU64::new(0),
            upload_failures: AtomicU64::new(0),
            run_fallbacks: AtomicU64::new(0),
        };
        let mut cache = GpuCache::new();
        cache.rows.insert(key.clone(), 1);
        engine
            .sender
            .send(Job::Upload {
                pid: key.clone(),
                col: "v".into(),
                provider: provider.clone(),
            })
            .unwrap();
        drop(provider);
        drop(codes);
        drop(key);
        assert!(weak.upgrade().is_some());
        // Same cleanup used on worker shutdown/panic, after cache allocations drop.
        engine.clear_residency_metadata();
        assert!(weak.upgrade().is_some());
        drop(rx.try_recv().unwrap());
        assert!(
            weak.upgrade().is_some(),
            "row metadata still pins exact owner"
        );
        // Same helper called by both last-column and last-codes eviction. No
        // live device buffers exist in this no-CUDA metadata-only fixture.
        let key = cache.rows.keys().next().unwrap().clone();
        cache.forget_rows_if_unused(&key);
        drop(key);
        assert!(cache.rows.is_empty());
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn resident_memory_planning_is_positive_but_mixed_remains_declined() {
        let provider = table(1.0);
        let mut context = crate::ExecutionContext::new();
        context.register_table_provider("t", provider.clone());
        let logical = context.logical_plan("SELECT SUM(v) FROM t").unwrap();
        fn find(plan: &crate::planner::LogicalPlan) -> &crate::planner::AggregateNode {
            match plan {
                crate::planner::LogicalPlan::Aggregate(n) => n,
                _ => find(plan.children()[0]),
            }
        }
        let tables = HashMap::from([("t".into(), provider)]);
        // Bypass only the process configuration check, not provider eligibility:
        // this pure test targets the same admitted construction helper's body.
        assert!(
            plan_gpu_agg_impl(find(&logical), &tables, true).is_some(),
            "resident planning shape must admit immutable MemoryTable"
        );
        assert!(plan_gpu_agg_impl(find(&logical), &tables, false).is_none());
    }
}

#[cfg(test)]
mod resident_scalar_edge_hardware_contracts {
    use super::*;
    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    fn context(values: Vec<f64>) -> crate::ExecutionContext {
        let mut ctx = crate::ExecutionContext::with_memory_limit(64 << 20);
        ctx.enable_gpu_offload();
        ctx.register_batch(
            "edge_values",
            RecordBatch::try_from_iter([("v", Arc::new(Float64Array::from(values)) as ArrayRef)])
                .unwrap(),
        );
        ctx
    }
    fn device(outcome: GpuResidentQueryOutcome) -> crate::execution::QueryResult {
        assert_eq!(
            (
                outcome.evidence.matched_operators,
                outcome.evidence.attempted_device_runs,
                outcome.evidence.completed_device_runs,
                outcome.evidence.failures
            ),
            (1, 1, 1, 0)
        );
        outcome.result.expect("actual resident device result")
    }
    fn scalar_empty(result: crate::execution::QueryResult) {
        assert_eq!(
            result.row_count, 1,
            "scalar aggregation must preserve its single output row even without matching inputs"
        );
        let batch = result
            .batches
            .iter()
            .find(|b| b.num_rows() != 0)
            .expect("scalar row");
        assert_eq!(batch.num_columns(), 5);
        let sum = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("SUM Float64");
        let count = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("COUNT Int64");
        assert!(sum.is_null(0), "empty SUM must be typed NULL");
        assert!(!count.is_null(0));
        assert_eq!(count.value(0), 0);
        for index in 2..5 {
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("MIN/MAX/AVG Float64");
            assert!(values.is_null(0), "empty MIN/MAX/AVG must be typed NULL");
        }
    }
    #[tokio::test]
    #[ignore = "requires actual CUDA; run alone under capped wrapper"]
    async fn all_filtered_scalar_sum_count_retains_null_and_zero_row_on_device() {
        GpuEngine::get().expect("actual GPU required; no skip or CPU fallback");
        let ctx = context(vec![1.0, 2.0, 4.0]);
        let sql = "SELECT SUM(v) AS s, COUNT(v) AS n, MIN(v) AS lo, MAX(v) AS hi, AVG(v) AS av FROM edge_values WHERE v < 0";
        let session = ctx
            .prepare_gpu_resident(sql, TIMEOUT)
            .await
            .expect("supported nonempty Float64 upload, all-filtered scalar plan");
        scalar_empty(device(ctx.sql_gpu_resident(sql, &session).await));
    }
    #[tokio::test]
    #[ignore = "requires actual CUDA; run alone under capped wrapper"]
    async fn empty_scalar_sum_count_retains_null_and_zero_row_on_device() {
        GpuEngine::get().expect("actual GPU required; no skip or CPU fallback");
        let ctx = context(vec![]);
        let sql = "SELECT SUM(v) AS s, COUNT(v) AS n, MIN(v) AS lo, MAX(v) AS hi, AVG(v) AS av FROM edge_values";
        let session=ctx.prepare_gpu_resident(sql,TIMEOUT).await.expect("empty scalar resident domain must prepare or be deliberately classified unsupported, never emit incorrect empty output");
        scalar_empty(device(ctx.sql_gpu_resident(sql, &session).await));
    }
    #[tokio::test]
    #[ignore = "requires actual CUDA; run alone under capped wrapper"]
    async fn all_nan_scalar_min_max_must_not_become_infinities_on_device() {
        GpuEngine::get().expect("actual GPU required; no skip or CPU fallback");
        let ctx = context(vec![
            f64::from_bits(0x7ff8000000000001),
            f64::from_bits(0x7ff8000000000002),
        ]);
        let sql = "SELECT MIN(v) AS lo, MAX(v) AS hi FROM edge_values";
        let error = match ctx.prepare_gpu_resident(sql, TIMEOUT).await {
            Err(error) => error,
            Ok(_) => panic!("nonfinite upload must be explicitly refused"),
        };
        assert!(
            error
                .to_string()
                .contains("unsupported physical dependency domain"),
            "{error}"
        );
    }
    #[test]
    fn nonfinite_numeric_uploads_decline_and_finite_extremes_remain_supported() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let batch = RecordBatch::try_from_iter([(
                "v",
                Arc::new(Float64Array::from(vec![value])) as ArrayRef,
            )])
            .unwrap();
            let provider: Arc<dyn TableProvider> = Arc::new(
                crate::physical::operators::MemoryTable::new(batch.schema(), vec![batch]),
            );
            assert!(load_column_f64(&provider, "v").unwrap().is_none());
        }
        let values = vec![-f64::MAX, -0.0, 0.0, f64::MAX];
        let batch = RecordBatch::try_from_iter([(
            "v",
            Arc::new(Float64Array::from(values.clone())) as ArrayRef,
        )])
        .unwrap();
        let provider: Arc<dyn TableProvider> = Arc::new(
            crate::physical::operators::MemoryTable::new(batch.schema(), vec![batch]),
        );
        let loaded = load_column_f64(&provider, "v").unwrap().unwrap();
        assert_eq!(
            loaded.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
        );
    }
    #[tokio::test]
    #[ignore = "requires actual CUDA; run alone under capped wrapper"]
    async fn all_filtered_grouped_aggregate_stays_zero_rows_on_device() {
        GpuEngine::get().expect("actual GPU required");
        let mut ctx = crate::ExecutionContext::with_memory_limit(64 << 20);
        ctx.enable_gpu_offload();
        ctx.register_batch(
            "edge_values",
            RecordBatch::try_from_iter([
                ("g", Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef),
                (
                    "v",
                    Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
                ),
            ])
            .unwrap(),
        );
        let sql = "SELECT g, SUM(v) AS s, COUNT(v) AS n FROM edge_values WHERE v < 0 GROUP BY g";
        let session = ctx.prepare_gpu_resident(sql, TIMEOUT).await.unwrap();
        assert_eq!(
            device(ctx.sql_gpu_resident(sql, &session).await).row_count,
            0
        );
    }
}

fn replacement_total(current: usize, replaced: usize, new: usize) -> Result<usize> {
    current
        .checked_sub(replaced)
        .and_then(|remaining| remaining.checked_add(new))
        .ok_or_else(|| {
            QueryError::Execution(
                "gpu: inconsistent/overflowing replacement byte accounting".into(),
            )
        })
}
impl GpuEngine {
    fn adjust_resident_bytes(&self, replaced: usize, new: usize) {
        // Cache checked the full resulting total before publishing replacement.
        // Mirror the delta atomically, rather than adding the whole new buffer.
        if new >= replaced {
            self.resident_bytes
                .fetch_add(new - replaced, Ordering::Relaxed);
        } else {
            self.resident_bytes
                .fetch_sub(replaced - new, Ordering::Relaxed);
        }
    }
}

// Numeric upload exactness does not prove exact SUM: individually representable
// integers can accumulate past 2^53. Only an explicitly floating SUM result uses
// this floating reduction. Keep this check shared by planning and worker entry.
fn supported_aggregate_domain(aggregates: &[GpuAgg], schema: &SchemaRef) -> bool {
    let Some(first_aggregate) = schema.fields().len().checked_sub(aggregates.len()) else {
        return false;
    };
    supported_extrema(aggregates)
        && aggregates.iter().enumerate().all(|(index, aggregate)| {
            !matches!(aggregate, GpuAgg::Sum(_))
                || schema.field(first_aggregate + index).data_type() == &DataType::Float64
        })
}

fn supported_extrema(aggregates: &[GpuAgg]) -> bool {
    aggregates.iter().all(|aggregate| match aggregate {
        GpuAgg::Min(input) | GpuAgg::Max(input) => matches!(input, GpuInput::Col(_)),
        _ => true,
    })
}

#[cfg(test)]
mod duplicate_gpu_upload_contract_tests {
    use super::*;
    #[test]
    fn replacement_bytes_remove_old_entry_and_check_before_mutating() {
        assert_eq!(replacement_total(100, 40, 25).unwrap(), 85);
        assert_eq!(replacement_total(100, 40, 60).unwrap(), 120);
        assert_eq!(replacement_total(100, 40, 40).unwrap(), 100);
        assert_eq!(replacement_total(100, 0, 25).unwrap(), 125);
        assert!(replacement_total(10, 11, 0).is_err());
        assert!(replacement_total(usize::MAX, 0, 1).is_err());
    }
    #[test]
    fn computed_extrema_decline_without_disabling_sum_avg_fusion() {
        let fused = GpuInput::MulOneMinusOnePlus("a".into(), "b".into(), "c".into());
        // All source values finite; the existing left-associated fused formula
        // can produce infinity*zero, so source-domain validation alone fails.
        let a = f64::MAX;
        let b = -f64::MAX;
        let c = -1.0_f64;
        assert!(a.is_finite() && b.is_finite() && c.is_finite());
        assert!((a * (1.0 - b) * (1.0 + c)).is_nan());
        assert!(!supported_extrema(&[GpuAgg::Min(fused.clone())]));
        assert!(!supported_extrema(&[GpuAgg::Max(fused.clone())]));
        assert!(supported_extrema(&[
            GpuAgg::Sum(fused.clone()),
            GpuAgg::Avg(fused)
        ]));
        assert!(supported_extrema(&[
            GpuAgg::Min(GpuInput::Col("a".into())),
            GpuAgg::Max(GpuInput::Col("b".into()))
        ]));
    }
    #[tokio::test]
    #[ignore = "requires actual CUDA; run alone under capped wrapper"]
    async fn delayed_duplicate_column_and_codes_jobs_do_not_change_resident_bytes() {
        let engine = GpuEngine::get().expect("actual GPU required");
        let mut ctx = crate::ExecutionContext::with_memory_limit(64 << 20);
        ctx.enable_gpu_offload();
        ctx.register_batch(
            "duplicates",
            RecordBatch::try_from_iter([
                (
                    "g",
                    Arc::new(StringArray::from(vec!["a", "b", "a", "b"])) as ArrayRef,
                ),
                (
                    "v",
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 4.0, 8.0])) as ArrayRef,
                ),
            ])
            .unwrap(),
        );
        let sql = "SELECT g, SUM(v) AS s FROM duplicates GROUP BY g ORDER BY g";
        let timeout = std::time::Duration::from_secs(30);
        let first = ctx.prepare_gpu_resident(sql, timeout).await.unwrap();
        let plan = first.plan.clone();
        let before = engine.snapshot();
        drop(first);
        // Model jobs enqueued while the dependency was absent, but processed
        // after preparation+release. No wall-clock race or sleep is required.
        engine
            .sender
            .send(Job::Upload {
                pid: plan.pid(),
                col: "v".into(),
                provider: plan.provider.clone(),
            })
            .unwrap();
        engine
            .sender
            .send(Job::BuildCodes {
                pid: plan.pid(),
                key: plan.codes_key().unwrap(),
                cols: plan.group_cols.clone(),
                provider: plan.provider.clone(),
            })
            .unwrap();
        // FIFO Prepare acknowledgment observes both queued jobs completed.
        let second = ctx.prepare_gpu_resident(sql, timeout).await.unwrap();
        let after = engine.snapshot();
        assert_eq!(after.resident_bytes, before.resident_bytes);
        assert_eq!(after.resident_columns, before.resident_columns);
        assert_eq!(after.eviction_count, before.eviction_count);
        assert_eq!(after.upload_failures, before.upload_failures);
        let outcome = ctx.sql_gpu_resident(sql, &second).await;
        assert_eq!(
            (
                outcome.evidence.completed_device_runs,
                outcome.evidence.failures
            ),
            (1, 0)
        );
        let result = outcome.result.unwrap();
        assert_eq!(result.row_count, 2);
        let mut rows = Vec::new();
        for batch in result.batches {
            let g = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let v = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((g.value(i).to_owned(), v.value(i)));
            }
        }
        assert_eq!(rows, vec![("a".into(), 5.0), ("b".into(), 10.0)]);
    }
}

#[cfg(test)]
mod sum_output_domain_contract_tests {
    use super::*;
    fn schema(types: &[DataType]) -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(
            types
                .iter()
                .enumerate()
                .map(|(i, t)| arrow::datatypes::Field::new(format!("f{i}"), t.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }
    fn col() -> GpuInput {
        GpuInput::Col("v".into())
    }
    #[test]
    fn sum_requires_exact_float64_output_not_individually_exact_integer_inputs() {
        let values = [1_i64 << 52, 1, 1_i64 << 52];
        let expected: i128 = values.iter().map(|v| i128::from(*v)).sum();
        assert_eq!(expected, 9_007_199_254_740_993);
        assert_ne!(
            values.iter().map(|v| *v as f64).sum::<f64>() as i128,
            expected
        );
        for ty in [
            DataType::Int64,
            DataType::Decimal128(38, 0),
            DataType::Float32,
        ] {
            assert!(!supported_aggregate_domain(
                &[GpuAgg::Sum(col())],
                &schema(&[ty])
            ));
        }
        assert!(supported_aggregate_domain(
            &[GpuAgg::Sum(col())],
            &schema(&[DataType::Float64])
        ));
    }
    #[test]
    fn domain_uses_aggregate_field_offset_and_keeps_count_and_direct_extrema() {
        let aggs = [
            GpuAgg::Count(col()),
            GpuAgg::Sum(col()),
            GpuAgg::Min(col()),
            GpuAgg::Max(col()),
        ];
        assert!(supported_aggregate_domain(
            &aggs,
            &schema(&[
                DataType::Utf8,
                DataType::Int64,
                DataType::Float64,
                DataType::Int64,
                DataType::Int64
            ])
        ));
        assert!(!supported_aggregate_domain(
            &aggs,
            &schema(&[
                DataType::Utf8,
                DataType::Float64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64
            ])
        ));
        assert!(!supported_aggregate_domain(
            &aggs,
            &schema(&[DataType::Float64])
        ));
        assert!(!supported_aggregate_domain(
            &[GpuAgg::Sum(col())],
            &schema(&[])
        ));
    }
    #[tokio::test]
    #[ignore = "requires actual CUDA; run alone under capped wrapper"]
    async fn resident_integer_sum_refuses_before_inexact_device_reduction() {
        GpuEngine::get().expect("actual GPU required; no skip or fallback");
        let mut ctx = crate::ExecutionContext::with_memory_limit(64 << 20);
        ctx.enable_gpu_offload();
        ctx.register_batch(
            "integer_sum",
            RecordBatch::try_from_iter([(
                "v",
                Arc::new(Int64Array::from(vec![1_i64 << 52, 1, 1_i64 << 52])) as ArrayRef,
            )])
            .unwrap(),
        );
        match ctx
            .prepare_gpu_resident(
                "SELECT SUM(v) FROM integer_sum",
                std::time::Duration::from_secs(30),
            )
            .await
        {
            Err(error) => assert!(
                error
                    .to_string()
                    .contains("requires exactly one GPU operator"),
                "{error}"
            ),
            Ok(_) => panic!("Int64 SUM must refuse; exact oracle is 9007199254740993"),
        }
    }
}

#[cfg(test)]
mod resident_count_star_contract {
    use super::*;
    #[test]
    fn wildcard_is_only_admitted_as_nondistinct_count_argument() {
        let aggregate = |func, distinct| Expr::Aggregate {
            func,
            args: vec![Expr::Wildcard],
            distinct,
        };
        assert!(resident_expr(&aggregate(AggregateFunction::Count, false)));
        assert!(!resident_expr(&aggregate(AggregateFunction::Count, true)));
        assert!(!resident_expr(&aggregate(AggregateFunction::Sum, false)));
        assert!(!resident_expr(&Expr::Wildcard));
    }
    #[tokio::test]
    #[ignore = "requires actual CUDA; run alone under capped wrapper"]
    async fn resident_count_star_with_sum_prepares_and_executes_on_device() {
        GpuEngine::get().expect("actual GPU required");
        let mut ctx = crate::ExecutionContext::with_memory_limit(64 << 20);
        ctx.enable_gpu_offload();
        ctx.register_batch(
            "count_star_values",
            RecordBatch::try_from_iter([(
                "v",
                Arc::new(Float64Array::from(vec![1.0, 2.0, 4.0])) as ArrayRef,
            )])
            .unwrap(),
        );
        let sql = "SELECT SUM(v) AS s, COUNT(*) AS n FROM count_star_values";
        let session = ctx
            .prepare_gpu_resident(sql, std::time::Duration::from_secs(30))
            .await
            .unwrap();
        let outcome = ctx.sql_gpu_resident(sql, &session).await;
        assert_eq!(
            (
                outcome.evidence.matched_operators,
                outcome.evidence.attempted_device_runs,
                outcome.evidence.completed_device_runs,
                outcome.evidence.failures
            ),
            (1, 1, 1, 0)
        );
        let result = outcome.result.unwrap();
        assert_eq!(result.row_count, 1);
        let batch = result.batches.iter().find(|b| b.num_rows() == 1).unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            7.0
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
    }
}
