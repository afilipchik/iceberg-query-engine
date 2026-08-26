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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
#[derive(Clone, Debug)]
pub enum GpuAgg {
    Sum(GpuInput),
    Min(GpuInput),
    Max(GpuInput),
    Count(GpuInput),
    Avg(GpuInput),
}

/// Everything the wrapper needs to try the GPU, or to arrange for it to be
/// possible next time.
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

    /// Cache identity: derived from `TableProvider::identity()` (default:
    /// a hash of the provider's PARQUET FILE LIST — see that method's doc
    /// comment in `src/physical/operators/scan.rs`; unchanged behavior for
    /// every parquet-backed provider). Table names collide across contexts
    /// (tests, shards, re-registration) and raw provider pointers can be
    /// reallocated at the same address (ABA); a stable data identity avoids
    /// both. Providers with no identity are never offloaded (`plan_gpu_agg`
    /// refuses them via the same `identity()` call).
    fn pid(&self) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        if let Some(id) = self.provider.identity() {
            id.hash(&mut h);
        }
        h.finish() as usize
    }

    fn codes_key(&self) -> Option<String> {
        if self.group_cols.is_empty() {
            None
        } else {
            Some(format!(
                "{:x}\u{1}{}",
                self.pid(),
                self.group_cols.join("\u{1}")
            ))
        }
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
    // Any provider with a stable, hashable identity (`TableProvider::
    // identity()`): today that's parquet-backed tables (file list, via the
    // trait's default) and, once a native-table provider opts in by
    // overriding `identity()` directly, native tables too (manifest
    // `table_id` + `snapshot.version`). A provider with no identity
    // (MemoryTable, a sharded/distributed provider, ...) is refused here —
    // `pid()` would collide for all of them and the resident cache would
    // alias unrelated datasets.
    provider.identity()?;

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
    Upload {
        pid: usize,
        col: String,
        provider: Arc<dyn TableProvider>,
    },
    BuildCodes {
        key: String,
        pid: usize,
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
    pid: usize,
    preds: Vec<GpuPred>,
    aggs: Vec<GpuAgg>,
    codes_key: Option<String>,
    schema: SchemaRef,
}

pub struct GpuEngine {
    sender: std::sync::mpsc::Sender<Job>,
    /// (provider identity, col) resident in VRAM.
    resident: Mutex<HashSet<(usize, String)>>,
    /// codes_key -> number of groups (resident code buffers).
    codes: Mutex<HashMap<String, usize>>,
    /// Upload requests already queued (dedup).
    queued: Mutex<HashSet<String>>,
    healthy: AtomicBool,
    /// Mirrors the worker thread's `GpuCache::total_bytes` (task 001's real
    /// byte accounting) for cheap, lock-free external reads — tests,
    /// diagnostics, and task 002's future observability work. The worker
    /// thread's own accounting is the source of truth used for actual
    /// eviction decisions; this is a same-step mirror, not a second
    /// independent count.
    resident_bytes: AtomicUsize,
    /// Total evictions performed since process start (task 001's LRU
    /// policy). Monotonically increasing; never reset.
    eviction_count: AtomicU64,
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
                        healthy: AtomicBool::new(true),
                        resident_bytes: AtomicUsize::new(0),
                        eviction_count: AtomicU64::new(0),
                    }),
                    _ => {
                        tracing::info!("gpu: no usable CUDA device/nvrtc; offload disabled");
                        None
                    }
                }
            })
            .as_ref()
    }

    fn is_resident(&self, pid: usize, col: &str) -> bool {
        self.resident
            .lock()
            .unwrap()
            .contains(&(pid, col.to_string()))
    }

    fn codes_groups(&self, key: &str) -> Option<usize> {
        self.codes.lock().unwrap().get(key).copied()
    }

    /// Queue whatever this plan needs that is not yet resident.
    pub fn request(&self, plan: &GpuAggPlan) {
        if !self.healthy.load(Ordering::Relaxed) {
            return;
        }
        let mut queued = self.queued.lock().unwrap();
        let pid = plan.pid();
        for col in plan.needed_columns() {
            let k = format!("{pid:x}\u{1}{col}");
            if !self.is_resident(pid, &col) && queued.insert(k) {
                let _ = self.sender.send(Job::Upload {
                    pid,
                    col,
                    provider: plan.provider.clone(),
                });
            }
        }
        if let Some(key) = plan.codes_key() {
            if self.codes_groups(&key).is_none() && queued.insert(key.clone()) {
                let _ = self.sender.send(Job::BuildCodes {
                    key,
                    pid: plan.pid(),
                    cols: plan.group_cols.clone(),
                    provider: plan.provider.clone(),
                });
            }
        }
    }

    /// Is everything resident so a run would succeed right now?
    pub fn ready(&self, plan: &GpuAggPlan) -> bool {
        if !self.healthy.load(Ordering::Relaxed) {
            return false;
        }
        let pid = plan.pid();
        let cols_ok = plan
            .needed_columns()
            .iter()
            .all(|c| self.is_resident(pid, c));
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

    fn mark_resident(pid: usize, col: &str, bytes: usize) {
        if let Some(e) = GpuEngine::get() {
            e.resident.lock().unwrap().insert((pid, col.to_string()));
            e.resident_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn mark_codes(key: &str, groups: usize, bytes: usize) {
        if let Some(e) = GpuEngine::get() {
            e.codes.lock().unwrap().insert(key.to_string(), groups);
            e.resident_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Undo `mark_resident`: called by `GpuCache::reserve` when LRU
    /// eviction drops a column buffer. Clears `resident` so `is_resident`/
    /// `ready` correctly report it gone, and decrements the byte mirror.
    fn mark_evicted_column(pid: usize, col: &str, bytes: usize) {
        if let Some(e) = GpuEngine::get() {
            e.resident.lock().unwrap().remove(&(pid, col.to_string()));
            e.resident_bytes.fetch_sub(bytes, Ordering::Relaxed);
            e.eviction_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Undo `mark_codes`: called by `GpuCache::reserve` when LRU eviction
    /// drops a group-codes buffer.
    fn mark_evicted_codes(key: &str, bytes: usize) {
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
    fn unmark_queued(key: &str) {
        if let Some(e) = GpuEngine::get() {
            e.queued.lock().unwrap().remove(key);
        }
    }

    fn mark_unhealthy() {
        if let Some(e) = GpuEngine::get() {
            e.healthy.store(false, Ordering::Relaxed);
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
    columns: HashMap<(usize, String), ColumnEntry>,
    code_bufs: HashMap<String, CodesEntry>,
    /// pid -> expected row count, used to detect and skip a column whose
    /// row count disagrees with a pid's other already-resident columns.
    rows: HashMap<usize, usize>,
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
                GpuEngine::mark_evicted_column(pid, &col, bytes);
                if !self.pid_in_use(pid) {
                    self.rows.remove(&pid);
                }
                tracing::info!(
                    "gpu: evicted column {col} (pid={pid:x}, {} MB) — over QE_GPU_CACHE_MB budget",
                    bytes / 1_000_000
                );
            } else {
                let (key, _, bytes) = oldest_codes.expect("checked Some above");
                self.code_bufs.remove(&key);
                self.total_bytes = self.total_bytes.saturating_sub(bytes);
                GpuEngine::mark_evicted_codes(&key, bytes);
                if let Some(pid) = pid_from_codes_key(&key) {
                    if !self.pid_in_use(pid) {
                        self.rows.remove(&pid);
                    }
                }
                tracing::info!(
                    "gpu: evicted group codes {key} ({} MB) — over QE_GPU_CACHE_MB budget",
                    bytes / 1_000_000
                );
            }
        }
    }

    fn insert_column(
        &mut self,
        pid: usize,
        col: String,
        buf: cudarc::driver::CudaSlice<f64>,
        bytes: usize,
    ) {
        let last_used = self.next_tick();
        self.total_bytes += bytes;
        self.columns.insert(
            (pid, col),
            ColumnEntry {
                buf,
                bytes,
                last_used,
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_codes(
        &mut self,
        key: String,
        buf: cudarc::driver::CudaSlice<u8>,
        labels: Vec<Vec<String>>,
        ngroups: usize,
        bytes: usize,
    ) {
        let last_used = self.next_tick();
        self.total_bytes += bytes;
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
    }

    /// Bump a resident column's LRU tick on use (a cache hit inside
    /// `run_on_device`). A no-op if the column is not resident (should not
    /// happen — `GpuEngine::ready` already checked — but never panics).
    fn touch_column(&mut self, pid: usize, col: &str) {
        let tick = self.next_tick();
        if let Some(e) = self.columns.get_mut(&(pid, col.to_string())) {
            e.last_used = tick;
        }
    }

    /// Bump a resident codes buffer's LRU tick on use.
    fn touch_codes(&mut self, key: &str) {
        let tick = self.next_tick();
        if let Some(e) = self.code_bufs.get_mut(key) {
            e.last_used = tick;
        }
    }

    /// True if any resident column or codes entry still references `pid` —
    /// codes keys embed the pid as a hex prefix (`GpuAggPlan::codes_key`).
    /// Used only to know when `rows[pid]` can finally be dropped too.
    fn pid_in_use(&self, pid: usize) -> bool {
        self.columns.keys().any(|(p, _)| *p == pid)
            || self
                .code_bufs
                .keys()
                .any(|k| pid_from_codes_key(k) == Some(pid))
    }
}

/// `GpuAggPlan::codes_key` formats as `"{pid:x}\u{1}{group_cols...}"` —
/// recover the pid prefix for `GpuCache::pid_in_use`'s cleanup check.
fn pid_from_codes_key(key: &str) -> Option<usize> {
    let hex = key.split('\u{1}').next()?;
    usize::from_str_radix(hex, 16).ok()
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
    let mut cache = GpuCache::new();

    while let Ok(job) = rx.recv() {
        match job {
            Job::Upload { pid, col, provider } => {
                match load_column_f64(&provider, &col) {
                    Ok(Some(values)) => {
                        let expect = cache.rows.entry(pid).or_insert(values.len());
                        if *expect != values.len() {
                            tracing::warn!("gpu: {col} row count mismatch; skipped");
                        } else {
                            let bytes = values.len() * std::mem::size_of::<f64>();
                            cache.reserve(bytes);
                            match stream.memcpy_stod(&values) {
                                Ok(buf) => {
                                    cache.insert_column(pid, col.clone(), buf, bytes);
                                    GpuEngine::mark_resident(pid, &col, bytes);
                                    tracing::info!("gpu: cached {col} ({} MB)", bytes / 1_000_000);
                                }
                                Err(e) => {
                                    tracing::warn!("gpu: upload {col} failed: {e}");
                                    GpuEngine::mark_unhealthy();
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::info!("gpu: {col} not cacheable (nulls/type); skipped")
                    }
                    Err(e) => tracing::warn!("gpu: scan {col} failed: {e}"),
                }
                // Always clear the dedup key, whatever happened above — see
                // `GpuEngine::unmark_queued`'s doc for why this must not be
                // skipped on any path (including the mismatch/failure ones).
                GpuEngine::unmark_queued(&format!("{pid:x}\u{1}{col}"));
            }
            Job::BuildCodes {
                key,
                pid,
                cols,
                provider,
            } => {
                match build_codes(&provider, &cols) {
                    Ok(Some((codes, labels))) => {
                        let expect = cache.rows.entry(pid).or_insert(codes.len());
                        if *expect != codes.len() {
                            tracing::warn!("gpu: group codes row mismatch; skipped");
                        } else {
                            let n = labels.len();
                            let bytes = codes.len(); // u8 codes: 1 byte/row
                            cache.reserve(bytes);
                            match stream.memcpy_stod(&codes) {
                                Ok(buf) => {
                                    cache.insert_codes(key.clone(), buf, labels, n, bytes);
                                    GpuEngine::mark_codes(&key, n, bytes);
                                    tracing::info!("gpu: cached group codes {key} ({n} groups)");
                                }
                                Err(e) => tracing::warn!("gpu: codes upload failed: {e}"),
                            }
                        }
                    }
                    Ok(None) => tracing::info!("gpu: group of {cols:?} not codeable; skipped"),
                    Err(e) => tracing::warn!("gpu: group scan failed: {e}"),
                }
                GpuEngine::unmark_queued(&key);
            }
            Job::Run { spec, reply } => {
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
        use cudarc::driver::{DevicePtr, LaunchConfig, PushKernelArg};
        let gpu_err = |e: cudarc::driver::DriverError| {
            QueryError::Execution(format!("gpu launch failed: {e}"))
        };

        // Touch phase first (task 001's LRU): every column/codes buffer this
        // run actually uses counts as "just used," in a separate pass so the
        // borrow below can hand out plain immutable refs without conflicting
        // with these `&mut self` calls.
        for c in &spec.columns {
            cache.touch_column(spec.pid, c);
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
                .get(&(spec.pid, c.clone()))
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

        // Groups with zero surviving rows are absent, like the CPU aggregate.
        let present: Vec<usize> = (0..ngroups)
            .filter(|g| merged[g * nslot + presence] > 0.0)
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
            let vals: Vec<f64> = present
                .iter()
                .map(|g| {
                    let v = merged[g * nslot + main];
                    match avg_cnt {
                        Some(cs) => {
                            let c = merged[g * nslot + cs];
                            if c > 0.0 {
                                v / c
                            } else {
                                f64::NAN
                            }
                        }
                        None => v,
                    }
                })
                .collect();
            let arr: ArrayRef = match field.data_type() {
                DataType::Int64 => {
                    Arc::new(Int64Array::from_iter_values(vals.iter().map(|v| *v as i64)))
                }
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
                    if v.abs() > (1i64 << 52) {
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
}

impl GpuAggExec {
    pub fn new(plan: GpuAggPlan, inner: Arc<dyn PhysicalOperator>) -> Self {
        Self {
            plan: Arc::new(plan),
            inner,
            decision: OnceLock::new(),
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
        let use_gpu = *self.decision.get_or_init(|| match GpuEngine::get() {
            Some(engine) => {
                let ready = engine.ready(&self.plan);
                if !ready {
                    engine.request(&self.plan);
                }
                ready
            }
            None => false,
        });
        if use_gpu {
            if partition == 0 {
                match GpuEngine::get().expect("decided").run(&self.plan).await {
                    Ok(batch) => {
                        tracing::debug!("gpu: served {} on device", self.plan.table);
                        return Ok(Box::pin(futures::stream::iter(vec![Ok(batch)])));
                    }
                    Err(e) => {
                        tracing::warn!("gpu: run failed, falling back: {e}");
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
    fn pid_from_codes_key_round_trips_through_codes_key_format() {
        // Mirrors `GpuAggPlan::codes_key`'s exact format:
        // format!("{:x}\u{1}{}", pid, group_cols.join("\u{1}"))
        let key = format!(
            "{:x}\u{1}{}",
            0xdeadbeefusize, "l_returnflag\u{1}l_linestatus"
        );
        assert_eq!(pid_from_codes_key(&key), Some(0xdeadbeef));
    }

    #[test]
    fn pid_from_codes_key_rejects_garbage() {
        assert_eq!(pid_from_codes_key("not-hex\u{1}col"), None);
        assert_eq!(pid_from_codes_key(""), None);
    }

    /// `GpuCache::reserve`/`insert_column`/eviction touch real
    /// `cudarc::driver::CudaSlice` handles and so cannot be unit-tested
    /// without a live CUDA device — that mechanism is instead validated
    /// end-to-end, on real hardware, by `tests/gpu_cache_tests.rs` (byte
    /// accounting, budget enforcement, LRU order, re-upload-after-eviction
    /// correctness, and the mutation-driven leak fix, all against a real
    /// RTX 5090) and `examples/gpu_cache_tiering_check.rs` (the real
    /// before/after VRAM measurement this task's own acceptance criteria
    /// require). This module's test list is deliberately narrow: only the
    /// pure, hardware-independent logic lives here.
    #[test]
    fn cache_new_starts_empty() {
        let cache = GpuCache::new();
        assert_eq!(cache.total_bytes, 0);
        assert!(cache.columns.is_empty());
        assert!(cache.code_bufs.is_empty());
        assert!(cache.rows.is_empty());
    }
}
