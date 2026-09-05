//! In-memory query log for `serve` nodes (query-ui epic, task 002).
//!
//! Every statement that reaches `execute_statement` — HTTP `/sql` and Arrow
//! Flight `DoGet` — and every `/fragment` a worker runs is recorded here: a
//! record is inserted when the statement STARTS (so a hung query is visible
//! while it hangs) and completed when it ends. The log is a bounded ring;
//! the oldest *finished* record evicts first so a running query is never
//! dropped mid-flight. Nothing here touches tables or blocks execution: the
//! mutex is held for a push or an in-place update, and every string the
//! record keeps was already built by the engine (plans, SQL, error).
//!
//! `/queries`, `/queries/{id}` and `/stats` are views over this ring; the
//! embedded UI is a client of those three endpoints and nothing else.

use chrono::{SecondsFormat, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default ring capacity (`--query-log-size`).
pub const DEFAULT_QUERY_LOG_SIZE: usize = 1000;
/// Smallest ring the flag accepts; below this the log is not a history.
pub const MIN_QUERY_LOG_SIZE: usize = 10;
/// Longest SQL text kept per record. `/sql` bodies are capped at 1MB; keeping
/// that much per record would let the ring reach ~1GB.
pub const SQL_CAP_BYTES: usize = 64 * 1024;
/// Characters of SQL shown in list rows.
pub const PREVIEW_CHARS: usize = 200;
/// Minutes covered by `StatsView::per_minute`.
pub const PER_MINUTE_WINDOW: u64 = 60;
/// Entries in `StatsView::slowest`.
pub const SLOWEST_N: usize = 5;

/// Which door a statement came through.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FrontDoor {
    /// `POST /sql`.
    Http,
    /// Arrow Flight `DoGet`.
    Flight,
    /// `POST /fragment` — this node ran one shard of another node's query.
    Fragment,
}

impl FrontDoor {
    pub fn as_str(self) -> &'static str {
        match self {
            FrontDoor::Http => "http",
            FrontDoor::Flight => "flight",
            FrontDoor::Fragment => "fragment",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "http" => Some(FrontDoor::Http),
            "flight" => Some(FrontDoor::Flight),
            "fragment" => Some(FrontDoor::Fragment),
            _ => None,
        }
    }
}

/// Lifecycle state of a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryState {
    Running,
    Finished,
    Failed,
}

impl QueryState {
    pub fn as_str(self) -> &'static str {
        match self {
            QueryState::Running => "running",
            QueryState::Finished => "finished",
            QueryState::Failed => "failed",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(QueryState::Running),
            "finished" => Some(QueryState::Finished),
            "failed" => Some(QueryState::Failed),
            _ => None,
        }
    }
}

/// Coarse statement class, from the first keyword. Computed by the log rather
/// than the engine so failed and refused statements are classified too.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StatementKind {
    Select,
    Ddl,
    Dml,
    Other,
}

impl StatementKind {
    pub fn classify(sql: &str) -> Self {
        let first = sql
            .trim_start()
            .split(|c: char| c.is_whitespace() || c == '(')
            .find(|w| !w.is_empty())
            .unwrap_or("")
            .to_ascii_uppercase();
        match first.as_str() {
            "SELECT" | "WITH" | "VALUES" | "EXPLAIN" | "SHOW" | "DESCRIBE" => StatementKind::Select,
            "CREATE" | "DROP" | "ALTER" | "REFRESH" => StatementKind::Ddl,
            "INSERT" | "UPDATE" | "DELETE" | "MERGE" => StatementKind::Dml,
            _ => StatementKind::Other,
        }
    }
}

/// Where a statement came from.
#[derive(Clone, Debug)]
pub struct QueryOrigin {
    pub front_door: FrontDoor,
    /// `ip:port` of the client, when the transport knows it.
    pub client_addr: Option<String>,
}

/// Spill facts copied out of `SpillMetrics`.
#[derive(Clone, Debug, Serialize)]
pub struct SpillFacts {
    pub bytes: usize,
    pub partitions: usize,
    pub files: usize,
    pub read_back_ms: f64,
}

/// A failure: the `QueryError` variant name and its message, verbatim.
#[derive(Clone, Debug, Serialize)]
pub struct ErrorFacts {
    pub kind: String,
    pub message: String,
}

/// The shard a fragment record executed.
#[derive(Clone, Debug, Serialize)]
pub struct ShardFacts {
    pub table: String,
    pub index: usize,
    pub count: usize,
}

/// Everything the node knows about one statement. Serialized whole by
/// `/queries/{id}`; `/queries` sends [`QuerySummary`] instead.
#[derive(Clone, Debug, Serialize)]
pub struct QueryRecord {
    pub query_id: String,
    /// Monotonic per process; newest is largest. Orders the list.
    pub seq: u64,
    pub node_id: u64,
    pub front_door: FrontDoor,
    pub client_addr: Option<String>,
    pub state: QueryState,
    /// RFC 3339 with milliseconds, UTC.
    pub submitted_at: String,
    pub submitted_unix_ms: u64,
    pub finished_at: Option<String>,
    pub sql: String,
    pub sql_truncated: bool,
    pub statement_kind: StatementKind,
    /// `auto` | `force` | `off` (what the client asked for).
    pub requested_mode: &'static str,
    /// `arrow` | `json` | `csv` | `flight`, once the response was encoded.
    pub result_format: Option<String>,
    /// Wall time as measured by `execute_statement`.
    pub elapsed_ms: Option<f64>,
    pub parse_ms: Option<f64>,
    pub plan_ms: Option<f64>,
    pub optimize_ms: Option<f64>,
    pub execute_ms: Option<f64>,
    pub rows: Option<usize>,
    pub batches: Option<usize>,
    /// Encoded response body size, when the front door reports it.
    pub result_bytes: Option<usize>,
    /// Pool high-water mark over this query's window (see
    /// `QueryMetrics::peak_memory_bytes`).
    pub peak_memory_bytes: Option<usize>,
    pub memory_limit_bytes: Option<usize>,
    /// Other statements running on this node when this one began — the
    /// number that says how much to trust `peak_memory_bytes` as "mine".
    pub concurrent_at_start: usize,
    pub spill: Option<SpillFacts>,
    pub files_pruned_by_stats: usize,
    pub files_pruned_by_partition: usize,
    pub rollup_answered: Vec<String>,
    pub tables: Vec<String>,
    pub optimized_plan: Option<String>,
    pub physical_plan: Option<String>,
    pub distributed: bool,
    pub fallback_reason: Option<String>,
    /// The coordinator's full `Distribution` record when the query
    /// distributed (what `/sql` truncates into `x-qe-distribution`).
    pub distribution: Option<serde_json::Value>,
    /// For fragments: the initiator that sent the work.
    pub initiator: Option<String>,
    pub shard: Option<ShardFacts>,
    pub error: Option<ErrorFacts>,
}

/// One row of `/queries`: the record without plans, distribution and full SQL.
#[derive(Clone, Debug, Serialize)]
pub struct QuerySummary {
    pub query_id: String,
    pub seq: u64,
    pub node_id: u64,
    pub front_door: FrontDoor,
    pub client_addr: Option<String>,
    pub state: QueryState,
    pub submitted_at: String,
    pub submitted_unix_ms: u64,
    pub finished_at: Option<String>,
    pub sql: String,
    pub sql_truncated: bool,
    pub statement_kind: StatementKind,
    pub requested_mode: &'static str,
    pub result_format: Option<String>,
    pub elapsed_ms: Option<f64>,
    pub rows: Option<usize>,
    pub result_bytes: Option<usize>,
    pub peak_memory_bytes: Option<usize>,
    pub spilled_bytes: usize,
    pub distributed: bool,
    pub tables: Vec<String>,
    pub error_kind: Option<String>,
    pub error_message: Option<String>,
}

impl QueryRecord {
    fn summary(&self) -> QuerySummary {
        let (sql, cut) = preview(&self.sql, PREVIEW_CHARS);
        QuerySummary {
            query_id: self.query_id.clone(),
            seq: self.seq,
            node_id: self.node_id,
            front_door: self.front_door,
            client_addr: self.client_addr.clone(),
            state: self.state,
            submitted_at: self.submitted_at.clone(),
            submitted_unix_ms: self.submitted_unix_ms,
            finished_at: self.finished_at.clone(),
            sql,
            sql_truncated: cut || self.sql_truncated,
            statement_kind: self.statement_kind,
            requested_mode: self.requested_mode,
            result_format: self.result_format.clone(),
            elapsed_ms: self.elapsed_ms,
            rows: self.rows,
            result_bytes: self.result_bytes,
            peak_memory_bytes: self.peak_memory_bytes,
            spilled_bytes: self.spill.as_ref().map(|s| s.bytes).unwrap_or(0),
            distributed: self.distributed,
            tables: self.tables.clone(),
            error_kind: self.error.as_ref().map(|e| e.kind.clone()),
            error_message: self.error.as_ref().map(|e| e.message.clone()),
        }
    }

    fn matches(&self, filter: &ListFilter) -> bool {
        if let Some(s) = filter.state {
            if self.state != s {
                return false;
            }
        }
        if let Some(d) = filter.door {
            if self.front_door != d {
                return false;
            }
        }
        if let Some(q) = &filter.q {
            let q = q.to_lowercase();
            let in_sql = self.sql.to_lowercase().contains(&q);
            let in_err = self
                .error
                .as_ref()
                .map(|e| e.message.to_lowercase().contains(&q) || e.kind.to_lowercase() == q)
                .unwrap_or(false);
            let in_tables = self.tables.iter().any(|t| t.to_lowercase().contains(&q));
            let in_id = self.query_id.starts_with(&q);
            if !(in_sql || in_err || in_tables || in_id) {
                return false;
            }
        }
        true
    }
}

/// The engine-side facts of a completed statement, copied from
/// `QueryMetrics` by the caller (so this module does not depend on the
/// execution module's types).
#[derive(Clone, Debug, Default)]
pub struct MetricFacts {
    pub parse_ms: f64,
    pub plan_ms: f64,
    pub optimize_ms: f64,
    pub execute_ms: f64,
    pub batches: usize,
    pub peak_memory_bytes: usize,
    pub spill: Option<SpillFacts>,
    pub files_pruned_by_stats: usize,
    pub files_pruned_by_partition: usize,
    pub rollup_answered: Vec<String>,
    pub tables: Vec<String>,
    pub optimized_plan: Option<String>,
    pub physical_plan: Option<String>,
}

/// What `finish` records.
#[derive(Clone, Debug, Default)]
pub struct Completion {
    pub elapsed_ms: f64,
    pub rows: usize,
    pub metrics: MetricFacts,
    pub memory_limit_bytes: Option<usize>,
    pub distribution: Option<serde_json::Value>,
    pub fallback_reason: Option<String>,
}

/// `/queries` query-string parameters.
#[derive(Clone, Debug, Default)]
pub struct ListFilter {
    /// `None` = everything the ring holds.
    pub limit: Option<usize>,
    pub state: Option<QueryState>,
    pub door: Option<FrontDoor>,
    pub q: Option<String>,
}

impl ListFilter {
    /// Parse `limit=N|all&state=..&door=..&q=..`. Unknown keys are ignored;
    /// bad values are errors so a typo never silently returns "everything".
    pub fn parse(query: &str) -> std::result::Result<Self, String> {
        let mut f = ListFilter {
            limit: Some(100),
            ..Default::default()
        };
        for pair in query.split('&') {
            let Some((k, v)) = pair.split_once('=') else {
                continue;
            };
            let v = percent_decode(v);
            match k {
                "limit" => {
                    f.limit =
                        if v == "all" {
                            None
                        } else {
                            Some(v.parse::<usize>().map_err(|_| {
                                format!("limit must be a number or 'all', got {v:?}")
                            })?)
                        }
                }
                "state" => {
                    f.state = Some(QueryState::parse(&v).ok_or_else(|| {
                        format!("unknown state {v:?}; expected running, finished or failed")
                    })?)
                }
                "door" => {
                    f.door = Some(FrontDoor::parse(&v).ok_or_else(|| {
                        format!("unknown door {v:?}; expected http, flight or fragment")
                    })?)
                }
                "q" => {
                    if !v.is_empty() {
                        f.q = Some(v)
                    }
                }
                _ => {}
            }
        }
        Ok(f)
    }
}

/// `/queries` response.
#[derive(Debug, Serialize)]
pub struct ListView {
    pub node_id: u64,
    pub capacity: usize,
    /// Records in the ring before filtering.
    pub total: usize,
    /// Records matching the filter (before `limit`).
    pub matched: usize,
    pub queries: Vec<QuerySummary>,
}

#[derive(Debug, Default, Serialize)]
pub struct Counts {
    /// Records in the ring by state.
    pub total: usize,
    pub running: usize,
    pub finished: usize,
    pub failed: usize,
    /// Finished statements that distributed / ran locally (failures are in
    /// neither: they never reached a distribution decision).
    pub distributed: usize,
    pub local: usize,
    pub fragments: usize,
    /// Process-lifetime counters (survive eviction).
    pub lifetime_total: u64,
    pub lifetime_failed: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct Latency {
    pub samples: usize,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct MinuteBucket {
    pub minute_start: String,
    pub minute_unix: u64,
    pub count: usize,
    pub failed: usize,
    pub running: usize,
    pub p95_ms: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MemoryView {
    pub used: usize,
    pub peak: usize,
    pub max: usize,
    pub spilled_total: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterCounts {
    pub members: usize,
    pub up: usize,
    pub ready: bool,
}

#[derive(Debug, Serialize)]
pub struct Slowest {
    pub query_id: String,
    pub elapsed_ms: f64,
    pub state: QueryState,
    pub sql_preview: String,
}

/// `/stats` response, computed on request from the ring plus the counters
/// the caller passes in.
#[derive(Debug, Serialize)]
pub struct StatsView {
    pub node_id: u64,
    pub uptime_s: f64,
    pub generated_at: String,
    pub log_capacity: usize,
    pub queries: Counts,
    pub latency_ms: Latency,
    pub per_minute: Vec<MinuteBucket>,
    pub rows_total: u64,
    pub bytes_total: u64,
    pub spilled_bytes_total: u64,
    pub spill_queries: usize,
    pub memory: MemoryView,
    pub errors_by_kind: BTreeMap<String, u64>,
    pub tables: BTreeMap<String, u64>,
    pub slowest: Vec<Slowest>,
    pub cluster: ClusterCounts,
}

/// Counters the log cannot compute by itself; the server passes them in.
#[derive(Clone, Debug, Default)]
pub struct StatsInputs {
    pub uptime_s: f64,
    pub lifetime_total: u64,
    pub lifetime_failed: u64,
    pub memory: MemoryView,
    pub cluster: ClusterCounts,
}

struct Inner {
    ring: VecDeque<QueryRecord>,
    next_seq: u64,
}

/// The bounded ring. Cheap to share behind `NodeState`'s `Arc`.
pub struct QueryLog {
    node_id: u64,
    capacity: usize,
    inner: Mutex<Inner>,
}

impl QueryLog {
    pub fn new(node_id: u64, capacity: usize) -> Self {
        let capacity = capacity.max(MIN_QUERY_LOG_SIZE);
        Self {
            node_id,
            capacity,
            inner: Mutex::new(Inner {
                ring: VecDeque::with_capacity(capacity.min(4096)),
                next_seq: 1,
            }),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.inner.lock().ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert a `running` record and return its id.
    pub fn begin(&self, sql: &str, origin: &QueryOrigin, requested_mode: &'static str) -> String {
        self.insert(sql, origin, requested_mode, None, None)
    }

    /// Insert a `running` fragment record (worker side of a distributed query).
    pub fn begin_fragment(
        &self,
        sql: &str,
        initiator: Option<String>,
        shard: ShardFacts,
    ) -> String {
        let origin = QueryOrigin {
            front_door: FrontDoor::Fragment,
            client_addr: initiator.clone(),
        };
        self.insert(sql, &origin, "fragment", initiator, Some(shard))
    }

    fn insert(
        &self,
        sql: &str,
        origin: &QueryOrigin,
        requested_mode: &'static str,
        initiator: Option<String>,
        shard: Option<ShardFacts>,
    ) -> String {
        let query_id = uuid::Uuid::new_v4().to_string();
        let (sql_kept, sql_truncated) = cap_sql(sql);
        let now_ms = unix_ms();
        let mut g = self.inner.lock();
        let concurrent_at_start = g
            .ring
            .iter()
            .filter(|r| r.state == QueryState::Running)
            .count();
        let seq = g.next_seq;
        g.next_seq += 1;
        let record = QueryRecord {
            query_id: query_id.clone(),
            seq,
            node_id: self.node_id,
            front_door: origin.front_door,
            client_addr: origin.client_addr.clone(),
            state: QueryState::Running,
            submitted_at: rfc3339_now(),
            submitted_unix_ms: now_ms,
            finished_at: None,
            sql: sql_kept,
            sql_truncated,
            statement_kind: StatementKind::classify(sql),
            requested_mode,
            result_format: None,
            elapsed_ms: None,
            parse_ms: None,
            plan_ms: None,
            optimize_ms: None,
            execute_ms: None,
            rows: None,
            batches: None,
            result_bytes: None,
            peak_memory_bytes: None,
            memory_limit_bytes: None,
            concurrent_at_start,
            spill: None,
            files_pruned_by_stats: 0,
            files_pruned_by_partition: 0,
            rollup_answered: Vec::new(),
            tables: Vec::new(),
            optimized_plan: None,
            physical_plan: None,
            distributed: false,
            fallback_reason: None,
            distribution: None,
            initiator,
            shard,
            error: None,
        };
        // Evict the oldest FINISHED record. If every record is running (only
        // possible when concurrency exceeds the capacity), the ring overflows
        // by that many entries rather than forgetting a live query.
        if g.ring.len() >= self.capacity {
            if let Some(pos) = g.ring.iter().position(|r| r.state != QueryState::Running) {
                g.ring.remove(pos);
            }
        }
        g.ring.push_back(record);
        query_id
    }

    /// Mark `query_id` finished with the engine's facts. Unknown ids (evicted
    /// under extreme overflow) are ignored.
    pub fn finish(&self, query_id: &str, done: Completion) {
        let mut g = self.inner.lock();
        if let Some(r) = find_mut(&mut g.ring, query_id) {
            let m = done.metrics;
            r.state = QueryState::Finished;
            r.finished_at = Some(rfc3339_now());
            r.elapsed_ms = Some(done.elapsed_ms);
            r.parse_ms = Some(m.parse_ms);
            r.plan_ms = Some(m.plan_ms);
            r.optimize_ms = Some(m.optimize_ms);
            r.execute_ms = Some(m.execute_ms);
            r.rows = Some(done.rows);
            r.batches = Some(m.batches);
            r.peak_memory_bytes = Some(m.peak_memory_bytes);
            r.memory_limit_bytes = done.memory_limit_bytes;
            r.spill = m.spill;
            r.files_pruned_by_stats = m.files_pruned_by_stats;
            r.files_pruned_by_partition = m.files_pruned_by_partition;
            r.rollup_answered = m.rollup_answered;
            r.tables = m.tables;
            r.optimized_plan = m.optimized_plan;
            r.physical_plan = m.physical_plan;
            r.distributed = done.distribution.is_some();
            r.distribution = done.distribution;
            r.fallback_reason = done.fallback_reason;
        }
    }

    /// Mark `query_id` failed.
    pub fn fail(&self, query_id: &str, elapsed_ms: f64, kind: &str, message: &str) {
        let mut g = self.inner.lock();
        if let Some(r) = find_mut(&mut g.ring, query_id) {
            r.state = QueryState::Failed;
            r.finished_at = Some(rfc3339_now());
            r.elapsed_ms = Some(elapsed_ms);
            r.error = Some(ErrorFacts {
                kind: kind.to_string(),
                message: message.to_string(),
            });
        }
    }

    /// Record the encoded response size and format (known only after the
    /// front door serialized the result).
    pub fn set_result(&self, query_id: &str, result_bytes: usize, format: &str) {
        let mut g = self.inner.lock();
        if let Some(r) = find_mut(&mut g.ring, query_id) {
            r.result_bytes = Some(result_bytes);
            r.result_format = Some(format.to_string());
        }
    }

    /// Full record by id.
    pub fn get(&self, query_id: &str) -> Option<QueryRecord> {
        self.inner
            .lock()
            .ring
            .iter()
            .rev()
            .find(|r| r.query_id == query_id)
            .cloned()
    }

    /// Newest-first summaries matching `filter`.
    pub fn list(&self, filter: &ListFilter) -> ListView {
        let g = self.inner.lock();
        let total = g.ring.len();
        let mut matched = 0usize;
        let limit = filter.limit.unwrap_or(usize::MAX);
        let mut queries = Vec::with_capacity(limit.min(total));
        for r in g.ring.iter().rev() {
            if !r.matches(filter) {
                continue;
            }
            matched += 1;
            if queries.len() < limit {
                queries.push(r.summary());
            }
        }
        ListView {
            node_id: self.node_id,
            capacity: self.capacity,
            total,
            matched,
            queries,
        }
    }

    /// Statistics over the ring plus the caller's counters.
    pub fn stats(&self, inputs: StatsInputs) -> StatsView {
        let g = self.inner.lock();
        let now_ms = unix_ms();
        let mut counts = Counts {
            lifetime_total: inputs.lifetime_total,
            lifetime_failed: inputs.lifetime_failed,
            ..Default::default()
        };
        let mut elapsed: Vec<f64> = Vec::with_capacity(g.ring.len());
        let mut rows_total = 0u64;
        let mut bytes_total = 0u64;
        let mut spilled_bytes_total = 0u64;
        let mut spill_queries = 0usize;
        let mut errors_by_kind: BTreeMap<String, u64> = BTreeMap::new();
        let mut tables: BTreeMap<String, u64> = BTreeMap::new();
        let window_start = now_ms.saturating_sub(PER_MINUTE_WINDOW * 60_000);
        let first_minute = window_start / 60_000 + 1;
        let mut buckets: Vec<(usize, usize, usize, Vec<f64>)> = (0..PER_MINUTE_WINDOW)
            .map(|_| (0, 0, 0, Vec::new()))
            .collect();

        for r in g.ring.iter() {
            counts.total += 1;
            match r.state {
                QueryState::Running => counts.running += 1,
                QueryState::Finished => counts.finished += 1,
                QueryState::Failed => {
                    counts.failed += 1;
                    if let Some(e) = &r.error {
                        *errors_by_kind.entry(e.kind.clone()).or_insert(0) += 1;
                    }
                }
            }
            // `distributed` / `local` classify statements that COMPLETED
            // (a failure never got as far as a distribution decision).
            if r.front_door == FrontDoor::Fragment {
                counts.fragments += 1;
            } else if r.state == QueryState::Finished {
                if r.distributed {
                    counts.distributed += 1;
                } else {
                    counts.local += 1;
                }
            }
            if let Some(ms) = r.elapsed_ms {
                elapsed.push(ms);
            }
            rows_total += r.rows.unwrap_or(0) as u64;
            bytes_total += r.result_bytes.unwrap_or(0) as u64;
            if let Some(s) = &r.spill {
                if s.bytes > 0 {
                    spill_queries += 1;
                    spilled_bytes_total += s.bytes as u64;
                }
            }
            for t in &r.tables {
                *tables.entry(t.clone()).or_insert(0) += 1;
            }
            let minute = r.submitted_unix_ms / 60_000;
            if minute >= first_minute {
                let idx = (minute - first_minute) as usize;
                if idx < buckets.len() {
                    let b = &mut buckets[idx];
                    b.0 += 1;
                    if r.state == QueryState::Failed {
                        b.1 += 1;
                    }
                    if r.state == QueryState::Running {
                        b.2 += 1;
                    }
                    if let Some(ms) = r.elapsed_ms {
                        b.3.push(ms);
                    }
                }
            }
        }

        let latency = latency_of(&mut elapsed);
        let per_minute = buckets
            .into_iter()
            .enumerate()
            .map(|(i, (count, failed, running, mut ms))| {
                let minute_unix = (first_minute + i as u64) * 60;
                MinuteBucket {
                    minute_start: rfc3339_of_unix_s(minute_unix),
                    minute_unix,
                    count,
                    failed,
                    running,
                    p95_ms: percentile(&mut ms, 0.95),
                }
            })
            .collect();

        let mut slowest: Vec<&QueryRecord> =
            g.ring.iter().filter(|r| r.elapsed_ms.is_some()).collect();
        slowest.sort_by(|a, b| {
            b.elapsed_ms
                .partial_cmp(&a.elapsed_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let slowest = slowest
            .into_iter()
            .take(SLOWEST_N)
            .map(|r| Slowest {
                query_id: r.query_id.clone(),
                elapsed_ms: r.elapsed_ms.unwrap_or(0.0),
                state: r.state,
                sql_preview: preview(&r.sql, 120).0,
            })
            .collect();

        let mut memory = inputs.memory;
        memory.spilled_total = memory.spilled_total.max(spilled_bytes_total as usize);

        StatsView {
            node_id: self.node_id,
            uptime_s: inputs.uptime_s,
            generated_at: rfc3339_now(),
            log_capacity: self.capacity,
            queries: counts,
            latency_ms: latency,
            per_minute,
            rows_total,
            bytes_total,
            spilled_bytes_total,
            spill_queries,
            memory,
            errors_by_kind,
            tables,
            slowest,
            cluster: inputs.cluster,
        }
    }
}

fn find_mut<'a>(ring: &'a mut VecDeque<QueryRecord>, id: &str) -> Option<&'a mut QueryRecord> {
    ring.iter_mut().rev().find(|r| r.query_id == id)
}

fn latency_of(samples: &mut [f64]) -> Latency {
    if samples.is_empty() {
        return Latency::default();
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = samples.len();
    Latency {
        samples: n,
        p50_ms: percentile(samples, 0.50),
        p95_ms: percentile(samples, 0.95),
        p99_ms: percentile(samples, 0.99),
        max_ms: samples[n - 1],
        mean_ms: samples.iter().sum::<f64>() / n as f64,
    }
}

/// Nearest-rank percentile; sorts in place. 0.0 for no samples.
pub fn percentile(sorted: &mut [f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn cap_sql(sql: &str) -> (String, bool) {
    if sql.len() <= SQL_CAP_BYTES {
        return (sql.to_string(), false);
    }
    let mut cut = SQL_CAP_BYTES;
    while !sql.is_char_boundary(cut) {
        cut -= 1;
    }
    (format!("{}\n…[truncated]", &sql[..cut]), true)
}

/// First `chars` characters, whitespace collapsed. Returns whether it cut.
fn preview(sql: &str, chars: usize) -> (String, bool) {
    let collapsed: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = collapsed.chars().take(chars).collect();
    let cut = collapsed.chars().count() > chars;
    if cut {
        out.push('…');
    }
    (out, cut)
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn rfc3339_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn rfc3339_of_unix_s(s: u64) -> String {
    chrono::DateTime::<Utc>::from_timestamp(s as i64, 0)
        .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_default()
}

/// Minimal `%XX` + `+` decoding for query-string values.
fn percent_decode(v: &str) -> String {
    let bytes = v.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin() -> QueryOrigin {
        QueryOrigin {
            front_door: FrontDoor::Http,
            client_addr: Some("127.0.0.1:1".into()),
        }
    }

    fn finish_ok(log: &QueryLog, id: &str, ms: f64) {
        log.finish(
            id,
            Completion {
                elapsed_ms: ms,
                rows: 3,
                metrics: MetricFacts {
                    tables: vec!["t".into()],
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }

    #[test]
    fn begin_finish_get_and_list_order() {
        let log = QueryLog::new(7, 100);
        let a = log.begin("SELECT 1", &origin(), "auto");
        let b = log.begin("select 2", &origin(), "auto");
        assert_eq!(log.len(), 2);
        let running = log.list(&ListFilter::parse("state=running").unwrap());
        assert_eq!(running.queries.len(), 2);
        assert_eq!(running.queries[0].query_id, b, "newest first");
        finish_ok(&log, &a, 12.5);
        log.fail(&b, 1.0, "Parse", "boom");
        let ra = log.get(&a).unwrap();
        assert_eq!(ra.state, QueryState::Finished);
        assert_eq!(ra.rows, Some(3));
        assert_eq!(ra.tables, vec!["t".to_string()]);
        assert_eq!(ra.node_id, 7);
        assert_eq!(ra.statement_kind, StatementKind::Select);
        let rb = log.get(&b).unwrap();
        assert_eq!(rb.state, QueryState::Failed);
        assert_eq!(rb.error.as_ref().unwrap().kind, "Parse");
        let failed = log.list(&ListFilter::parse("state=failed&q=boom").unwrap());
        assert_eq!(failed.matched, 1);
        assert_eq!(failed.queries[0].error_message.as_deref(), Some("boom"));
        assert!(log.get("nope").is_none());
    }

    #[test]
    fn eviction_skips_running_records() {
        let log = QueryLog::new(1, 10);
        let ids: Vec<String> = (0..10)
            .map(|i| log.begin(&format!("SELECT {i}"), &origin(), "auto"))
            .collect();
        // Finish all but the first; the first stays running.
        for id in &ids[1..] {
            finish_ok(&log, id, 1.0);
        }
        let extra = log.begin("SELECT 99", &origin(), "auto");
        assert_eq!(log.len(), 10);
        assert!(log.get(&ids[0]).is_some(), "the running record survived");
        assert!(
            log.get(&ids[1]).is_none(),
            "the oldest finished record was evicted"
        );
        assert!(log.get(&extra).is_some());
        // Capacity floor.
        assert_eq!(QueryLog::new(1, 1).capacity(), MIN_QUERY_LOG_SIZE);
    }

    #[test]
    fn overflow_when_everything_is_running() {
        let log = QueryLog::new(1, 10);
        for i in 0..12 {
            log.begin(&format!("SELECT {i}"), &origin(), "auto");
        }
        assert_eq!(log.len(), 12, "live queries are never dropped");
    }

    #[test]
    fn filter_parsing() {
        let f = ListFilter::parse("").unwrap();
        assert_eq!(f.limit, Some(100));
        let f = ListFilter::parse("limit=all&door=flight&q=line%20item+x").unwrap();
        assert_eq!(f.limit, None);
        assert_eq!(f.door, Some(FrontDoor::Flight));
        assert_eq!(f.q.as_deref(), Some("line item x"));
        assert!(ListFilter::parse("limit=lots").is_err());
        assert!(ListFilter::parse("state=weird").is_err());
    }

    #[test]
    fn percentiles_and_stats_reconcile() {
        let log = QueryLog::new(1, 100);
        let mut ids = Vec::new();
        for i in 1..=20 {
            let id = log.begin("SELECT * FROM lineitem", &origin(), "auto");
            finish_ok(&log, &id, i as f64);
            ids.push(id);
        }
        let f = log.begin("SELECT nope", &origin(), "auto");
        log.fail(&f, 0.5, "Bind", "no such column");
        let _running = log.begin("SELECT slow", &origin(), "auto");
        let s = log.stats(StatsInputs {
            uptime_s: 1.0,
            lifetime_total: 22,
            lifetime_failed: 1,
            ..Default::default()
        });
        assert_eq!(s.queries.total, 22);
        assert_eq!(
            s.queries.finished + s.queries.failed + s.queries.running,
            s.queries.total
        );
        assert_eq!(s.queries.running, 1);
        assert_eq!(s.latency_ms.samples, 21);
        assert_eq!(s.latency_ms.max_ms, 20.0);
        assert_eq!(s.latency_ms.p50_ms, 10.0);
        assert_eq!(s.errors_by_kind.get("Bind"), Some(&1));
        assert_eq!(s.tables.get("t"), Some(&20));
        assert_eq!(s.rows_total, 60);
        assert_eq!(s.slowest.len(), SLOWEST_N);
        assert_eq!(s.slowest[0].elapsed_ms, 20.0);
        assert_eq!(s.per_minute.len(), PER_MINUTE_WINDOW as usize);
        let in_window: usize = s.per_minute.iter().map(|b| b.count).sum();
        assert_eq!(
            in_window, 22,
            "everything submitted just now lands in the window"
        );
        assert_eq!(s.per_minute.last().unwrap().running, 1);
    }

    #[test]
    fn percentile_is_nearest_rank() {
        let mut v = vec![5.0, 1.0, 3.0, 2.0, 4.0];
        assert_eq!(percentile(&mut v, 0.5), 3.0);
        assert_eq!(percentile(&mut v, 0.95), 5.0);
        assert_eq!(percentile(&mut v, 0.0), 1.0);
        assert_eq!(percentile(&mut [], 0.5), 0.0);
    }

    #[test]
    fn statement_kind_and_previews() {
        assert_eq!(
            StatementKind::classify("  with x as (select 1) select * from x"),
            StatementKind::Select
        );
        assert_eq!(StatementKind::classify("(SELECT 1)"), StatementKind::Select);
        assert_eq!(
            StatementKind::classify("CREATE TABLE t AS SELECT 1"),
            StatementKind::Ddl
        );
        assert_eq!(
            StatementKind::classify("insert into t values (1)"),
            StatementKind::Dml
        );
        assert_eq!(StatementKind::classify("???"), StatementKind::Other);
        let (p, cut) = preview("select\n\n   a,   b   from t", 8);
        assert_eq!(p, "select a…");
        assert!(cut);
        let big = "x".repeat(SQL_CAP_BYTES + 5);
        let (kept, truncated) = cap_sql(&big);
        assert!(truncated && kept.ends_with("…[truncated]"));
    }

    #[test]
    fn list_summary_hides_plans_and_truncates_sql() {
        let log = QueryLog::new(1, 10);
        let long_sql = format!("SELECT {}", "x, ".repeat(200));
        let id = log.begin(&long_sql, &origin(), "auto");
        log.finish(
            &id,
            Completion {
                metrics: MetricFacts {
                    physical_plan: Some("HashJoin".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let v = log.list(&ListFilter::default());
        let s = &v.queries[0];
        assert!(s.sql_truncated);
        assert!(s.sql.chars().count() <= PREVIEW_CHARS + 1);
        let json = serde_json::to_value(s).unwrap();
        assert!(json.get("physical_plan").is_none());
        let full = serde_json::to_value(log.get(&id).unwrap()).unwrap();
        assert_eq!(full["physical_plan"], "HashJoin");
        assert_eq!(full["state"], "finished");
        assert_eq!(full["front_door"], "http");
    }
}
