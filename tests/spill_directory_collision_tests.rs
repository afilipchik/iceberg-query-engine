//! spill-join-correctness-2 epic, task 004: VERIFICATION (not re-fix) that
//! task 001's PID-embedded default `spill_path`
//! (`src/execution/memory.rs`, `ExecutionConfig::default()`) resolves the
//! spill-directory-collision bug for the case task 001's OWN regression
//! test (`execution::memory::tests::default_spill_path_is_disambiguated_by_pid`,
//! a single-process unit test asserting the path's file name embeds
//! `std::process::id()`) and task 001's own `KeyChecksum` unit tests do NOT
//! cover: two REAL, independent, concurrently-running `query_engine serve`
//! OS processes, sharing the same inherited `$TMPDIR`, neither overriding
//! `--spill-path` (no such flag exists — confirmed by `grep`), both racing
//! to spill at once.
//!
//! Before task 001's fix, both processes' first spilling operator computed
//! the IDENTICAL default spill directory
//! (`$TMPDIR/query_engine_spill/join_0_0/...`) and collided — this is the
//! archived `spill-join-correctness` epic's own task 003 finding, whose
//! documented symptom was "fails loudly: `Parquet error: Required field
//! schema is missing`, HTTP 400" (see
//! `.claude/epics/archived/spill-join-correctness/003.md`'s Outcome, "5.
//! Distributed (M1/M2)" — a real 3-node cluster hit exactly this before
//! task 001's own fix, reproduced there with `TMPDIR` isolation removed).
//! Task 001's own later investigation found the SAME collision could also
//! silently return a WRONG answer, not just crash — but that finding came
//! from an incidental, ambient concurrent process, not a deliberate,
//! reusable, committed regression test. This file is that test: real
//! processes, real concurrency, real spill I/O, run every time the suite
//! runs, matching the standing "3 real processes" precedent already
//! established by `tests/distributed_cluster.rs`'s own
//! `three_real_processes_serve_and_survive_a_sigterm`.
//!
//! Fixture: `data/tpch-10mb` (SF=0.01) — same one `tests/spill_tests.rs`
//! uses to force `SpillableHashJoinExec`'s spill path (SF=0.001's
//! `tpch-1mb` fits in a single Arrow batch and never triggers the
//! partition-eviction spill logic at all; see that file's own module doc
//! comment).

use query_engine::distributed::http_client;
use query_engine::ExecutionContext;
use std::net::TcpListener as StdListener;
use std::time::Duration;

const DATA: &str = "data/tpch-10mb";
const TABLES: [&str; 8] = [
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
// Same query and same 256KB budget `spill_tests.rs`'s own
// `join_spill_matches_in_memory` uses to force the join spill path over
// this exact fixture — reused deliberately, not re-derived, so this test's
// spill shape is already known-good.
const SPILL_SQL: &str = "SELECT o_orderpriority, COUNT(*) AS cnt, SUM(l_extendedprice) AS total \
     FROM lineitem, orders WHERE l_orderkey = o_orderkey \
     GROUP BY o_orderpriority ORDER BY o_orderpriority";
const MEMORY_LIMIT: &str = "256KB";

fn data_dir() -> String {
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), DATA)
}

/// Kills every child on drop, including when an assertion panics part-way
/// through — matches `tests/distributed_cluster.rs`'s own `Children` guard.
struct Children(Vec<std::process::Child>);

impl Drop for Children {
    fn drop(&mut self) {
        for c in &mut self.0 {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Reserve `n` ports by binding and immediately releasing them — same
/// approach `tests/distributed_cluster.rs` uses, for the same reason (a
/// fixed port collides the instant two test binaries run at once).
fn reserve_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<StdListener> = (0..n)
        .map(|_| StdListener::bind("127.0.0.1:0").expect("cannot reserve a port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

async fn wait_ready(addr: &str, deadline: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if matches!(http_client::get(addr, "/readyz", HTTP_TIMEOUT).await, Ok(r) if r.is_success())
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Same CSV-rendering approach `tests/distributed_cluster.rs`'s own
/// `csv_of` helper uses, so this test's baseline is byte-comparable to the
/// server's own `/sql?format=csv` output without any float-formatting
/// guesswork.
fn csv_of(result: &query_engine::QueryResult) -> String {
    let mut buf = Vec::new();
    {
        let mut w = arrow::csv::WriterBuilder::new()
            .with_header(true)
            .build(&mut buf);
        for b in &result.batches {
            w.write(b).expect("csv encode");
        }
    }
    String::from_utf8(buf).expect("csv is utf-8")
}

/// The correct answer, computed in-process with no memory limit (the
/// in-memory `SortExec`/`HashJoinExec` path, never spilling) — the oracle
/// every spilled, concurrent, cross-process run below must match exactly.
async fn baseline_csv() -> String {
    let mut ctx = ExecutionContext::new();
    for t in TABLES {
        let path = format!("{}/{t}.parquet", data_dir());
        ctx.register_parquet(t, &path)
            .unwrap_or_else(|e| panic!("cannot load {path}: {e}"));
    }
    let result = ctx.sql(SPILL_SQL).await.expect("baseline query failed");
    csv_of(&result)
}

/// GATE (spill-join-correctness-2 epic, task 004, bug 1 verification): two
/// real `query_engine serve` processes, neither overriding `--spill-path`,
/// sharing this test process's own inherited `$TMPDIR`, both firing the
/// SAME spilling INNER-join query CONCURRENTLY, repeated across several
/// overlapping rounds to maximize the chance of temporal overlap between
/// the two processes' own spill write/read windows (matching task 001's
/// own "fire concurrent queries" methodology). Every response must be
/// HTTP 200 with a cell-exact-correct answer — never the archived epic's
/// own documented `Parquet error: Required field schema is missing` /
/// HTTP 400 collision symptom, and never a silently wrong one either.
#[tokio::test]
async fn two_concurrent_processes_sharing_tmpdir_do_not_collide_on_spill_path() {
    let bin = env!("CARGO_BIN_EXE_query_engine");
    let ports = reserve_ports(2);

    let mut children = Children(Vec::new());
    for p in &ports {
        let child = std::process::Command::new(bin)
            .args([
                "serve",
                "--bind",
                &format!("127.0.0.1:{p}"),
                "--data",
                &data_dir(),
                "--memory-limit",
                MEMORY_LIMIT,
            ])
            // Deliberately NOT setting TMPDIR (or any `--spill-path`
            // equivalent — none exists) for either child: both inherit
            // this test process's own `$TMPDIR`, exactly the "co-located
            // processes on one host" precondition the archived bug needs.
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("cannot spawn query_engine serve");
        children.0.push(child);
    }

    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect();
    for a in &addrs {
        assert!(
            wait_ready(a, Duration::from_secs(60)).await,
            "{a} never became ready"
        );
    }

    let expected = baseline_csv().await;

    // Several overlapping rounds: fire both processes' queries at the same
    // instant via `tokio::join!`, repeated, so the two processes' own
    // spill windows have many independent chances to overlap in time (a
    // single round could get unlucky and never truly overlap).
    for round in 0..6 {
        let (r0, r1) = tokio::join!(
            http_client::post_text(&addrs[0], "/sql?format=csv", SPILL_SQL, HTTP_TIMEOUT),
            http_client::post_text(&addrs[1], "/sql?format=csv", SPILL_SQL, HTTP_TIMEOUT),
        );
        for (i, r) in [r0, r1].into_iter().enumerate() {
            let r =
                r.unwrap_or_else(|e| panic!("round {round}, process {i}: transport error: {e}"));
            assert!(
                r.is_success(),
                "round {round}, process {i}: HTTP {} {} — this is exactly the archived epic's \
                 own documented spill-directory-collision symptom \
                 (`Parquet error: Required field schema is missing`, HTTP 400); \
                 task 001's PID-embedded default spill_path should make it structurally \
                 impossible",
                r.status,
                r.text()
            );
            assert_eq!(
                round_csv_floats(&r.text()),
                round_csv_floats(&expected),
                "round {round}, process {i}: spilled concurrent-process answer differs from \
                 the unlimited-memory baseline (beyond float accumulation-order noise) — a \
                 silent wrong answer, the more severe symptom task 001's own investigation \
                 found this exact collision can also produce"
            );
        }
    }
}

/// Round every numeric CSV field to 2 decimal places before comparing.
/// SUM(l_extendedprice) accumulates in a different batch/partition order
/// between the in-memory baseline and a spilled, partitioned, cross-process
/// run — legitimate last-few-ULP float differences, the same reasoning
/// `tests/spill_tests.rs`'s own `cell()` helper documents ("Floats are
/// rounded to 3 decimals so legitimate accumulation-order differences...
/// don't produce false mismatches"). Non-numeric fields (the header,
/// `o_orderpriority`) pass through unchanged.
fn round_csv_floats(csv: &str) -> String {
    csv.lines()
        .map(|line| {
            line.split(',')
                .map(|field| match field.trim().parse::<f64>() {
                    Ok(v) => format!("{v:.2}"),
                    Err(_) => field.to_string(),
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join("\n")
}
