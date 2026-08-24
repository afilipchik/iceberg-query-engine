//! Task 005 (native-tables-mutation epic) adversarial diagnostic: proves,
//! with a REAL cross-process `kill -9` (never simulated/mocked), that:
//!
//! 1. **Single-writer lock (task 001 Decision 5)** genuinely blocks a
//!    second concurrent writer to the SAME table with a clean, named
//!    failure -- and that the kernel releases the lock immediately when a
//!    writer is SIGKILL'd, for the REAL mutation code path (not the
//!    bare-lock-only spike task 001's own design phase used).
//! 2. **Crash safety mid-mutation**: a writer killed AFTER it has written
//!    new segment file(s) to disk but BEFORE the final manifest rename
//!    leaves the table in EXACTLY its pre-mutation state -- the old
//!    manifest byte-for-byte untouched, orphan segment files harmless and
//!    unreferenced, and a subsequent read/write against the table works
//!    normally (the crash does not brick the table going forward).
//!
//! Mirrors phase 1 task 001's own `lock_holder`/`lock_contender` spike in
//! shape, but drives the REAL `native_write::write_append_segments` /
//! `publish_manifest_update` building blocks (task 002) instead of a bare
//! `File::try_lock()` — this is the actual mutation code path production
//! code runs, not a synthetic lock-only harness. The crash window is made
//! DETERMINISTIC (not a timing gamble): the writer role writes a readiness
//! marker file the instant its segment file(s) are on disk, then sleeps;
//! the orchestrator polls for that marker and sends a REAL `kill -9 <pid>`
//! (a real `kill` subprocess, not `Child::kill()`'s in-process syscall
//! wrapper, to remove any doubt this is a real, external, unrecoverable
//! hard kill) the instant it appears -- guaranteeing the kill lands inside
//! the write-then-publish window every single run.
//!
//! ```text
//! scripts/claude-safe-build.sh cargo build --release --example native_crash_kill_check
//! scripts/claude-safe-build.sh ./target/release/examples/native_crash_kill_check            # crash-kill scenario
//! QE_CRASH_MODE=concurrent scripts/claude-safe-build.sh ./target/release/examples/native_crash_kill_check   # two real concurrent writers
//! ```

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use query_engine::physical::{RecordBatchStream, TableProvider};
use query_engine::storage::native_manifest;
use query_engine::storage::native_write::{self, NativeWriteMode, NativeWriteOptions};
use query_engine::storage::NativeTable;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

fn schema() -> SchemaRef {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]))
}

fn make_batch(start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let vals: Vec<f64> = ids.iter().map(|&i| i as f64 * 2.0).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            std::sync::Arc::new(Int64Array::from(ids)),
            std::sync::Arc::new(Float64Array::from(vals)),
        ],
    )
    .expect("build synthetic batch")
}

fn stream_of(batches: Vec<RecordBatch>) -> RecordBatchStream {
    Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
}

fn marker_path(dir: &Path) -> PathBuf {
    dir.with_file_name(format!(
        "{}.crash_test_ready",
        dir.file_name().unwrap().to_string_lossy()
    ))
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if std::env::var("QE_CRASH_ROLE").as_deref() == Ok("writer") {
        return run_writer_role().await;
    }
    match std::env::var("QE_CRASH_MODE").as_deref() {
        Ok("concurrent") => run_concurrent_writers_scenario().await,
        _ => run_kill_scenario().await,
    }
}

/// Child-process role: acquire the REAL single-writer lock, write new
/// segment(s) via the REAL `write_append_segments` building block, signal
/// readiness the INSTANT they are durably on disk, then sleep -- giving the
/// orchestrator a wide, deterministic window to `kill -9` this process
/// before it ever reaches `publish_manifest_update`. If NOT killed (e.g.
/// run standalone), it proceeds to publish normally and exits 0, so this
/// role is independently runnable/debuggable too.
async fn run_writer_role() -> query_engine::Result<()> {
    let dir = PathBuf::from(std::env::var("QE_CRASH_TABLE_DIR").expect("QE_CRASH_TABLE_DIR"));
    let pause_secs: u64 = std::env::var("QE_CRASH_PAUSE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let rows: i64 = std::env::var("QE_CRASH_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);
    let id_base: i64 = std::env::var("QE_CRASH_ID_BASE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    eprintln!(
        "[writer {}] acquiring lock on {:?}",
        std::process::id(),
        dir
    );
    let _lock = native_write::lock_table_for_write(&dir)?;
    eprintln!("[writer {}] lock acquired", std::process::id());

    let existing = native_manifest::read_manifest(&dir)?;
    let batch = make_batch(id_base, rows);
    let new_segments = native_write::write_append_segments(
        stream_of(vec![batch]),
        &existing,
        &dir,
        NativeWriteOptions::default(),
    )
    .await?;
    eprintln!(
        "[writer {}] wrote {} new segment file(s) to disk -- NOT yet published",
        std::process::id(),
        new_segments.len()
    );

    // Signal readiness: segment file(s) are durably on disk, manifest is
    // still the OLD one. This is the exact window the crash-safety
    // requirement cares about.
    std::fs::write(marker_path(&dir), b"ready").expect("write readiness marker");
    eprintln!(
        "[writer {}] wrote readiness marker, sleeping {pause_secs}s (or until killed)",
        std::process::id()
    );
    tokio::time::sleep(Duration::from_secs(pause_secs)).await;

    // Only reached if NOT killed in time.
    let mut all_segments = existing.segments.clone();
    all_segments.extend(new_segments);
    let manifest = native_write::publish_manifest_update(
        &dir,
        existing.arrow_schema().as_ref(),
        existing.table_id.clone(),
        existing.snapshot.version + 1,
        all_segments,
        chrono::Utc::now().timestamp_millis(),
    )?;
    eprintln!(
        "[writer {}] NOT killed in time -- published version {}",
        std::process::id(),
        manifest.snapshot.version
    );
    println!(
        "APPEND OK: version={} rows={}",
        manifest.snapshot.version, manifest.snapshot.row_count
    );
    Ok(())
}

/// Orchestrator: seed a table, spawn a real child process running the
/// writer role, wait for its readiness marker, send a REAL `kill -9 <pid>`
/// (a genuine external subprocess, not an in-process syscall wrapper),
/// then verify every crash-safety postcondition against the REAL mutation
/// code path.
async fn run_kill_scenario() -> query_engine::Result<()> {
    let scratch = tempfile::tempdir().expect("tempdir");
    let dir = scratch.path().join("t");
    let seed = make_batch(0, 1_000);
    native_write::write_batches(
        stream_of(vec![seed]),
        schema(),
        &dir,
        NativeWriteMode::Create,
    )
    .await?;
    let pre_manifest_bytes =
        std::fs::read(native_manifest::manifest_path(&dir)).expect("read pre-crash manifest");
    let pre_manifest = native_manifest::read_manifest(&dir)?;
    let pre_segment_files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "arrow"))
        .collect();
    println!(
        "=== Crash-kill scenario ===\nseeded table: {} rows, {} segment(s), manifest={} bytes",
        pre_manifest.snapshot.row_count,
        pre_manifest.segments.len(),
        pre_manifest_bytes.len(),
    );

    let exe = std::env::current_exe().expect("current_exe");
    let marker = marker_path(&dir);
    let _ = std::fs::remove_file(&marker);

    let mut child = Command::new(&exe)
        .env("QE_CRASH_ROLE", "writer")
        .env("QE_CRASH_TABLE_DIR", &dir)
        .env("QE_CRASH_PAUSE_SECS", "20")
        .env("QE_CRASH_ROWS", "50000")
        .spawn()
        .expect("spawn writer child");
    let child_pid = child.id();
    println!("spawned writer child pid={child_pid}, waiting for readiness marker...");

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut saw_marker = false;
    while Instant::now() < deadline {
        if marker.exists() {
            saw_marker = true;
            break;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("writer child exited BEFORE readiness marker appeared: {status:?}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        saw_marker,
        "writer child never signaled readiness within 30s"
    );
    let saw_marker_at = Instant::now();

    // A REAL `kill -9 <pid>` subprocess -- an external, unambiguous hard
    // kill, not `Child::kill()`'s in-process wrapper.
    let kill_status = Command::new("kill")
        .args(["-9", &child_pid.to_string()])
        .status()
        .expect("run kill -9");
    assert!(
        kill_status.success(),
        "kill -9 itself failed to run: {kill_status:?}"
    );
    println!(
        "sent REAL `kill -9 {child_pid}` {:?} after marker appeared",
        saw_marker_at.elapsed()
    );

    use std::os::unix::process::ExitStatusExt;
    let exit_status = child.wait().expect("wait for killed child");
    println!(
        "writer child reaped: {exit_status:?} (signal={:?})",
        exit_status.signal()
    );
    assert_eq!(
        exit_status.signal(),
        Some(9),
        "child must have died from SIGKILL(9), got {exit_status:?}"
    );

    // ---- Postconditions ----
    let mut all_ok = true;

    // (a) old manifest byte-for-byte untouched.
    let post_manifest_bytes =
        std::fs::read(native_manifest::manifest_path(&dir)).expect("read post-crash manifest");
    let manifest_untouched = post_manifest_bytes == pre_manifest_bytes;
    println!(
        "[{}] manifest byte-for-byte unchanged after the kill: {} bytes before, {} after",
        if manifest_untouched { "PASS" } else { "FAIL" },
        pre_manifest_bytes.len(),
        post_manifest_bytes.len(),
    );
    all_ok &= manifest_untouched;

    // (b) new orphan segment file(s) exist but are unreferenced.
    let post_segment_files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "arrow"))
        .collect();
    let orphan_count = post_segment_files.len() - pre_segment_files.len();
    let post_manifest = native_manifest::read_manifest(&dir)?;
    let orphans_unreferenced =
        orphan_count > 0 && post_manifest.segments.len() == pre_manifest.segments.len();
    println!(
        "[{}] {orphan_count} orphan segment file(s) written before the kill, present on disk \
         but referenced by NEITHER the old nor any manifest (manifest still lists exactly the \
         pre-crash {} segment(s))",
        if orphans_unreferenced { "PASS" } else { "FAIL" },
        pre_manifest.segments.len(),
    );
    all_ok &= orphans_unreferenced;

    // (c) the lock is immediately re-acquirable -- kernel auto-release.
    let relock = native_write::lock_table_for_write(&dir);
    let relock_ok = relock.is_ok();
    println!(
        "[{}] lock immediately re-acquirable after the SIGKILL'd holder died (kernel \
         auto-release): {:?}",
        if relock_ok { "PASS" } else { "FAIL" },
        relock
            .as_ref()
            .map(|_| "acquired")
            .map_err(|e| e.to_string())
    );
    drop(relock);
    all_ok &= relock_ok;

    // (d) the table reads back as EXACTLY its pre-crash state via the REAL
    // TableProvider::scan()/statistics() read path.
    let table = NativeTable::try_new(&dir)?;
    let stats = table.statistics();
    let batches = table.scan(None)?;
    let scanned_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let rows_match = scanned_rows as u64 == pre_manifest.snapshot.row_count
        && stats.map(|s| s.row_count as u64) == Some(pre_manifest.snapshot.row_count);
    println!(
        "[{}] table reads back as EXACTLY pre-crash state: scan()={scanned_rows} rows, \
         expected={}",
        if rows_match { "PASS" } else { "FAIL" },
        pre_manifest.snapshot.row_count,
    );
    all_ok &= rows_match;

    // (e) bonus: the table is still writable normally after the crash --
    // the orphan segment does not brick future mutations.
    let follow_up = native_write::append_to_native_table(
        stream_of(vec![make_batch(2_000_000, 10)]),
        &dir,
        NativeWriteOptions::default(),
    )
    .await;
    let follow_up_ok = follow_up.is_ok();
    println!(
        "[{}] table still normally writable after the crash: {:?}",
        if follow_up_ok { "PASS" } else { "FAIL" },
        follow_up.map(|r| format!("version={} total_rows={}", r.version, r.total_rows))
    );
    all_ok &= follow_up_ok;

    println!(
        "\n=== OVERALL: {} ===",
        if all_ok { "PASS" } else { "FAIL (see above)" }
    );
    if !all_ok {
        std::process::exit(1);
    }
    Ok(())
}

/// Two REAL concurrent writer processes racing to append to the SAME
/// table. Confirms exactly one succeeds and the other fails cleanly and
/// namedly (never silent data loss, never corruption).
async fn run_concurrent_writers_scenario() -> query_engine::Result<()> {
    let scratch = tempfile::tempdir().expect("tempdir");
    let dir = scratch.path().join("t");
    let seed = make_batch(0, 1_000);
    native_write::write_batches(
        stream_of(vec![seed]),
        schema(),
        &dir,
        NativeWriteMode::Create,
    )
    .await?;
    let pre = native_manifest::read_manifest(&dir)?;
    println!(
        "=== Concurrent-writers scenario ===\nseeded table: {} rows, {} segment(s)",
        pre.snapshot.row_count,
        pre.segments.len()
    );

    let exe = std::env::current_exe().expect("current_exe");
    let mut children = Vec::new();
    for (i, id_base) in [(1, 5_000_000i64), (2, 9_000_000i64)] {
        let child = Command::new(&exe)
            .env("QE_CRASH_ROLE", "writer")
            .env("QE_CRASH_TABLE_DIR", &dir)
            .env("QE_CRASH_PAUSE_SECS", "2")
            .env("QE_CRASH_ROWS", "20000")
            .env("QE_CRASH_ID_BASE", id_base.to_string())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn writer child {i}: {e}"));
        children.push((i, child));
    }

    let mut outcomes = Vec::new();
    for (i, child) in children {
        let output = child.wait_with_output().expect("wait_with_output");
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        println!(
            "writer #{i}: exit={:?}\n  stdout: {}\n  stderr(tail): {}",
            output.status.code(),
            stdout.trim(),
            stderr.lines().last().unwrap_or("")
        );
        outcomes.push((i, output.status.success(), stdout, stderr));
    }

    let successes = outcomes.iter().filter(|(_, ok, ..)| *ok).count();
    let failures = outcomes.iter().filter(|(_, ok, ..)| !*ok).count();
    let failure_named_lock_contention = outcomes
        .iter()
        .filter(|(_, ok, _, err)| !*ok && err.contains("another writer already holds the lock"))
        .count();

    let post = native_manifest::read_manifest(&dir)?;
    let table = NativeTable::try_new(&dir)?;
    let scanned: usize = table.scan(None)?.iter().map(|b| b.num_rows()).sum();

    println!(
        "\nresult: {successes} succeeded, {failures} failed ({failure_named_lock_contention} \
         with a named lock-contention error), final table: {} rows (manifest), {scanned} rows \
         (scan)",
        post.snapshot.row_count
    );

    let ok = successes == 1 && failures == 1 && failure_named_lock_contention == 1;
    println!(
        "\n=== OVERALL: {} ===",
        if ok { "PASS" } else { "FAIL (see above)" }
    );
    if !ok {
        std::process::exit(1);
    }
    Ok(())
}
