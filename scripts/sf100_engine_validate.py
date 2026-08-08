#!/usr/bin/env python3
"""
Run engine TPC-H at SF=100, save CSV results, compare with DuckDB results.
Memory limit: 64GB. Timeout: 10x DuckDB time (min 60s, max 600s).
"""

import subprocess
import time
import csv
import os
import sys
import re

ENGINE = "./target/release/query_engine"
DATA_DIR = "data/tpch-100gb"
ENGINE_RESULTS = "data/sf100_engine_results"
DUCKDB_RESULTS = "data/sf100_duckdb_results"

# DuckDB best times at SF=100 (ms) from previous benchmark run
DUCKDB_MS = {
    1: 2773, 2: 313, 3: 2755, 4: 1793, 5: 2265, 6: 1191,
    7: 2411, 8: 2698, 9: 28094, 10: 1096, 11: 96, 12: 985,
    13: 1251, 14: 463, 15: 994, 16: 709, 17: 2191, 18: 4748,
    19: 2625, 20: 3210, 21: 4012, 22: 393,
}

# TPC-H queries matching those in the DuckDB script
# These need to match exactly what the engine's benchmark-parquet runs
# We'll use the engine's built-in queries via --query flag

def timeout_for_query(qnum):
    """10x DuckDB time, min 60s, max 600s"""
    dms = DUCKDB_MS.get(qnum, 5000)
    t = max(60, min(600, dms * 10 // 1000))
    return t

def run_engine_query(qnum):
    """Run a single TPC-H query and return (time_ms, row_count, output)"""
    timeout_s = timeout_for_query(qnum)
    cmd = [ENGINE, "benchmark-parquet", "--path", DATA_DIR, "--query", str(qnum),
           "--iterations", "1", "--sf", "100"]

    t0 = time.perf_counter()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        if result.returncode != 0:
            return None, None, f"ERROR (exit={result.returncode}): {result.stderr[:200]}"

        # Parse output: "Q01:        6 rows in 3426.215ms"
        output = result.stdout
        match = re.search(r'Q\d+:\s+(\d+)\s+rows\s+in\s+([\d.]+)ms', output)
        if match:
            rows = int(match.group(1))
            time_ms = float(match.group(2))
            return time_ms, rows, output
        else:
            return elapsed_ms, None, f"Could not parse output: {output[:200]}"
    except subprocess.TimeoutExpired:
        return None, None, f"TIMEOUT (>{timeout_s}s, DuckDB={DUCKDB_MS.get(qnum)}ms)"

def parse_csv_file(path):
    """Parse a CSV file into (headers, rows) where rows are lists of strings"""
    if not os.path.exists(path):
        return None, None
    with open(path, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)
    return headers, rows

def normalize_float(s, precision=2):
    """Normalize a float string for comparison"""
    try:
        v = float(s)
        return round(v, precision)
    except (ValueError, TypeError):
        return s

def compare_results(qnum):
    """Compare engine CSV with DuckDB CSV. Returns (match, message)"""
    engine_path = os.path.join(ENGINE_RESULTS, f"q{qnum:02d}.csv")
    duckdb_path = os.path.join(DUCKDB_RESULTS, f"q{qnum:02d}.csv")

    e_headers, e_rows = parse_csv_file(engine_path)
    d_headers, d_rows = parse_csv_file(duckdb_path)

    if e_headers is None:
        return False, "Engine CSV missing"
    if d_headers is None:
        return False, "DuckDB CSV missing"

    # Compare row counts
    if len(e_rows) != len(d_rows):
        return False, f"Row count mismatch: engine={len(e_rows)}, duckdb={len(d_rows)}"

    # Compare each row with float tolerance
    mismatches = 0
    first_mismatch = None
    for i, (e_row, d_row) in enumerate(zip(e_rows, d_rows)):
        if len(e_row) != len(d_row):
            return False, f"Column count mismatch at row {i}: engine={len(e_row)}, duckdb={len(d_row)}"
        for j, (ev, dv) in enumerate(zip(e_row, d_row)):
            ev_s = ev.strip()
            dv_s = dv.strip()
            # Try float comparison
            try:
                ef = float(ev_s)
                df = float(dv_s)
                # Use relative tolerance for large numbers, absolute for small
                if abs(df) > 1:
                    if abs(ef - df) / abs(df) > 0.01:  # 1% tolerance
                        mismatches += 1
                        if first_mismatch is None:
                            first_mismatch = f"Row {i}, Col {j}: engine={ev_s}, duckdb={dv_s}"
                else:
                    if abs(ef - df) > 0.01:
                        mismatches += 1
                        if first_mismatch is None:
                            first_mismatch = f"Row {i}, Col {j}: engine={ev_s}, duckdb={dv_s}"
            except (ValueError, TypeError):
                # String comparison (dates, names, etc.)
                if ev_s != dv_s:
                    mismatches += 1
                    if first_mismatch is None:
                        first_mismatch = f"Row {i}, Col {j}: engine='{ev_s}', duckdb='{dv_s}'"

    if mismatches > 0:
        return False, f"{mismatches} mismatches. First: {first_mismatch}"
    return True, f"OK ({len(e_rows)} rows match)"


def main():
    os.makedirs(ENGINE_RESULTS, exist_ok=True)

    # Check if we should skip queries that already have results
    skip_existing = "--skip-existing" in sys.argv
    only_query = None
    for arg in sys.argv[1:]:
        if arg.startswith("--query="):
            only_query = int(arg.split("=")[1])

    queries = [only_query] if only_query else list(range(1, 23))

    print(f"Engine TPC-H SF=100 Benchmark + Validation")
    print(f"Data: {DATA_DIR}")
    print(f"Engine results: {ENGINE_RESULTS}")
    print(f"DuckDB results: {DUCKDB_RESULTS}")
    print("=" * 80)
    print(f"{'Query':<8} {'Time (ms)':<14} {'Rows':<8} {'DuckDB (ms)':<14} {'Ratio':<8} {'Correct'}")
    print("-" * 80)

    total_engine = 0
    total_duckdb = 0
    passed = 0
    failed = 0
    timeout_count = 0
    correct_count = 0
    incorrect_count = 0
    results_summary = {}

    for qnum in queries:
        # Check if we should skip
        engine_csv = os.path.join(ENGINE_RESULTS, f"q{qnum:02d}.csv")
        if skip_existing and os.path.exists(engine_csv):
            print(f"Q{qnum:<7} SKIPPED (result exists)")
            continue

        time_ms, rows, output = run_engine_query(qnum)
        duckdb_time = DUCKDB_MS.get(qnum, 0)

        if time_ms is None:
            # Timeout or error
            status = output  # contains the error/timeout message
            print(f"Q{qnum:<7} {'TIMEOUT/ERR':<14} {'-':<8} {duckdb_time:<14} {'-':<8} -")
            print(f"         {status}")
            if "TIMEOUT" in status:
                timeout_count += 1
            else:
                failed += 1
            results_summary[qnum] = {"status": "TIMEOUT" if "TIMEOUT" in status else "ERROR", "time": None}
            continue

        # Calculate ratio
        ratio = time_ms / duckdb_time if duckdb_time > 0 else 0
        total_engine += time_ms
        total_duckdb += duckdb_time
        passed += 1

        # Compare results
        correct, msg = compare_results(qnum)
        correct_str = "MATCH" if correct else f"FAIL: {msg}"
        if correct:
            correct_count += 1
        else:
            incorrect_count += 1

        print(f"Q{qnum:<7} {time_ms:<14.1f} {rows if rows else '-':<8} {duckdb_time:<14} {ratio:<8.1f} {correct_str}")

        results_summary[qnum] = {
            "status": "OK",
            "time": time_ms,
            "rows": rows,
            "duckdb_time": duckdb_time,
            "ratio": ratio,
            "correct": correct,
        }

    print("=" * 80)
    print(f"\nCompleted: {passed}, Failed: {failed}, Timeout: {timeout_count}")
    print(f"Correct: {correct_count}, Incorrect: {incorrect_count}")
    if total_duckdb > 0:
        print(f"Total engine: {total_engine:.0f}ms, Total DuckDB: {total_duckdb}ms, Ratio: {total_engine/total_duckdb:.1f}x")


if __name__ == "__main__":
    main()
