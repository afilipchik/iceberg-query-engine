#!/usr/bin/env python3
"""Task 002 (native-table-pruning epic): real before/after measurement.

Runs the SAME queries against the SAME on-disk native table data with TWO
different engine binaries:
  - "before": built from the commit immediately preceding this epic
    (`e0e10cc`, the parent of the epic's own PRD/task-creation commit) --
    `NativeTable::scan_with_filter` is the `TableProvider` trait default,
    every query decodes every segment in full.
  - "after": this branch's current HEAD -- real segment-level pruning.

Both binaries read the EXACT SAME on-disk native table directories (the
manifest/segment format is unchanged by this epic), so any timing
difference is attributable ONLY to the pruning mechanism, not to any data
difference.

Two legs:
  1. Full 22-query TPC-H sweep at SF=10 (`data/tpch-10gb-native`) -- doubles
     as the "no regression on unfiltered/full-scan queries" check (most of
     the 22 queries touch every row of at least one large table, so a
     regression there would show up directly) AND gives fresh, real
     before/after numbers for Q4/Q12/Q13 at the scale CLAUDE.md's own
     "Current limitations" section names ("only Q12 at SF=10").
  2. Q4/Q12/Q13 only at SF=100 (`data/tpch-100gb-native`) -- the scale
     CLAUDE.md names as where "all three" queries cross the join-spill
     threshold.

Reuses `native_bench_compare.py`'s own `engine_native_side` /
`get_queries` (no duplicated HTTP-driving logic).

Usage:
  .venv/bin/python scripts/native_pruning_before_after.py \
      --before-binary /tmp/qe-wt-before-pruning/target/release/query_engine \
      --after-binary target/release/query_engine
"""
import argparse
import sys

sys.path.insert(0, "scripts")
from native_bench_compare import engine_native_side, get_queries  # noqa: E402


def run_leg(binary: str, native_dir: str, memory_limit: str, queries: dict, only_queries, iterations: int):
    ms, _csv, err, _proc = engine_native_side(
        binary, native_dir, queries, iterations, memory_limit, None, 900.0, only_queries
    )
    return ms, err


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--before-binary", required=True)
    ap.add_argument("--after-binary", required=True)
    ap.add_argument("--sf10-dir", default="data/tpch-10gb-native")
    ap.add_argument("--sf100-dir", default="data/tpch-100gb-native")
    ap.add_argument("--iterations", type=int, default=2)
    ap.add_argument("--skip-sf10-full", action="store_true")
    ap.add_argument("--skip-sf100", action="store_true")
    args = ap.parse_args()

    if not args.skip_sf10_full:
        queries10 = get_queries(10.0)
        print("=== SF=10, all 22 queries, data/tpch-10gb-native ===")
        print("--- before (pre-native-table-pruning-epic) ---")
        before_ms, before_err = run_leg(
            args.before_binary, args.sf10_dir, "40G", queries10, None, args.iterations
        )
        print("--- after (this branch's HEAD) ---")
        after_ms, after_err = run_leg(
            args.after_binary, args.sf10_dir, "40G", queries10, None, args.iterations
        )
        print(f"\n{'query':>6} {'before_ms':>12} {'after_ms':>12} {'delta':>10} {'ratio':>8}")
        before_total = 0.0
        after_total = 0.0
        for q in range(1, 23):
            b = before_ms.get(q)
            a = after_ms.get(q)
            be = before_err.get(q)
            ae = after_err.get(q)
            if be or ae:
                print(f"{q:>6} ERROR before={be} after={ae}")
                continue
            before_total += b
            after_total += a
            ratio = a / b if b else float("nan")
            marker = "  <-- Q4/Q12/Q13" if q in (4, 12, 13) else ""
            print(f"{q:>6} {b:>12.1f} {a:>12.1f} {a - b:>10.1f} {ratio:>8.3f}{marker}")
        print(f"{'TOTAL':>6} {before_total:>12.1f} {after_total:>12.1f} {after_total - before_total:>10.1f} {after_total / before_total:>8.3f}")

    if not args.skip_sf100:
        queries100 = get_queries(100.0)
        print("\n=== SF=100, Q4/Q12/Q13 only, data/tpch-100gb-native ===")
        print("--- before (pre-native-table-pruning-epic) ---")
        before_ms, before_err = run_leg(
            args.before_binary, args.sf100_dir, "100G", queries100, [4, 12, 13], args.iterations
        )
        print("--- after (this branch's HEAD) ---")
        after_ms, after_err = run_leg(
            args.after_binary, args.sf100_dir, "100G", queries100, [4, 12, 13], args.iterations
        )
        print(f"\n{'query':>6} {'before_ms':>12} {'after_ms':>12} {'delta':>10} {'ratio':>8}")
        for q in (4, 12, 13):
            b = before_ms.get(q)
            a = after_ms.get(q)
            be = before_err.get(q)
            ae = after_err.get(q)
            if be or ae:
                print(f"{q:>6} ERROR before={be} after={ae}")
                continue
            ratio = a / b if b else float("nan")
            print(f"{q:>6} {b:>12.1f} {a:>12.1f} {a - b:>10.1f} {ratio:>8.3f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
