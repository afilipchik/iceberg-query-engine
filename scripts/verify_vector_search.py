#!/usr/bin/env python3
"""
Independent end-to-end verification of the engine's vector search.

Runs the ground-truth queries through the ENGINE BINARY (not a library call),
then scores the results against the exact GPU ground truth.

SCORING IS TIE-ROBUST, and it has to be. This corpus is template-generated, so
many rows share identical text and therefore identical embeddings: for query 0,
**11 rows fall within the 10th-place distance** and the exact top-10 contains
only 9 distinct distances. The top-k *set* is genuinely ambiguous, and rank
order among equal distances is arbitrary. Comparing returned ids against
ground-truth ids therefore reports spurious failures — an engine that returns a
different but equally-close row is correct, not broken.

So the primary metric is DISTANCE-MULTISET equality: look up the true distance
of each returned row and compare the sorted list against ground truth's. That
is exactly "did the query return k rows that are as close as the best possible
k rows", which is what the SQL actually asks for.

  * distance-exact@k -- sorted returned distances == sorted ground-truth
                        distances (the correctness gate for the exact path)
  * id-recall@k      -- |returned ∩ ground truth| / k; informative for the
                        approximate index path, but under-reports under ties
  * category precision@k -- did the search return semantically right rows

Both engine modes are exercised: the default exact path and the opt-in indexed
path (QE_VECTOR_SEARCH=indexed), so the cost of approximation is measured
rather than assumed.

Usage:
    .venv/bin/python scripts/verify_vector_search.py \
        --gt .scratch/vector_gt.json --data data/vectors.lance
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time

BIN = "./target/release/query_engine"


def run_sql(sql: str, data: str, env_extra: dict) -> tuple[list, float]:
    """Run one SQL statement through the REPL in CSV mode; return (rows, ms)."""
    script = f".lance {data} vectors\n.mode csv\n{sql}\n.quit\n"
    env = dict(os.environ)
    env.update(env_extra)
    t0 = time.perf_counter()
    p = subprocess.run(
        [BIN, "repl"], input=script, capture_output=True, text=True, env=env, timeout=600
    )
    wall = (time.perf_counter() - t0) * 1000
    rows = []
    for line in p.stdout.splitlines():
        line = line.strip()
        # csv data lines look like: 12345,footwear,waterproof hiking boots ...
        m = re.match(r"^(\d+),([a-z]+),", line)
        if m:
            rows.append((int(m.group(1)), m.group(2)))
    if not rows and p.returncode != 0:
        print(p.stdout[-500:], p.stderr[-500:])
    return rows, wall


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gt", default=".scratch/vector_gt.json")
    ap.add_argument("--data", default="data/vectors.lance")
    ap.add_argument("--modes", default="exact,indexed")
    ap.add_argument("--refine", default="10")
    args = ap.parse_args()

    gt = json.load(open(args.gt))
    k = gt["k"]
    print(f"ground truth: {gt['rows']:,} rows x {gt['dim']} dims, k={k}, "
          f"{len(gt['queries'])} queries\n")

    # Load embeddings once so returned ids can be scored by their TRUE distance.
    import lance
    import numpy as np

    ds = lance.dataset(args.data)
    t = ds.to_table(columns=["id", "embedding"])
    n = len(t)
    emb = np.asarray(
        t["embedding"].combine_chunks().values, dtype=np.float32
    ).reshape(n, gt["dim"])
    id_to_row = {int(v): i for i, v in enumerate(np.asarray(t["id"]))}

    def true_dists(ids, qvec):
        q = np.asarray(qvec, dtype=np.float32)
        rows = [id_to_row[i] for i in ids if i in id_to_row]
        if len(rows) != len(ids):
            return None
        return np.sort(1.0 - emb[rows] @ q)

    for mode in [m.strip() for m in args.modes.split(",") if m.strip()]:
        env = {}
        label = mode
        if mode == "indexed":
            env = {"QE_VECTOR_SEARCH": "indexed", "QE_VECTOR_REFINE": args.refine}
            label = f"indexed(refine={args.refine})"
        recalls, dexacts, precs, times = [], [], [], []
        print(f"=== mode: {label} ===")
        for rec in gt["queries"]:
            lit = ", ".join(f"{x:.6f}" for x in rec["vector"])
            sql = (
                f"SELECT id, category, text FROM vectors "
                f"ORDER BY cosine_distance(embedding, [{lit}]) LIMIT {k};"
            )
            rows, wall = run_sql(sql, args.data, env)
            got_ids = [r[0] for r in rows][:k]
            want_ids = [n["id"] for n in rec["exact_top_k"]][:k]
            if not got_ids:
                print(f"  {rec['query'][:44]:44} NO ROWS RETURNED")
                recalls.append(0.0); dexacts.append(0.0); precs.append(0.0)
                continue
            recall = len(set(got_ids) & set(want_ids)) / len(want_ids)
            # Tie-robust gate: are the returned rows as close as the best k?
            got_d = true_dists(got_ids, rec["vector"])
            want_d = np.sort([n_["distance"] for n_ in rec["exact_top_k"]][:k])
            dexact = (
                1.0
                if got_d is not None
                and len(got_d) == len(want_d)
                and bool(np.allclose(got_d, want_d, atol=1e-5))
                else 0.0
            )
            prec = sum(1 for r in rows[:k] if r[1] == rec["expected_category"]) / len(
                rows[:k]
            )
            recalls.append(recall); dexacts.append(dexact); precs.append(prec)
            times.append(wall)
            flag = "" if dexact else "  <-- NOT distance-equivalent to exact"
            print(f"  {rec['query'][:44]:44} dist-exact={'yes' if dexact else 'NO '} "
                  f"id-recall={recall:.2f} prec={prec:.2f}{flag}")
        print(f"  MEAN distance-exact@{k}={statistics.mean(dexacts):.3f}  "
              f"id-recall@{k}={statistics.mean(recalls):.3f}  "
              f"category-precision@{k}={statistics.mean(precs):.3f}  "
              f"median wall={statistics.median(times):.0f}ms (incl. process start)\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
