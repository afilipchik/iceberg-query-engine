#!/usr/bin/env python3
"""
Ground truth for the engine's vector search.

Emits, for a set of natural-language queries:
  * the query embedding (the exact float32 vector the engine must search with),
  * the EXACT top-k neighbours computed by brute force on the GPU,
  * the expected semantic category, so a test can assert the search is
    meaningful and not merely arithmetically self-consistent.

Exact brute force is the oracle. The IVF_PQ index is approximate, so the
engine's indexed path is scored by recall@k against this file rather than by
exact equality — approximate results are not a bug, but silently returning them
where exact ones were asked for would be.

Also emits ready-to-run SQL (with the 384-float literal inlined) so the exact
query the engine runs is reproducible by hand.

Usage:
    .venv/bin/python scripts/vector_ground_truth.py \
        --data data/vectors.lance --k 10 --out .scratch/vector_gt.json
"""

import argparse
import json
import os
import sys
import time

import lance
import numpy as np

MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# (query text, the category a correct semantic search should return)
QUERIES = [
    ("waterproof hiking boots for mountain trails", "footwear"),
    ("lightweight running shoes for marathon training", "footwear"),
    ("stainless steel blender for small kitchens", "kitchen"),
    ("programmable espresso machine for entertaining guests", "kitchen"),
    ("noise-cancelling wireless headphones for remote work", "electronics"),
    ("color-accurate 4K monitor for video editing", "electronics"),
    ("ultralight tent for backcountry trips", "outdoor"),
    ("four-season sleeping bag for alpine conditions", "outdoor"),
    ("illustrated field guide about marine biology", "books"),
    ("annotated biography about ancient history", "books"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data/vectors.lance")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--out", default=".scratch/vector_gt.json")
    ap.add_argument("--sql-out", default=".scratch/vector_queries.sql")
    args = ap.parse_args()

    import torch
    from sentence_transformers import SentenceTransformer

    dev = "cuda" if torch.cuda.is_available() else "cpu"
    model = SentenceTransformer(MODEL, device=dev)
    qtexts = [q for q, _ in QUERIES]
    qvecs = model.encode(
        qtexts, convert_to_numpy=True, normalize_embeddings=True
    ).astype(np.float32)

    ds = lance.dataset(args.data)
    tbl = ds.to_table(columns=["id", "category", "text", "embedding"])
    ids = np.asarray(tbl["id"])
    cats = [str(c) for c in tbl["category"].to_pylist()]
    n = len(ids)
    dim = qvecs.shape[1]
    # FixedSizeList -> flat buffer -> (n, dim); avoids a per-row Python loop.
    emb = np.asarray(tbl["embedding"].combine_chunks().values, dtype=np.float32)
    emb = emb.reshape(n, dim)

    print(f"{n:,} rows x {dim} dims; exact search for {len(qtexts)} queries on {dev}")
    E = torch.from_numpy(emb).to(dev)
    Q = torch.from_numpy(qvecs).to(dev)
    if dev == "cuda":
        torch.cuda.synchronize()
    t0 = time.time()
    # Vectors are unit-norm, so cosine distance = 1 - dot. Matching Lance's
    # "cosine" metric exactly matters: a different convention silently changes
    # the ranking and would make recall look broken.
    sims = Q @ E.T
    dists = 1.0 - sims
    top = torch.topk(dists, args.k, dim=1, largest=False)
    if dev == "cuda":
        torch.cuda.synchronize()
    dt = time.time() - t0
    print(f"exact brute force: {dt*1000:.1f}ms for {len(qtexts)} queries "
          f"({len(qtexts)*n/dt/1e6:.1f}M comparisons/s)")

    idxs = top.indices.cpu().numpy()
    dvals = top.values.cpu().numpy()

    out = []
    for i, (qt, expect) in enumerate(QUERIES):
        neigh = [
            {
                "id": int(ids[j]),
                "category": cats[j],
                "distance": round(float(dvals[i][r]), 6),
            }
            for r, j in enumerate(idxs[i])
        ]
        hit = sum(1 for x in neigh if x["category"] == expect)
        out.append(
            {
                "query": qt,
                "expected_category": expect,
                "vector": [float(x) for x in qvecs[i]],
                "exact_top_k": neigh,
                "category_precision": hit / len(neigh),
            }
        )
        print(f"  {qt[:46]:46} -> {expect:12} precision@{args.k}={hit/len(neigh):.2f}")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({"k": args.k, "rows": int(n), "dim": int(dim), "queries": out}, f)
    print(f"wrote {args.out}")

    with open(args.sql_out, "w") as f:
        for rec in out:
            lit = ", ".join(f"{x:.6f}" for x in rec["vector"])
            f.write(
                f"-- {rec['query']}  (expect {rec['expected_category']})\n"
                f"SELECT id, category, text FROM vectors\n"
                f"ORDER BY cosine_distance(embedding, [{lit}])\n"
                f"LIMIT {args.k};\n\n"
            )
    print(f"wrote {args.sql_out}")

    mean_prec = sum(r["category_precision"] for r in out) / len(out)
    print(f"\nmean category precision@{args.k} = {mean_prec:.3f} "
          f"(1.0 means every neighbour came from the intended category)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
