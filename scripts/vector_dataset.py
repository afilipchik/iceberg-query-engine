#!/usr/bin/env python3
"""
Build a Lance dataset with REAL sentence-embedding vectors, computed on the GPU.

Random vectors can only prove that k-NN arithmetic is correct. Real embeddings
additionally prove the search is *semantically* meaningful — a query for
"waterproof hiking boots" must return footwear, not blenders — which is what
makes the vector-search path testable as a feature rather than as arithmetic.

The corpus is generated from templates rather than downloaded, so the dataset is
reproducible offline and every row has a known category. That known category is
what the semantic assertions in scripts/vector_ground_truth.py check against.

Usage:
    .venv/bin/python scripts/vector_dataset.py --rows 200000 \
        --out data/vectors.lance
"""

import argparse
import os
import random
import shutil
import sys
import time

import lance
import numpy as np
import pyarrow as pa

MODEL = "sentence-transformers/all-MiniLM-L6-v2"  # 384-dim, small and fast

# (category, [nouns], [adjectives], [use-cases]) — the vocabulary is disjoint
# enough between categories that a semantic query should not cross over.
CATEGORIES = [
    (
        "footwear",
        ["hiking boots", "running shoes", "sandals", "winter boots", "trail runners"],
        ["waterproof", "lightweight", "insulated", "breathable", "cushioned"],
        ["for mountain trails", "for marathon training", "for wet weather"],
    ),
    (
        "kitchen",
        ["blender", "espresso machine", "cast iron skillet", "food processor", "kettle"],
        ["stainless steel", "compact", "programmable", "non-stick", "high-power"],
        ["for small kitchens", "for daily cooking", "for entertaining guests"],
    ),
    (
        "electronics",
        ["wireless headphones", "mechanical keyboard", "4K monitor", "webcam", "SSD drive"],
        ["noise-cancelling", "backlit", "color-accurate", "low-latency", "high-capacity"],
        ["for remote work", "for gaming", "for video editing"],
    ),
    (
        "outdoor",
        ["tent", "sleeping bag", "camping stove", "backpack", "headlamp"],
        ["ultralight", "four-season", "weather-resistant", "packable", "rechargeable"],
        ["for backcountry trips", "for family camping", "for alpine conditions"],
    ),
    (
        "books",
        ["novel", "cookbook", "biography", "field guide", "textbook"],
        ["illustrated", "annotated", "bestselling", "award-winning", "revised"],
        ["about ancient history", "about marine biology", "about modern architecture"],
    ),
]


def build_corpus(n: int, seed: int = 7):
    rng = random.Random(seed)
    texts, cats = [], []
    for i in range(n):
        cat, nouns, adjs, uses = CATEGORIES[i % len(CATEGORIES)]
        text = (
            f"{rng.choice(adjs)} {rng.choice(nouns)} {rng.choice(uses)}"
            f" — model {rng.randint(100, 999)}"
        )
        texts.append(text)
        cats.append(cat)
    return texts, cats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--out", default="data/vectors.lance")
    ap.add_argument("--batch-size", type=int, default=2048)
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--num-partitions", type=int, default=0, help="0 = sqrt(rows)")
    ap.add_argument("--num-sub-vectors", type=int, default=48)
    args = ap.parse_args()

    import torch
    from sentence_transformers import SentenceTransformer

    device = args.device if torch.cuda.is_available() else "cpu"
    if device.startswith("cuda"):
        print(f"GPU: {torch.cuda.get_device_name(0)}  torch {torch.__version__}")
    else:
        print("WARNING: falling back to CPU; embedding will be slow")

    print(f"Building corpus of {args.rows:,} product descriptions...")
    texts, cats = build_corpus(args.rows)

    print(f"Loading {MODEL} on {device}...")
    model = SentenceTransformer(MODEL, device=device)

    t0 = time.time()
    emb = model.encode(
        texts,
        batch_size=args.batch_size,
        convert_to_numpy=True,
        normalize_embeddings=True,  # unit norm: cosine distance == 1 - dot
        show_progress_bar=True,
    ).astype(np.float32)
    dt = time.time() - t0
    dim = emb.shape[1]
    print(f"Embedded {len(texts):,} texts in {dt:.1f}s "
          f"({len(texts)/dt:,.0f} texts/s) -> dim {dim}")

    tbl = pa.table(
        {
            "id": pa.array(range(len(texts)), pa.int64()),
            "category": pa.array(cats),
            "text": pa.array(texts),
            "price": pa.array((np.arange(len(texts)) % 500 + 10).astype(np.float64)),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(emb.ravel()), dim
            ),
        }
    )

    if os.path.exists(args.out):
        shutil.rmtree(args.out)
    ds = lance.write_dataset(tbl, args.out, max_rows_per_file=100_000)
    print(f"Wrote {ds.count_rows():,} rows to {args.out} "
          f"({len(ds.get_fragments())} fragments)")

    parts = args.num_partitions or max(1, int(np.sqrt(len(texts))))
    t0 = time.time()
    ds.create_index(
        "embedding",
        index_type="IVF_PQ",
        num_partitions=parts,
        num_sub_vectors=args.num_sub_vectors,
        metric="cosine",
        accelerator=device if device.startswith("cuda") else None,
    )
    print(f"Built IVF_PQ index ({parts} partitions, {args.num_sub_vectors} "
          f"sub-vectors, cosine) in {time.time()-t0:.1f}s")
    print("indices:", [(i["name"], i["type"]) for i in ds.list_indices()])
    return 0


if __name__ == "__main__":
    sys.exit(main())
