#!/usr/bin/env python3
"""Generate a small Lance dataset exercising NESTED column types.

Real LanceDB tables routinely carry struct metadata blobs and list columns
alongside their vectors, so the engine's Lance reader has to survive them.
This builds a deterministic fixture the Rust tests assert against:

    id      int64          1..N, the join/order key
    name    string
    meta    struct<source: string, score: double, active: bool>
    tags    list<string>
    scores  list<int32>
    vec     fixed_size_list<float, 4>     (a vector column, already supported)
    deep    struct<inner: struct<a: int32, b: string>, n: int64>

`meta` and `deep` are the Phase-1 targets: struct columns, including a nested
struct, plus a struct field that is NULL for some rows (row 3) so null handling
in the passthrough path is covered.

Usage:
    .venv/bin/python scripts/lance_nested_gen.py --out data/nested.lance
"""

import argparse
import shutil
from pathlib import Path

import lance
import pyarrow as pa

N = 12


def build_table() -> pa.Table:
    ids = list(range(1, N + 1))
    names = [f"row-{i}" for i in ids]

    # struct<source, score, active>; row 3 (index 2) is a NULL struct entirely,
    # row 5 (index 4) has a NULL score inside a present struct.
    meta = pa.array(
        [
            None
            if i == 2
            else {
                "source": f"src-{i % 3}",
                "score": None if i == 4 else round(0.5 * i, 3),
                "active": (i % 2 == 0),
            }
            for i in range(N)
        ],
        type=pa.struct(
            [
                pa.field("source", pa.string()),
                pa.field("score", pa.float64()),
                pa.field("active", pa.bool_()),
            ]
        ),
    )

    tags = pa.array(
        [None if i == 6 else [f"t{i % 4}", f"u{i % 2}"] for i in range(N)],
        type=pa.list_(pa.string()),
    )
    scores = pa.array(
        [[i, i * 2, i * 3] for i in range(N)],
        type=pa.list_(pa.int32()),
    )
    vec = pa.array(
        [[float(i), float(i) + 0.5, float(i) + 1.0, float(i) + 1.5] for i in range(N)],
        type=pa.list_(pa.float32(), 4),
    )
    deep = pa.array(
        [{"inner": {"a": i, "b": f"b{i}"}, "n": i * 100} for i in range(N)],
        type=pa.struct(
            [
                pa.field(
                    "inner",
                    pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())]),
                ),
                pa.field("n", pa.int64()),
            ]
        ),
    )

    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "name": pa.array(names, type=pa.string()),
            "meta": meta,
            "tags": tags,
            "scores": scores,
            "vec": vec,
            "deep": deep,
        }
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/nested.lance")
    args = ap.parse_args()

    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    out.parent.mkdir(parents=True, exist_ok=True)

    table = build_table()
    lance.write_dataset(table, str(out))

    ds = lance.dataset(str(out))
    print(f"wrote {out}  rows={ds.count_rows()}  version={ds.version}")
    print(ds.schema)


if __name__ == "__main__":
    main()
