#!/usr/bin/env python3
"""Generate a multi-version Lance dataset for time-travel tests.

Lance commits a new manifest on every write and keeps the old ones, so the same
path holds every snapshot. This builds three deliberately DIFFERENT versions so
a reader that quietly serves the latest one instead of the requested one fails
the test rather than passing it by coincidence:

    v1  create     ids 1..3         3 rows
    v2  append     ids 4..5         5 rows
    v3  overwrite  id 9 only        1 row

Row counts 3 / 5 / 1 are mutually distinct, and v3 is SMALLER than v1, so no
"reads the newest" or "reads the biggest" bug can masquerade as correct.

Usage:
    .venv/bin/python scripts/lance_versions_gen.py --out data/versioned.lance
"""

import argparse
import shutil
from pathlib import Path

import lance
import pyarrow as pa


def tbl(ids, vals):
    return pa.table({"id": pa.array(ids, pa.int64()), "v": pa.array(vals, pa.string())})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/versioned.lance")
    args = ap.parse_args()

    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    out.parent.mkdir(parents=True, exist_ok=True)

    lance.write_dataset(tbl([1, 2, 3], ["a", "b", "c"]), str(out))
    lance.write_dataset(tbl([4, 5], ["d", "e"]), str(out), mode="append")
    lance.write_dataset(tbl([9], ["z"]), str(out), mode="overwrite")

    ds = lance.dataset(str(out))
    print(f"wrote {out}  latest version={ds.version}  rows={ds.count_rows()}")
    for v in sorted(ds.versions(), key=lambda x: x["version"]):
        n = lance.dataset(str(out), version=v["version"]).count_rows()
        print(f"  v{v['version']}: {n} rows")


if __name__ == "__main__":
    main()
