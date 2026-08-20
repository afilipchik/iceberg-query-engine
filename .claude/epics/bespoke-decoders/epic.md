---
name: bespoke-decoders
status: completed
created: 2026-08-20T03:28:57Z
updated: 2026-08-20T03:28:57Z
progress: 100%
prd: .claude/prds/bespoke-decoders.md
github: (will be set on sync)
---

# Epic: bespoke-decoders

## Epic close-out (2026-08-20) — REFUTED by the kill-switch microbenchmark

`examples/decode_bench.rs` (SerializedPageReader+decompress floor vs
ParquetRecordBatchReader, 67M rows × 64 SF=100 row groups, warm):

| column | type/encoding | arrow-rs | vs raw floor |
|---|---|---|---|
| l_extendedprice | f64 PLAIN+snappy | 1.7 ns/row | **0.47x — FASTER than the floor** |
| l_orderkey | i64 RLE_DICTIONARY | 3.4 ns/row | 1.06x |
| l_shipdate | i32 PLAIN | 1.0 ns/row | 2.05x (≈0.3s absolute per full pass) |

arrow-rs 53 already decodes the dominant fixed-width columns at
memcpy-class speed; a bespoke decoder has no meaningful headroom. The
8-10s decode delta the IPC A/B measured is the PIPELINE (compressed
page reads + snappy pass + allocation + batch assembly), which the v2
IPC sidecar removes wholesale via mmap zero-copy — that lever is
already shipped. The like-for-like premise's decode gap is therefore a
storage-format cost, not an implementation gap.

Fifth measured-and-rejected rewrite of the program (after take-based
lance materialization, unconditional lance pushdown, radix joins, radix
aggregation) — an hour of microbenchmark against weeks of decoder work.
