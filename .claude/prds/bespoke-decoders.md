---
name: bespoke-decoders
description: Bespoke hot-type parquet decoders (PARITY-PLAN Rewrite 3.3) — the last same-premise like-for-like lever; kill-switch microbenchmark first
status: completed
created: 2026-08-20T03:28:57Z
---

# PRD: bespoke-decoders

Attack the ~8-10s decode share of the like-for-like gap (engine 65.1s vs
DuckDB 40.1s on identical parquet) with hand-rolled decoders for the hot
physical types, IF a microbenchmark shows arrow-rs meaningfully above
the raw page-pipeline floor. STOP if arrow-rs is within ~1.2x on the
dominant columns.
