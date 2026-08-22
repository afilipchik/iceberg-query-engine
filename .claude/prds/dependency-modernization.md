---
name: dependency-modernization
description: Upgrade every dependency to latest where possible, staged and gated; report the impossible ones with evidence
status: completed
created: 2026-08-22T16:34:05Z
---

# PRD: dependency-modernization

## Executive Summary

Inventory (2026-08-22, rustc 1.93): 20+ crates behind latest majors. Three
coupled clusters dominate: (1) the ARROW cluster — arrow/parquet/
arrow-flight 53 -> 59.2, whose pin exists only for lance 0.23, while lance
itself is now at 10.0 (arrow 58/59 era) — so the whole cluster may move
together, taking chrono (pinned 0.4.39 by the old arrow) and tonic with it;
(2) SQLPARSER 0.52 -> 0.62 — ten releases of AST churn against a 3700-line
binder and the distributed AST rewriter; (3) the DIGEST family (sha1/sha2/
md-5/hmac 0.10/0.12 -> 0.11/0.13 move together). Everything else is
independent: apache-avro 0.22, rand 0.10, reqwest 0.13, rustyline 18,
hashbrown 0.17, itertools 0.15, ordered-float 5, statrs 0.19, thiserror 2,
tungstenite 0.30, base64 0.23, criterion 0.8. NOT upgraded by policy:
libc (1.0 is alpha-prerelease). cudarc already latest.

## Success Criteria

1. Every crate on latest stable, or a written impossibility/deferral note
   with the exact error or churn evidence.
2. After each stage: full suites green in ALL feature combos (default,
   lance, gpu, pulsar), TPC-H cell-exact spot check, cluster M1 gate.
3. Benchmarks sanity: SF=1 suite within noise of 1.38s (a perf regression
   from arrow 59 would be reported, not hidden).
4. Published summary in CLAUDE.md; merged; pushed.

## Constraints

- Stage order: independents first (small blast radii), then the arrow
  cluster (one atomic move: arrow+parquet+arrow-flight+tonic+chrono+lance),
  then sqlparser last (largest unknown; timeboxed — if the fallout is
  unbounded it lands partially or is reported as deferred with a churn
  estimate).
- The Cargo.lock add-only discipline is retired FOR THIS EPIC by design —
  the deliverable is a coherent latest-stable lock.

## Out of Scope

- Python .venv dependencies; JDK/Gravitino/Pulsar tarballs (pinned infra).
