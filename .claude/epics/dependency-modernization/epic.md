---
name: dependency-modernization
status: in-progress
created: 2026-08-22T16:34:05Z
updated: 2026-08-22T19:26:29Z
progress: 60%
prd: .claude/prds/dependency-modernization.md
github: (will be set on sync)
---

# Epic: dependency-modernization

Staged upgrade to latest. Tasks:
1. Independents batch (thiserror2, itertools, ordered-float, statrs,
   hashbrown, base64, tungstenite, rand, digest family, rustyline,
   criterion, apache-avro, reqwest) — compile-fix + suites per batch.
2. Arrow cluster atomically: arrow/parquet/arrow-flight 59.2 + tonic +
   chrono latest + lance 10 — API fallout across storage/flight/ipc;
   all gates + benchmark sanity.
3. sqlparser 0.62 — binder/plan rewriter churn; timeboxed, honest report.
4. Publish verdicts (upgraded / impossible / deferred) + merge.

## Tasks Created
- [x] 001.md - Independent crates batch (parallel: false)
- [x] 002.md - Arrow cluster + lance + chrono + tonic (parallel: false)
- [ ] 003.md - sqlparser to latest (parallel: false)
- [ ] 004.md - Publish verdicts and merge (parallel: false)
- [x] 005.md - OOM-safe build sandbox (inserted 2026-08-22 after the
      cargo/oomd incident; all remaining builds go through
      scripts/claude-safe-build.sh)
