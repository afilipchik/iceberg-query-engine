# Right-built semi runtime-filter validation — 2026-09-11

Job94085 runs both disjoint and experimental partial ownership against the frozen521-input right-semi repair snapshot. The driver uses locked/offline `lance,gpu` feature tests, Rayon16, a48GiB memory cap and one compiler job. The exact command is `TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh python3 .scratch/parallel-aggregate-input/run_right_semi_validation.py`.

Validation94085 completed with exit1. Each ownership mode passed1092 library tests/11ignored,125 contract integrations and28 spill/numeric/focused tests. Native/IPC passed58 with2 failures in disjoint,57 with3 failures in partial; six legacy spill failures persist in each mode. Compared with empty-scalar validation16808, no failure names were added or removed. This does not prove unchanged denial boundaries or close those historical resource/correctness gates.

All521 source hashes verify after execution. The [25-file archive](benchmarks/2026-09-11-right-semi-validation/manifest.json) preserves exact commands, eight suite logs, prior logs, failure comparison and the linked source snapshot.

The [repair contract](right-built-semi-runtime-filter-2026-09-11.md) explains scope and independent exact-output regressions. This gate does not measure canonical performance or establish general resource acceptance.
