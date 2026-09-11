# Repeated validity candidate: feature/resource validation — 2026-09-11

Validation23407 completed with exit1. Both default disjoint and experimental partial ownership passed1094 library tests/11ignored,125 contract integrations and28 spill/numeric/focused tests. Native/IPC has58passes/2failures in disjoint and57passes/3failures in partial. Six legacy spill failures remain in each mode. Comparison with right-semi validation94085 adds/removes no failure names; this does not certify unchanged denial boundaries or close the known failures.

Locked/offline lance,gpu features,Rayon16,48GiB memory cap,one build job,repository TMPDIR. Exact command: `TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh python3 .scratch/parallel-aggregate-input/run_validity_repeated_validation.py`. All521 inputs verify after execution against the candidate source archive. Full commands,environments,eight suite logs,prior logs and failure comparison are retained in the [archive](benchmarks/2026-09-11-validity-repeated-validation/manifest.json).

Sequence66120 accepted the failure comparison and is building the optimized candidate. The120-output paired diagnostic is queued afterward. Source remains frozen. No optimized speedup or full resource/leadership acceptance is claimed.
