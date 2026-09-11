# GPU residency benchmark gate — 2026-09-06

The frozen587 floating-point smoke has39 device executions and one measured CPU fallback among40 GPU-enabled requests. This is mixed routing coverage, not a fully resident-device run. Its preserved trace shows five numeric-column uploads reaching24MB, a second not-ready request, then group-code completion adding600KB before successful device execution. No upload failure is needed to produce this race.

Current `scripts/benchmark/run.py` performs one warmup. GPU readiness requires both numeric columns and grouping codes, and each GpuAggExec fixes its CPU/GPU decision when selected. Trace-span classification preserved the fallback correctly, but warmup count is not a residency contract. Evidence: [frozen587 counters](benchmarks/2026-09-06-join-index-ownership/gpu-execution-summary.json) and the full GPU archive's smoke/gpu/s1-engine.stderr.

## Implementation sequence

1. Add an explicit manifest residency policy. Keep automatic/mixed routing separate from device-resident measurements. Preserve old samples and their classifications.
2. Add an asynchronous worker preparation request built from actual GPU operators and their dependencies. Request numeric columns and grouping codes once, then await completion acknowledgments within a deadline. Avoid fixed sleeps and repeated SQL warmups.
3. Return structured preparation outcomes: resident, unsupported plan/type/null/range/group count, insufficient capacity, upload failure, worker failure or timeout. Unsupported canonical decimal coverage must terminate explicitly, not warm forever.
4. Hold dependency leases through dispatch and completion so readiness cannot be invalidated by eviction. If a first implementation instead isolates each query in a fresh worker, drains preparation and checks immediately before dispatch, document that narrower isolation contract; a snapshot alone is not a general residency guarantee.
5. Report preparation latency/bytes separately from resident query latency. Automatic cold-start samples retain production CPU fallback. A true cold-device track must include preparation through device completion in its declared timing boundary.
6. Correlate structured device events with request and operator identities. Require all expected device operators to execute for every resident sample. A fallback remains a recorded correctness/timing outcome but fails residency certification; never discard or rerun it away.

## Acceptance tests and limits

Deterministically delay group-code completion beyond numeric uploads. Test unsupported decimals, upload errors, worker shutdown, preparation timeout and eviction between readiness and dispatch. A fitting hardware fixture must execute every resident sample on device. Keep cold/mixed outcomes separately visible.

This is a source-grounded follow-up design, not implemented behavior. It does not resolve [GPU hard-admission gaps](gpu-hard-admission-design-2026-09-06.md), and the small floating-point fixture does not establish canonical SQL GPU acceleration.

[Source593 implementation contract](gpu-resident-session-implementation-2026-09-06.md) specifies the complete serial resident-session slice, worker outcomes, cancellation, exact request/operator accounting and harness gates. It remains an unimplemented design; a readiness-only barrier is insufficient.
