# Direct-input candidate: canonical SF10 providers — September 8, 2026

Frozen binary `a023079ff45b8159614b4a313a7874a4599b355b85ef1cfa449996465d6df0e2`
uses direct fixed-width aggregate input reads. The [paired study](inline-aggregate-input-2026-09-08.md)
preserves493 source hashes, the single-file change and its separate CPU effects.
Provider84971 is terminal1; independent audit86520 is terminal0. Source and harness
were unchanged throughout measurement.

All321 completed engine outputs pass independent full-oracle typed validation:
240 measured outputs and81 warmups.240/264 measured pairs meet all original gates.

| Provider | Valid pairs | Completed warmups | Remaining failure |
|---|---:|---:|---|
| Raw Parquet | 57/66 | 20 | Q9/Q13 warmup timeouts; Q12 late warmup |
| Native | 60/66 | 20 | Q1/Q13 warmup timeouts |
| Iceberg | 66/66 | 22 | None in this screen |
| Lance | 57/66 | 19 | Q1/Q13 warmup timeouts; Q9 invalid reference calibration |

Raw Q12 is correct but its warmup takes1030.127043ms against965.645961ms. Six
warmup timeouts, that late warmup and the Lance reference failure leave24 measured
engine slots not run. No completed measured engine output is late or wrong. These
failures remain failures; single-session completion differences from preceding
binaries cannot establish a causal speedup or regression.

Conditions: all22 canonical SF10 queries,16threads,4GiB query/12GiB process, three
samples in one session, matched provider data/schema and a fresh10× DuckDB ceiling.
Tracks execute sequentially inside the48GiB/no-swap wrapper with repository TMPDIR.
Full commands, feature/runtime provenance and plans are preserved. Scope peak
25160314880bytes with zero max/OOM/kill events is cumulative host containment
telemetry, not exact query RSS or proof of query-wide admission.

## Complete Iceberg result and its limits

Within this one-session Iceberg screen, engine/DuckDB suite-sum ratio is0.595808
(20397.843ms/34235.593ms), geometric mean0.380469, with18 wins of22 queries.
The worst ratio is Q13 at2.048083; Q9/Q12/Q16 also lose. Every sample and per-query
ratio is preserved in the report. The reported bootstrap intervals are conditional
on this session's samples, not the required independent-session confirmation.

This comparison uses the matched Iceberg reader paths: DuckDB1.4.4's native
`iceberg_scan` extension1095c1fa and the engine's snapshot/provider route over the
same validated files. It is format-specific and must not be presented as a raw
Parquet or general CPU-kernel speedup. No three-session, history/delete/evolution,
resource/concurrency or held-out certification is established. The worst-query
and broader leadership gates remain unmet despite the complete Iceberg screen.

Evidence: `benchmarks/2026-09-08-inline-input-providers/`, with exact commands,
plans, samples, failures, independent completed-output/warmup checks, provenance
and the frozen source archive link. [Residency evidence](inline-input-residency-2026-09-08.md)
is separate: custom required GPU succeeds, while canonical mixed GPU is blocked
by incomplete CPU control. The next source work is the IPC extent reproducer and
repair; its future changes must not be attributed to this frozen binary.
