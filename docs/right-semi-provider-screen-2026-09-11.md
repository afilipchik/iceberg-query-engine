# Frozen68912c23 canonical provider screen — 2026-09-11

Sequence82829 terminal1; supplemental audit31858 terminal0 independently validates330completed outputs,247measured and83warmups. There are247/264valid measured pairs:raw66,native58,Iceberg63,Lance60. All521 source inputs and binary/driver/harness hashes verify after execution. No provider leadership is certified.

Raw completes22queries with geomean2.713836 and suite2.975360 versus DuckDB,0wins,worstQ17ratio8.309851. Engine suite14,138.324133ms versus DuckDB4,751.803480ms. This is one session,3samples,16threads CPU0–15,default disjoint,4/12GiB query/process,GPUoff. Prior release screens are not paired causal controls.

NativeQ1/Q6 fail warmup query gates; Q12 first measured output is correct at603.013895ms, second times out against1282.679951ms and third is not run. Native58completed measurements validate. IcebergQ9 DuckDB cal0 crashes with exit-11 after successful oracle/warmup; the engine is not run without a valid ceiling. LanceQ1 warmup times out; Q9 DuckDB cal0 refuses134217728bytes at133.900272ms. Those reference failures are distinct from engine correctness errors. All remaining completed outputs validate.

The shared sequence scope records31,709,450,240bytes peak,zeroOOM/max events and swap disabled. This peak includes preceding diagnostics, not provider-only RSS. Full resource, concurrency and multi-session acceptance remain open.

Commands, samples, failures, plans, independent output checks and provenance are retained in the [archive](benchmarks/2026-09-11-right-semi-providers/manifest.json). Source snapshot: right-semi-repair. The [same-source aggregate control](aggregate-route-control-2026-09-11.md) identifies a large generic grouped-ingestion gap; these suite ratios do not isolate its cause.
