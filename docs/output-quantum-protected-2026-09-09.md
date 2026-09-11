# Output quantum protected comparison — September 9, 2026

## Outcome

Frozen01bb077a is compared withbd689bf6 using default disjoint ownership on both
sides. All519completed engine outputs are independently typed-correct;
514meet their fresh timing ceilings and5correct outputs are late.
Preserved outcomes include10timeouts,87not-run slots and
0invalid reference blocks. This does not certify a complete suite.

CanonicalQ9/Q12 studies remain incomplete. Every complete canonical study has an
upper95%ratio below1.04. Q19 has a small measured slowdown of2.10% (interval
1.002642–1.039478), not a confirmed greater-than10% regression. CustomQ1's upper
bound is1.052021. CustomQ6's upper bound1.136243 does **not** clear the10% bound.
Its mean block ratio1.052437 is not sufficient to waive uncertainty.

| Workload/query | Candidate/control ratio | Block-bootstrap95% interval |
|---|---:|---|
|Canonical raw q01|0.933067|0.895729–0.972300|
|Canonical raw q02|0.996653|0.974424–1.009828|
|Canonical raw q05|1.015975|0.995965–1.039353|
|Canonical raw q09|Incomplete|Not estimated|
|Canonical raw q10|0.936338|0.917745–0.950160|
|Canonical raw q12|Incomplete|Not estimated|
|Canonical raw q13|0.931372|0.926183–0.936589|
|Canonical raw q19|1.021008|1.002642–1.039478|
|Canonical raw q20|0.994894|0.978975–1.011073|
|Custom memory q01|1.017357|0.983836–1.052021|
|Custom memory q06|1.052437|0.987050–1.136243|

## Conditions and limitations

Canonical raw Parquet coversQ1/Q2/Q5/Q9/Q10/Q12/Q13/Q19/Q20. Four fresh blocks
use one warmup and six measured pairs per side, balanced startup and request
order, sixteen threads,4GiB query/12GiB process caps and affinity0–15. The custom
memoryQ1/Q6 workload is separately labelled, using original custom float SQL/data,
four threads and4/8GiB limits. It is not canonical SF10 or GPU execution.
Every block calibrates DuckDB afresh, validates against an independent typed
oracle, and applies a10×query ceiling. The48GiB scopes have zero swap.

Q9 times out during warmup on both binaries in all four canonical blocks with
ceilings around5.0–5.3s. Q12 has mixed completion/timing failures. Their missing
slots are retained and receive no full-study ratio. Complete-block ratios above
are observations; do not infer causality on unchanged raw routes from a lower
number alone. The directly attributed native Q18 output improvement remains a
[separate experiment](output-quantum-native-2026-09-09.md).

Canonical job43611 exits1 because required slots fail; custom32643 exits0 with
all112outputs correct/gated. Source511inputs, both binary hashes, driver, harness
and data verify after each run. Correctness and timing completion are separate.
The manifest/source archive is frozen; no engine or harness change occurred.

The prior short-query null-control failure qualifies alternating-request Q6
precision. Fresh identical01bb077a null91329 now exits1: all32,784outputs correct/
gated and all16windows complete with sufficient exposure, but95%interval
0.976383–1.130588 fails the upper1.10gate. The dependent candidate run is not
executed. Its33,055-file archive verifies511inputs. This leaves Q6's regression
bound unresolved; see [control evidence and next measurement work](output-quantum-short-query-control-2026-09-09.md).

Drivers: `measure_output_quantum_protected.py` and `measure_output_quantum_smoke.py`
in `.scratch/parallel-aggregate-input`, run through `scripts/claude-safe-build.sh`
with repository scratch asTMPDIR. [Immutable archive](benchmarks/2026-09-09-output-quantum-protected/manifest.json)
preserves all samples/failures/plans, dataset manifests, independent oracles,
drivers, harness and source. Temporary spill payload omissions are listed.

Full provider/residency/resource/concurrency acceptance for01bb077a remains open.
