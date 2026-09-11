# Capability availability changes execution cost — 2026-09-11

The mixed-numeric compiler candidate changes more than expression evaluation. The
full runtime queue evidence shows a different prepared input route even when logical
and physical plan strings match. This qualifies the earlier QE_COMPILE attribution:
the switch measures the combined capability/decoder/evaluator choice, not a pure
compiled-versus-interpreted expression loop.

In both three-binary blocks, generic rawQ6 emits1,139,264selected rows. Baseline13210a20
uses copied input,16slots and458producer batches. Both97e53169 andece6a4d6 use admitted
input,16slots and7323batches. Parallel slot count does not increase, while batch count
increases15.99x. NativeQ6 keeps copied input,1slot and916batches for all binaries.
Resident4Q6 changes copied to admitted input with4slots and916batches throughout.
Default morsel rawQ6 has no queue_prepare/queue_producer events in this trace; absent
events are not evidence of zero input work or the same runtime decoder.

The code explains the generic raw route transition: StreamingParquetScanExec's
prepare_admitted_queue_input delegates to admitted::prepare. Reader::open requires
an admitted predicate compiler; unsupported predicates decline this optional route
before page reads. Expanding compilation eligibility therefore admits a route that
previously declined. The admitted reader uses ROWS=8192 and VALUE_BYTES=65536 even
when the scan's copied route was configured for a different reader quantum. Its
factory is distinct from uses_admitted_reader, which describes ordinary execute's
memory-pressure route. Capability availability alone is not a cost comparison.

Float dispatch repair92933 completes42typed-correct outputs/792join traces and
14unchanged plan groups,with453verified archive files/526inputs. Compared with the
regressing97e53169, generic rawQ6 ratio0.928567 improves both blocks. Compared with
pre-coercion13210a20, it remains2.074367; default rawQ6 remains1.086430 andresident4
Q6 remains1.263582. Native results are noisy; preserve individual blocks. The overall
candidate is not accepted. [All ratios and conditions](float-dispatch-triage-2026-09-11.md).

Profile44421 terminal0 supplies67stopped-thread snapshots and one typed-correct
output. There are104evaluator leaves across threads:72eval_chunk,32closures. Mapping
the exact new binary's sql_impl static address0x15eb3d0 to its initial breakpoint
normalizes PCs:32leaves fall in the decimal operand reader0x5fbcff0–0x5fbd193;
23fall in the decimal-to-float conversion loop0x5fb6000–0x5fb603e. These are sampled
counts, not CPU percentages. Packed float comparisons are now present in the optimized
evaluator, corroborating the dispatch repair, but the route/batch change remains.
[22-file profile and route-audit archive](benchmarks/2026-09-11-float-dispatch-profile/manifest.json)
links the original timed queue traces and source526.

Next steps must separate these costs. Preserve hard memory admission and terminal
source-error semantics; do not force an unadmitted reader merely to erase a regression.
Record actual prepared route, decoder and quantum alongside plan names. Measure
expression kernels on the same batches and ownership route before attributing the
whole query difference to them. Investigate admitted reader quantum and multi-column
working-space coordination together with the [first-batch refusal](first-batch-refusal-ledger-2026-09-11.md).
Choosing a safe route should consider its decoder, batch quantum, effective parallelism
and working-memory requirement, rather than treating preparation success as a speed
proof. Any scheduling estimate remains costing information, not memory or SQL proof.
No further engine source change has been applied after the dispatch candidate.
