# Preserved checkpoint narrative — September 9, 2026

Historical snapshot before consolidation. Superseded pending jobs below are not
current status; consult [the working guide](../AGENTS.md).

## Current checkpoint — 2026-09-09

Frozen1efb2554 honors the configured aggregate spill threshold instead of a
separate quarter-root group-count cap. Checked reservations and prepared spill/
exact-row retry remain. Red5041 reproduces5.45MB unnecessary spill at16MiB;
green82548 passes fitting filtered state without spill and retained full output.
Broad93472 passes1,033 library tests/11ignored and36 integrations; spill80448
remains6pass/7same failures. Release68533 terminal0 in8m51s verifies507 inputs,
only two changed versus322c8042;8-file correctness archive verified.
Matched Lance84734 terminal0:4correct outputs, observed paired ratio0.23712,
zero candidate spill at4GiB. Diagnostic48836 has optional GPU startup failures
followed by correct CPU fallback; explicit GPU-disabled repeat44630 passes all3
outputs without panic (raw0.97s/native5.72s/resident32GiB5.62s). Archives222/182
files verify. Protected22140 terminal1:527correct/521gated,6late,8timeouts,
81not_run;Q9/Q12 incomplete. Q6follow-up35348 has208correct/gated but upper95
ratio1.21064 fails uncertainty acceptance. Archives1202/460files verify.
Identical-binary Q6 control33603 terminal1:208correct/gated, ratio1.09628
(95%1.03061–1.17850),460-file archive verified; precision remains open.
Provider43384 terminal1/audit57471 terminal0:319completed typed-correct,
237/264strict valid pairs (raw60/native56/Iceberg63/Lance58);1400files verified.
New harness isolated_windows.py enforces one live engine/window, fixed request
counts, measured-duration sufficiency, cleanup and primary mean-time ratios.
Ten lifecycle tests pass; broad28452 runs136tests,134pass/2skip. No engine/source
or dependency change. Pilot72600 terminal0:66correct/gated engine outputs,245-file archive verified.
Null50497 terminal1:32784correct/gated, all exposure gates pass, but95%ratio
0.94767–1.13276 fails precision;33055archive files verify. Fatal isolation-error
propagation is repaired after a red regression. Green61660:137tests/2skips,
162-file harness archive verifies. Physical-core null60984 terminal0:32784correct/gated,16graceful windows,
95%0.90747–1.03210 passes;33055files verify. Candidate97043 terminal0 under that same new protocol:32784correct/gated,
95%0.94622–1.04071 passes,33055archive files verify. Prior failed conditions
remain separately recorded. Clock diagnostic74684 terminal0 has correct output but a timer-change warning
and weak output-stage coverage;26-file qualified archive verifies. Residency92429
terminal1/audit34442 terminal0:252 completed typed-correct outputs,206 valid measured
pairs; canonical IPC/CPU each63/66 with Q1 warmup timeout. Canonical GPU not executed;
custom required GPU40/40 has request-scoped device evidence. Archive1098files verifies.
Current source adds admitted borrowed key_rows/bound_arrays.rs bindings inside
prepared-key chunks. Fixed primitive metadata/downcasts bind once per batch/chunk;
canonical bytes, float normalization and checked dictionary/nested fallback remain.
Focused46830 passes3 independent regressions. Default21188 passes974library/3ignored
and10integrations; lance,gpu77560 passes1036library/11ignored and36integrations.
Spill76725 remains6pass/7same failed names. Nine-file correctness archive verifies
508 source inputs. Release43237 terminal0 in8m52s freezes0f30c946 with508 inputs;
only key_rows.rs/prepared_keys.rs and the new bound_arrays.rs differ from1efb2554.
Matched Lance49598 terminal0:four typed-correct outputs, observed ratio0.91304
(8.70% lower), no candidate spill; two-block instrumented diagnostic only.
CPU diagnostic63930 terminal0:raw0.954s/native5.374s/resident32GiB5.200s,
all3typed-correct without GPU/panic. Archive223files/508source inputs verifies.
Protected2204 terminal1:511completed typed-correct/504gated,7late,9timeouts,
96not_run; Q9/Q12 incomplete. Q10 ratio0.97314 and Q13 ratio0.93371 have95%
intervals below1; Q2 ratio1.02126 is slower. All complete upper bounds below1.059.
Archive1180files/508inputs verifies. Provider5434 terminal1/audit70354 terminal0:
325completed typed-correct outputs;242/264valid pairs(raw60/native60/Iceberg65/Lance57).
Five warmup timeouts, one late correct warmup and two reference refusals remain.
Archive1415files verifies. CPU Q1 attribution98218 terminal0:raw0.633s/native11.930s/
resident11.193s, all3typed-correct. Native state processing6.950s versus output0.035ms.
Actual four-key hash routing sends99.35% of59.14M rows to two owners despite16threads;
189-file diagnostic archive verifies. This is a structural low-cardinality/skew
parallelism limit, not an output cost. Residency69745 terminal1/audit95178 terminal0:
252completed outputs correct, IPC/CPU63/66 each; canonicalGPU not executed, custom
CPU/GPU40/40 each with required-device evidence. Archive1098files verifies.
Fresh fixed-window null45941 terminal0:32784correct/gated outputs,95%ratio
0.900449–1.054570 passes;33055archive files verify. Candidate4841 terminal0:32784correct/gated,95%ratio
0.903637–1.062568 passes the10% bound;33055archive files verified before edits.
Optional output validity is repaired after red95500; green54743 passes3tests.
Broad61944 passes1039library/11ignored and42integrations; spill1471 remains6/7.
Ten-file correctness archive verifies508inputs, only admitted_output.rs changed
versus0f30c946. No dependency/routing/budget change. Partition typed-cause
red50825 is repaired by shared execution/collection context; green64859 passes3.
Cap classifier red24697 rejects actual typed errors and accepts lookalike text;
it now uses typed causes. Broad97446 terminal0:1042library/11ignored,
61integrations and9cap-example passes. Retained spill95510 passes both contracts:
256KiB typed refusal and16MiB/2%-policy completion of14785independently checked
groups,2741595spill bytes,3057480retained bytes. Full76280 is8pass/6fail after
replacing the impossible success assertion; six other failures remain open.
See docs/partition-error-and-spill-contract-2026-09-09.md and
docs/optional-output-validity-2026-09-09.md. Sixteen-file typed-error/spill archive verifies508inputs. All jobs are terminal. The resource changes were followed by experimental releasecd8098d5 below;
full performance certification remains open and frozen0f30c946 stays distinct. The cross-owner merge component now feeds experimental live balanced ownership:
QE_AGG_OWNERSHIP=partial splits retained rows across up to16workers; default
ownership stays disjoint/up to4. Local resident partials and original spill runs
transfer directly, without local final merges. The global run ledger is admitted
before input; startup denial may choose one owner before consumption. Focused64700
passes1/4/16-worker and actual local-spill merging. Default47088 passes1050library/
11ignored and10integrations; partial41773 adds startup-fallback coverage and passes
1051library/11ignored plus12integrations. Full6177:both modes8pass/6same failures.
Ten-file correctness archive verifies509source inputs; no dependency change.
Release76858 terminal0 in8m53s freezescd8098d5/509inputs. Native scaling4049
terminal0 validates24outputs and actual ownership. At16threads Q1 observes46.99%
lower time, but Q18 observes40.80%higher time. Q18 ingestion improves while finish
outside output grows40.348→3642.100ms: serial cross-owner reduction is the next
bottleneck. Archive293files/509inputs verifies; scope peak7741480960,zeroOOM/max.
Keep default disjoint; next parallelize final reduction with admitted routing and
exact canonical-key ownership. These two-block diagnostics are not DuckDB timing
acceptance. See docs/balanced-aggregate-native-scaling-2026-09-09.md.
Current source adds parallel_merge.rs: pre-admitted bounded canonical-key routing
feeds disjoint resident reducers; local spill selects the pre-admitted serial
consumer before reduction. Prepared reducer spooling frees sibling resident
capacities before decode/output. Default disjoint ownership and dependencies
remain unchanged. Focused85728 passes20; broad2924 passes1054library/11ignored
and12integrations; default15122 passes1054library/11ignored. Full6026 retains
8pass/6same failures in both modes. Correctness archive11files verifies510inputs.
Release97747 terminal0 in8m53s freezesbd689bf6. Paired92740 terminal0 validates
16outputs/traces; Q18 improves27.46%/34.61% at4/16threads versus serial reduction,
Q1 close to unchanged. Same-binary ownership81286 terminal0 validates16outputs/
traces; partial vs disjoint at16threads improvesQ1 48.48%,Q18 12.84%, but4thread
Q18 regresses16.17%. Both253-file measurement archives verify510inputs; zero
spill/OOM/max events. Those jobs are terminal. Full provider81715 terminal1 and audit4915 terminal0:
all331completed outputs typed-correct (247measured/84warmups),247/264valid pairs:
raw63/native57/Iceberg66/Lance61. Four warmup timeouts, one late correct nativeQ11
warmup and one LanceQ9 DuckDB134217728-byte refusal remain. Iceberg single-session
geomean0.415091/suite0.592510,17wins,worst1.830853; leadership still uncertified.
Archive1426files verifies510inputs; scope peak20523143168,zeroOOM/max events.
All jobs terminal; default disjoint remains. See
[provider screen](docs/parallel-reduction-provider-screen-2026-09-09.md).
Current source now adds output_quantum.rs: at most1024complete output rows per
construction, halved on actual typed admission denial down to one row. Only pure
construction retries; collection/HAVING/publication remain terminal, exact cursor
and owners retained. Target resets per complete owner; short tail preserves it.
Red98327 has2expected failures; correctedgreen93352 passes2tests after repairing
the test's NULL-row headroom assumption. Broad27479/partial99929 each pass1056
library/11ignored and13integrations; full26167 retains8pass/6same failures in both
modes. Archive11files verifies511inputs. Release34350 terminal0 in8m53s freezes
01bb077a. Paired59512 terminal0 validates16outputs/traces; Q18 observes5.33%/5.28%
improvement at4/16threads. Second16-thread output1316.582→1005.099ms; other finish
738.675→738.255ms. Q1 observes+1.36%/-1.32% time at4/16threads, not a regression
bound. Archive253files/511inputs verifies; peak6909325312,zero spill/OOM/max.
Those jobs are terminal. Protected43611 terminal1/custom32643 terminal0:
all519completed outputs typed-correct,514gated,5late,10timeouts,87not_run.
CanonicalQ9/Q12 incomplete; all complete canonical upper95ratios below1.04.
CustomQ6 upper1.136243 does not clear10%bound. Archive1195files/511inputs verifies.
Fresh fixed-window identical-binary null91329 terminal1:32784correct/gated outputs,
all16windows complete/sufficient/graceful. Ratio1.040493,95%0.976383–1.130588
fails precision; dependent candidate not run. Archive24759 terminal0 verifies
33055files/511inputs; decision3files preserved. Q6bound remains open. Residency28279/audit69638 terminal0 on frozen01bb077a,
explicit partial ownership:348completed typed-correct outputs,278/278valid pairs.
Canonical IPC/CPU/mixedGPU each66/66; customCPU/GPU40/40 each. Canonical mixed
records zero successful device executions; required customGPU has40/40request
proofs. Archive1455files/511inputs verifies. Scope peak20585652224,zeroOOM/max.
Q9diagnostic67776 terminal0:3typed-correct outputs,180complete join traces;
raw/native/resident5.948/2.738/2.830s. All frontiers retain one slot despite16/8/16
partitions; raw process CPU128%, aggregate ingestion222ms and output0.033ms.
Archive188files/511inputs verifies; scope peak19690856448,zeroOOM/max.
Computed Project declines prepared capability; copied bounds also require
pool-independent pulls, so fixed output width cannot alone authorize a repair.
Next reproduce and repair compositional admitted input through computed
projection/inner joins. No source/module/dependency change in that measurement continuation;
all jobs terminal. See docs/output-quantum-residency-2026-09-09.md and
docs/output-quantum-q09-attribution-2026-09-09.md.
Current source adds planner/reserved_decimal.rs: checked Decimal128 arithmetic
writes admitted values/validity buffers and retains metadata through extraction;
operand coercion/SQL types/default ownership remain. Pure type inference avoids
query allocation. Red8805 reproduces64KiB output under4KiB; corrected52917 and
expanded14954 pass2/5tests. Broad73649 passes1061library/11ignored and12targeted
integrations; systemic numeric11pass/1refusal repeats with prior evaluator24942.
Default integrations86252 pass17; partial5output-filter/6native pass,4native
refusals reproduce exactly with prior evaluator95935. Partial99675 passes1061
library/11ignored and2typed-memory tests. Spill37366 remains8pass/6same failed
names in both modes. Original compile/target-name failures are preserved, not
counted as tests. Candidate restored after both controls. Archive24files verifies
514inputs; formatting/whitespace pass. No new release/performance certification.
The preceding decimal jobs are terminal. Current source adds closed admitted
computed projection, Date32 EXTRACT and audited inner-join probe composition.
Unsupported expressions decline before child preparation; admitted output owners,
exact cursors and terminal errors prohibit replay. Mixed signed/unsigned division
now has consistent Float64 typing/execution. Combined38284 passes1072library/
11ignored and43integrations; partial library70820 also1072/11ignored. Separate
gates: default native10pass, partial6pass/4refusals; aggregate-memory10pass each;
spill8pass/6fail and systemic numeric11pass/1refusal each. Changed denial boundaries
remain recorded. Release82930 terminal0 in8m53s freezes3172f9e5/518verified inputs. Correctness
archive26files verifies. Diagnostic18871 terminal0:3typed-correct outputs, but
all frontiers still1slot. Archive188files verifies. Planner SpillableHashJoin
delegation excludedInner; wrapper red11478 reproduces, green81365 passes the
new four-partition duplicate/cleanup test. Current source delegates only existing
in-memory decisions; spill fallback unchanged. Broad70213:1073library/11ignored and47integrations. Both-mode17836 terminal1:
partial library1073/11ignored; native10default/6partial passes; aggregate-memory
10each; spill8pass/6fail and numeric11pass/1fail each. Archive20files/518inputs
verified. Release66680 is active. No performance claim. See docs/admitted-pipeline-q09-measurement-2026-09-09.md.
No dependency/default-ownership change. See
[admitted computed pipeline](docs/admitted-computed-pipeline-2026-09-09.md).
No source changes during measurement.
See docs/output-quantum-protected-2026-09-09.md. Default ownership/dependencies
unchanged; broader acceptance for01bb077a remains open. See
[output range contract](docs/aggregate-output-quantum-2026-09-09.md) and
[output measurements](docs/output-quantum-native-2026-09-09.md).
Next evaluate
protected/provider/resource gates and general cardinality/skew/worker cost policy;
do not infer DuckDB leadership. See
[reduction contract](docs/parallel-aggregate-reduction-2026-09-09.md),
[paired reduction](docs/parallel-reduction-native-2026-09-09.md), and
[ownership tradeoff](docs/parallel-reduction-ownership-2026-09-09.md).
Exact fixture audit finds14,785 groups and295,700 bytes of
key/date/COUNT output alone versus262,144-byte legacy test budget;4-file archive
verifies. Preserve this as a refusal case and separately prove positive spill;
do not call the whole legacy test fixed by removing a validity buffer. Partition
error stringification also erases typed causes. See docs/spill-test-output-floor-2026-09-09.md.
See docs/q1-aggregate-owner-skew-2026-09-09.md for bounded partial-state merge work.
Debugger91207 locates the514-byte aggregate refusal in output validity allocation
for a15-row range; inferior fails101, one matching denial. Four-file archive verifies.
Output builder always creates validity, even for all-valid values. Next candidate:
prove actual nullness and omit unnecessary bitmap before testing adaptive output
quantum; see docs/aggregate-output-quantum-audit-2026-09-09.md. No full candidate performance acceptance. See
docs/aggregate-key-binding-audit-2026-09-09.md and
docs/aggregate-threshold-residency-2026-09-09.md.
See docs/isolated-window-benchmark-2026-09-09.md and provider report.
No dependency or query-limit change. The new key-binding source is not the frozen threshold binary.
See [threshold contract](docs/aggregate-spill-threshold-2026-09-09.md) and
[protected comparison](docs/aggregate-threshold-paired-2026-09-09.md).

Frozen322c8042 adds optional admitted read/write buffering in
`morsel_agg/spill_io.rs`, preserving logical seek/retry, checked framing and
flush-before-publication. Its broad38093 passed1,033 library tests/11ignored and
35 integrations; spill73325 remained6/7. The6-file correctness archive verifies. Release84684 passed in8m52s, frozen322c8042/507inputs. Matched Lance57170 terminal0: four typed-correct outputs, observed paired
ratio0.23995 (about76% lower time), still23s. Native36153 takes23.7s; raw/resident
also correct. All7 diagnostic outputs correct,222archive files verified.
Protected8626 terminal1:519completed correct/512gated,7late,8timeouts,89not_run;
Q9/Q12 incomplete. All complete canonical upper95 ratios below1.033; Q2 has
a small slowdown. Prespecified custom Q6 follow-up16613 terminal0:208correct/
gated outputs, upper95 ratio1.05178. Archives1191/303files verified.
Provider36745 terminal1/audit66363 terminal0:322 completed outputs correct,
240/264 valid measured pairs (raw63/native57/Iceberg63/Lance57). Four warmup
timeouts, two late warmups, two reference refusals;1398archive files verified.
Generic quarter-budget reproduction completes correctly:16MiB spills5.42MB,
32MiB does not spill and peaks at12.66MB tracked reservations. It is a policy
lead reproduced by red5041 and repaired in current source;41archive files retain
all setup attempts. Residency20505 terminal1/audit37204 terminal0:252completed
outputs correct,206measured/46warmups. IPC/CPU each63/66 (Q1 incomplete); canonical
GPU not executed. Custom required GPU40/40 with request-scoped evidence.
Archive1092files verified. See docs/spill-io-residency-2026-09-09.md.
No dependency, wire-format or memory-limit change. See
[spill I/O contract](docs/admitted-spill-io-2026-09-09.md).

Frozen fe3cc8fe connects shared admitted HAVING output after complete group merging
and exact decimal predicate comparisons. Its generic refusal regression passes;
1,029 library tests/11ignored and35 integrations passed, with7 preceding spill
failures. Release1709 passed in8m51s. Lance67286 returns two correct Q18 outputs
versus two control refusals but takes98.6–98.7s, largely spill finalization.
Raw/native/resident47517 passes three typed outputs at970/98,978/5,757ms; resident
uses32GiB and does not spill. Archives189/30files verified. See
[HAVING contract and measured spill cost](docs/admitted-having-output-2026-09-09.md).

The preceding frozen candidate `acdb8c51` includes bounded outer-probe output,
prepaid resident input domains, direct contiguous copies, checked runtime-filter
lineage and retained filter-payload admission. Existing IPC extent and queue
metadata repairs remain included. No dependency change. Preserve the dirty tree
and immutable archives; no commit or push has been requested. Filter allocation
bounds depend on hashbrown0.17.1. Prepaid domains do not certify query-wide memory.

Diagnostics88718/51590:24 completed outputs typed-correct; Q13 has16 admitted
producer slots. Resident Q12 second requests took221/178ms versus312/332ms in the
preceding candidate. This is diagnostic evidence, not a paired speed claim.
Implementation/diagnostic archives contain6/261 verified files.

Balanced40018 is terminal1:510 completed outputs correct,506 timing passes,
4 late results,13 timeouts and93 not-run requests. Candidate Q13 passes all24
measured requests; the older control times out in4 warmups. Q9/Q12 remain
incomplete. Canonical Q1/Q2/Q20 show small regressions; all complete upper95%
ratios stay below1.10. The1,179-file archive is verified. See
[balanced results](docs/contiguous-copy-paired-2026-09-09.md).

Provider10708 terminal1; audit52220 terminal0:330 completed outputs correct,
247/264 valid pairs (raw63, native63, Iceberg64, Lance57). Three engine warmup
timeouts, one Lance Q18 input-domain refusal and two reference failures remain.
No scope OOM events;1,410 archive files verified. See
[provider screen](docs/contiguous-copy-provider-screen-2026-09-09.md).
Residency92861 terminal1/audit45040 terminal0:244 completed outputs correct.
IPC/CPU each60/66 (Q1 timeout,Q18 input-domain refusal); canonical GPU not executed.
Custom CPU/GPU each40/40 with request-scoped device evidence. Archive1,085 files
verified. See [residency](docs/contiguous-copy-residency-2026-09-09.md).
Matched Lance Q18 attribution47364 terminal0 proves an aggregate-collector
regression; Q9 diagnostic72893 terminal0 validates6 outputs and confirms a
single-slot frontier behind computed Project. Archive225 files verified. See
[attribution](docs/input-retention-attribution-2026-09-09.md).

Next: finish release68533 and run prepared original-budget Lance/native Q18
comparisons plus protected/provider/residency gates. Broader resource/
concurrency acceptance remains open. Buffered finalization still costs17–18s. The fixed quarter-budget group-count
cap may cause premature spills: native4GiB reserved peak1.94GB with spill versus
resident32GiB peak2.72GB without spill. Different ownership means this is a
hypothesis, not proof of4GiB feasibility. Reproduce generically under the same
budget after gates; preserve checked admission, prepared spill scratch and retry.
The HAVING/spill report also records provider-dependent primitive-key kernel routing. A live process sample shows over70M
reads and95M writes; finalization dominates, not HAVING output filtering. Other collecting consumers remain open.
Computed-projection admission is a measured Q9 boundary; do not infer a speedup
without implementing and measuring it.
[Outer-count preaggregation](docs/outer-count-preaggregation-design-2026-09-09.md)
is an unimplemented candidate: retain final aggregation when left uniqueness is
unknown. Do not remove the existing structural proof or tune routing by query ID.
Full workload/provider/resource/concurrency acceptance, low-budget spill failures
and query-wide admission gaps remain open. No DuckDB leadership is certified.
Historical checkpoints and their superseded pending states are preserved in
[checkpoint history](docs/agent-checkpoint-history-2026-09-08.md).

