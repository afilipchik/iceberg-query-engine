# Current execution checkpoint

## Active work

Preceding frozen engine0f30c946 binds aggregate key metadata once per chunk;
508 source inputs verify. Feature gate passes1036 library tests/11ignored and36
integrations; full spill gate remains6pass/7fail. Protected comparison2204:
511 completed outputs correct,504gated; Q9/Q12 incomplete. Provider5434:
325 correct outputs,242/264 valid pairs. Residency69745:252 correct outputs,
IPC/CPU63/66 each, canonical GPU unexecuted; custom CPU/GPU40/40 with device
evidence. All archives verified. Fresh Q6 null45941 and candidate4841 both
complete32784 correct/gated outputs; candidate95%ratio0.903637–1.062568 passes
10% bound without proving a speedup. Each study archives33055 files.

Optional aggregate-output validity is now repaired after an independent red
regression;1039 library and42integration passes,10-file/508-source archive.
Typed partition causes are preserved across SELECT collection and shared native
partition setup. The cap classifier uses typed admission rather than diagnostic
text. Broad97446 passes1042library/11ignored,61integrations and9example tests.
Retained aggregate tests separately prove256KiB refusal and16MiB/2%-policy actual
spill completion:14785groups,2741595spill bytes,3057480retained bytes, independent
typed oracle. Full spill76280 is8pass/6fail after replacing the impossible prior
aggregate success expectation; the six other failures are unchanged. All jobs
are terminal. The16-file typed-error/spill archive verifies508source inputs.
No new release/performance certification exists for these edits.
See [typed causes and spill contract](../../../docs/partition-error-and-spill-contract-2026-09-09.md).
Experimental balanced ownership is connected to the live grouped path, with
up to16workers, direct partial/run transfer and a mandatory global merge. Default
remains disjoint; both respect query admission. Partial41773 passes1051library/
11ignored and12integrations; default47088 passes1050/10. Full6177 remains8/6 in
both modes. Ten-file correctness archive verifies509inputs. Release76858 completed in8m53s,
freezingcd8098d5. Diagnostic4049 validates24native outputs and ownership;293files
verify. Q1 improves about47%at16threads while Q18 is about41%slower. Its serial
non-output final merge adds about3.6s, overriding faster ingestion. Next parallelize
final reduction; keep default disjoint until protected/provider/resource gates.
All jobs are terminal. See
[ownership integration](../../../docs/balanced-aggregate-ownership-2026-09-09.md).
Current source adds bounded resident parallel reduction with pre-admitted serial
spill fallback. Broad2924 passes1054library/11ignored and12integrations;
default15122 also passes1054library/11ignored. Full6026 preserves8pass/6same
failures in both modes. Eleven-file correctness archive verifies510source inputs.
Release97747 exits0 in8m53s, freezingbd689bf6. Paired92740 and same-binary81286
exit0 with32correct outputs/traces total; both253-file archives verify. At16threads
partial ownership improvesQ1 48.48%,Q18 12.84% versus disjoint; at4threads Q18
regresses16.17%. Default stays disjoint; all jobs terminal. Provider81715 terminal1/audit4915 terminal0:331completed outputs correct,
247/264valid pairs (raw63/native57/Iceberg66/Lance61). Four warmup timeouts,
one late nativeQ11 warmup and one LanceQ9 reference refusal remain. Iceberg
single-session geomean0.415091/suite0.592510,17wins,worst1.830853; no leadership
certification. Archive1426files verifies510inputs,zeroOOM/max. All jobs terminal.
See [provider results](../../../docs/parallel-reduction-provider-screen-2026-09-09.md).
Bounded output construction is implemented in output_quantum.rs. A1024-row target
halves on actual typed admission denial; HAVING/publication remain outside retry.
Broad27479/partial99929 each pass1056library/11ignored and13integrations; spill26167
retains8pass/6same failures. Archive11files/511inputs verifies. Release34350 exits0
in8m53s, freezing01bb077a. Paired59512 exits0 with16correct outputs/traces; Q18
observes5.33%/5.28% improvement at4/16threads, Q1+1.36%/-1.32% time. Output phase
falls1316.582→1005.099ms in the second16-thread block, other finish unchanged.
Archive253files/511inputs verifies. All jobs terminal; no full regression bound
or01bb077a provider/resource certification. See
[output contract](../../../docs/aggregate-output-quantum-2026-09-09.md) and
[measurements](../../../docs/output-quantum-native-2026-09-09.md).
Protected43611 terminal1/custom32643 terminal0:519completed outputs correct,
514gated,5late,10timeouts,87not_run. CanonicalQ9/Q12 incomplete; complete canonical
upper95ratios<1.04. CustomQ6 upper1.136243 remains uncertain. Archive1195files
verifies511inputs. Fixed-window identical-binary null91329 terminal1:32784correct/gated outputs,
16complete/sufficient/graceful windows,95%0.976383–1.130588 fails precision.
Candidate follow-up not run. Archive24759 verifies33055files/511inputs;3decision
files preserved. All jobs terminal; Q6regression bound remains open. See
[control evidence](../../../docs/output-quantum-short-query-control-2026-09-09.md). See
[protected results](../../../docs/output-quantum-protected-2026-09-09.md).

Residency28279 and independent audit69638 both terminate0:348typed-correct outputs,
278/278 measured pairs. Canonical IPC/CPU/mixed each66/66; customCPU/requiredGPU
40/40 each. Canonical mixed records no successful device execution; required
custom GPU40/40 has request-scoped evidence. Archive1455files verifies511inputs;
32GiB capacity qualification, default ownership and Q6uncertainty remain open.
Q9diagnostic67776 terminates0:3correct outputs,180complete join traces. Raw query
5.948s versus222ms aggregate ingestion;16partitions collapse to one frontier slot,
process CPU128%. Native/resident reproduce the same one-slot boundary. Archive188
files verifies511inputs. Next reproduce and repair admitted input composition
through computed projections/inner joins, preserving query budgets and no replay.
No engine/harness/source changes; all measurement jobs terminal.
[Residency evidence](../../../docs/output-quantum-residency-2026-09-09.md),
[Q9 attribution and repair contract](../../../docs/output-quantum-q09-attribution-2026-09-09.md).

Current source repairs a prerequisite: planner/reserved_decimal.rs admits checked
Decimal128 arithmetic output and retains values/validity metadata. Red8805 proves
unadmitted64KiB output under4KiB; focused5tests pass. Default/partial library gates
pass1061/11ignored each; new SQL2,coercion3,projection7 and default native10 pass.
The systemic numeric1refusal and partial native4refusals reproduce under prior
evaluator controls24942/95935. Full spill remains8/6 in both modes. Corrected
integration targets and compile errors are preserved. Candidate restored; all
jobs terminal. Archive24files verifies514inputs. No optimized performance claim
or parallel-input promotion. Next close temporal/coercion/projection/inner-join
admitted ownership; partial-worker startup headroom remains a separate open risk.
[Decimal prerequisite and tests](../../../docs/decimal-expression-admission-2026-09-09.md).

Current admitted pipeline passes1072 library tests in each ownership mode and43
combined integrations. Default native10/10; partial native6/10. Spill8/6 and
systemic numeric11/1 remain in each mode. Release82930/3172f9e5 completed; Q9 diagnostic18871 returned3correct outputs
but retained1slot. The planner wrapper excluded inner delegation; repaired after
red11478, green81365. Broad70213:1073library/11ignored plus47integrations.
Both-mode gates retain spill8/6, numeric11/1, partial native6/10. Archive20files
verifies518inputs; corrected release66680 completed, freezing e4608ccf. Diagnostic57734 validates
3outputs and16resident input slots; raw/native remain1. Paired91923 validates
12outputs/720traces: resident ratio0.565225, raw0.994857/native1.001873.
Archives188/252files verify518inputs; all jobs terminal. Two-block diagnostic
only. See [Q9 results and next sequence](../../../docs/admitted-pipeline-q09-measurement-2026-09-09.md). No default promotion or performance certification.
[Contracts and full gate record](../../../docs/admitted-computed-pipeline-2026-09-09.md).

Current fixed-width scan capability reuses admitted decoding; metadata-only
refusal preserves the certified copied route. Red63061 and broad51852 reproduced
the gate and small-budget regression; corrected90721 passes1075library/11ignored
and37integrations. Both-mode5005 retains resource failures; archive20files verifies
518inputs. Release44979/2f9ad9f1 completed; raw Q9 still1slot. Probes locate
materialization replacing logical schema with physical dictionary schema. Current
source preserves declared schema and fixes lost typed refusal for unsplittable
aggregate partitions. Red75599/62644, green7587:1077library/11ignored and43integrations;
both-mode20831 retains resource failures. Probe69311 confirms full rawQ9 admitted
preparation with0output pulls. Archive41files verifies518inputs. Release76303/703b8564 terminal0 in8m53s.
Diagnostic74715 validates3outputs and16raw input slots. Paired24687 validates
12outputs/720traces: rawQ9ratio0.253285, native1.001460/resident0.979491.
Archives188/252files verify,zeroOOM/max; two-block diagnostic only. Full canonical
provider screen52333 terminal1/audit42325 terminal0:336typed-correct outputs,
252/264valid pairs; raw66/native63/Iceberg66/Lance57. Rawgeo2.993145 versus
Iceberg0.437360. Native/Lance Q1 timeouts, Lance Q7 memory refusal/Q9 reference
failure remain. Archive1424files verifies. Diagnostic15989 terminal0:6typed-correct
outputs; nativeQ1 11.485s,8declared partitions/1slot,9.348saggregate ingestion.
Archive205files verifies518inputs,zeroOOM/max. Next: incremental native ownership
and measured general aggregate costing; no default promotion or full acceptance.
[Attribution](../../../docs/build-schema-bottleneck-attribution-2026-09-10.md).

Compact fixed state is implemented:32-byte Copy numeric cells, preserving whole-row
transaction/selected ownership and shared numeric semantics. Red27101 shows64bytes;
corrected26228 and partial51787 each1082library/11ignored. Both-mode15164 retains
known resource/plan/float comparison failures. Archive15files/519inputs verified.
Release64441 terminal0 in8m53s freezes8b6a82e9. Paired4982 terminal0 validates
64outputs/1820traces;573-file archive verifies519inputs,zeroOOM/max. Resident16thread
Q18ratio1.169559 and rawQ1ratio1.132091 fail the performance intent, despite~512MiB
lower Q18 reservations. Candidate remains provisional. Direct fixed updates now pass1083library/11ignored
in each ownership mode; integrations16252 retain known failures. Archive15files
verifies519inputs; release99456 completed in8m52s, frozen ea1e9019. Assembly
confirms per-row conversion calls removed. Paired52198 terminal0:64typed-correct
outputs/1820complete traces,578-file archive verifies519inputs,zeroOOM/max.
Native/resident16Q1 improve10.12%/9.67%, but resident16Q18 regresses14.59%.
RawQ1 has opposing blocks. Candidate remains provisional; owned Q18 debugger
control99009/candidate43017 complete100snapshots and correct output each;
34-file archive verifies519inputs. Current borrowed scalar/in-place token candidate
passes focused82858:162tests/1ignored and default68938/partial36369 each
1084library/11ignored. Both-mode integration15226 retains the same failure
sets;16-file archive verifies519inputs. Release78412 completed in8m49s,
frozen fde271b1. Assembly confirms the intended input/token transformations.
Paired51623 terminal0:64typed-correct outputs/1820complete traces,578-file archive
verifies519inputs,zeroOOM/max. NativeQ1 improves26.10%, resident16Q1 27.27%;
residentQ18 regressions recover. NativeQ9 is5.99%slower. Full provider19994 terminal1:333typed-correct outputs,249/264measured pairs,
1423verified archive files/519inputs,zeroOOM/max. Raw geomean2.975591,
Iceberg0.419400; native/Lance remain incomplete. See
docs/borrowed-fixed-provider-screen-2026-09-10.md. No source/harness edits during measurement.
Current progress-credit fix follows generic red72396:1088library/11ignored in both
ownership modes,41memory/prepared integration passes, unchanged existing broader
resource failure names. See docs/retained-input-progress-credit-2026-09-10.md.
Release42098 freezes38966ae6; LanceQ7 diagnostic72127 clears the reproduced
refusal twice with typed-correct results. Archive191files/519inputs verifies.
Paired88178 completed64correct outputs/1820complete traces;572files verify.
Small slowdowns remain documented. Trace2503 confirms native retained-input
admission; startup repair is not implemented. Full provider4926 terminal1; audit49224 validates333outputs/249measured pairs;
1418files/519inputs verify. Startup preparation/first-pull repair passes5new integrations and all10partial
native streaming cases; full both-mode99343 is running. See
docs/aggregate-startup-headroom-2026-09-10.md. See docs/borrowed-fixed-input-2026-09-10.md and
docs/direct-fixed-measurement-2026-09-10.md. See docs/direct-fixed-updates-2026-09-10.md. The direct-update and borrowed-input comparisons are complete; their historical
regressions remain recorded below.
[Negative screen](../../../docs/compact-fixed-measurement-2026-09-10.md).
[Contract and validation](../../../docs/compact-fixed-state-2026-09-10.md).



Debugger attribution40031 terminal0 captures40nativeQ1 snapshots and a typed-correct
output on c20b0648;518source inputs verify. prepare_arrays_indexed/ingest_inner
appear in18snapshots; clone/commit/update frames identify the fixed row transaction
as the next shared optimization target. These are not CPU percentages. Initial
console-run protocol failure and pre-execution path typo remain archived. All jobs
terminal; investigate compact fixed state without weakening row atomicity, spill
cursor, numeric semantics or selected-payload admission.
See [row transaction attribution](../../../docs/aggregate-row-transaction-attribution-2026-09-10.md).



Current native scan now decodes and deletion-filters one IPC batch per pull:
RowGroupReader/NativeSegmentReader share checked IPC framing and DeletionCursor
with collecting APIs. Late errors terminate without replay; detached arrays retain
mapping ownership. This removes whole-segment survivor queues, not metadata/output
admission. Red6687 fails first-output laziness; green44913 passes. Library6970:
1080passes/11ignored. Default native/IPC/mutations56pass; partial51pass/5fail,
spill8pass/6fail both modes. Dictionary2pass/2plan failures and partial floating
comparison failure reproduce on518verified prior inputs in controls89731/84090;
candidate restored. Archive32files/518inputs verified. Release32424 terminal0 in8m52s freezes c20b0648. Paired11044 terminal0:
36typed-correct outputs/1236complete traces,400-file archive verifies518inputs,
zeroOOM/max. NativeQ1 ratio0.983112; Q9 mean0.942912 but block ratios0.812436/1.090269
preclude a consistent speedup claim. ResidentQ1 has16slots yet~9s aggregate ingestion.
Next: shared aggregation ownership/kernel costing and native admission, with all
protected/provider/resource gates still open;
All measurement jobs terminal. No speedup or native admission certification yet.
See [incremental native contract](../../../docs/native-incremental-ipc-2026-09-10.md).


[New contracts](../../../docs/build-schema-admission-2026-09-09.md).
[Contract and gates](../../../docs/fixed-width-admitted-scan-2026-09-09.md).



Next protected/provider/
resource acceptance and general ownership costing remain open.
See [parallel reduction](../../../docs/parallel-aggregate-reduction-2026-09-09.md)
and [ownership tradeoff](../../../docs/parallel-reduction-ownership-2026-09-09.md).
The key-bound Q1 diagnostic proves99.35% of rows route to only two aggregate
owners. Generic row-parallel partial-state merging is the next structural CPU
work; changing routing without cross-owner merging is incorrect. See
[output floor](../../../docs/spill-test-output-floor-2026-09-09.md),
[Q1 owner skew](../../../docs/q1-aggregate-owner-skew-2026-09-09.md), and
[short-query gate](../../../docs/bound-keys-short-query-control-2026-09-09.md).
No provider/resource/concurrency leadership acceptance is waived.

Earlier threshold/spill-buffer evidence remains in the linked reports and
AGENTS checkpoint. All threshold residency jobs are terminal and archived;
bound key arrays have already replaced the earlier planned metadata work.

Current shared spill buffering: broad38093 terminal0 passes1,033 library tests/
11ignored plus35 integrations; spill73325 remains6/7. Interrupted-refill red46737
is repaired. New spill_io.rs owns admitted buffers and logical read positions;
flush errors prevent run publication. Three changed/new inputs versus fe3cc8fe,
507 total;6-file archive verified. Release84684 terminal0 in8m52s,322c8042/507verifiedinputs. Matched Lance57170 terminal0: all4outputs typed-correct, observed paired ratio
0.23995 (~76% lower), still23s. Native36153 takes23.7s; raw/resident also correct.
All7diagnostic outputs correct;222-file archive verified. Protected8626 terminal1:519correct/512gated,7late,8timeouts,89not_run;Q9/Q12
incomplete. Complete canonical upper95 ratios below1.033 (Q2 small slowdown).
Q6follow-up16613 terminal0:208correct/gated,upper95 ratio1.05178.
Archives1191/303files verified. Provider36745 terminal1/audit66363 terminal0:322correct completed outputs,
240/264valid measured pairs;1398archive files verified. Four warmup timeouts,
two late warmups and two reference refusals remain. Generic quarter-budget
fixture now reproduces spill at16MiB, no spill at32MiB with12.66MB tracked peak;
both outputs correct,41-file archive preserves all attempts. Same-budget policy
repair is now validated as recorded above. Residency20505 completed as recorded above. See provider and quarter-budget
reports linked from docs/README.md.
Prepared matched Lance Q18 preserves fe3cc8fe as control; native/resident and
protected gates follow. See docs/admitted-spill-io-2026-09-09.md.

Current HAVING candidate: generic red36427 repaired; exact decimal predicates
added after the first filter adapter still declined decimals. Broad56714 passes
1,029 library tests/11ignored and35 integrations; spill82440 remains6/7. Shared
admitted batch filter applies only after final group merging, including real
spills. Archive8files/506inputs verified. Release1709 terminal0 in8m51s, fe3cc8fe. Lance67286 terminal0: two correct
candidate outputs versus two control refusals, but98.6–98.7s per request.
Spill finalization takes76s; a process sample shows70M reads/96M writes.
Archive189files/506inputs verified. Raw/native/resident47517 terminal0: three
typed outputs correct at970ms/98,978ms/5,757ms (resident32GiB, no spill).
Archive30files verified. Next is admitted buffered spill I/O with exact logical
seek/retry, checked framing and flush-before-publication; all measurement jobs
are terminal. Broader acceptance remains open. No dependency change. See docs/admitted-having-output-2026-09-09.md.

Matched47364 proves the prior aggregate-collector regression; Q9 diagnostic72893
validates6 outputs and confirms computed Project has one input slot. Archive225
files verified. Earlier residency92861/audit45040:244 correct outputs, custom
required GPU40/40 with device evidence; IPC/CPU Q1/Q18 incomplete. Archive1085files.

Contiguous resident copies remove identity row-index staging; red25100/green98115
confirm65,536 fitting rows versus17,476. Broad98963 passes1,027 library tests
(11ignored) and27 integrations; spill42219 remains6/7. New range.rs module,
no dependency change. Release24874 terminal0 in8m51s,acdb8c51/503inputs.
Diagnostics88718/51590 terminal0:24typed-correct outputs, resident Q12 second221/178ms
versus312/332ms; Q13 retains16admitted slots. Archive6/261files verified, no scope
OOM events. Balanced40018 terminal1:510correct/506gated,4late,13timeouts,93not_run; candidateQ13
24/24measured pass versus4control warmup timeouts. Q9/Q12 incomplete. Q1/Q2/Q20
small regressions; all complete upper95%ratios below1.10. Archive1179files verified.
Provider10708 terminal1; audit52220 terminal0:330correct outputs,247/264valid pairs
(raw63/native63/Iceberg64/Lance57),3warmup timeouts,1Lance input-domain refusal and
2reference failures. Archive1410files verified. Residency92861 terminal1/audit45040 terminal0:244 correct outputs; IPC/CPU60/66
(Q1/Q18 incomplete), required custom GPU40/40 with device evidence. Archive1085
files verified. Matched Lance Q18 diagnostic47364 running after correcting an
initial driver assertion on ready/progress records. Full acceptance remains open. See docs/contiguous-copy-provider-screen-2026-09-09.md.
See docs/contiguous-copy-paired-2026-09-09.md.
See docs/contiguous-resident-copy-2026-09-09.md.

Current runtime-filter ownership candidate borrows evaluated hash-table arrays,
removes staging/expression replay and pre-admits retained bitmap/set payloads.
Red66681 repaired; final3586 plus supplement pass1,023 library tests/11ignored
and74 integrations. Exact join-under-optional-refusal passes; spill remains6/7.
Archive8files/502inputs verified; all jobs terminal. New runtime_filter.rs module,
no dependency change. No release yet; next attribute/repair resident copy cost.
See docs/runtime-filter-admission-2026-09-09.md.

Current semantic candidate replaces runtime-filter pointer/name linking with
explicit output ordinal targets. Red33715 reproduces missing row; focused95772
passes4SQL/domain tests. Broad33167 terminal0:1,020 library passes/11ignored and72integrations with
retained/probe/capability coverage. Spill20703 remains6/7. Archive9files/501inputs
verified; all jobs terminal.
No new module/dependency; payload admission remains open. See
 docs/runtime-filter-lineage-2026-09-09.md.

Current shared-selection/resident-quantum candidate passes1,020 library tests
(11ignored) and67 integrations in46852. Spill44438 remains6/7. Fitting-batch
regression was red8192versus65536 before the quantum edit; mixed encoding and
wide decimal oracle passes. Only two source files differ from7b5b7e93. Archive
7files/500inputs verified. Release85464 terminal0 in8m50s asdbca414a,500inputs verified. Diagnostics53339/34649 terminal0:24correct outputs, Q13 retains16slots; resident
Q12 returns to916batches and312ms second request, still above186ms baseline.
Archive261files verified. Lineage repair now passes33167, recorded above; timing candidate not accepted.
See docs/bound-selection-and-resident-quantum-2026-09-09.md.

Independent frozen7b5b7e93 reproduction: runtime filtering through a computed
Project named like its input loses a valid row (2 actual versus3 expected).
Renaming the output or RT_DISABLE restores the exact multiset. Archive30files
verified. Prioritize explicit ordinal lineage after the active frozen measurement;
do not expand provider runtime filters using the current name-based registry.
See docs/runtime-filter-lineage-2026-09-09.md.

Frozen11d16e73 release15551 completed;495 source inputs verified. IPC, metadata,
page lifetime, output quantum and ID-prefix repairs pass1008 library tests
(11ignored) and43 integrations; spill6pass/7fail. All current jobs are terminal.
Paired82129:467 correct/460 gated; no established speed change, Q9/Q12/Q13
incomplete. Q2 confirmation7394:208correct/gated,95%0.96282–1.06577.
Provider94344/audit79244:320 completed correct,240/264 valid pairs; all modes
incomplete. Residency48225/audit79688:244 correct, canonical IPC/control60/66,
canonical mixed GPU not executed, custom required GPU40/40 with device evidence.
Archives verified:1133paired,305follow-up,1397provider,1100residency files.
See docs/dictionary-chunks-paired-2026-09-09.md,
docs/dictionary-chunks-provider-screen-2026-09-09.md and
docs/dictionary-chunks-residency-2026-09-09.md.

Collected phase telemetry is frozen b61ce3d3 (release12129,495 inputs);32 traced
integration plus32 default join library tests pass. Diagnostics40828/22219:18
completed typed-correct outputs; outer frontier16 partitions/1 slot in all modes.
Archive246files verified. The current candidate adds bounded outer output and
transitive admitted resident/filter input. A lifetime-bound source credit caused
13 library tests to stall; direct buffer admission removes that dependency.
Gate60127:1,018 library passes/11 ignored and67 integration passes. Optional filter
compilation declines admission failure before child consumption. Spill16583 remains
6pass/7same failing names. Archive13files/500sources verified. Release79400 completed8m48s as7b5b7e93, all500inputs verified. Diagnostics57985/49264 terminal0:18 typed-correct outputs, all Q13 outer frontiers
16slots instead of1. Q13 improves diagnostically, but resident Q12 regresses;
7,323 vs916 filtered build batches and extra copying need attribution.
Archive243files verified. All jobs terminal; candidate is not accepted.
See docs/bounded-outer-pipeline-2026-09-09.md. Full parent acceptance remains open.
Historical frozen-candidate details below retain their original scope.

Frozen476ec119 join-index repair: native10/10,998 library passes/11ignored,32
ownership passes. Seven spill failures also occur on isolated pre-change source.
Paired26121 terminal1:358 correct/353 gated, Q9/Q12/Q13 incomplete. Q19 improves
1.19%; no broad component gain. Archive931files verified. Provider53182 terminal1,
supplemental23286 terminal0: all322 completed outputs correct;240 valid pairs of
264. Six warmup timeouts, two reference failures. Archive1404files verified.
Broader residency/resource/concurrency acceptance remains open.
docs/join-index-admission-2026-09-08.md and docs/join-index-provider-screen-2026-09-08.md.

Preceding bound-array candidate8670b94c completed paired measurement: custom Q1
improves8.71%, canonical Q5 improves3.29%;457 completed outputs correct,450 gated.
Q12/Q13 still fail. Both prespecified protected follow-ups terminal0:416 additional
correct/gated outputs; Q2/Q6 upper95% ratios below1.05. Archive1680files verified.
No full provider/resource/leadership certification.
docs/bound-aggregate-arrays-2026-09-08.md.

The completed evidence below belongs to preceding frozen engine e6a60347.

Frozen e6a60347 adds scoped parallel admitted key preparation.995 library tests
pass/11 ignored,14 selected integrations pass; native9pass/1unchanged join refusal.
All493 source inputs verified. Canonical paired Q10 improves14.29% against serial
preparation; Q12/Q13 still fail. Q2 follow-up15499 terminal0:96 measured pairs,
208 correct/gated outputs, ratio0.98058 (95%0.91485–1.04999). Both studies preserved.
See docs/parallel-key-preparation-2026-09-08.md.

Full provider48932 terminal1: raw60/native57/Iceberg63/Lance57 valid of66 each.
All317 completed outputs correct; six engine warmup timeouts, one late warmup,
two reference refusals and27 not_run measured slots. Supplemental50146 terminal0;
archive1391files verified, zero cgroup OOM events. This is not suite acceptance.
See docs/parallel-key-provider-screen-2026-09-08.md.

Residency29847 terminal1: canonical decoded IPC and CPU control at32GiB/48GiB
validate60/66 each, Q1/Q13 warmup timeouts. All202 completed outputs correct.
Custom control passes40/40; required GPU startup fails missing NVRTC loader path.
Canonical GPU rejects incomplete control. Archive1055files verified;16GiB refusal
remains. docs/parallel-key-residency-2026-09-08.md.

Existing repository NVRTC libraries supplied by process-local LD_LIBRARY_PATH:
fresh custom pair81085 terminal0, CPU40/40 and required GPU40/40 valid. All84
completed measured/warmup outputs correct,40 measured request-scoped device runs,
no fallback or measured upload. Preparation recorded separately. Archive415files
verified. This is custom float smoke, not canonical SF10 or cold GPU timing.
docs/parallel-key-gpu-runtime-2026-09-08.md.

Harness startup diagnostics now preserve the first failure. Red6 subcases;
full126run/124pass/2optional skips. Archive159files verified. Engine unchanged;
no benchmark rerun needed for the message-only change. All preceding measurement jobs terminal; cargo fmt and git diff checks pass.
The new provider measurement job is active as recorded above.
docs/resident-startup-diagnostics-2026-09-08.md.

Remaining: provider/residency/resource/concurrency acceptance and Q12/Q13 cost
attribution. Local upstream sources reviewed: DuckDB1c27c54f and ClickHousea1b25f3f.
Collected outer joins still need distinct attribution; inner-stream profiles do
not cover their candidate/gather work. No new join optimization is justified yet.

## Historical checkpoints

Read-only follow-up during build33874: completed4/4GiB failure-trace queue spans
show Project16 partitions reduced to1 slot (unknown bound),458 batches/310803rows,
845.053ms summed poll spans and2.116ms copies. Source/evidence and safe next-step
requirements: docs/q12-input-serialization-2026-09-08.md. This is not a full-query
speedup claim; the release is now terminal; this is the next measured scheduling investigation.

Corrected build33874 completed; frozen ea6fb42d matches299 source hashes.
Diagnostic9671 exited1: both1/12GiB and4/4GiB timed out at fresh932.45/940.74ms
ceilings; no complete typed engine output. Both cgroup resource gates pass.
Evidence: docs/benchmarks/2026-09-08-padding-q12/. No heavy job remains active.
Next: address the shared unknown-bound Project/join-build queue serialization
with a valid bounded/admitted output contract; preserve strict ceilings and prior
candidates. Ordinary4GiB path and new1GiB decoder both still need measured gains.

Current result: release79278 succeeded; frozen557a9582 failed strict Q12 diagnostic
98429.1/12GiB exposed packed-block padding rejection;4/4GiB exceeded the fresh
953.44ms gate. No cgroup OOM/max events; watchdog left4 spill files. Evidence:
docs/benchmarks/2026-09-08-admitted-q12/.
Current source fixes the general padding assumption: caller page counts bound
logical values, encoded extents/domains remain checked. Red24836 reproduced;
real DuckDB fixture passes. Final library64280:972 pass,10 existing ignores;
live79628:9 pass/no skips. Prepare a new frozen release and use the gated driver's
explicit binary/manifest/output arguments. See packed-block-padding report.

Prior source checkpoint: bounded ZSTD now uses admitted static context and a
fixed destination through pinned zstd-sys2.0.13+zstd1.5.6 (experimental bindings;
no version upgrade). Library24107 passes970/0 failures/10 existing ignores;
integrations1592 pass36 with no skips. Live SNAPPY/ZSTD × V1/V2 × dictionary matrix
passes. Actual canonical SF10 footers use supported codecs/encodings. Freeze the
release candidate next, then run prepared isolate_admitted_q12.py with verified
admitted-release.json. No new performance claim. Evidence: admitted-zstd;
`docs/admitted-zstd-2026-09-08.md`. Footer/query metadata and other gates remain.

Prior source checkpoint: live raw variable-width pressure scans now select the
admitted reader with planner query pool, admitted static/runtime masks and survivor
projection. Library83054 passes968/zero failures/10 existing ignores; selected
integrations78413 pass36 with no skips. The live nullable-string quantum regression
changes39 batches to1 with exact values. Canonical SF10 is ZSTD and currently refuses
on this new pressure path. Bounded ZSTD is the required next implementation before
Q12/release measurement; do not change benchmark compression. Footer/query metadata
admission and other routes remain open. Evidence: live-admitted-scan; report:
`docs/live-admitted-parquet-scan-2026-09-08.md`. No performance claim.

Prior source checkpoint: cache retention is bounded to256 schema variants/
256MiB reported estimates. All eight canonical SF10 footers remain reused in the
contained working-set probe. Live raw scan work now owns the pruning metadata
and validates each independently opened file version before decode; changed paths
refuse and cache eviction cannot trigger per-group reparsing. Full library17707:
965 pass,0 failures,10 existing ignores. Integrations89712:34 pass,zero skips.
Evidence: scan-metadata-ownership and metadata-retention; dedicated report:
`docs/metadata-cache-identity-2026-09-08.md`. Parser/query metadata admission, IPC
lifecycle and admitted scan/query-pool routing remain. No fresh performance claim.

Prior source checkpoint: a live metadata-cache identity bug is reproduced and
fixed (preserved-mtime replacement returned2 rows instead of5). Cache acquisition
now uses opened-file identity and hands builders the same handle; schema keys
retain owners. Focused85952 passes3, library91595 passes962/0 failures/10 existing
ignores, and selected integrations35500 pass32 with no skips. Footer/cache
admission, immutability policy and live admitted scan routing remain before
Q12/SF10 measurement. Evidence: metadata-cache-identity.

Prior source checkpoint: reserved predicate compilation now preflights bounded
AST/type/literal shape before cloning, then retains actual program capacities
with owner allowances after releasing construction headroom. Shared qualified
lookup no longer allocates formatted names. Compiler47358/identity98795 pass11/12;
full library71312 passes959, zero failures,10 existing ignores. The real Parquet
fixture uses reserved compilation. Footer/cache and live scan/query-pool routing
remain before Q12/SF10 measurement. Evidence: reserved-compilation.

Prior source checkpoint: opt-in admitted string comparison/IN/NOT IN/LIKE
capability is implemented, with NULL-list and mixed Boolean validity. Ordinary
production compiler selection is unchanged. Full library15375 passes957, zero
failures,10 existing ignores; focused1570 passes9 with no skips. Real Parquet
mixed predicate masks and survivors match independent input values. Compilation/
metadata admission and live query-pool/scan routing remain before Q12/SF10.
Evidence: admitted-string-predicate.

Prior source checkpoint: admitted compiled numeric masks and per-register SQL
NULL validity are implemented, with explicit refusal distinct from type decline.
Full library77670 passes954, zero failures,10 existing ignores. Tests compose
real Parquet decode/mask/gather and independent truth-table/typed oracles.
Strings/IN/LIKE, compilation metadata, live query-pool/metadata wiring and routing
remain before Q12/SF10 measurement. Evidence: admitted-compiled-predicate.

Prior source checkpoint: admitted flat survivor gather is implemented and
composed with the real nine-column Parquet fixture. Full library45937 passes951,
zero failures,10 existing ignores. Subsequent focused30367 passes3 tests including
empty fixed-array ownership (test-only addition). Exact values/NULLs, duplicate
IDs, partial-output refusal and extracted leases are checked against independent
Arrow oracles. Predicate-mask admission, live query-pool/routing and provider
metadata accounting remain before Q12/SF10 measurement. Evidence: admitted-gather.

Prior source checkpoint: `admitted_row_group` validates flat schema/counts,
logical annotations, codecs and encoding capabilities before constructing readers.
Full library62226 passes948, zero failures,10 existing ignores. Actual V1/V2
files validate all nine columns and reordered/repeated projections. Timestamp/
decimal reinterpretation and unsupported delta preflight regressions pass.
Live physical routing is still open; follow `updates/2026-09-08-live-scan-integration.md`
for query-pool, metadata and predicate/gather contracts before Q12/SF10 measurement.

Prior source checkpoint: aligned admitted RecordBatch assembly is implemented.
Full library92639 passes947, zero failures,10 existing ignores. Nine mixed actual
Parquet columns validate across unequal chunk boundaries, V1/V2 and dictionary/
plain pages. Source and handoff denial preserve pending rows; output buffers
retain metadata leases through extracted arrays. Live physical scan routing,
provider metadata accounting and Q12/SF10 measurement remain open. Evidence:
`2026-09-08-admitted-batch`.

Prior source checkpoint: shared flat string/fixed column state is implemented
in `admitted_flat_column`, with `admitted_plain_fixed` for numeric/date/decimal/
Boolean/timestamp PLAIN and dictionary output. Full library27567 passes943,
zero failures,10 documented existing ignores. Typed actual-file oracles pass
across V1/V2 and dictionary/plain pages. Aligned multi-column batch assembly and
production routing remain before changing the scalar fallback or claiming SF10
gains. Evidence: `2026-09-08-admitted-fixed-column`.

Prior source checkpoint: retained flat UTF8 column composition was implemented.
Full library81145 passes937 with10 existing ignores and no failures.
All31 admitted-component tests pass (24387), including a12-case actual-file matrix
and explicit delta refusal/poison without source replay. It retains handed-off
pages across downstream admission failure and output cursors across chunks.
Fixed-width decoding, aligned batch assembly and production routing remain before
changing the measured scan fallback. No new SF10 performance claim.

Latest source checkpoint:29 admitted-decoder tests pass with no skips (58315);
full library17456 passes935 with10 existing documented ignores and no failures.
Flat NULL masks and dictionary UTF8 output now admit their buffers; owned
PLAIN/hybrid pages and retained dictionary/ID/validity handles support pauses
between chunks. The real nullable dictionary fixture composes these components.
Retained column state, fixed-width decoding, aligned batches and scan routing
remain before changing the one-row fallback or making a new SF10 claim.

Current checkpoint, September8: the frozen bounded parallel-input candidate
passes896 library tests (10 documented ignores),39 selected integrations and
two additional real-spill integrations. Balanced Q5/Q10/Q20 latency is
83.4%/57.2%/84.8% lower than its frozen control;144 typed outputs pass.
Raw/native/Iceberg/Lance validate57/60/66/58 of66 pairs respectively. Iceberg
suite ratio is0.623841, but its worst query is2.198947× DuckDB. No leadership pass.

Resource matrix36181 and limit isolation6280 are complete; no heavy job remains
from this checkpoint. The matrix validates70/72 requested outputs, with two
watchdog terminations on the second low-budget Q12 request. Independent limits
confirm a query-budget-triggered one-row scan policy:159.1s versus1.37s, with
310,803 build batches versus458 for identical rows. The bug predates the candidate.
Its production policy remains unchanged. The next implementation priority is
bounded variable-width decoding and batching, preserving real byte admission.
Provider/residency, full resource and concurrency acceptance remain open.

Subsequent diagnostic source checkpoint: physical plans now expose Streaming
Parquet reader quantum, pressure, projected types, route and copy capability.
A new test compares plan details with actual nullable-string batches. Library
gate83759 is terminal0:897 pass,10 documented ignores; fmt/whitespace pass.
These edits postdate the frozen binary. The dependency audit identifies direct
page-decompression and separate string-output allocations outside our query pool;
the next decoder change must control both. No scan performance fix is claimed.

The subsequent admitted PLAIN UTF8 component completes pre-admitted output
construction with resumable byte/row chunks and slice-held buffer owners.
Four focused tests pass (6468 terminal0), including real PLAIN pages. It is
not routed into scans yet. Page/decompression, definition-level and dictionary
ownership remain the next integration work. [Checkpoint](updates/2026-09-08-admitted-plain-utf8.md).

Decoded page storage now has a separate admitted UNCOMPRESSED/SNAPPY component;
9 combined tests pass (75621 terminal0). It reuses snap1.1.1 directly without a
package upgrade. Header parsing/encoded reads, levels/dictionaries and routing
remain open. Existing candidate benchmarks do not apply to this unrouted work.

Encoded reads now use pre-admitted positional buffers; the Unix file-body chain
through Snappy and PLAIN string output validates with independently retained
owners. Combined gate62164 is terminal0:14 pass, zero skips, including source
truncation. No heavy job remains from this checkpoint. Bounded header parsing
and snapshot integrity are next, followed by levels/dictionaries and routing.

Header envelopes now have bounded allocation-free parsing and admitted immutable
windows with body CRC checks. Gate3273 is terminal0:7 new tests pass, zero skips.
Typed page-kind fields and owned cursor handoff are next, then levels/dictionaries
and routing. No heavy job remains from this checkpoint and no new SF10 claim.

Typed page validation and the owned column cursor are now implemented. The cursor
commits only after validated admitted handoff, preserves position on memory
denial and poisons hard failures. Full library80733 completes924 passes and10
existing ignores, zero failures. Actual-file comparisons exercise our parsed
counts and cursor. Next integration boundary: admitted definition levels and
dictionary IDs, aligned column batches, then production scan routing. No live
heavy job or new performance result from this checkpoint.

Hybrid level/ID decoding now emits pre-admitted UInt32 chunks with checked runs,
domains and retry-safe commits. Gate58699 is terminal0:5 focused tests pass,
including257 exact nullable dictionary values from real Parquet files. Latest
full-library924/10 gate predates this module. Next: admitted NULL masks and
dictionary expansion plus retained page/column assembly; scan routing remains
unchanged. No live heavy job or new benchmark result.

[Candidate evidence](../../../docs/parallel-aggregate-input-2026-09-08.md) and
[scan diagnosis and implementation sequence](../../../docs/scan-budget-batching-cliff-2026-09-08.md).

## Historical checkpoints

The entries below preserve earlier evidence and superseded live-job states;
their pending handles are not current work. Use the active checkpoint above.

Borrowed UTF8 output now measures9.0% lower SF10 Q10 median versus the frozen
control, with32/32 typed outputs passing in four alternating blocks. A separate
four-output validated phase check reduces output construction470–474ms to215–222ms.
The full canonical SF10 raw rerun is active in tool session43983. The optimization
does not yet meet Q10's normal deadline, and no suite leadership is claimed.
[Measured output evidence](../../../docs/borrowed-key-output-2026-09-07.md).

The frozen4GiB SF10 matrix is recorded through GPU preflight. GPU CPU control
passes51/66 pairs and misses five deadlines; the GPU track exits2 because the
harness requires a complete correct control. No GPU execution is claimed.
The output candidate's release build is running in tool session71002; its balanced
Q10 comparison is prepared. All660 recorded source hashes were verified before
the build. IPC preparation safety remains a separate open failure.
[Consolidated provider outcomes](../../../docs/current-canonical-sf10-2026-09-07.md).

Lance SF10 has58 valid paired samples:engine Q1/Q13 timeouts, reference Q9
allocation refusals; all60 completed engine outputs are independently correct.
Decoded IPC was stopped after42 invalid sample records across14 queries because
its unadmitted full-table preload repeatedly aborts under the12GiB process cap.
The interrupted supervisor/workers are confirmed stopped. GPU CPU control is
running with raw-Parquet setup. IPC preparation safety and GPU residency remain
open; no provider suite score is valid.
[Failure evidence](../../../docs/current-canonical-sf10-2026-09-07.md).

Iceberg SF10 now has66/66 correct completed engine outputs but only63 valid
paired measurements: the DuckDB reference refuses allocation on Q13, then
segfaults on its next request. Engine Q13 outputs separately match the preserved
oracle; no suite score is valid. Lance is running against the same frozen binary
and matched4GiB query settings, with the pinned extension-optimizer limitation.
[Evidence](../../../docs/current-canonical-sf10-2026-09-07.md).

The borrowed UTF8 group-output candidate passes889 library tests (10 ignores)
and31 selected integration tests. Release measurement remains pending. Iceberg
SF10 validation is running with the same frozen outer-ON binary as raw/native,
preserving a consistent provider baseline.
[Output candidate evidence](../../../docs/borrowed-key-output-2026-09-07.md).

Fresh canonical SF10 completes raw/native runs with17/22 and20/22 queries
validating all three measured pairs respectively. Raw deadline failures:Q5/Q9/
Q10/Q12/Q13; native:Q1/Q13. No valid suite score. Ten longer-watchdog raw
diagnostics all pass typed oracles. Q10 exposes477–480ms of grouped output work;
a borrowed UTF8-key output candidate is prepared with compile/tests pending.
Other provider/residency reruns remain open.
[Current SF10 evidence](../../../docs/current-canonical-sf10-2026-09-07.md).

The implemented outer-join rule now measures17.0% lower Q13 median on unchanged
SQL versus its frozen control (12 steady samples/binary,4 alternating blocks,
32/32 typed outputs pass). All blocks improve. Canonical SF1 remains21/22:
Q13 still misses its deadline. Fresh canonical SF10 raw validation is running in
`.scratch/public-bench/outer-on-sf10-raw-01/`; no result or suite leadership claim yet.
[Measured implementation evidence](../../../docs/outer-on-predicate-pushdown-2026-09-07.md).

A balanced equivalent-SQL experiment lowers Q13 median15.6%, with all eight
typed oracle checks passing. The corresponding general LEFT/RIGHT non-preserved
ON-predicate pushdown is now implemented with an explicit total-expression
policy and separate WHERE handling. The before-fix plan failure is reproduced;
final gates pass888 library tests (10 ignores) and28 selected integration tests.
Actual optimized-binary performance remains pending.
[Evidence and limits](../../../docs/outer-on-predicate-pushdown-2026-09-07.md).

Release revalidation after the dictionary fix now passes63 typed measured pairs
across21/22 canonical SF1 queries. Q13 alone times out. Four correct diagnostic
executions measure inner ingestion180–224ms versus output10–12ms; output chunking
is therefore secondary to investigate. Host perf sampling is unavailable;
opt-in sampled ingestion phases pass110 component tests both disabled and enabled.
The optimized diagnostic build is running; measurement remains pending.
[Release evidence](../../../docs/live-aggregate-profile-2026-09-07.md).

The first live-integration release SF1 smoke failed:15/22 queries validated,
six dictionary type errors and a separate Q13 deadline failure. The shared
dictionary boundary is repaired:888 library passes (10 ignores),26 integration
passes, and all six queries match typed DuckDB oracles in debug diagnostics.
Release performance revalidation and Q13 attribution remain open; no new SF10
baseline or speedup is claimed.
[Evidence](../../../docs/live-dictionary-boundary-2026-09-07.md).

The partial-state spill controller is now connected to the production simple
grouped aggregate route. Both unchanged consuming-source replay tests and the
unchanged256KiB decimal spill-completion test pass. Final gates:887 library passes
(10 explicit ignores),25 integration passes, zero failures. COUNT(*) binding and
runtime/binding error boundaries are covered. One aggregation working set is
provisional; post-filters/unsupported layouts choose the ordinary path before input.
Performance and broader resource/provider gates remain open.
[Live integration evidence](../../../docs/live-spill-integration-2026-09-07.md).

Complete groups now emit admitted Arrow buffers with retained payload/type/batch
owners. Exact unsigned SUM partial states and empty global COUNT/AVG are covered,
including spill/merge-to-Arrow. Final gate:109 component passes, five new tests,
no failures/ignores; a before-fix unsigned failure is preserved. Live operator
routing and original query gates remain open.
[Evidence](../../../docs/admitted-group-output-2026-09-07.md).

An owning ingestion controller now flushes on real admission pressure, drops
resident capacities, compacts bounded runs and resumes retained batches without
reapplying rows. Its256KiB test completes1000 exact decimal/COUNT/AVG groups across
four batches, including with no finite group limit. Final gate:104 component
passes, three new tests, no failures/ignores. Live worker/output routing and original
end-to-end gates remain open. [Evidence](../../../docs/ingestion-controller-2026-09-07.md).

Evaluated Arrow arrays now feed transactional partial group rows with an exact
first-unapplied cursor. Numeric values stay inline; variable winners receive
admitted owners. Late denial rolls back both existing/new group updates; resume
does not duplicate the prefix. Final gate:101 component passes, three new tests,
no failures/ignores. Live controller/output routing and original gates remain open.
[Evidence](../../../docs/arrow-state-input-2026-09-07.md).

Borrowed Arrow key encoding now validates before admission and writes canonical
bytes without intermediate owned string/list scalars. Final gate:98 aggregate
component passes, three new tests, no failures/ignores. Live ingestion/output and
original resource gates remain open; no performance claim.
[Evidence](../../../docs/arrow-key-input-2026-09-07.md).

Transactional partial-run compaction and bounded flush preparation now pass
111 focused tests (two new), including repeated weighted AVG/exact decimal merges
and original-file preservation after admission/write failure. Live ingestion/output
and the original resource gates remain open; no performance claim.
[Compaction evidence](../../../docs/run-compaction-2026-09-07.md).

The admitted completed-run scheduler now merges or globally splits
partitions, processes smaller children first and emits complete leaf groups.
A real256KiB query-pool test completes2048 COUNT groups/32 runs with actual
admission-induced splits. This is not the unresolved live decimal gate. Final
gates:93 morsel plus16 ownership passes,0 failures/ignores;3 new tests. Twelve
archive members and654 source-input hashes verify. Live ingestion/output,
run-accumulation bounds and worker integration remain open; no performance claim.
[Current scheduler evidence](../../../docs/partition-scheduler-2026-09-07.md).

Global multi-run partition plans now select one split across all
sources, prove combined nonempty child counts and reuse one admitted reader
slot. Full-pool 3/5 splitting and late-source rollback pass. Final gates:90 morsel
plus16 ownership passes,0 failures/ignores;2 new tests. Nine archive members
and653 source-input hashes verify. Bounded task scheduling/working-set policy
and live worker integration remain open; no performance claim.
[Current global partition evidence](../../../docs/partition-plan-2026-09-07.md).

Full-run split scans now prove two nonempty child counts, bind
the plan to its source and decline empty/equal-key runs only after full integrity
checks. Production repartition verifies the finished counts. Final gates:88 morsel
plus16 ownership passes,0 failures/ignores;2 new tests. Seven archive members
and653 source-input hashes verify. Bounded multi-run scheduling/global progress
and live worker integration remain open; no performance claim.
[Current split evidence](../../../docs/split-progress-2026-09-07.md).

Two-way exact-key repartition now copies partial rows with one
admitted frame/two writers and retains source ownership until both children
finish. Full-pool copying and late-failure cleanup pass. Final gates:86 morsel
plus16 ownership passes,0 failures/ignores;3 new tests. Twelve archive members
and653 source-input hashes verify. Split-progress selection, bounded partition
scheduling and live worker integration remain open; no performance claim.
[Current repartition evidence](../../../docs/repartition-2026-09-07.md).

Owned spill directories now survive controller/run handle drops
through active readers. Production writer/flush APIs require the directory owner;
cleanup removes only owned files and empty directories. Final gates:83 morsel
plus16 ownership passes,0 failures/ignores;2 new tests. Eight archive members
and652 source-input hashes verify. Bounded partition scheduling/repartition and
live worker integration remain open; no performance claim.
[Current directory evidence](../../../docs/spill-directory-2026-09-07.md).

Run merge now retains a verified frame or staged partial through
admission denial, without advancing the file or applying state twice. Overflow
remains terminal and atomic. Final gates:81 morsel plus16 ownership passes,
0 failures/ignores;3 new tests. Ten archive members and652 source-input hashes
verify. Bounded partition scheduling/repartitioning, parent ownership and live
worker integration remain open; no performance claim.
[Current merge evidence](../../../docs/run-merge-2026-09-07.md).

Admitted run collection and prepared flush now publish completed
files before clearing resident groups. Full-pool flush succeeds; abandonment,
finish corruption and write failure preserve source and earlier runs. Final
gates:78 morsel plus16 ownership passes,0 failures/ignores;2 new tests. Six archive
members and651 source-input hashes verify. Bounded partition merge, parent
ownership and live worker integration remain open; no performance claim.
[Current flush evidence](../../../docs/run-collection-2026-09-07.md).

Private spill files now have admitted path/owner metadata,
completed-run publication, expected count/length validation and reader-held
cleanup. Real-file tests verify independent readers, failure cleanup, suffix
loss, appended bytes and admission retry. Final gates:76 morsel plus16 ownership
passes,0 failures/ignores;4 new tests. Nine archive members and651 source-input
hashes verify. Bounded flush/run collection and merge/controller integration
remain open; no live routing or performance claim.
[Current file ownership evidence](../../../docs/spill-files-2026-09-07.md).

Spill framing now checks query-layout/run UUIDs, frame ordinal,
header integrity and payload CRC32. Bound-query scratch admits before payload
read; budget denial rewinds, while other errors poison the cursor. Final gates:
72 morsel plus16 ownership passes,0 failures/ignores;3 new tests. Nine archive
members and650 source-input hashes verify. Complete-file publication/count
validation, bounded flush/merge and live worker integration remain open.
[Current framing evidence](../../../docs/spill-frames-2026-09-07.md).

Whole-group restoration now validates complete row payloads,
stages fixed/selected state and publishes key/state/index only on commit. Failed
or discarded preparation drops decoded owners and rolls back logical rows.
Restored duplicate run keys merge via staging with exact COUNT/AVG/decimal algebra.
Final gates:69 morsel plus16 ownership passes,0 failures/ignores;2 new tests.
Thirteen archive members and649 source-input hashes verify. File publication,
bounded flush/merge and live worker integration remain open; no performance claim.
[Current restoration evidence](../../../docs/row-restoration-2026-09-07.md).

Selected scalar read-back now validates before admission and owns
all decoded variable payload/type metadata. Bound selected slots return pending
payloads, with numeric/NULL values kept inline. Final gates:67 morsel plus16
ownership passes,0 failures/ignores;6 new tests. Thirteen archive members and649
source-input hashes verify. Whole-row restoration, file publication and bounded
controller integration remain open; no live routing or performance claim.
[Current reader evidence](../../../docs/scalar-state-read-2026-09-07.md).

Grouped partial rows now serialize directly from borrowed state,
including fixed arithmetic and selected strings/lists with preserved floating
bits. Whole-row prevalidation precedes output; partial IO leaves source intact.
Final gates:66 morsel plus11 ownership passes,0 failures/ignores;3 new tests.
Fourteen archive members and648 source-input hashes verify. Validated state
read-back, file identity/publication and bounded controller integration remain
open. No live spill or performance claim.
[Current writer evidence](../../../docs/state-row-write-2026-09-07.md).

Canonical keys now support full encoding validation and admitted
framed reads: budget denial consumes no payload, and malformed/partial reads
invalidate the key view. Final gates:63 morsel plus11 ownership passes,0 failures
or ignores;4 new tests included. Eight archive members and647 source-input hashes
verify. File identity, complete state serialization and live spill integration
remain open. No performance or end-to-end spill gate ran.
[Current key-reader evidence](../../../docs/key-read-validation-2026-09-07.md).

Canonical group storage now has an admitted exact-key hash index
and atomic key/state/index publication. Failed or discarded preparation leaves
no new group, while indexed partial merge preserves COUNT/AVG/decimal algebra
and selected payload ownership. Final gates: 59 morsel plus 11 ownership passes,
0 failures/ignores, 4 new tests included. All 647 source-input hashes and 14 archive
members verify. No ingestion/spill routing or performance change is claimed.
[Current component evidence](../../../docs/indexed-group-storage-2026-09-07.md).

`physical/morsel_agg/state_rows.rs` composes fixed/selected
states into flat admitted arrays, with reusable preparation scratch, whole-row
rollback, checked integer update/merge and bound final-value access. Final gates:
48 morsel plus11 ownership passes,0 failures/ignores,4 new tests included.
645 source-input hashes are archived. It is not connected to ingestion or
spilling. No end-to-end spill, cap or performance gate ran.
[Row component and evidence](../../../docs/aggregate-state-rows-2026-09-07.md).

`physical/morsel_agg/selected_state.rs` binds selected-state
operation/type metadata, prepares replacements before mutation, rolls back
dropped tokens, and shares merge winners only in the same bound layout. Numeric
payloads remain inline. Final focused gates:44 morsel plus5 ownership passes,
0 failures/ignores;4 new tests included.644 source-input hashes are archived.
Existing ingestion/controller routing is unchanged. No full-library, integration,
spill, cap or performance gate ran.
[Selected-state component](../../../docs/selected-state-slots-2026-09-07.md).

`execution/reserved_scalar.rs` supplies an admitted
owner for selected scalar payloads, with fallible String/List copying and shared
lease lifetime. Five focused tests pass,0 failures/ignores,818 filtered out.
The archive preserves643 current source-input hashes. No aggregate-slot or
controller integration, end-to-end spill, full-library or performance gate ran.
[Component scope](../../../docs/selected-scalar-ownership-2026-09-07.md).

Local DuckDB/ClickHouse source review exposed a floating extrema
update/merge ordering divergence. MIN/MAX now share the existing SQL comparator
across typed ingestion and hash fallback updates/merges. Before: one state test
and all three operator reproducers fail. Final:808 library passes/10 ignores
plus33 integration passes; no failures. Five new tests are included, not added
again to that total. No spill routing or performance change is claimed.
[Repair and evidence](../../../docs/float-extrema-ordering-2026-09-07.md).
The indexed storage components above follow this semantic repair; complete
serialization and worker integration remain required.

The preceding change adds an unconnected fixed-state codec and
pre-admitted IO/decode workspace in `physical/morsel_agg/state_codec.rs`.
Its final selected gate passes39 morsel tests (7 new codec tests), with777
filtered out. No execution routing changed and no new end-to-end spill or
performance gate ran. [Codec scope](../../../docs/aggregate-state-codec-2026-09-07.md).

The preceding implementation adds an exact applied-row cursor to the
common morsel update path. `process_evaluated_from` preserves the retained batch
and applied state on typed admission denial, including partial migration.
Ordinary callers still return the original error; fused state spilling remains
unimplemented. Both consuming-source budget-transition fixtures still fail
because each input partition executes twice.

Next: connect live evaluated input/cursors to prepared flush, bound ingestion
run accumulation, implement admitted result emission and integrate workers.
Completed-run task scheduling is implemented. Global multi-run split progress is implemented. Single-run split progress is proven before repartition. Two-way exact-key repartition is implemented. Parent-directory lifetime ownership is implemented. The retained-frame/staged-row merge primitive is
implemented. Pre-admitted flush/run publication is implemented. Completed-run publication and count validation
are implemented. Ordered
query-local frame identity and checksums are implemented. Transactional complete-row
restoration is implemented. Row writing and key payload
validation are implemented. The row preparation component must still be connected to
ingestion before advancing its cursor. Implement bounded flush/merge and
oversized-partition handling, connect the cursor to fused workers, and remove the
`Ok(None)` source-replay branch. Do not replay the
original input or retry a partially applied batch at offset0. Preserve AVG
sum/count, decimal fulli128 coefficient/scale/overflow and NULL/seen state.

[Implementation contract](updates/2026-09-07-fused-spill-implementation.md)
[Cursor evidence](../../../docs/aggregate-ingestion-cursor-2026-09-07.md)

## Current source and validation

Preceding ordering-repair source:808 library passes,0 failures,10 ignored; eight
affected integration targets pass33 tests. Archive:
`docs/benchmarks/2026-09-07-float-extrema/`, including642 source-input hashes.
Dedicated GPU/cap/performance and spill-transition tests were not rerun.

Earlier full gate, on the cursor source:799 library passes,0 failures,10 ignored. Nine integration
targets:39 pass,0 fail,0 ignored. The838 passing tests are unique across these
final gates. The10 ignores are flatten-EXISTS, eight dedicated actual-CUDA tests,
and the dedicated IPC-cache test. Both unchanged consuming-source transition
tests fail (exit101). The separate256KiB query-budget failure remains open and
was not rerun. No current optimized performance or dedicated cap run is claimed.

The new component archive `docs/benchmarks/2026-09-07-aggregate-state-codec/`
retains640 current source hashes and its39-test selected result. It is not a
complete state codec or an operator acceptance pass.

The preceding archive `docs/benchmarks/2026-09-07-aggregate-ingestion-cursor/` retains the
patch, before/after source, fixture, logs and639 input hashes. Formatting passes.
The production cursor delta is confined to `physical/morsel_agg.rs`.

Retained earlier repairs:
- Evaluated group/aggregate arrays are shared by disjoint routing and updates,
  fixing reproduced duplicate groups from volatile expressions.
- Pool denial has typed `MemoryLimit` data; display/log category stay compatible.
- Generic/raw-SUM migration retains source entries until destination admission.
- Fused errors and task panics propagate, pending siblings cancel, and worker
  failure wakes the producer supervisor. Error branches never retry input.
- The separate decimal precision-bound lookup removes repeated exponentiation
  seen in frozen662 machine code. Its optimized performance remains unmeasured.

## Completed frozen performance evidence

The full653/662 canonical SF10 run completed20 cells and9680 typed/time-valid
requests. Equal-weight required-track suite ratio is0.978298 against653.
Four-order suite ratios against DuckDB: raw2.331445, native3.432811,
Iceberg0.316632, Lance1.254983. IPC is extra; the Lance reference disables its
known-incorrect extension optimizer. These frozen binaries exclude subsequent
decimal, lifecycle, evaluated-input, admission/migration and cursor changes.
[Full report](../../../docs/dense-domain-full-sf10-2026-09-07.md).

Current653/662 and historical647/662 protected follow-ups each completed1632
validated requests, with no per-order mean/median ratio above1.10. Historical
Lance Q17's median-ratio geometric mean is1.052746. The earlier647/649 regression
and native Q1 diagnostic control timeout remain preserved as separate evidence.
The four orderings are not four independent sessions or an equivalence claim.
[Current follow-up](../../../docs/dense-domain-current-protected-2026-09-07.md)
[Historical follow-up](../../../docs/dense-domain-historical-protected-2026-09-07.md)

## Remaining acceptance

No DuckDB leadership, complete SQL/resource certificate or full query-wide
ownership is claimed. General/nested/bare-float state, queue/consumer headroom,
provider/result admission, public JOB/ClickBench, holdouts, lower memory,
concurrency and wider provider/GPU gates remain open. Preserve all accepted
parent requirements; task007 and the full epic remain active.

Use `scripts/claude-safe-build.sh`, repository TMPDIR and serialized heavy jobs.
Project edits and safe contained commands are authorized. No commit/push was
requested. Older chronological status is preserved in the dated evidence and
[the original history snapshot](updates/2026-09-07-execution-history-through-fused-lifecycle.md).

Q2 follow-up15499 terminal0:8 blocks/96 measured pairs,208 engine outputs all
correct and gated. Ratio0.98058 (95%0.91485–1.04999), below the protected10%
regression threshold; original study retained. Next full e6a60347 provider screen.
See docs/parallel-key-preparation-2026-09-08.md.

## Superseded active checkpoint before residency29847

## Active work

Provider76528 terminal1: raw57/native60/Iceberg61/Lance57 of66 valid pairs each;
all315 completed measured/warmup engine outputs validate. Archive1075 provider
files plus470 raw/harness files verified. Reference refusal/crashes invalidate
specific Iceberg/Lance pairs; no suite acceptance.

Failed references now retire before reuse (red/green regression,118 harness
passes/2 optional skips). Residency22984 terminal1, same3ff868c7 engine. Canonical
control refuses28.6GB preload at16GiB; decoded IPC invocation has redundant flag.
Custom float Q1 warmup165.806ms exceeds124.908ms gate;20 Q6 pairs validate.
Dependent GPU cases reject incomplete CPU controls; no device execution proved.
Archive538 files verified. No heavy job active. Next inspect generic memory/filter
input capability and aggregation with local DuckDB/ClickHouse source references.
Evidence: docs/budget-quantum-residency-2026-09-08.md.

Diagnostic25084 terminal1 rules out missing parallel input in custom float Q1:
four slots/workers, no spill; routing77.890ms and processing138.297ms within
216.293ms ingestion. Q1 warmup fails; three Q6 measured pairs validate. Archive56
files verified. Local source comparison identifies duplicate canonical key
preparation; next measure admitted reusable key batches, preserving disjoint
owners, exact equality and low-memory spill semantics. No engine change in this
diagnostic. docs/memory-aggregate-source-comparison-2026-09-08.md.

Component54832 terminal0 under48GiB: one selected optimized test passes,932
filtered, zero failures/skips,2.24s execution. Five shapes show retained/double
encoding ratios0.593–0.602, eight paired blocks each. All168 source hashes match;
seven evidence files archived. Test-only source; no production optimization or
query-level gain claimed. No heavy job active. Next integrate admitted prepared
keys with layout/lifetime/first-unapplied-row contracts and pre-mutation fallback,
then semantic/resource gates and full-query measurement. Evidence:
docs/retained-key-component-2026-09-08.md.

Production prepared-key integration implemented: exact batch/layout identity,
shared admitted bytes/hashes, full canonical equality and unchanged spill cursor.
Pre-mutation admission fallback, child cap one eighth available query memory.
Focused127pass/1ignored; added identity/canonical/fallback tests and prepared
decimal selection-spill oracle. Broad45262 terminal101:994library pass/11ignored,
14 selected integrations pass, native9pass/1fail with unchanged383984-byte join
refusal at256KiB. Release57568 completed1a0ece71,493 verified inputs,8m43s.
Paired81974 terminal1 under48GiB. Custom-memory
Q1 all8 warmups correct but late,48 dependent not_run; Q6 ratio0.94988
(95%0.88890–1.01079), no confirmed gain. Canonical Q10 improves3.97%
(95%2.54–5.34%), Q19 improves1.67% (0.27–3.61%); other complete-query intervals
include no change. All346 completed canonical outputs correct,339 gated;7 late,
9 timeouts,93 not_run. Q12/Q13 remain failures. Archive1030files verified.
Full22-query screen69779 terminal1, frozen1a0ece71: raw57/native60/Iceberg63/Lance57
valid pairs of66 each. All317 completed measured/warmup outputs correct;27 not_run,
six engine warmup timeouts, one late warmup, two reference-invalid queries.
Postprocessing72379 terminal0; provider archive1393 files verified. No heavy job
active. Overall acceptance remains open. docs/prepared-key-provider-screen-2026-09-08.md.
Shared reference calibration now stops at first failure. Resident invalid-reference
preparation and dependent requests are blocked; late/wrong engine samples stop
later engine calls. Red regression fails11 subcases; full125run/123pass/2optional
skips. Archive158files verified. No engine rebuild for this harness-only change.
docs/reference-calibration-stop-2026-09-08.md.
Read-only source comparison identifies batch update dispatch as the next general
CPU investigation: docs/aggregate-update-boundary-2026-09-08.md. Typed batch binding
can preserve current row transactions; transposing aggregate loops requires a new
rollback/admission contract. No source edits during provider69779.
Subsequent complete Q10 diagnostic57037 shifts immediate priority: preparation/
routing418.405ms of557.951ms ingestion in the candidate sample, with four workers
and16 input slots. The custom Q1 diagnostic26035 timed out and is partial evidence.
All eight Q10 diagnostic outputs typed/time valid; profiles add overhead and are
not paired certification. Archive231files verified. No heavy job active. Next
measure scoped parallel key preparation with shared admission and unchanged owners/
spill cursors. docs/prepared-key-state-profile-2026-09-08.md.
Scoped parallel preparation implemented in prepared_keys.rs only: up to four
chunks, shared child admission, checked row mapping, completed scoped cleanup
before fallback. Focused128pass/1ignored; broad995pass/11ignored,14 selected
integrations pass; native9pass/1same join refusal. Release16406 completed e6a60347,
493 verified inputs,8m44s. Paired6123 terminal1. Canonical Q10 improves14.29%
(95%14.05–14.53%); all347 completed outputs correct,339 gated;8 late,8 timeouts,
93 not_run. Q12/Q13 remain failures. Q2 ratio1.01655 (95%0.91137–1.13388) does
not rule out a10% regression. Custom Q1 candidate completes18 samples across3/4
blocks; no valid paired ratio, all82 completed custom outputs correct,77 gated.
Archive1048 files verified. Next prespecified Q2 follow-up:8 fresh process blocks,
12 measured pairs/block, unchanged binaries/conditions,20000 whole-block bootstrap
samples with seed20260908. Preserve both studies; no retries until passing.
docs/parallel-key-preparation-2026-09-08.md.
Parallel-key test archive8files/493source inputs verified; exact before/after patch
confirms one changed source input. Comparison drivers are prepared with fail-fast
reference calibration. Measurements launched sequentially as6123 after release.
Test archive14files and493 source inputs verified. Eight-query canonical
and separate custom-memory paired drivers launched sequentially as81974.
The driver excludes incomplete/invalid-reference blocks before median computation.

Newest: budget/width scheduled expression quantum1–8192, retaining1024 at256KiB
for the tested shape. Actual admissions/source leases/permits unchanged. Library
992pass/10ignores; focused14pass; native9pass/1fail. Only scheduling code and its
regression test differ from0ba65b55. Release29075 completed as3ff868c7,491 verified source inputs,8m44s. Paired49103 terminal1:Q10 ratio0.92279 (95%0.88987–0.95692), Q5+1.29%, other
three intervals include1. All294 completed outputs correct;286 gates pass,8 Q12
gate failures,42 not-run. No heavy job active. Fix general harness gates before
full canonical/provider validation. docs/budget-aware-expression-quantum-2026-09-08.md.
Selected Q10 improvement measured; no full-suite certification.

Previous completed expression-quantum checkpoint:
Newest: bounded1024-row grouped-expression evaluation retains full source owners,
leases and permits. New exact-value red/green regression; focused13pass;
library992pass/10ignores. Native improves9pass/1fail at unchanged256KiB, closing
ordinary/filtered aggregate and deletion completion failures. Join383984-byte
refusal remains. Release74944 completed as0ba65b55,491 verified inputs,8m43s. Paired44625 terminal1:293 completed outputs correct,285 gated passes;8 Q12 time-gate failures,43 not-run requests. Q10 ratio1.07701 (95%1.06557–1.09282), other four query intervals include1. No heavy job active. Next measure larger admitted quanta without weakening low-budget safety.
After source-verified freeze, preserve evidence and measure paired/canonical/
provider/resource performance. docs/aggregate-expression-quantum-2026-09-08.md.

Native optional-prescan checkpoint: preflight now respects the provider scan
budget before consumption. New real-native planning regression proves two
streaming occurrences; final native suite6pass/4fail. Join now reaches execution
and refuses383984bytes under262144byte budget, rather than prescan refusal.
Library988pass/10ignores, six focused integration passes. No heavy job active.
Next resource work: byte-admitted native segment/decoder/deletion/consumer batches;
no budget increase or slicing-only workaround. Evidence:
docs/native-shared-prescan-budget-2026-09-08.md. Performance/capability work remains.

Newest checkpoint: shared Parquet/native projection-elision guard fixes a direct
physical-planner wrong answer (4 versus34). Current tests:988 library passes,
10 ignores;48 selected integration passes,4 native streaming completion failures
reproduced against an isolated488-hash-verified frozen9c868dc0 source checkout.
No heavy job remains active. Evidence and outstanding resource/routing work:
docs/aggregate-source-projection-correctness-2026-09-08.md. This source is not
performance-certified; continue exact binding/capability propagation and preserve
existing strict wrapped-query failures. Earlier admitted-frontier work follows.

The generic aggregate InputFrontier now consumes PreparedAdmittedInput, validating
pool ancestry/partition count and preserving buffer-owned admission in both one-
slot and parallel execution. It avoids duplicate consumer charging and copied
output envelopes. Seven new tests pass, including a real scan -> repeated Project
-> live aggregate with actual spill and independent NULL/COUNT/SUM oracle.
Library97252:987 pass,10 existing ignores,30.03s. Integrations71603:45 pass/no skips.
Formatting and whitespace pass. Details: docs/admitted-aggregate-frontier-2026-09-08.md.

Release76420 completed as frozen9c868dc0, with488 input hashes verified.
Equivalent-SQL job41823 reached its failure assertion: bare32/32 typed/gated
outputs, ratio0.99654 (95%0.96729–1.01520); derived/CTE/column-alias forms24
warmup timeouts across both binaries and72 subsequent requests not run.
No new performance win is demonstrated. Scope max/OOM events zero.
No heavy job remains active. Preserve this failure; do not loosen ceilings.
Next bounded task: exact capability/column-lineage propagation and shared
aggregate input contracts; docs/aggregate-capability-routing-2026-09-08.md.
Canonical/provider/resource/residency gates remain open.

Paired job29163 completed:112 engine validations/time gates pass, all24 workers
exit0 without forced teardown, no scope max/OOM events. Q2 block ratio1.0096
(bootstrap95%0.9177–1.1195) and Q19 ratio0.9665 (0.9319–0.9973); neither earlier
separate-screen regression is confirmed, but Q2 remains uncertain. Archive206
verified files: docs/benchmarks/2026-09-08-admitted-protected-pairs/.

Earlier frozen ed721285 full screens: raw60/66, Native60/66, Iceberg63/66,
Lance57/66 valid pairs. All246 completed measured engine outputs validate after
supplemental Q9 checks. Reference crashes invalidate Q9 timings on Iceberg/Lance;
engine timeouts remain. Archive1267 verified files and scope evidence:
docs/benchmarks/2026-09-08-admitted-queue-sf10/.
Normal Worker shutdown now permits bounded EOF cleanup, while deadlines/protocol
errors abort immediately.112 harness tests pass with2 existing skips. Real DuckDB
spill cleanup passes;13 verified Native temporary files (11238014976 bytes) were
removed after their completed run. No production engine change was part of that
harness-only checkpoint. See docs/benchmark-worker-shutdown-2026-09-08.md.

