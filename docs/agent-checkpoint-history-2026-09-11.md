# September 11 checkpoint history

Preserved before consolidating the current working guide. Pending statements below
belong to their recorded candidates; AGENTS.md and newer linked reports take precedence.

## Current checkpoint — 2026-09-11

Current source uses32-byte Copy numeric cells in StateRows/RowWorkspace, direct
fixed-word updates sharing decimal/AVG/Welford arithmetic, and checked Arrow
values consumed by reference. PreparedGroup publishes its contained PreparedRow
in place while retaining consuming commit and state-before-key rollback. Whole-row
scratch, selected-payload admission, exact retry cursors and wire frames remain.
No dependency, binding allocation or ownership-default change. The representation
is provisional: earlier frozen compact candidates reduced memory but regressed
residentQ18. Current borrowed-input candidate passes focused82858:162/1ignored;
default68938 and partial36369 each pass1084library/11ignored. Both-mode integration
15226 terminal1 retains the same failure sets as ea1e9019;16-file correctness
archive verifies519inputs. Release78412 terminal0 in8m49s freezes fde271b1;
assembly confirms borrowed input and in-place token access. Paired51623 terminal0:
64typed-correct outputs/1820complete traces,578-file archive verifies519inputs,
zeroOOM/max. NativeQ1ratio0.738961/resident16Q1 0.727316; residentQ18 now0.960917
at16threads/0.971558at4threads, recovering earlier regression. NativeQ9 1.059907
remains a concern. Two blocks are diagnostic only. Full provider19994 terminal1:333typed-correct outputs,249/264measured pairs
(raw66/native60/Iceberg66/Lance57), default disjoint16threads,4/12GiB,3samples/1session.
Raw geomean2.975591/suite3.371426, zero wins; Iceberg0.419400/0.469339,19wins.
NativeQ1timeout/Q6late; LanceQ1timeout/Q7memory refusal/Q9reference failure.
Archive1423files verifies519inputs,zeroOOM/max. No source/harness edits during measurement.
Generic red72396 reproduces retained scan-output refusal with25.2MBparent headroom.
Current MemoryPool::child_with_progress_credit preserves the prepaid minimum while
charging excess output to ancestors; fixed prepaid caps stay fixed. Green86260 and partial73252 each pass1088library/11ignored. Corrected91137 passes
41memory/prepared integrations; existing native/spill gate failure names match fde.
Two engine files plus the existing unwind test changed; formatting/whitespace pass.
The18-file correctness archive verifies519source inputs.
Release42098 terminal0 in8m52s freezes38966ae6. LanceQ7 diagnostic72127 terminal0:
old fde refuses twice; new completes twice,553–557ms,2typed-correct outputs and
164complete join traces. Archive191files verifies519inputs; zeroOOM/max.
Shared paired88178 terminal0:64typed-correct outputs/1820complete traces,
572-file archive verifies519inputs,zeroOOM/max. RawQ1/nativeQ18/resident16Q18
are about3%slower on average; nativeQ1/Q9 apparent gains have inconsistent blocks.
Owned partial-native trace2503 confirms retained-input admission at live_spill157:
163434requested,183961used/262144limit;7-file archive. Startup repair validation is recorded below.
Full provider4926 terminal1; audit49224 terminal0 validates333completed outputs,
249/264measured pairs (raw66/native60/Iceberg63/Lance60). Archive1418files verifies
519inputs,zeroOOM/max. Rawgeo3.001650/suite3.373702,0wins. LanceQ7now3/3correct.
NativeQ1timeout/Q17late; IcebergQ9referenceSIGSEGV; LanceQ1timeout/Q9reference
allocation refusal. No source/harness edits during measurement. Full acceptance remains open.
All38966ae6jobs are terminal. Startup red83869 and preparation/first-pull reds
86662/61483 reproduce optional-worker starvation. Current live_spill primes and
admits the first batch before optional workers; an irreversible ingestion flag
permits only unused-worker reclamation, with no source/expression replay.
Focused90096 passes5new integrations and all10partial-native streaming tests.
Full both-mode99343 is terminal1: each mode1090library/11ignored and82contract
integrations pass; all10native streaming tests pass in both modes. Four prior
partial-memory failures are removed; remaining failure names are unchanged.
Three engine files plus new aggregate_startup_headroom.rs;520source inputs frozen.
Correctness archive37files verifies. Release78261 terminal0 in8m51s freezes
008d92f7. Paired81205 terminal0:64typed-correct outputs/1820complete traces,
572-file archive verifies520inputs,zeroOOM/max. Native queries are2–4%slower
and rawQ9 3.97%slower; two blocks do not certify performance neutrality.
Owned decimal-denial trace46836 terminal0 reproduces reserve_index refusal at
row24; test still fails,6-file archive verifies520inputs. Full provider61495 terminal1/audit18656 terminal0:334typed-correct outputs,
249/264valid pairs (raw66/native59/Iceberg64/Lance60). Rawgeo2.971048/suite3.369192,
0wins. NativeQ1/Q17timeouts,Q6late; IcebergQ18reference allocation refusal;
LanceQ1timeout/Q9oracle allocation refusal. Archive1424files verifies520inputs,
zeroOOM/max. All timed jobs terminal. Frontier trace79319 terminal0 confirms9copied slots
reserve228060/262144bytes across16partitions; test still fails. Six-file archive
verifies520inputs. Current copied-frontier policy caps optional queue envelopes at half available
space. Generic red63124 and9-test green16432 verify downstream workspace and
all16partitions/48batches without replay. Original decimal4757 still refuses
1536bytes at261469used/262144; trace27619 terminal0 locates merge/repartition FrameScratch denial during
run compaction, with4slots/101360-byte copied envelope. Intermediate11-file
archive verifies520inputs. Incomplete resource repair;
no new optimized measurements. Current peer coordination parks all resident
workers before compaction and retains exact selected-row cursors. Red39463
and green24294 reproduce/fix the136KiB retained-pressure fixture; original
decimal11636 terminal0 now passes at unchanged256KiB. Full both-mode
validation23676 terminal1:1092library/11ignored each, disjoint numeric12/12,
partial numeric11/12 (1016-byte refusal). Initial half-pool policy added one
concurrency failure per mode. Current working-window cap1MiB restores all6
concurrency tests45421. Partial trace27661 locates first routing metadata
refusal before rows apply. Current source delays the irreversible flag until
routing succeeds. Partial32808 completes but1000groups now fit without spill.
Strengthened2000-group spill fixture retains256KiB, plus a separate1000-group
exact oracle. Partial97824 passes3decimal cases. Corrected broad validation
77936 terminal1: each mode1092library/11ignored,82contract passes,13numeric
passes. No new failure names; existing native/IPC and six legacy spill failures
remain. Five source/test files changed;50-file correctness archive verifies520
inputs. Release56527 terminal0 in8m51s freezes a4103dfa. Paired69904 is active
against008d92f7; source performance remains unmeasured. See
[coordinated spill](docs/coordinated-spill-progress-2026-09-11.md). See [working-space follow-up](docs/copied-frontier-working-space-2026-09-11.md). See
[current provider screen](docs/startup-headroom-provider-screen-2026-09-10.md) and
[paired comparison](docs/startup-headroom-measurement-2026-09-10.md). See
[startup contract](docs/aggregate-startup-headroom-2026-09-10.md).
See [current provider screen](docs/retained-input-credit-provider-screen-2026-09-10.md) and
[paired results](docs/retained-input-credit-measurement-2026-09-10.md).
See [LanceQ7 evidence](docs/retained-input-credit-q07-2026-09-10.md).
See [progress credit](docs/retained-input-progress-credit-2026-09-10.md). See
[provider screen](docs/borrowed-fixed-provider-screen-2026-09-10.md). See
[borrowed comparison](docs/borrowed-fixed-measurement-2026-09-10.md). See
[borrowed input](docs/borrowed-fixed-input-2026-09-10.md).

Preceding direct-update ea1e9019 release99456 terminal0 in8m52s,519inputs.
Assembly confirms no per-row FixedCell::try_from call. Correctness20590/15025:
1083library/11ignored each; integration16252 retains known failures and denial
boundaries,15-file archive verified. Paired52198 terminal0:64typed-correct outputs,
1820complete traces,578-file archive verifies519inputs,zeroOOM/max. NativeQ1ratio
0.898758/resident16Q1 0.903304, but resident16Q18 1.145935 in two consistent blocks;
rawQ1 1.099127 with opposing blocks. This is not accepted performance improvement.
Owned-child Q18 debugger99009/43017 each completes100snapshots and a correct output;
34-file archive verified. Repeated PCs land on scalar/token copies, without proving
hardware stalls or CPU percentages. See [direct comparison](docs/direct-fixed-measurement-2026-09-10.md),
[update validation](docs/direct-fixed-updates-2026-09-10.md) and
[Q18 attribution](docs/direct-fixed-q18-attribution-2026-09-10.md).

Earlier compact8b6a82e9 retains~512MiB lower residentQ18 reservations but regresses
resident16Q18 16.96% and rawQ1 13.21%;573-file paired archive verified519inputs.
Its full correctness history, unchanged-budget strengthened spill fixtures and
known resource failures remain in [compact state](docs/compact-fixed-state-2026-09-10.md)
and [negative screen](docs/compact-fixed-measurement-2026-09-10.md).

The preceding native reader now decodes/deletion-filters one IPC batch per pull,
with checked extents, snapshot/cursor preservation and terminal late errors. It
removes whole-segment survivor queues but does not yet admit decoder metadata,
dictionaries or selected output. Library1080/11ignored and56default native tests
pass; dictionary-plan and partial float-comparison failures reproduce on prior
source. Archive32files/518inputs verifies. Frozen c20b0648 release32424 completed
in8m52s. Paired11044:36typed-correct outputs/1236complete traces,400-file archive,
zeroOOM/max; native speedup is inconsistent. ResidentQ1 already has16input slots
but~9s aggregate ingestion. GDB40031 captures40snapshots and correct output;
18snapshots contain the row transaction path, without establishing CPU percentages.
See [reader](docs/native-incremental-ipc-2026-09-10.md),
[measurements](docs/native-incremental-measurement-2026-09-10.md),
[row attribution](docs/aggregate-row-transaction-attribution-2026-09-10.md) and
[native admission requirements](docs/native-admitted-reader-design-2026-09-10.md).

Earlier frozen703b8564 preserves logical build schema across physical dictionary
materialization and retains typed unsplittable aggregate refusals. Shared admitted
computed projections, Date32 EXTRACT, eligible inner joins/wrappers and fixed-width
Parquet decoding remain. Paired rawQ9 improves74.67% versus2f9ad9f1; this is a
small diagnostic study, not suite leadership. Full provider52333/audit42325:
336completed typed-correct outputs,252/264valid pairs, default disjoint16threads,
4/12GiB,3samples/1session. Raw66/native63/Iceberg66/Lance57. Rawgeomean2.993145,
Iceberg0.437360/19wins. Native/LanceQ1timeouts, LanceQ7memory refusal andQ9reference
failure remain. Archive1424files verified,zeroOOM/max. This certification does not
transfer to newer source. See [provider screen](docs/build-schema-admission-provider-screen-2026-09-09.md).

No DuckDB leadership is certified. Remaining work includes measured shared
aggregate throughput/costing, native admission, protected short-query precision,
whole-page decompression and legacy join reservations, and full provider/residency/
resource/concurrency acceptance. Experimental partial ownership improves some
16thread shapes but regresses Q18 at4threads; default remains disjoint. Earlier
01bb077a residency passes278pairs at larger canonical32GiB budgets/partial mode;
canonical GPU has no successful device execution, while custom600k-float GPU has
40request-scoped proofs. This does not clear16GiB preload or certify canonical GPU.
Current identical-binary Q6 precision control remains failed (upper95%1.130588).
Chronology is preserved in the [September10 snapshot](docs/agent-checkpoint-history-2026-09-10.md),
[September9 snapshot](docs/agent-checkpoint-history-2026-09-09.md) and existing epic.


## Archived before repeated-validity measurement consolidation

The following checkpoint is historical; its intermediate active statuses are superseded by the current guide. Preserved2026-09-11 while sequence66120 builds the repeated-validity candidate.

## Current checkpoint — 2026-09-11

Current source coordinates aggregate spill progress across owners: retain each
worker's exact selected-row cursor, join all work, publish resident partial states,
release peer working sets, then compact and resume. Startup ownership becomes
irreversible only when rows can be applied, after routing succeeds. Copied input
prefetch leaves half the available pool, capped at a 1 MiB downstream window.
That window is a scheduling heuristic, not proof that arbitrary consumers fit.
Hard admission, prepared-input ownership and terminal-error contracts remain.

Corrected validation77936 is terminal1: each ownership mode passes 1,092 library
tests/11 ignored, 82 contract integrations and 13 numeric tests. The decimal
resource failure is cleared; its separate fitting and strengthened actual-spill
fixtures retain the 256 KiB budget. No new failure names remain. Native/IPC has
58 passes/2 historical dictionary-plan failures by default, 57/3 in partial mode
(including the formatted float comparison). Legacy spill remains 8 passes/6
failures each. The 50-file correctness archive verifies 520 source inputs; five
source/test files differ from008d92f7, with no dependency/default change.
See [coordinated spill](docs/coordinated-spill-progress-2026-09-11.md).

Release56527 completed in 8m51s, freezing a4103dfa. Paired69904 terminal0:
64 typed-correct outputs/1,820 complete traces against008d92f7; 572-file archive
verifies 520 inputs, zero OOM/max events. Small mixed timing changes: native
Q1/Q17 ratios0.958631/0.961119, resident16 Q1 ratio1.022320. Two blocks do not
certify performance neutrality. Full provider25249 and its independent audit
are terminal:332 typed-correct outputs,249/264 valid measured pairs
(raw66/native60/Iceberg63/Lance60). Raw geomean2.955826/suite3.348167, zero wins.
Native Q1/Q6 and Lance Q1 warmups time out; Iceberg/Lance Q9 reference calibration
allocation refusals leave missing pairs. Archive1,420 files verifies520 inputs,
zero cgroup OOM/max events. No source/harness changes during measurement.
See [provider screen](docs/peer-spill-provider-screen-2026-09-11.md) and
[comparison](docs/peer-spill-measurement-2026-09-11.md).

Current source repairs LEFT COUNT qualifier/name binding and now adds a costed
right preaggregate with retained final SUM when left uniqueness is unknown.
Statistics never permit removing duplicate reduction. Scope remains one group/key
and one right-column COUNT; broader shapes decline. Broad39449 passes1,030
library/3ignored and46 integrations with default features; lineage/costing tests
cover both planner modes, duplicates, NULLs, empties and convergence. Release14244 completed in8m51s, freezing0c867c5c with520 verified source inputs
and lance/gpu features. Diagnostic10779 terminal0 validates three typed-correct Q13 outputs and active
retained-count plans in raw/native/resident modes. Archive193 files verifies520
inputs, zero OOM/max. Paired97041 terminal0:80 typed-correct outputs/2,036 complete traces,
672-file archive/520 inputs, zero OOM/max. Q13 ratios raw0.294943/native0.871506/
resident16 0.919702/resident4 0.826499, with gains in both blocks. RawQ9 1.041648
and resident4Q9 1.028211 are slower in both blocks; no neutrality/leadership claim.
Independent generated resource test returns10,002 correct groups at1/4/16MiB,
with1,960,572bytes spilled at1MiB;29-file archive/520 inputs. Both-mode feature validation20503 terminal1:each1,092 library/11ignored,
115 contracts pass; native/IPC58/2default57/3partial and legacyspill8/6each retain
historical failures, with no added names. Archive26 files verifies520 inputs.
Full provider20601 terminal1/audit89129 terminal0:336 typed-correct outputs,
251/264 valid pairs(raw66/native60/Iceberg65/Lance60). Raw geomean2.831285/
suite3.059779, zero wins. Native Q1/Q17 and Lance Q1 warmups time out; Iceberg
Q18 measured-reference and Lance Q9 oracle allocation refusals remain. Archive
1,426 files verifies520 inputs, zero OOM/max. See
[provider screen](docs/left-count-provider-screen-2026-09-11.md).

Frozen0c867c5c additionally fails a generic unaliased correlated SUM/COUNT probe:
duplicate outer keys multiply inner aggregate input, returning[2,1,1] instead of
[2]. Aliased controls remain correct but do not decorrelate. Archive34 files
verifies520 frozen inputs. Current source repairs the membership reduction to
SEMI, preserving original aggregate input multiplicity/schema. Regression95874
is red; focused95743 passes34 integrations. Expanded broad96185 terminal0 passes1,030 library/3ignored and47 integrations
with default features. Repair archive8 files verifies520 inputs. Both-mode feature validation45945 terminal1:each1,092 library/11ignored and
116 contracts pass; historical failures unchanged,25-file archive/520 inputs.
Regression16461/96126 exposed missing empty scalar aggregate results. Current
source adds symbolic typed empty values and a fresh right-row presence marker;
lazy CASE distinguishes absent groups from matched NULL. Scalar output identity
requires one exact name. Unsupported grouped/HAVING/LIMIT/function shapes retain
scalar execution. Corrected21478 and expanded34129 pass; broad25322 terminal0 passes1,030
library/3ignored and48 integrations with default features.
Repair archive9 files verifies521 source inputs. Both-mode feature validation16808 completed: each1092 library/11ignored and117 contracts pass; historical native/spill failure names persist. Validation archive25 files verifies521 inputs. New module:subquery_decorrelation/empty_result.rs.
No dependency/default change.
Right-built SEMI runtime-filter wiring now selects the left probe through proven
lineage; right-built Anti/Left remain excluded to preserve unmatched rows.
Red20522 reproduces missing linkage; green28770 passes29 integrations, expanded
broad83080 passes1,030 library/3ignored and43 integrations including empty builds.
Archive10 files verifies521 inputs; only planner.rs and runtime-filter lineage
regression differ from the empty-scalar snapshot. Both-mode feature/resource
validation94085 completed:each1092 library/11ignored,125 contracts,28 spill/numeric
passes; historical native/spill failures unchanged. Archive25 files verifies521
inputs. Release72847 completed in8m52s, freezing68912c23/521 inputs; source
frozen. Sequence82829 is running9 Q9/Q13/Q17 diagnostic outputs,16 same-binary
Q1/Q6 aggregate-route controls, and the full canonical raw/native/Iceberg/Lance
screen sequentially; diagnostic failure stops later stages. Diagnostic stage
terminal0:9 typed-correct outputs/319 traces, zeroOOM/max,229-file archive verifies
521 inputs. Q17 shows SEMI reduction and lazy CASE but raw aggregate input remains
1slot. Same-binary route control terminal0:16 typed-correct outputs,276-file
archive/521 inputs. Q1 morsel/generic mean ratios0.078444 at16threads and0.179815
at4threads; Q6 mixed/slightly regressed. Generic Q1 already has16admitted slots;
ingestion remains expensive. Diagnostic only, not DuckDB leadership. Full provider
screen is active in sequence82829:raw completes22 queries,geomean2.713836,
suite2.975360,0wins; native terminal1 has58valid measured pairs,7not_run
and1timeout (Q1/Q6 warmup gates, Q12 measured timeout). Iceberg terminal1:
63valid pairs; DuckDB Q9 first calibration crashes with exit-11 after oracle and
warmup complete, so engineQ9 is not_run. Lance terminal1:60valid pairs,Q1 warmup
timeout and Q9 reference134217728-byte refusal. Full sequence82829 terminal1:
247/264valid pairs,521-source after-check passes,zeroOOM/max; supplemental audit
and provider archive await completion of active timing jobs. Follow-up92202 waits for
terminal provider sequence, then profiles current generic Q1 and runs default
disjoint IPC/GPU residency plus separate custom required-GPU smoke. Profile stage
terminal0:100snapshots,typed-correct result,22-file archive/521inputs; mixed decoder
and aggregate leaf frames do not prove one dominant function. Decoded IPC stage
terminal1:63valid measured pairs,Q1 warmup timeout. Canonical preloaded CPU
control also terminal1:63valid pairs,Q1 warmup timeout; mixed GPU stage active.
Audit31858 waits for terminal residency before independent provider/residency
typed-output and request-scoped GPU checks; no overlap with measured queries.
All68912c23 jobs terminal. Audit31858 terminal0:provider330correct outputs,
1418-file archive; residency252correct outputs,206/212requested pairs,1100-file
archive. Canonical mixed GPU ran0requests because CPU control was incomplete;
custom required GPU40measured requests validate device evidence. Source archives
verify521inputs. Current source now bulk-fills validated repeated validity runs,
preserving reservation, cursor commit and poison semantics. Focused7509 passes9;
expanded broad52973 terminal0:1032library/3ignored and54integrations. Archive7files
verifies521inputs. Both-mode feature/resource validation23407 terminal1:each1094
library/11ignored,125contracts,28spill/numeric passes; historical native/spill
failure names unchanged. Validation archive25files/521inputs. Source frozen.
Sequence66120 passed the failure comparison and is building
a verified release and runs120 paired typed-output diagnostics against68912c23
(seven queries/raw-native-resident16/resident4 plus generic rawQ1/Q6).
Only admitted_hybrid.rs differs from frozen68912c23.
No optimized performance claim for this candidate.
See [validity candidate](docs/repeated-validity-decoding-2026-09-11.md).
See [profile](docs/generic-aggregate-profile-2026-09-11.md). See [route control](docs/aggregate-route-control-2026-09-11.md).
See [semi filter contract](docs/right-built-semi-runtime-filter-2026-09-11.md).
See [empty-result contract](docs/correlated-empty-result-contract-2026-09-11.md).
No performance claim belongs to this repair. See
[correlated reduction](docs/correlated-reduction-proof-audit-2026-09-11.md). See
[measurement](docs/left-count-measurement-2026-09-11.md). See
[retained reduction](docs/left-count-retained-reduction-2026-09-11.md) and
[binding contract](docs/left-count-binding-contract-2026-09-11.md).

Retained contracts include borrowed fixed numeric input and 32-byte state cells,
progress-credit child pools, incremental native IPC pulls, logical join build
schemas across physical dictionaries, and admitted computed/temporal/inner-join
pipelines. Native IPC decoding still lacks full admitted ownership. Earlier raw
Q9 gains and recovered resident Q18 regressions remain in linked historical reports.

No DuckDB leadership is certified. Remaining gates include shared CPU throughput,
native admission, full provider/residency/resource/concurrency acceptance, legacy
join/page/result allocation boundaries and short-query precision. Default ownership
stays disjoint. Earlier larger-budget partial residency does not clear 16 GiB
preload refusal. Canonical GPU has no successful device execution; custom float
GPU evidence is separate. The identical-binary Q6 precision control remains failed
(upper95%1.130588). Preserve all negative results.

Chronology: [September11 snapshot](docs/agent-checkpoint-history-2026-09-11.md),
[September10 snapshot](docs/agent-checkpoint-history-2026-09-10.md), and the existing epic.



## Snapshot before coordinated-reader guide consolidation

Current source retains coordinated aggregate spill across owners, exact retry
cursors, admitted input ownership, compact fixed cells and borrowed numeric input.
Copied-input prefetch leaves half the available pool, capped at1MiB, as a scheduling
heuristic; actual reservations remain authoritative. Native IPC decoding still
lacks full admitted ownership. Default ownership remains disjoint.

Semantic repairs preserve duplicate multiplicity in LEFT COUNT preaggregation and
correlated aggregates. Statistics never prove uniqueness. Correlated reduction
uses SEMI; scalar empty-result derivation plus a fresh presence marker distinguishes
missing groups from matched NULL. Unsupported shapes retain scalar execution.
Right-built SEMI runtime filters target the left probe by proven lineage;
right-built Anti/Left preserve unmatched probe rows. See [correlated reduction](correlated-reduction-proof-audit-2026-09-11.md),
[empty results](correlated-empty-result-contract-2026-09-11.md),
[LEFT COUNT](left-count-retained-reduction-2026-09-11.md) and
[semi filtering](right-built-semi-runtime-filter-2026-09-11.md).

Packed-word sequence56279 and evidence19981 are terminal0: release63cabb3a,
120typed-correct outputs/2476join traces,60identical plan pairs,915-file archive,
521verified inputs and zeroOOM/max. The optimization was rejected: genericQ1
ratio1.035281, nativeQ6 1.136598/nativeQ9 1.035107 and resident16Q18 1.047894;
several regressions persist in both blocks. RawQ18 0.970651 does not establish
broad throughput improvement. No confidence-certified gain or neutrality.
Current production hybrid code is restored exactly to measured240cd5e2, retaining
new widths0–32 regression tests. Three-file rollback archive verifies521inputs.
See [measurement and rejection](packed-word-measurement-2026-09-11.md) and
[rollback](decoder-rollback-2026-09-11.md).

Native Anti test-contract repair82790 is terminal0 after the benchmark archive:
27focused integrations pass in each mode;15-file source/evidence archive verifies
521inputs. NOT EXISTS now exercises actual dictionary Anti/build orientations;
original NOT IN comparisons and independent NULL/duplicate/empty-RHS expectations
remain. No engine rewrite or memory policy was changed. Prior packed-word broad
feature gates had1095library/11ignored,125contracts and28spill/numeric passes each,
with native58/2 disjoint,57/3 partial and six legacy spill failures each. These
pre-repair failures remain archived. Rollback validation79575 is terminal1:1095library/11ignored,125contracts and28
spill/numeric passes each; native63/0 disjoint and62/1 partial, six legacy spill
failures each.25-file archive verifies521inputs. See [test repair](native-anti-test-contract-2026-09-11.md).

Current batch-view candidate adds `state_rows/array_view.rs`: checked fixed-array
views bind once into16stack slots; wider layouts/dictionaries retain the adapter.
No heap descriptors, default/budget change or early row publication. Production
hybrid code still matches240cd5e2. Broad80443 passes1036library/3ignored and62
integrations;11-file source archive verifies523inputs. Both-mode64579 terminal1:
1098library/11ignored,125contracts and28spill/numeric each; native63/0disjoint,
62/1partial,six legacy spill failures each.25-file validation archive verifies.

Sequence35732/evidence29174 terminal0: release480109f6 in8m52s,120typed-correct
outputs/2476traces,60identical plan pairs,914-file archive/523inputs,zeroOOM/max.
Q1 ratios generic raw0.819005/native0.789009/resident16 0.792398/resident4 0.813728
improve in both blocks. Q10 regresses in both blocks on raw1.028254,resident16
1.011322/resident4 1.031138; Q6 remains noisy. Retain provisionally, no confidence
or neutrality certification. See [measurement](batch-views-measurement-2026-09-11.md).

Refusal trace79512/archive86197 terminal0: six reproduced failing tests,12denial
stacks,19files/523inputs/eight fixtures and frozen debug binary verify,zeroOOM/max.
8KiB semi/anti traces reach Reader::open schema admission; grouped join/outer
reach whole decoded-page admission after output-quantum halving; COUNT(DISTINCT)
reaches the speculative header read window. These remain unresolved resource
failures, not successful spill coverage. See [attribution](legacy-spill-refusal-results-2026-09-11.md).

Full sequence31964 terminal1 and independent evidence13409 terminal0 for480109f6.
Providers:332typed-correct outputs,248/264valid pairs(raw66/native57/Iceberg64/
Lance61),1428verified archive files. Raw geomean2.557747/suite2.790526,zero wins;
other tracks incomplete. Residency:348typed-correct outputs,278/278valid pairs,
1458verified files. All three canonical tracks complete at32/48GiB capacity:
decoded IPC geomean0.760895/suite1.325514,14wins. Canonical mixed GPU has0device
requests; custom required GPU verifies40measured device requests. Preserve16GiB
preload, resource/concurrency and leadership gates.523inputs verify; screen peak
26892062720bytes,zeroOOM/max. See [providers](batch-views-providers-screen-2026-09-11.md),
[residency and corrected device qualification](batch-views-residency-screen-2026-09-11.md)
and [failure attribution](batch-views-screen-failure-attribution-2026-09-11.md).

After archival, initial header baseline88461 exposed omitted buffer-owner overhead
in the draft tests. Corrected10231 reproduces3failures/3passes while preserving
that charge. Candidate now grows parser-proven header prefixes and shares checked
initial allocation cost in ReservedVec/ReservedBufferBuilder. Only three existing
source files plus the new test module differ from480109f6. Focused4648 passes15;
broad50886 passes1042library/3ignored and77integrations,retaining six legacy spill
failures. COUNT(DISTINCT) now requests9512bytes; no resource/performance closure.
Thirteen-file source/red/green/broad archive verifies524inputs.
Both-mode38824 terminal1:1104library/11ignored,125contracts,28spill/numeric
passes each; native63/0disjoint62/1partial,six legacy spill failures each. No new
failure names or missing executables;27-file archive verifies524inputs,zeroOOM/max.
Sequence57421/evidence18433 terminal0:13210a20 built8m53s,120typed-correct outputs,
2476traces,60equal plan pairs,914verified files/524inputs,zeroOOM/max. RawQ9ratio
0.963374 improves both blocks; nativeQ6ratio1.182007 regresses both, as do smaller
raw/nativeQ13 and resident16Q1 cases. No neutrality/leadership claim. Precision12690 terminal1: identical480 nativeQ6 fails interval0.820231–1.259049;
Q9 passes0.985811–1.016126. Dependent comparison NOTRUN.379-file archive verifies
32correct outputs/624traces and524worktree inputs; executed480 has523-input provenance.
Native execution profile40662 terminal0:149stopped snapshots/onecorrect result,32-file
archive.75snapshots contain filter comparison,44numeric coercion; no CPU-percent
claim. Source-line attempt failed before query execution; function-symbol retry used
the same480binary. Numeric coercion red25456 reproduces four compiler gaps.
Current candidate adds chunk-local float coercion/reuse and shared decimal type
admission. Focused88389 passes17tests; earlier57397 passed16 with one stale
unsupported-shape expectation, now corrected. Contracts28884 pass22tests;
library76131 passes1046/3ignored. Both-mode lance/gpu resource gate65485
terminal1: each library1108/11ignored,contracts125,spill28/6fail; native63/0
disjoint62/1partial. No changed failures;25-file archive verifies526inputs,zeroOOM/max.
Source526 frozen. Native probe95635 terminal0 proves actual decimal conversion
at source1171,one float register,divisor100,len1024,typed-correct canonicalQ6.
51-file archive verifies526inputs,zeroOOM/max; three earlier debugger assertion/display
failures remain preserved. Release43258 completed8m51s, freezes97e53169/526inputs.
Paired43258/evidence36280 terminal0:120typed-correct outputs,2476traces,
60unchanged plan pairs;914-file archive verifies526inputs,zeroOOM/max. Q6ratios
candidate/control:generic raw2.224442,raw1.203546,native1.421561,resident16
1.166881,resident4 1.376582,all slower in both blocks. Candidate not accepted.
Attribution28368 terminal0:48typed-correct controls/onecorrect profile,76snapshots,
350-file archive verifies526inputs,zeroOOM/max. Genericraw compiler on/off2.232903
candidate versus1.004317old; nativeQ6 remains noisy.63sampled evaluator leaves map
to a per-row float operator jump table. Current one-file candidate selects float
operators outside row loops; focused99196 passes17. Both-mode71658 terminal1:
each library1108/11ignored,contracts125,spill28/6fail;native63/0disjoint62/1partial.
No changed failures;27-file archive verifies526inputs,zeroOOM/max. Sequence92933
passes22numeric contracts; release completes8m55s,freezes ece6a4d6/526inputs.
Triage92933 terminal0:42correct/792traces/14equal plan groups,453verifiedfiles.
GenericrawQ6 ratio0.928567vs97e53169 but2.074367vs13210a20; candidate not accepted.
Profile44421 terminal0:67snapshots/onecorrect output,22-file archive verifies526inputs.
Runtime audit exposes confound: genericraw copied458batches becomes admitted7323
at unchanged16slots; resident4changes ownership route at unchanged916batches/4slots.
Native keeps916batches/1slot. Plan text equality misses these changes. Current
source propagates planned scan.batch_size into admitted Reader instead of fixed8192.
Red30866 reproduces both exceeded17-row and capped32768-row requests; green18452
passes14scanner tests. Source527 archived; both-mode93080 terminal1:1110library/11ignored,125contracts and28spill/numeric passes each; native63/0disjoint,62/1partial and six legacy spill failures each. No added/removed failures. Release95646 terminal0 in8m49s freezes beb0cdfd. Checkpoint37633 terminal1;
evidence56910 terminal0 verifies527inputs,452diagnostic/1433provider/1458residency
files. Providers340typed-correct,255/264valid pairs; raw geomean2.579082/suite2.808489.
NativeQ1 warmup timeout,IcebergQ9 referenceSIGSEGV,LanceQ9 join-index refusal remain.
Residency348typed-correct,278/278valid pairs at32/48GiB; canonicalGPU0device requests,
custom required40measured device proofs. Peak23061450752bytes,zeroOOM/max. Genericraw
Q6 still7323batches versus458baseline; no throughput recovery certified. All jobs
terminal. Intermediate commit0c554142 was pushed and exact remote branch hash verified.
Next-cycle matrix78387 terminal101:2empty-selection passes and4fixed/string
dense/sparse batching failures after independent values and ownership checks pass.
Production is unchanged; source528/red evidence verifies. See
[post-filter reproduction](admitted-filter-batching-2026-09-11.md). Current
source removes predicate-only survivor copies via filter_projected and reuses a
static-only nullable mask directly. Gather5passes/scanner16passes with exactly4
known batching failures; source528/component archive verifies. Current source now adds admitted_coalesce with typed fixed-capacity buffers, bounded
UTF8 bytes and pre-reserved handoff. Reader retains one pending batch/exact offset;
whole-chunk construction refusal may bypass packing, source errors stay terminal.
Accumulator4/scanner23 passes, including the4batching regressions and post-prefix
I/O failure. Source530 archive verifies; broad84021 terminal1:1125library/11ignored,125contracts,
28spill/numeric passes each; native63/0disjoint,62/1partial and6spill failures each,
no added/removed failures. Validation26files verify,peak21864136704,zeroOOM/max.
Release63185 terminal0 in8m48s freezes1bba20b3/530inputs. Checkpoint11187 terminal1;
evidence76516 terminal0 verifies452diagnostic/1436provider/1097residency files.
Triage42typed-correct/792traces/14equal plan groups; genericrawQ6 restores458batches
from7323,ratio0.897777 versusbeb0cdfd but1.828793 versuspre-coercion13210a20.
Native/resident4Q9 regress versuscontrol in both blocks; no broad neutrality claim.
Providers339typed-correct,253/264valid pairs; rawgeomean2.447789/suite2.629099,0wins.
NativeQ1/Q6 warmup timeouts,IcebergQ13 referenceSIGSEGV,LanceQ9 join-index refusal.
Residency256typed-correct,209/212emitted measured pairs; intended278 not complete:
CPUcontrolQ1 timeout blocks all66canonical mixedGPU requests. DecodedIPC completes,
geomean0.708858/suite1.002784,14wins at32/48GiB capacity,not16GiBpreload acceptance.
CustomrequiredGPU40measured device proofs; canonicaldevice coverage0/notrun.
Peak22185889792bytes,zeroOOM/max. All jobs terminal. Intermediate commitde7605f was pushed and the exact remote
branch hash verified. Next-cycle working_space_tests adds same-budget small/large
reader targets and an oversized-page refusal control; production is unchanged.
Focused89920 terminal101: fresh1row reader completes4096rows at160KiB, but fresh
4096target refuses first output and retained1row retry also refuses. Oversized
page control passes at32KiB. Four-file red archive verifies531inputs; coordinated
page preparation/output sizing is now a reproduced progress issue, not speculation.
Current candidate separates page preparation and reversible output trials, with
admitted checkpoints and common row-target halving on typed memory denial. No
source replay; non-memory errors remain terminal. Initial56840 passes2, existing
admitted39033 passes135. COUNTdistinct22126 still fails (23043requested,248034used
at256KiB), not resource closure. Full library77899 passes1069/3ignored. Final
checkpoint storage is only for missing columns; final531source archive verifies.
Both-mode Lance/GPU38419 terminal1:1131library/11ignored,125contracts,28spill/numeric
passes each; native63/0disjoint62/1partial,6spill failures each. No added/removed
failures;25-file archive verifies531inputs. Scope reached48GiB/103max events,
zeroOOM; combined compilation/tests, not query-only telemetry. Diagnostic18068
terminal0 identifies whole-page decompression before trials, already1row target
and zero join batches;10-file archive verifies531inputs/eightfixtures,zeroOOM/max.
Release5788 terminal0 in8m49s freezes7ddbf9ce/531inputs; checkpoint98921 now
runs three-binary triage, then fullSF10 providers/residency. Evidence72426 waits until all timing ends. No source
edits or duplicate jobs. No current SF10/performance claim. See
[coordinated reader](coordinated-reader-output-2026-09-11.md).
See [current checkpoint](admitted-coalesce-checkpoint-2026-09-11.md).
No performance or first-batch memory recovery claimed. See [quantum contract](admitted-planned-quantum-2026-09-11.md).
See [route audit](admitted-route-cost-confound-2026-09-11.md). See [dispatch attribution](compiled-coercion-attribution-2026-09-11.md). See [measurement](compiled-coercion-measurement-2026-09-11.md).
Trace69517/archive94698 terminal0: COUNTdistinct still fails,3stacks,13files/524inputs/
eightfixtures and frozen debug binary verify,zeroOOM/max. New barrier is encoded
page-body read during join build-input polling, not speculative header admission.
Ledger65075 terminal0 on source526 proves zero retained join batches at refusal;
11-file archive verifies,zeroOOM/max. The first batch reader requires investigation,
not accumulated join input. See [ledger](first-batch-refusal-ledger-2026-09-11.md). Schema-only
reduction is insufficient: handoff4096bytes per column plus owners exceeds8KiB for
two columns. See [minimum-progress review](legacy-spill-refusal-results-2026-09-11.md)
and [header implementation](incremental-header-admission-plan-2026-09-11.md).

Sequence66120 is terminal0. Release240cd5e2 built in8m51s with521verified inputs;
120paired outputs against68912c23 are typed-correct,with2476join traces and zeroOOM/max.
All60logical/physical plan pairs are identical;915-file archive verifies. RawQ9/Q10
mean ratios0.959229/0.953205 improve in both blocks. Generic rawQ1 does not improve
(1.011340); rawQ1 and nativeQ17 are slower in both blocks (1.019667/1.019884).
Q6 controls have substantial variation. Candidate is retained provisionally;
no confidence-certified speedup,neutrality or DuckDB leadership. All240cd5e2 jobs terminal.
See [measurement](validity-repeated-measurement-2026-09-11.md),
[implementation](repeated-validity-decoding-2026-09-11.md) and
[validation](validity-repeated-validation-2026-09-11.md).

Earlier completed full provider/residency measurement binary:68912c23,521 source inputs,lance/gpu features.
All its jobs and independent audits are terminal:

- Provider screen:330typed-correct outputs,247/264valid measured pairs
  (raw66/native58/Iceberg63/Lance60),1418-file archive. Raw completes22queries with
  geomean2.713836/suite2.975360 versus DuckDB,zero wins. NativeQ1/Q6 warmup gates and
  Q12 measured timeout remain. IcebergQ9 reference cal0 crashes with exit-11;
  LanceQ1 times out and Q9 reference refuses134217728bytes. No OOM/max cgroup events.
  [Provider evidence](right-semi-provider-screen-2026-09-11.md).
- IPC/residency:252typed-correct outputs,206/212requested valid measured pairs,
  1100-file archive. Decoded IPC and preloaded CPU each have63/66pairs,Q1 warmup
  timeout. Canonical mixed GPU executes0requests because CPU control is incomplete.
  Custom float smoke has40required-device measured requests with valid request-scoped
  evidence. Default-disjoint32/48GiB canonical budgets do not clear16GiB preload.
  [Residency evidence](right-semi-residency-2026-09-11.md).
- Same-binary aggregate-route control:16typed-correct outputs,276-file archive.
  Q1 morsel/generic ratios0.078444 at16threads and0.179815 at4threads; Q6 mixed or
  slightly slower. Generic Q1 already has16admitted input slots. These are two-block
  diagnostics, not confidence-certified comparisons. [Route control](aggregate-route-control-2026-09-11.md).
- Current generic-Q1 profile:100snapshots and a correct output,22-file archive.
  Decoder and aggregate-update leaf frames suggest several costs; inlining does
  not identify validity decoding as dominant. [Profile](generic-aggregate-profile-2026-09-11.md).
  Q9/Q13/Q17 diagnostic validates9outputs; Q17 shows SEMI/CASE but raw aggregate
  input remains1slot. [Diagnostic](right-semi-measurement-2026-09-11.md).

No DuckDB leadership is certified. Remaining gates include shared CPU throughput,
native admission, full provider/residency/resource/concurrency acceptance, legacy
join/page/result allocation boundaries and short-query precision. Native Anti coverage now separates NOT IN semantics from ordinary Anti operators;
full native resource acceptance remains open. Earlier partial
ownership gains do not establish a safe default; the4thread Q18 regression and
16GiB preload refusal remain historical negative evidence. Canonical GPU has no
successful device execution. Identical-binary customQ6 precision control remains
failed (upper95%1.130588); its dependent comparison was not run.

Earlier candidate measurements, intermediate checkpoints and validation histories
are retained in the [September11 history](agent-checkpoint-history-2026-09-11.md)
and linked reports. Their active states and binary-specific results are historical.

