# Checkpoint before compact fixed-state consolidation

Historical evidence only; current source and AGENTS.md take precedence.

## Current checkpoint — 2026-09-09

Current source repairs transitive admitted input through computed projections,
Date32 EXTRACT and eligible inner joins. `project/admitted.rs` binds a closed
numeric program before child preparation, retains admitted output ownership and
terminates errors without source/expression replay. `filter/temporal.rs` avoids
row-wise expansion of EXTRACT metadata. `reserved_decimal.rs` admits exact decimal
arithmetic; shared numeric typing now handles mixed signed/unsigned division as
Float64. Unsupported domains decline before source preparation. Copied-output
bounds stay column-only; no dependency or default-ownership change.

The initial release3172f9e5 (82930 terminal0,8m53s,518verified inputs) still left
canonical Q9 at one input slot. Diagnostic18871 terminal0 validates3outputs:
raw5894ms/native2721ms/resident2827ms, zeroOOM/max. Its188-file archive is verified.
The physical planner uses SpillableHashJoinExec, whose admitted factory still
excludedInner. Current source fixes that wrapper: only the existing InMemory
build decision delegates, while Spill still declines. Red11478 reproduces the
barrier; green81365 proves four-partition lazy pulls, duplicates and cleanup.

Broad70213 terminal0:1,073 library passes/11ignored and47 integrations. Both-mode
17836 terminal1: partial library1,073/11ignored, native10default versus6partial
passes/4failures, aggregate-memory10passes each, spill8pass/6fail and systemic
numeric11pass/1fail each. Failure names persist but denial boundaries can change;
these are unresolved resource gates. Correctness archives verify26initial and
20wrapper files, each518source inputs. Formatting/whitespace pass. Corrected
release66680 terminal0 in8m55s freezes e4608ccf. Diagnostic57734 terminal0:
3correct outputs, residentQ9 uses16slots/66admitted probe traces at1.626s; raw
5.903s/native2.734s remain1slot. Archive188files verifies518inputs,zeroOOM/max.
Paired91923 terminal0:12typed-correct outputs/720complete traces; resident
Q9ratio0.565225 (43.48%lower query time), raw0.994857/native1.001873. Two blocks
are diagnostic only. Archive252files verifies518inputs,zeroOOM/max. All jobs
terminal for that frozen candidate. Current source now lets fixed-width raw scans
prepare the existing admitted decoder independently of their copied-output bound.
Preparation-only memory refusal may decline to a certified copied route before
pages/output are consumed; selected-stream errors remain terminal. Red63061 and
initial broad51852 exposed the gate and tiny-budget compatibility issue; corrected
90721 passes1075library/11ignored and37integrations. Both-mode5005 retains native
10default/6partial passes, spill8pass/6fail and numeric11pass/1fail each; partial
library1075/11ignored. Archive20files verifies518inputs; only admitted.rs changed
versus e4608ccf. Release44979/2f9ad9f1 completed; diagnostic38254 validates3outputs
but raw still1slot. Generic initialization probes locate another barrier:
SpillableHashJoin replaced declared build schema with first-batch physical encoding.
Current source preserves declared schema; dictionary/codebook regression75599 is
red,72928 green. Broad65131 exposed lost typed memory cause in unsplittable aggregate
partitions; deterministic red62644 reproduces and the scheduler now retains it.
Corrected7587 passes1077library/11ignored and43integrations; both-mode20831 keeps
resource failures, partial library1077/11ignored. Probe69311 confirms full rawQ9
admitted preparation/16declared partitions with0output pulls and cleanup to0.
Archive41files verifies518inputs. Release76303 terminal0 in8m53s freezes703b8564.
Diagnostic74715 terminal0:3typed-correct outputs; rawQ9 now16slots/1.521s, native
1slot/2.710s, resident16slots/1.631s;188-file archive verifies,zeroOOM/max.
Paired24687 terminal0:12typed-correct outputs/720complete traces; rawQ9ratio
0.253285 (74.67%lower query time), native1.001460/resident0.979491. Two-block
diagnostic only;252-file archive verifies518inputs,zeroOOM/max. Full canonical
provider screen52333 terminal1/audit42325 terminal0:336completed outputs typed-correct,
252/264valid pairs (raw66/native63/Iceberg66/Lance57), default disjoint,16threads,
4/12GiB,3samples/1session. Raw geometric ratio2.993145; Iceberg0.437360 with19wins.
Native Q1 and Lance Q1 time out; Lance Q7 refuses memory, Q9 reference fails.
Archive1424files verifies,zeroOOM/max. Diagnostic15989 terminal0 validates6outputs:
nativeQ1 takes11.485s with8declared partitions/1slot and9.348s aggregate ingestion.
Archive205files verifies518inputs. No source changes in this measurement step.
Native incremental ownership and general aggregation costing remain next; no
provider/residency/resource/concurrency leadership certification.
See [provider results](build-schema-admission-provider-screen-2026-09-09.md)
and [next attribution](build-schema-bottleneck-attribution-2026-09-10.md).
See [fixed scan](fixed-width-admitted-scan-2026-09-09.md) and
[schema/refusal contracts](build-schema-admission-2026-09-09.md). See [implementation](admitted-computed-pipeline-2026-09-09.md)
and [Q9 evidence](admitted-pipeline-q09-measurement-2026-09-09.md).

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
All measurement jobs terminal; source unchanged during comparison against703b8564. Local Arrow58.4.0 audit confirms alignment copies,
compression and dictionary-delta concatenation must be covered before admission;
see [allocation design](native-admitted-reader-design-2026-09-10.md).
No speedup or native admission certification yet.
See [incremental native contract](native-incremental-ipc-2026-09-10.md).

Debugger attribution40031 terminal0 captures40nativeQ1 snapshots and a typed-correct
output on c20b0648;518source inputs verify. prepare_arrays_indexed/ingest_inner
appear in18snapshots; clone/commit/update frames identify the fixed row transaction
as the next shared optimization target. These are not CPU percentages. Initial
console-run protocol failure and pre-execution path typo remain archived. All jobs
terminal; investigate compact fixed state without weakening row atomicity, spill
cursor, numeric semantics or selected-payload admission.
See [row transaction attribution](aggregate-row-transaction-attribution-2026-09-10.md).

Last broader measurements belong to earlier binaries, not current source:

- Frozen01bb077a: protected519correct outputs/514gated,5late,10timeouts,87not_run;
  Q9/Q12 incomplete. Current custom Q6 identical-binary precision control failed
  (upper95%1.130588); dependent comparison was not run. Preserve that failure.
- Same01bb residency:348typed-correct outputs and278/278measured pairs at explicitly
  larger canonical32GiB budgets and experimental partial ownership. Canonical
  mixed-GPU recorded **no successful device execution**. Custom600k-float required
  GPU40/40 had request-scoped device evidence. This does not clear16GiB preload
  refusal or certify canonical GPU execution. Archive1455files verifies511inputs.
- Frozenbd689bf6 provider screen:331correct outputs,247/264valid measured pairs
  (raw63/native57/Iceberg66/Lance61). Iceberg completes one session; other provider
  timeouts/reference failures and multi-session acceptance remain open.
- Experimental partial ownership plus parallel final reduction improves observed
  16-thread Q1/Q18, but Q18 regresses at4threads. Default stays disjoint/up to4
  owners; general ownership costing remains open. Output construction uses a
  maximum1024-row quantum with typed-denial halving and exact cursor preservation.

No DuckDB leadership is certified. Remaining work includes full provider,
residency, resource and concurrency acceptance; short-query measurement precision;
partial-worker startup headroom; whole-page decompression refusals; legacy join
reservations and collected results. Preserve SQL proof rules and memory safety.
Detailed chronological evidence is retained in the
[September9 checkpoint snapshot](agent-checkpoint-history-2026-09-09.md),
[September8 history](agent-checkpoint-history-2026-09-08.md) and existing epic.

