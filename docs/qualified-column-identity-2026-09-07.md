# Lossless column identity across execution schemas

The source 647 OR-rule fix exposed a separate pipeline defect: qualified column
identity was flattened into Arrow field names and then guessed back from strings.
The current worktree adds lossless identity metadata and shared resolution across
execution paths. Focused schema, SQL and compiled-loader regressions pass. Broad
debug validation now passes; optimized validation is pending. This is not performance acceptance.

## Independent reproduction

The six-query probe uses two small Parquet tables, multiple row groups, duplicate
values and NULLs. It compares full typed results with DuckDB. Columns
`a."b.c"` and `"a.b".c` both appeared as the physical name `a.b.c`.

Both frozen 642 and frozen 647 completed all six queries, but three answers were
wrong: projection, OR filtering and OR/IN filtering. The distinct-ID, left-filter
and right-filter controls matched. Projection copied the right value into both
outputs. The OR queries should return eight rows; 642 returned six and 647 four.
Thus this is an existing physical/schema defect, not proof that the OR-rule
implication fix repaired the entire pipeline.

Source 647 binary SHA256 is
`94e37892b880480c75fcdcf1ff79e84ae10700b1e0c350aa1f8ec334851a12d1`.
The preserved probes are under
`.scratch/or-column-identity-repair/sql-identity-probe{,-642}/`.
All normal optimized semantic checks for 647 also ran successfully, retaining the
explicit eight preexisting integer-division differences from DuckDB and the
separate bare-NULL value comparison. Those checks do not cover this new defect.

## Contract changes

- Logical qualified indexes preserve separate relation/name keys. Duplicate exact
  qualified identities are ambiguous instead of overwriting a previous field.
- Engine-created Arrow fields retain their existing display name and carry
  `query_engine.bound_column.name` plus an optional
  `query_engine.bound_column.relation`. The explicit name also distinguishes an
  unqualified literal dotted name from a qualified pair.
- Execution-schema conversion reads that metadata. Raw provider-schema conversion
  reconstructs identity from actual provider field names, rather than trusting
  supplied internal identity metadata.
- Interpreted lookup, compiled type lookup, compiled runtime loading and join
  lookup use the same resolver. It makes no temporary vector of matches and
  rejects ambiguity. The current alias candidate explicitly relabels emitted
  physical roots using borrowed-column ProjectExec; cached CTE consumers get
  separate namespaces. Annotated qualified lookup cannot cross namespaces.
  This is not a new bound-ordinal expression representation.
- Join-column pruning uses structural field identity. Unknown legacy identity
  cannot prove a field unused, so that field is retained conservatively.
- Memory, Parquet and native scan type adaptation, projection, join output and
  execution normalization preserve field metadata when changing physical types.
  Inferred projection/aggregate schemas use the execution-aware conversion.

These changes apply to column identity generally. No table/query-name branch or
benchmark timing-boundary change was introduced. Field metadata and schema
allocations are not a claim of complete query-wide memory admission; the existing
resource-ownership gaps remain open.

## Red/green evidence and current limits

Contained session 93427 produced three schema assertion failures and three SQL
assertion failures; three SQL controls passed. The schema failures were qualified
lookup, execution-Arrow roundtrip and duplicate qualified identity. After the
initial boundary changes, sessions 36548 and 74991 passed all three schema tests
and all six SQL cases across both multi-batch memory and Parquet sources.

A further explicit compiled-path test failed in session 48493: it returned
`[false,false,NULL]` instead of `[true,false,NULL]`. Compilation had used the
shared resolver but runtime loading still duplicated the old string lookup.
After routing runtime loading through the same resolver, session 83272 passed.

The first broad run, session 18365, passed 762 library tests and failed four,
with ten explicit ignores. Two tests assumed schemas had no identity metadata;
two encoded the old first-match behavior for ambiguous names. Updated tests keep
their value and memory-bound checks, verify the bound output schemas, and require
both execution and queue/emission certificates to reject ambiguous references.
Unique qualified roots and repeated output expressions retain positive coverage.

Session 1344 passed 766 library and 113 integration tests (879 unique passes),
with ten explicit ignores. Session 30219 separately passed eight actual CUDA
tests and the dedicated IPC-sidecar case: 888 selected passes, leaving only
the preexisting flatten-dependent-join ignore. These results precede the alias
change below. No release binary or provider benchmark
is yet attributed to these latest boundary changes. Evidence, original source
copies, red logs and implementation records are under
`.scratch/qualified-column-identity-repair/`.

Remaining validation includes broader alias/CTE and encoding paths, dedicated
CUDA checks, optimized independent SQL oracles, canonical/provider performance
and resource gates. The new metadata contract must not be declared complete from
these small reproductions alone.


## Physical alias boundary follow-up

`red-alias.log` preserves an actual shared-CTE failure: the cross product of two
aliases of a four-row CTE could not resolve `x.aid`. The derived dotted-alias
control passed. Ordinary and delim-state physical planning had discarded the
alias node; a shared cache did not establish each consumer's namespace.

The candidate uses the existing ProjectExec to replace the relation of every
emitted physical field while retaining its name and actual type. Binder source
confirms SubqueryAlias changes relation only; column-list renames are explicit
projections. Thus this handles pruned physical schemas without zipping them to a
full logical schema. Existing correct scan aliases return the input unchanged.
ProjectExec forwards all prepared partitions and queue/gather bounds, and borrows
column arrays. The planner supplies its query pool. Shared cache contents are not
mutated. Session24996 passed all ten focused identity cases after this change.

A strict resolver follow-up rejects a qualified reference to a contradictory
annotated namespace, retaining raw-provider compatibility. Session74400 passed 767 library and 84 selected integration tests for this
stricter contract (851 unique passes; ten explicit library ignores). No release
or performance acceptance is attributed to this alias candidate yet.


The extended twelve-case SQL gate89023 then reproduced another alias-boundary
failure: `SELECT d."b.c" FROM (SELECT aid, "b.c" FROM left_table) d WHERE d.aid > 1`
could not resolve `d.aid`. Eleven cases passed, including distinct projected roots
of a shared CTE. Predicate pushdown had carried the outer alias reference below
the namespace-changing node unchanged. The candidate now keeps predicates above
such a boundary, while still optimizing its child and preserving ordinary scan
pushdown when bound identities already match. Explicit expression rebinding is a
future optimization; restoring basename guessing is not a correctness fix.
Session31224 passed all twelve focused cases across memory and Parquet. This additional production change invalidates
attribution of earlier passes to the final candidate. Performance impact of the
conservative boundary must be measured; no speedup is claimed.


## Final debug gate and source freeze

After the predicate-boundary repair, session44586 passed 767 library tests and
126 integration tests (893 unique passes), with ten explicit library ignores.
Session51450 separately passed all eight real CUDA cases and the dedicated IPC
sidecar case. The final selection is therefore 902 unique passes; only the
preexisting `flatten_dependent_join::tests::test_flatten_exists` ignore remains
excluded. No SKIP/unavailable markers occurred in the hardware/sidecar logs.
`cargo fmt --all -- --check` and `git diff --check` pass. No dependency changes.

The complete production delta from frozen647 is saved in
`.scratch/qualified-column-identity-repair/production-vs647.patch`. The candidate
will be frozen with source hashes before the optimized build. Next gates are the
independent typed release oracles (including nine collision/alias SQL probes),
actual-spill resource checks, GPU residency validation and balanced canonical
SF10 comparisons across raw Parquet, decoded IPC, native, Iceberg and Lance.
The four predeclared startup/execution-order cells remain development evidence;
they do not establish three-session leadership or holdout coverage.

Frozen649 source archive SHA256:
`d8957b73c8d52603004388744a58f3ec595a16b716c7b28e4f33054c7e371d07`.
The649-file archive was verified member-by-member. Release build73598 is active
with Lance+GPU and both benchmark/cap examples,64GiB scope/jobs1. No other heavy
job is live. Do not launch runtime gates until this build terminates successfully
and `validate-release.py` verifies source identity and copies the binaries.


Prepared follow-up CPU diagnostic (not yet executed):
`.scratch/qualified-column-identity-repair/diagnose-shared-cpu.py` runs 36 bounded
requests covering Q18/Q13/Q1 across native/raw Parquet/decoded IPC. Each case uses
fresh typed DuckDB oracle/calibration, a10x ceiling and the frozen candidate hash.
It records AGG_TIMING, QE_AGG_PROF, HJ_PROF, QE_SPILL_DEBUG and process CPU/RSS
samples. These are diagnostic observations, not exclusive operator CPU or
acceptance latency; missing branch logs cannot be interpreted as zero cost.
Run only after release validation and separately from the balanced timing screen.


## Optimized semantic validation

Release73598 completed in10m45s. Binary SHA256:
`1f189fc6ac191c6b390e1c966ba46be2320dd4800bfed8d32f8faf3937cc387f`.
The cap example SHA256 is
`35a0c0ff730cd96145f5509b353d3afa97ab90c17ef14bd40ab460efabe0cbc2`.
Both match frozen649 production/examples/Cargo hashes.

Initial validation43219 completed89 float/date queries and58 primitive queries,
then stopped because its historical comparison compared formatted schema strings.
The primitive values and SQL types were equal; the new internal field metadata
changed the text. The original driver/log/output remain preserved. Resume53647
compares the historical and candidate Arrow schemas with `check_metadata=False`
(names, types and nullability retained), retaining exact values and raw Arrow.
Timestamp metadata probes preserve raw schema text and separately compare typed
name/type/nullability tuples. No production change was made for this driver fix.

Resume53647 completed successfully:10 dense-float and56 coercion queries match
DuckDB;58 primitive cases retain the old exact values/types, including the eight
known integer-division differences from DuckDB. Literal validation preserves19
canonical matches plus one explicit bare-NULL value contract. Two invalid decimal
metadata cases reject; three timestamp metadata and43 exact timestamp queries
match. The64KiB budget probe borrows direct input successfully and refuses expanded
literal, DOUBLE and DECIMAL outputs by memory name.

The independent nine-query Parquet probe also completes with full typed DuckDB
agreement: original collision/OR cases, two aliases of one shared CTE, differently
projected CTE consumers and a pruned derived-table alias. This closes the actual
small release reproducers; it is not general SQL certification. Raw Arrow, SQL,
plans, logs and source/binary provenance remain under the candidate scratch root.
Actual-spill cap27117 is active. Provider/performance/GPU residency gates remain
pending; the source is not accepted for performance yet.


Cap27117 completed both levers:1GiB cgroup peak402MiB and2GiB RLIMIT peak404MiB,
1,000,003exact groups from250million rows,3,855,541,894spill-accounted bytes.
GPU72014 completed40CPU-control and40required-device comparisons; all correct,
all required samples device-executed,256MiB resident cache target. This remains
the supported600k-row float fixture, not canonical SF10 GPU or hard VRAM admission.

Diagnostic89923 completed36requests; see
[shared aggregate finalization evidence](shared-aggregate-finalization-profile-2026-09-07.md).
Full balanced canonical SF10 screen20226 is now the sole heavy job, using all four
predeclared startup/execution cells across five providers. No performance
acceptance is claimed while it is running.


First balanced-screen cell completed: decoded IPC, before-first startup,
execution offset0,10steady pairs/query plus warmups. All484engine requests across
22queries pass typed/time gates. Suite candidate/control ratio0.996609 and
geomean0.999709. No query exceeds1.10 in either median or mean. This is one of
20predeclared cells; it does not close provider or performance acceptance.
Raw Parquet is active in20226. Results remain in
`.scratch/public-bench/qualified-identity-full-decoded_ipc-before-first-0-01/`.


Second screen cell completed: raw Parquet, before-first/offset0. All484requests
pass; suite0.997399×control,geo0.996862; no query>1.10 in median or mean. Together
with IPC,968requests across2/20cells complete. Native is active in20226. These
are component-preservation results for the identity repair, not DuckDB leadership.


Third cell completed: native, before-first/offset0. All484requests pass;
suite0.994924×control,geo1.000286; no query>1.10 in median or mean. Completed total
is1452requests across3/20cells. Iceberg is active in20226; later order cells and
Lance remain required before judging preservation of performance.


Independent report audit: `audit-balanced-results.py` checks exact query/side/
iteration membership (including warmup labels), successful outcome/comparison
records, positive finite timings, fresh-reference ceilings, and recomputes
per-query means/medians plus suite/geometric ratios. It preserves immutable
prefix reports (`balanced-audit-03.json` currently). It reuses typed-comparison
records rather than claiming a new Arrow-oracle execution. All three completed
cells reconcile. Candidate/fresh-DuckDB suite ratios in those cells are IPC0.4603,
raw2.3998 and native3.6265. Thus preservation versus647 is not DuckDB leadership.
The full20-cell screen is still incomplete; no successful-only final ratio is
emitted for the unfinished matrix.


Fourth cell completed: Iceberg, before-first/offset0. All484requests pass;
suite0.999525×control,geo1.000709; no query>1.10 in median or mean. The independent
four-cell audit reconciles1936requests, including an Iceberg candidate/DuckDB
suite ratio0.319127 under this provider contract. Lance is active in20226. All
remaining startup/execution cells and broader acceptance requirements stay open.


### Resumed full-provider audit: nine completed cells

The original coordinator is running its unchanged20-cell plan after the separately
archived651screen. The independent prefix audit now validates9cells/4356requests
with all typed/time gates passing. Iceberg after-first/offset1has649/647suite
ratio.995068 and geometric mean.999947, with no>10%median/mean flags. The original
Lance Q11 mean flag remains unresolved; eleven full-provider cells remain.
Evidence: .scratch/qualified-column-identity-repair/balanced-audit-09.json.


### Ten-cell audit and Lance Q17 flag

The original full-provider run now independently audits10cells/4840typed and
time-gated requests. The second Lance cell (after-first/offset1) has suite ratio
649/647=1.002467 and geometric mean.994431, but Q17 median ratio1.136501 flags
the protected threshold (mean ratio1.044328). Q11 improves in this cell
(median.907567,mean.921613); that does not erase its first-cell mean1.104273 flag.
Ten original cells remain. Preserve both flags until complete balanced evidence
and appropriate protected follow-up are available.

For Q17, the saved before/after optimized and physical plan displays are identical.
Displayed-plan equality is not proof of equal internals or allocation behavior.
The per-query timing records narrow the next investigation:

| Lance cell / side | Total median ms | Parse mean ms | Optimize mean ms | Plan median ms | Execute median ms |
|---|---:|---:|---:|---:|---:|
| before-first0 /647 | 356.234 | 44.095 | .964 | 208.071 | 126.043 |
| before-first0 /649 | 389.521 | 20.795 | 31.435 | 207.268 | 124.582 |
| after-first1 /647 | 341.254 | 13.376 | 23.629 | 207.218 | 127.528 |
| after-first1 /649 | 387.836 | 14.733 | 43.941 | 206.843 | 126.123 |

The flagged649cell has optimizer median24.788ms versus.636ms for647, while
planning and execution medians remain close. Do not add stage medians or equate
wall time with optimizer CPU instructions. This evidence does not support a
simple claim that the displayed join plan got worse. Parse/optimizer spikes and
previous allocator findings warrant targeted attribution after the remaining
orders complete; allocator causation is not yet established for this case.

Evidence: balanced-audit-10.json and q17-plan-review/{stage-metrics.json,
optimized_plan.diff,physical_plan.diff} in the qualified-column-identity scratch
directory. These use recorded requests; no new engine run or source change
was performed during the active baseline.


### Eleven-cell audit and IPC Q22 lead

The immutable prefix audit now validates11cells/5324typed/time-gated requests.
IPC after-first/offset0has suite649/647ratio1.002465 and geometric mean1.005351,
but Q22 median ratio1.130157 flags the protected threshold (mean1.090342). Its
other completed IPC cells have median ratios1.048948 and1.023763. Retain every
cell; the new flag does not establish a confirmed regression across sessions.

Q22's optimized plan display is unchanged, but its physical plan contains an
additional Project above the existing Filter/Join subtree. On the flagged cell,
execution median rises25.291→29.985ms; plan median is8.618→8.634ms and parse
medians are both about.107ms. The saved plans and per-stage records are in
`.scratch/qualified-column-identity-repair/q22-plan-review/`.

Source `PhysicalPlanner::apply_relation_alias` constructs this identity-column
projection to preserve the namespace contract fixed by649. Do not remove it or
restore ambiguous binding as a performance shortcut. If protected follow-up and
operator attribution confirm this overhead, a generic opportunity is folding a
relation-only output-schema change into an existing ProjectExec. That must preserve
the inner expressions, input, subquery executor, query pool, field metadata and
prepared-input contracts, and must not mutate a shared CTE consumer. It would be
a separate experiment, not a Q22-specific branch. No such code change has been
made, and the extra projection's causal contribution is not yet measured.

Lance Q11/Q17 and IPC Q22 remain open flags. Nine baseline cells remain; the
guarded successor still waits before the resource probe.


### Thirteen-cell prefix audit

The third raw-Parquet and native cells (after-first/offset0) completed with all
484requests each passing typed/time gates and no new>10%flags. Their suite649/647
ratios are.997455 and.997277, and geometric means1.002193 and1.001160. The
independent prefix now covers13cells/6292requests. Existing Lance Q11/Q17 and
IPC Q22 flags remain open; seven cells remain. Successor98089 is confirmed live
and still waiting, with no probe engine launched.

### Fifteen-cell audit and Iceberg Q8 flag

The independent prefix audit now verifies15cells/7260requests. All recorded
comparisons and time gates pass. Iceberg after-first/offset0 has suite
649/647=1.006129, with Q8 median357.900→395.772ms (ratio1.105816) and
mean370.465→386.592ms (ratio1.043532). Its other completed orders have median
ratios1.034359 and.983871. Preserve this new flag alongside Lance Q11/Q17 and
IPC Q22; neither suite stability nor another order's improvement closes it.

Q8's optimized plan display remains equal, while the physical display gains
an alias Project above the join subtree. The flagged cell's planning median
is49.971→57.784ms and execution median301.030→340.495ms; optimizer medians
are1.476→1.631ms. These stage walls are diagnostic leads, not exclusive CPU
attribution or proof that the added Project caused the timing difference. Do
not add medians to reconstruct query medians. The generic alias-projection
review described for Q22 may also apply here, but requires a separate controlled
experiment that preserves namespace semantics.

Evidence: `.scratch/qualified-column-identity-repair/balanced-audit-15.json` and
`q08-iceberg-review/` (driver, all samples, saved plans and diffs). The third
Lance cell has no new flags and suite ratio1.015824. Five full-provider cells
remain. The queued order is unchanged: complete baseline, audit all9680requests,
run frozen651 budget probe, then session93502 validates the expanded query-pool
correction. None of those pending outcomes is claimed as passed.

### Sixteen-cell audit: IPC has all four execution orders

`balanced-audit-16.json` verifies7744requests. The fourth IPC cell completes
with suite649/647=1.003026, geometric mean.992328, and no new timing flags.
Q22 in this order improves: median37.464→35.427ms (ratio.945610),
mean37.248→36.433ms (ratio.978117). This makes the earlier1.130157 median
flag order-sensitive; it does not justify deleting that flag or declaring a
confirmed regression from that single cell. A balanced protected follow-up is
still required. All four IPC cells preserve typed correctness and time gates.
The original baseline is now running its final raw-Parquet cell; four cells
remain overall. No pending source-validation result is claimed here.

### Protected follow-up driver prepared, not executed

`run-protected-followup.py` takes the union of every flagged track/query from
`balanced-audit-20.json`; it cannot select only favorable query subsets. It
pins the original647/649 binaries and the complete audit hash, preserves fresh
outputs, and runs50steady pairs plus warmup in each of the four original
startup/execution orders. It reuses the paired harness's typed oracle, matched
provider setup, fresh DuckDB calibration and timing gates. A failed cell stops
with its evidence retained. Original flags remain in the first screen.

The driver requires96GiB containment and affinity0–15, a completed9680-request
baseline audit, terminal before-fix probe, empty original baseline scope, and
terminal queued source validation. Start only at an otherwise idle boundary;
those guards are not a global scheduler. Syntax parsing passed without executing
the driver. The full-screen tail, budget probe and regression tests retain their
existing priority. This follow-up is development regression evidence, not the
three-session/public-workload/holdout leadership gate.

### Eighteen-cell audit: raw Parquet and native complete

`balanced-audit-18.json` verifies8712requests. The final raw-Parquet cell
has suite649/647=1.002775 and geometric mean1.001252; the final native cell
has suite.992647 and geometric mean.994005. Neither adds a timing flag.
All four execution orders for both tracks pass recorded typed comparisons,
time gates and membership checks without protected flags. This establishes
stability against647 for these tracks, not DuckDB leadership: the final raw
and native suite ratios against their DuckDB references remain2.412860 and
3.684387 respectively. Iceberg and Lance final cells remain; the queued budget
probe and current source regression validation have not started.

### Nineteen-cell audit: Iceberg complete

`balanced-audit-19.json` verifies9196requests. The final Iceberg cell has suite
649/647=.998608 and geometric mean1.001480, with no additional flags. Q8's
fourth-order median is347.162→345.919ms (ratio.996420), mean ratio.981842.
The earlier1.105816 median flag remains preserved for the balanced follow-up;
three other completed orders do not reproduce a>10%median slowdown. All four
Iceberg cells pass recorded typed comparisons and time gates. The final Lance
cell is live; the before-fix budget probe and current source tests remain queued.

### Full baseline complete: all20cells/9680requests audited

The original coordinator terminated successfully with an empty cgroup.
`balanced-audit-20.json` passes independent membership, outcome-record and timing
arithmetic checks for every planned request. All measured typed comparisons and
fresh10×DuckDB time gates pass. No cell was omitted or restarted in this tail.
The final Lance cell has suite649/647=.999638 and geometric mean1.003427, but
adds repeat protected flags: Q11 median ratio1.129793/mean1.117482, and Q17
median1.158934/mean.992692. These remain open together with the earlier IPC Q22
and Iceberg Q8 observations; full-suite stability does not close query flags.
The prepared balanced50-pair follow-up must include all four flagged track/query
combinations. Full provider performance acceptance remains incomplete.

Successor98089 then completed the frozen651 SQL budget probe:512-group exact
control passes, both100,000-group cases refuse by name in query1 at1MiB. This
probe does not reproduce the fallback ownership escape. Session93502 is now
compiling/running the isolated query-pool regressions; do not launch follow-up
benchmarks or release builds until that scope is terminal.
