# First-batch reader refusal: ownership ledger — 2026-09-11

Contained diagnostic65075 reproduces the existing COUNT(DISTINCT) spill-test refusal
on source526. The selected query pool has262144bytes. Crucially, the live
SpillableHashJoin::compute_build_decision frame reports flat_size=0,flat_rows=0,
flat_batches.len=0,with threshold209715. The refusal occurs while producing the first
build batch; accumulated join input is not the cause in this reproduction.

The first two denials request66048then33280bytes with236553used. They are the
existing fixed-output decoder's quantum halving. The final denial requests9512bytes
for a9000-byte encoded body at offset86424,with254089used. Only8055bytes remain.
The current batch reader has3columns,15000remaining rows,requested max_rows8192 and
value_bytes65536. Directly observed reservation fields are:

| Live owner | Reservation bytes |
|---|---:|
| Reader schema |37376|
| Runtime-filter vector |512|
| Output-position vector |536|
| Batch column vector |4088|
| Batch pending vector |608|
| Provisional handoff vector |560|
| Provisional handoff metadata |12288|

These fields account for55968bytes; they are a partial ledger, not a complete
reconciliation of254089used. Page/decoder/dictionary owners and pending output
buffers still need separation. Unavailable symbols in unrelated generic wrapper
frames are retained explicitly rather than reported as zeros.

Source next_batch pulls columns sequentially and retains successful chunks in
pending state. Per-column fixed output halving chooses a locally affordable quantum;
it does not reserve the next column's encoded/decoded page requirement. The measured
first-batch boundary therefore requires investigation of coordinated reader working
space and output sizing. Lowering a join spill threshold or spilling already retained
join batches cannot repair this case, because there are no retained batches.
No speculative retry of a terminated selected stream is justified. Existing cursor
state preserves pending pages/chunks across internal memory refusals; changing that
contract requires separate tests for exact cursor progress, errors and ownership.

One possible design to evaluate is to prepare each selected column's next page or
chunk requirements before allocating the batch's output chunks, then budget output
across the selected columns. This is a hypothesis, not an implemented or validated
solution. First capture the remaining live page/output allocation composition and
reproduce the issue with a small multi-column reader test. Keep actual budget limits
and all existing refusal assertions; do not guess smaller reservation constants.

The probe ran during optimized compilation after checking approximately87GiB host
availability. Its separate16GiB scope plus the48GiB build scope fit the128GiB host.
A30second watchdog bounded the owned test process; before/after sequence checks prove
that the release step was still running, so no benchmark overlapped the probe.
The inferior's expected test exit101 is preserved; diagnostic exit0 is not a passing
resource test. Scope peak2,513,014,784bytes,swap0,zero max/OOM events.

[11-file verified archive](benchmarks/2026-09-11-first-batch-refusal-ledger/manifest.json)
retains all3denial stacks,selected-field ledgers,fixture hashes,source526 and a hash
of the separately frozen debug test executable. [Previous allocation-site evidence](incremental-header-refusal-results-2026-09-11.md).

## Next controlled reproduction after the active SF10 cycle

Use a small multi-column Parquet fixture and one unchanged query pool limit.
First prove that a fresh admitted reader with a one-row target completes every row
with an independent typed oracle. Then run an independently opened reader with a
large target at the same limit and record whether it refuses before its first
output. If it does, test whether lowering the target on that retained reader can
recover; pending early-column arrays may retain the space needed by later columns.
Keep all attempts and owner cleanup evidence. This comparison tests output
allocation order rather than assuming that a small budget is intrinsically enough.

Include a separate case where a single required page exceeds the pool: even the
fresh one-row reader must refuse cleanly there. Do not count that as a scheduling
bug or remove page/metadata ownership to make it pass. Inspect dictionary bodies,
encoded/decoded pages, ID scratch and output buffers separately. A coordinated
page-preparation phase and common output budget should be evaluated only after
the feasible one-row versus large-target case is reproduced. No such new experiment
has run while the current SF10 timing is active.

## Same-budget reproduction after pushed checkpoint de7605f

Focused89920 is terminal101: one expected failure and one passing refusal control.
The new fixture has three nullable Int64 columns, 4,096 rows, duplicate values,
PLAIN encoding, no compression and one data page per column. At an unchanged
163,840-byte pool, a fresh reader with a one-row target returns every value in
order against an independently constructed oracle and releases all owners.
A fresh reader with a4,096-row target refuses before its first output. Its retained
state uses137,944bytes; during the failed call, provisional handoff storage brings
usage to151,432 and the next request needs32,320bytes. Retrying that retained
reader with a one-row target reaches the same refusal. Dropping it returns pool
usage to zero. This is a reproduced allocation-order/progress bug: the budget can
complete the workload at a smaller initial target. It is not a join spill issue.

The separate8,192-row single-column fixture at32KiB refuses even with a one-row
target: it requests64,123bytes with7,512used and releases all retained owners on
drop. That passing test preserves the genuine whole-page working-space floor.
No metadata or output reservation was reduced to produce either result.

Production source remains identical to pushedde7605f. Only the row-group test
module and declaration changed. Source531 and four evidence files verify in the
[red archive](benchmarks/2026-09-11-first-batch-working-space-red/manifest.json).
The run used the48GiB/no-swap wrapper, one compile job, repository TMPDIR, locked
offline default features and one test thread. Separate terminal cgroup telemetry
was not captured, so this report makes no exact peak/event-count claim.

Next separate required page/dictionary/validity preparation from output allocation,
then coordinate output sizing across all selected columns. Cover uneven page ends,
dictionary ID prefixes, NULLs, UTF8 byte limits and retained outputs; preserve
terminal source errors and exact cursors. Preparing pages alone is insufficient
if the first output can still consume every byte needed by later columns. A new
policy must prove minimum progress without reserving guessed semantic guarantees
or forcing all output into one-row batches.
