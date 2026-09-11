# Admitted input queue protocol — September 8, 2026

The shared join-build queue serialized the measured variable-width Project input
because its old certificate requires pulls independent of the consuming pool.
The admitted decoder necessarily allocates from that pool. The new protocol
expresses buffer ownership directly instead of making an invalid independence
claim. This is a shared operator change, with no query-name or SQL-text special case.

## Implementation under validation

`PreparedAdmittedInput` has internal fields and binds a reserved vector of streams
to the requested pool or a constrained descendant. Preparation starts no producers
and does not poll output. Unsupported capabilities decline before consumption;
admission, identity and IO failures propagate. Prepared stream boxes retain their
own construction leases.

Eligible raw variable-width Parquet scans preflight every selected row group,
then prepare admitted cursors against the consuming pool. Both ordinary and
pressure scans can use this queue route. Fixed-width and IPC paths retain their
previous policy. Preflight currently constructs and drops a reader per group;
this avoids page reads but duplicates some construction work. It is not a claim
that preflight costs nothing. Column/alias-only Project passes the descriptor
through reserved index/output vectors and metadata-owning shallow array handoff.

The queue validates pool ancestry and partition count before starting producers.
It admits a conservative scheduler allowance (16 KiB per partition, four times
channel element storage, plus 4 KiB) and retains that allowance through task
cancellation. At most min(partitions, Rayon threads) pulls/queued values hold
demand permits. Already-admitted buffers need no deep copy or copy envelope.
Their leases survive consumer handoff. Allocation failures remain immediate typed
refusals; this does not introduce a reservation wait that could deadlock downstream.
The lifecycle audit also reproduced a pre-existing error-return race: aborting
the other tasks did not wait for a running synchronous decoder poll to release
its memory. The queue now retains the first error, requests cancellation, and
reaps every producer before emitting that error. Ordinary stream drop still
requests cancellation asynchronously, with producer-owned leases retained until
the work actually stops. This correction applies to both queue protocols.

Original plan/footer ownership and conservative allocator overhead estimates
remain limitations. This is not complete query-wide accounting or exact RSS.
Increasing concurrency can expose genuine budget exhaustion; provider, resource
and concurrency acceptance must be measured on a newly frozen binary.

## Validation status

The first compile found a missing StreamExt import and an existing test initializer
missing the scheduler guard; both were corrected. Focused regressions now cover
overlapping pulls, preparation once with no legacy replay, wrong-pool rejection,
metadata refusal before producers start, output lease lifetime, and a real
multi-partition nullable/duplicate-string Parquet scan through repeated Project
columns. Focused job91820 passes all six tests with no skips. A subsequent
deterministic test parks a synchronous producer holding a reservation and makes
another producer fail: job30434 reproduces early error escape (one failure),
preserved in `.scratch/parallel-aggregate-input/admitted-queue-cleanup-red.log`.
The cleanup fix passes full library gate41692:980 passed, zero failures,10 existing
ignores,29.22s. Integration48793 passes33 tests with no skips: streaming hash joins7,
actual parallel-input spill2, partition contracts17, unknown wrappers2, shared
prescan errors3 and streaming prepared joins2. Formatting and whitespace pass.
Commands used locked/offline lance,gpu, Rayon4, one build job,48GiB scope and
repository TMPDIR. Logs are under `.scratch/parallel-aggregate-input/` with
`admitted-queue-` prefixes. Release77076 completed and froze ed721285. Diagnostic34658
returns correct results at both budgets but fails both exact time ceilings;
16 admitted slots execute with no OOM or residual spill files. See
`admitted-queue-q12-2026-09-08.md`. No suite speedup is claimed for this source.

Remaining gates: resolve the still-failing performance gate and pass protected
performance/resource/provider workloads. Prior timeout evidence is unchanged.
