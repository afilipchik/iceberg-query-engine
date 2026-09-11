# Balanced aggregate ownership — 2026-09-09

The live grouped path now exposes experimental `QE_AGG_OWNERSHIP=partial`, with
balanced contiguous row selections across up to16 workers and a mandatory final
cross-owner partial-state merge. Unset or `disjoint` retains the existing default
(up to4 fixed-key owners); unknown strings fail before consumption. This is a
measurable algorithm candidate, not yet a default policy or performance claim.
The purpose is to remove the [measured hot-key worker limit](q1-aggregate-owner-skew-2026-09-09.md).

## Implementation and contracts

`ParallelControllers::new_partial` admits the complete worker set, cross-owner
consumer and bounded run ledger before opening input. A typed startup admission
refusal cleans up the abandoned set and can choose one ordinary owner. Runtime
errors never select a new algorithm or replay consumed input. Row ranges differ
by at most one row and preserve original per-worker order. Existing evaluated
batch, prepared-key identity and selected-row retry contracts remain in force.

The same key may now appear in several workers. Serial versus Rayon dispatch
changes scheduling only; final output cannot concatenate those local groups.
`IngestionController::into_partials` transfers resident GroupRows and existing
partial spill runs directly, dropping its unused writer/workspaces. It does not
perform a redundant local final merge. The cross-owner consumer merges resident
partials or spools them with its prepared writer under pressure, and adopts local
runs into metadata reserved for `actual_workers * max_runs + 1` handles. Adoption
checks exact layout identity and cannot grow the ledger after input opens.

After the last local owner is released, one PartitionScheduler merges all adopted
runs and any consumer spool. SUM/COUNT and AVG's sum/count remain partial states
until this finish. HAVING and Arrow output remain after it. Local spill metrics
are added once, and consumer spill/merge metrics are added separately. Existing
DISTINCT/unsupported-layout fallback is retained. Query-wide reservations and
memory/timeout defaults are unchanged; no dependency change.

Profiling now emits actual `workers` and `ownership`. A requested partial mode
may legitimately report disjoint/one-worker startup fallback; a benchmark must
record this instead of claiming that many-worker partial execution happened.

## Validation

Focused64700 passes the controller test:1/4/16 requested workers, four batches
with hot overlapping keys, NULL keys and aggregate values, exact large Decimal128
SUM, weighted AVG, independent multiplicity and actual high-cardinality spill.
It asserts parallel dispatch where the batch size supports it and rejects duplicate
final groups. A second test verifies equal dictionary values across reversed
codebooks, dictionary NULL values and NULL codes. Routing tests cover0/1/3/2049
rows,1/4/16 workers, exact coverage/order, invalid dimensions and full-pool denial.

The startup sweep observes denial, one-worker fallback and a full four-worker
set, and specifically finds budgets where workers alone fit but merge admission
forces fallback. Every attempt releases abandoned owners. Parser tests reject
unknown modes. Existing cross-owner corruption/poisoning/pressure tests remain.

Default broad47088 passes1,050 library tests/11ignored and10 integrations. After
adding the startup sweep, explicit-partial broad41773 passes1,051 library tests/
11ignored and12 integrations (including DISTINCT fallback). Full spill6177 runs
both modes sequentially: each is8pass/6same failed names, exit101. No failure is
waived. Formatting and whitespace checks pass.

Commands use `TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu`.
The default broad arguments are `--lib --test aggregate_binding_transparency
--test dictionary_group_contract_tests --test parallel_input_spill_contract
--test aggregate_post_filter_admission`. The partial gate adds
`QE_AGG_OWNERSHIP=partial` and `--test distinct_sum_empty_contract`.
The full spill gate uses `--test spill_tests` with each ownership spelling.

The [correctness archive](benchmarks/2026-09-09-balanced-aggregate-ownership/manifest.json)
preserves509 source inputs and all test logs. Six existing inputs differ from
the preceding merge-component archive: ingestion_controller.rs, live_spill.rs,
parallel_controllers.rs, partial_merge.rs, row_router.rs and spill_files.rs.

## Frozen measurement sequence

Release76858 completed in8m53s with48GiB/one build job, locked/offline lance,gpu
features, freezingcd8098d5/509inputs. Diagnostic4049 subsequently completed all24
outputs correctly; its293-file archive verifies. At16threads Q1 improves about47%
but Q18 is about41%slower because serial final merging reverses its ingestion gain.
See [measurements and revised next action](balanced-aggregate-native-scaling-2026-09-09.md).
Source remained frozen through terminal verification and archiving. The
prepared native scaling diagnostic uses this one binary for both modes, canonical
Q1 and Q18,1/4/16 query threads on the same CPU0–15 affinity,4GiB query/12GiB
process budgets, two fresh reversed-order blocks and independent typed oracles.
There are24 planned outputs. GPU is explicitly disabled; actual ownership traces
must match the requested experiment or the case remains unproven.

The180-second diagnostic watchdog allows attribution of already-failing queries;
it is not ordinary10× DuckDB timing acceptance. Two-block ratios are observations,
not confidence intervals. At16 threads the disjoint control still has four owners,
so that comparison includes the intended removal of the worker cap as well as
ownership policy. One-thread partial requests use the single ordinary owner.
Prespec, driver, source/binary/provider hashes, all responses and failures must be
preserved. Do not select a default policy from the low-cardinality case alone:
high-cardinality behavior, protected queries and provider/resource gates remain
required. Iceberg, Lance, decoded residency and GPU acceptance stay separate.
