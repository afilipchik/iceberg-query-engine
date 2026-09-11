# Admitted buffers in the aggregate input scheduler — September 8, 2026

The generic spillable aggregate now consumes the same admitted-buffer protocol as
the join-build queue. Previously its separate InputFrontier only asked for the
legacy copied-output certificate, so variable-width sources remained serial even
when they could provide query-pool-owned buffers. This is a shared scheduling and
ownership change, without query-name or SQL-text special cases.

Preparation binds the requested pool or a constrained descendant and validates
the declared partition count before any producer starts. Admitted stream/vector
owners survive transfer into pending jobs. Scheduler metadata has a conservative
16KiB-per-partition allowance plus512 bytes; original plan residency and exact
allocator/RSS accounting remain separate. Unsupported sources retain the previous
serial route. Preparation errors do not cause fallback or execution replay.

Admitted mode permits at most min(partitions, Rayon threads) owned tasks, without
a copied-output envelope or a deep copy. InputBatch distinguishes consumer-charge
required, copied-envelope ownership and buffer-attached ownership. The live
aggregate avoids charging already-admitted buffers again, including with one slot.
Demand permits remain held through consumer processing. Existing live-aggregate
shutdown joins/cancels producers and drops the frontier before finalization or
returning an input error. Retained output buffers keep their leases after handoff.

## Verification and production routing

Seven new focused tests pass. They cover overlapping pulls, empty prefixes and
duplicate rows, preparation once, one-slot admission, wrong pool/count rejection,
metadata denial before polling, cancellation/late-error ownership, and a real
Parquet -> repeated-column Project -> live aggregate with forced spill. The latter
compares every NULL/string group and exact COUNT/SUM value against a separately
constructed oracle, asserts actual spill, and checks all leases release.

Full library97252:987 passed, zero failures,10 existing ignores,30.03s. Selected
integrations71603:45 passed, no skips, including budget transitions, evaluated
keys, input errors, partitions, prepared joins and actual parallel-input spill.
Formatting and whitespace pass. Commands use locked/offline lance,gpu,
Rayon4, one build job,48GiB containment and repository TMPDIR.

Q1's bare raw-Parquet aggregate uses a separate direct-file MorselAggregate, so
this change is not evidence of a Q1 gain. The existing initialization-only plan
probe, built as job81923 and run on the small fixture, confirms ordinary SQL
derived-table, CTE and table-column-alias forms lower to SpillableHashAggregate ->
Project -> StreamingParquetScan. Their bare equivalent uses MorselAggregate.
The probe never polls output and establishes routing only, not result correctness
or performance. This provides a natural workload family for the changed scheduler
without disabling the default optimizer. Plans/SQL are under
`.scratch/parallel-aggregate-input/admitted-route-probe/`.

## Frozen measurement outcome

Release76420 completed successfully in 8m45s. Candidate SHA256
`9c868dc069af26168a26a68afd24f799d039174f93fabc25d1d750ac2261f926`
verified all 488 input hashes before/after compilation. Job41823 ran
`measure_admitted_frontier.py` through the 48GiB safe-build scope with pinned
Python, 16 threads, 4GiB query budget and 12GiB process cap. It compared frozen
ed721285 against this candidate on unchanged canonical SF10 data. This is an
equivalent-SQL component workload, not a canonical suite result.

Four fresh blocks per SQL form balance startup and execution order independently.
Each has a fresh typed DuckDB oracle, one reference warmup and three calibration
samples, then one engine warmup and three measured pairs. Engine warmups also
obey the fresh 10× DuckDB query ceiling; failures stop that side without replay.

- Bare form: all 32 engine outputs validate and pass the gate, including eight
  warmups. Geometric mean of four after/before block-median ratios is 0.99654;
  whole-block bootstrap 95% interval [0.96729, 1.01520]. No demonstrated gain.
- Derived-table, CTE and column-alias forms: all 24 attempted engine warmups
  time out on both binaries at fresh ceilings of 250.67–309.34ms. The remaining
  72 scheduled requests are explicitly not run. There are no completed outputs
  or valid speedup ratios for these forms.
- All 16 references complete calibration. The scope peak is 740,888,576 bytes;
  memory.events max/oom/oom_kill are zero. This is scope evidence, not exact
  query-memory accounting. The driver terminates at its failure assertion;
  failures are preserved, not recategorized as passes.

The bare form's engine medians are 121.63–132.69ms. The initialization probes
and planner source establish a routing discontinuity: harmless binding wrappers
select the generic aggregate, while the bare form selects the direct-file
MorselAggregate. The timeout results establish the practical cliff, but do not
measure its complete duration or assign its cost exclusively to decoding,
grouping, spilling or scheduling. The new scheduler remains semantically tested;
its intended performance improvement is not demonstrated by this experiment.

Evidence is archived under `docs/benchmarks/2026-09-08-admitted-frontier/`, including
frozen source, input hashes, build/test logs, SQL, probes, all worker responses,
plans, typed outputs, scope events and the exact driver/harness. Interrupted spill
payloads are inventoried by path/size and excluded from the archive.

## Next bounded implementation task

Review `docs/aggregate-capability-routing-2026-09-08.md` before extending routing.
Do not simply skip alias/project nodes: compose exact bound-column mappings and
retain expression/filter semantics, then expose the same aggregate capability
through semantically transparent inputs. General grouped execution must also
remain performant for inputs that cannot fuse. Preserve the existing strict
failures and rerun this component plus canonical/provider/resource gates for any
new frozen candidate. Q2 remains protected and statistically uncertain according
to the separate ed721285 paired confirmation.
