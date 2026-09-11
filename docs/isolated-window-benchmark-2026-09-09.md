# Process-isolated short-query measurement — 2026-09-09

The [failed identical-binary control](short-query-null-control-2026-09-09.md)
motivates an explicit benchmark lifecycle and longer fixed observation windows.
Engine binary1efb2554 is unchanged. The new implementation is
`scripts/benchmark/isolated_windows.py`; it adds no dependency.

## Implemented contract

`run_isolated_pair` receives worker-start and typed-query callbacks, executes
one complete side at a time and closes/reaps it before starting the other. One
gated warmup precedes a fixed request count. Incorrect, late or failed requests
stop that side; subsequent slots are explicitly not run. The primary setup
failure is retained. Unknown startup ownership or stuck cleanup prevents the
second side from starting and produces an isolation error.

Warmup time does not contribute to measured duration. Correct completion and
sufficient observation time are separate fields. Short windows never gain
extra requests automatically. The primary ratio uses all measured query time
(sum/count); medians remain secondary so bursts are not dropped from acceptance.
All outputs are independently typed-validated by the existing comparator, and
per-request10× DuckDB ceilings remain enforced by the existing Worker.

Resource snapshots read process CPU ticks, affinity and context switches of
currently live threads; exited threads can disappear from the latter snapshots.
Host metadata reads affinity, topology, governor/frequency information and load.
Unavailable data stays unavailable. No host settings change. The driver journals
each attempt once and keeps bounded progress summaries, avoiding quadratic
whole-history logging between requests. Final records retain the full history.

## Validation and live study

Ten focused tests pass, covering single-worker ordering, fixed counts, exclusion
of warmup exposure, insufficient precision, wrong answers, invalid time/gates,
startup refusal, unknown ownership, stuck cleanup and mean-versus-median burst
accounting. Full harness28452 terminal0:136 tests run,134 pass and2 optional skips.

Pilot72600 terminal0 uses32 measured requests and one warmup per side. All66
engine outputs pass typed/time gates; both workers close gracefully. Its fastest
measured request is1.421564ms. The count rule fixed before the null study takes the next power
of two at least2000/that duration, yielding2048. The245-file
[pilot archive](benchmarks/2026-09-09-aggregate-threshold-q6-isolated-pilot-01/manifest.json)
verifies507 source inputs. The initial pilot driver emits a degenerate one-block
bootstrap interval; it is not confidence evidence. The later driver explicitly
omits pilot confidence intervals and requires at least eight acceptance blocks.

Null50497 is terminal1 under a recorded prespecification: both labels use the
exact1efb2554 hash, eight alternating AB/BA blocks,2048 measured requests per side
plus one warmup, at least2000ms measured exposure in every fixed window,
20,000 bootstrap samples and seed20260909. The primary interval must contain1
and stay within0.90–1.10, with all correctness/time/cleanup gates passing.
All32,784 engine outputs are typed-correct/gated, all eight blocks complete and
all fixed windows clear the minimum exposure. The ratio1.03987 has95% interval
0.94767–1.13276, failing the precision requirement. Scope peak825,430,016 bytes
with zero max/OOM/kill;507 source hashes verify. The33,055-file
[null archive](benchmarks/2026-09-09-aggregate-threshold-q6-isolated-null-01/manifest.json)
is verified. This null cannot clear the earlier candidate Q6 failure.

A new regression reproduces the integration gap: a reported isolation error
initially stops only the second side, allowing the caller to attempt later
blocks. The red test fails with RuntimeError not raised. The helper now raises
IsolationError with the preserved result, and the driver records that evidence
before propagating the fatal error out of the whole loop. Green61660 terminal0:
137 tests run,135 pass and2 skip. The successful measurement path is unchanged.
The162-file [harness archive](benchmarks/2026-09-09-isolated-window-harness/manifest.json)
verifies. Skips are the opt-in high-cardinality exact-bag spill test and the
opt-in local Lance-extension test; neither is counted as passing coverage.

## Placement evidence during the live study

The recorded host context shows CPUs0–15 as eight SMT sibling pairs (0–1,2–3,
through14–15), with the powersave governor. Two cores report a5.8GHz maximum and
the others5.5GHz. Four query workers share that allowed mask with runtime work.
Source `execution/topology.rs::placement_policy` defaults to `Placement::Node`;
on this single-node allowed topology, `init_global_pool` treats the node mask as
trivial and skips binding. Its historical comment that workers never migrate
does not describe this default. `current_cpu` reads the actual running CPU; the
nominal PINNED_ORDER value is not evidence of actual placement.

These are recorded conditions, not a proof that SMT sharing, migration or power
management caused the observed variation. If the fixed null fails precision, a
separately prespecified comparison using one allowed sibling per physical core
can test that placement condition, with matching DuckDB affinity and unchanged
query budgets. Preserve the current study; do not adjust host governors/services
or drop samples to manufacture a pass.

Physical-core null60984 is terminal0 under its own prespecification. CPU
affinity0,2,4,6,8,10,12,14 selects the lowest allowed sibling in each physical
package/core pair from the preceding mask; DuckDB and both engine labels inherit
the same mask. All other fixed counts, budgets, acceptance conditions and the
engine hash remain unchanged. No governor, service or engine placement setting
is changed. This separately tests the affinity condition and preserves the prior
null failure.

All32,784 physical-core null outputs are typed-correct and gated; all16 windows
close gracefully and contain6,530–8,010ms of measured query time. The ratio is
0.96355 with95%0.90747–1.03210, satisfying its declared interval rule. Scope
peak825,057,280 bytes with zero max/OOM/kill. The33,055-file
[physical-core null archive](benchmarks/2026-09-09-aggregate-threshold-q6-isolated-physical-null-01/manifest.json)
verifies507 source inputs. This passes one defined null protocol; it does not
prove an exclusive cause for earlier variability or establish candidate speed.

Candidate comparison97043 is terminal0 under a fresh prespecification, using
control322c8042 and candidate1efb2554, the same physical-core affinity, budgets,
fixed counts and primary mean-time metric. Acceptance requires all requests
correct/gated, complete/graceful windows and upper95 ratio no greater than1.10.
The original0–15-affinity short studies remain preserved and unresolved under
their own conditions; this new comparison will be labelled separately.

All32,784 candidate/control outputs validate and pass their fresh timing gates;
all16 windows close gracefully and contain6,820–7,894ms measured query time.
The primary ratio is0.99105 with95%0.94622–1.04071, satisfying the declared
upper-bound requirement. Scope peak824,487,936 bytes, zero max/OOM/kill events.
The33,055-file [candidate archive](benchmarks/2026-09-09-aggregate-threshold-q6-isolated-physical-candidate-01/manifest.json)
verifies507 source inputs and the unchanged harness. This bounds slowdown below
10% for this new physical-core, mean-window protocol; it does not establish a
speedup or rewrite the earlier0–15-affinity outcomes.
