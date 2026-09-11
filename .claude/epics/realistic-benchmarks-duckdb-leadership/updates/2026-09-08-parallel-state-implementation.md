# Parallel state implementation checkpoint

Implemented validated selection cursors, admitted canonical-key routing and
up-to-four shared-pool spillable controllers in the live grouped path. The
global route stays single-controller. Worker startup can fall back to one only
before source consumption; no failed worker can cause input replay. All scoped
batch work finishes before input/selection owners release. Complete worker groups
concatenate after their own partial-state merge; no finalized aggregate merging.

Final gate:895 library passes with10 documented ignores;37 selected integration
passes. Dedicated tests prove actual four-worker spill and exact decimal/AVG
results, plus startup refusal/one/four-worker admission and cleanup. The initial
router test used an unsupported empty aggregate layout; corrected and preserved.

Release58738 completed in8m42s. All280 source hashes were verified and the binary
was frozen as `parallel_benchmark_embedded` (SHA256459baeb88b93f9fbdddcbc10acf29632bf395b77ffdef1161e3f5f504ad491d2).
Balanced Q13 completes32 typed outputs, with12 steady samples per binary:
7371.668ms control to5043.654ms candidate,31.6% lower. Matching Q10 validates32
outputs and is1.0041× control. A separate four-output phase run passes and shows
inner ingestion4.4s to1.64s, but outer ingestion0.13s to0.48s across23,440 small
batches. Batching/dispatch overhead is a measured remaining cost.

Full raw SF10 development validation29434 is terminal1 in
`.scratch/public-bench/parallel-state-sf10-raw-01/`:51 typed completions,48 also
inside the timing gate. Q5/Q9/Q10/Q12/Q13 still time out. Q20 completes correctly
but misses its fresh deadline. Balanced Q20 then validates32 outputs and confirms
4.4% slower (1598.013 to1668.491ms); all four blocks regress. Its separate
four-output phase check shows ingestion32.5–32.9ms to62.0–67.2ms across458 batches
averaging214 rows. Routing/scoped processing overhead is the next general target.

All build, paired, phase and full-suite jobs in this checkpoint are terminal.
No parent-task completion or leadership acceptance is claimed. The current
candidate is not accepted; do not weaken deadlines or remove Q20. Preserve the
frozen candidate while testing a general small-batch dispatch/batching repair.

[Implementation, validation and evidence](../../../../docs/parallel-aggregate-candidate-2026-09-08.md).
