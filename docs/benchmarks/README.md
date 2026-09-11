# Benchmark evidence checkpoints

Each completed implementation cycle runs canonical SF10 provider and residency
screens before an intermediate Git commit and push. Failures remain recorded;
a checkpoint is not a claim of DuckDB leadership or full acceptance.

The September 11 planned-quantum checkpoint includes the current cycle's frozen
source, tests, complete provider/residency samples, typed outputs and independent
oracles, plans, resource evidence, and exact drivers. The report distinguishes
canonical GPU fallback from successful device execution, and the 32 GiB residency
capacity experiment from the unresolved 16 GiB preload gate.

Earlier dated reports link to historical archives retained in the development
workspace. Those archives total about 28 GB and are not all included in this Git
checkpoint. Their absence in a fresh checkout is not a missing test result or a
successful gate: consult the dated report and its qualifications. No historical
payload is deleted by this checkpoint.

Source and benchmark harness hashes describe the measured dirty-tree candidate.
The subsequent Git commit records that candidate and its terminal evidence; the
pre-build Git revision alone does not identify the full measured source.
