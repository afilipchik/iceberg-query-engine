# Batch scheduling implementation checkpoint

Implemented serial processing for small routed batches and single active owners
without changing canonical group ownership. Scoped Rayon remains for batches
averaging at least256 rows per active owner with multiple owners. This provisional
policy addresses measured Q13 outer/Q20 scheduling overhead, not query identity.

896 library tests and37 selected integrations pass;10 documented library ignores.
An alternating128/2048-row test proves serial/parallel transition, exact partial
state spill/merge and cleanup. A large hot NULL key stays serial with correct
COUNT(non-NULL). Source is frozen in `dispatch-source-hashes.json` (280 files).

Release52208 completed in8m41s; all280 source hashes were verified before freezing
`dispatch_benchmark_embedded` (SHA2561571d7211c085c29a002a7441b557c4d1019bd8d1927a293521c2b1fe4f52122).
Three-way Q20 validates54 outputs and recovers most of the regression:1.003×
original serial and0.961× first parallel. Q13 validates32 outputs and is34.7%
faster than serial. Q10 validates32 outputs and is1.016× serial. Separate phase
checks validate8 outputs and confirm the intended dispatch paths.

Raw SF10 is terminal1:51/66 pairs complete correctly within deadline; Q20 now
passes its fresh gate. The original Q5/Q9/Q10/Q12/Q13 timeouts remain. Native
SF10 is terminal1:60/66 valid pairs, with Q1/Q13 deadlines missed. The matched
native reference completes all66 outputs. Iceberg is terminal0:66/66 valid pairs,
suite ratio0.878601, geomean0.576103 and12/22 wins; Q13 remains2.18724×.
One session does not establish leadership. Lance is terminal1 with57/66 valid
pairs: engine Q1/Q13 timeouts and reference Q9 allocation refusals. All four
runs retain the same frozen binary and4/12/32GiB budgets. No parent-task completion, full-suite score or leadership pass.

AGENTS.md was consolidated from416 to205 lines without changing its containment
and memory/correctness rules; the entire previous guide is preserved in
`docs/agent-checkpoint-history-2026-09-08.md`. Current status now updates in place.

[Full contract and commands](../../../../docs/aggregate-batch-dispatch-2026-09-08.md).

The local join-source audit finds that HJ_PROF detailed counters instrument the
older collected path, while current inner execution returns a pull-driven stream.
The active stream needs phase attribution before another join optimization.
No engine source changed during these frozen provider runs.
