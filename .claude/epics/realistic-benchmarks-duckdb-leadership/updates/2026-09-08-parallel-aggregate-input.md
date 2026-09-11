# Bounded parallel aggregate input candidate

Implemented input_frontier with admitted output slots, owned pull tasks and
batch-held demand permits. Prepared streams are consumed once; unknown or
underfunded inputs remain serial. Errors stop/join tasks before propagation.
Decoder scratch remains a separate provider contract.

896 library tests pass with10 documented ignores;39 integrations pass without
skips. Tests cover overlap, two-slot admission, insufficient budget, unknown and
prepared fallback, typed duplicates/NULLs and prefix errors with joined producers
and zero retained pool bytes. Source is frozen at282 Rust/Cargo hashes.
Release26752 completed in8m41s;282 source hashes verified before freeze.
Phase run88024 completes12/12 typed outputs and confirms16 slots and higher CPU
use for Q5/Q10/Q20. Unknown-bound Q9/Q12/Q13 remain serial. Paired run51723 is
terminal0:144/144 typed outputs pass. Q5/Q10/Q20 improve83.4%/57.2%/84.8%;
Q9/Q12 are unchanged and Q13 is1.9% lower. Fresh raw SF10 is terminal1,
57/66 valid pairs: Q5/Q10 recover their deadlines; Q9/Q12/Q13 still time out.
Native is terminal1:60/66 valid pairs, Q1/Q13 deadlines remain missed.
Iceberg is terminal0:66/66 valid pairs, suite0.623841× DuckDB, geometric mean
0.389071,18/22 wins, worst Q13 at2.198947×. Lance is terminal1:58/66 valid pairs,
Q1/Q13 engine timeouts and Q9 reference refusal/crash. Same4/12/32GiB budgets.
Two new parallel real-spill tests pass; each spills346,848 bytes and verifies
exact states, once-only input and zero retained pool charges. This test file is
additional to the unchanged282 frozen binary inputs. Resource diagnostic36181
completes36 processes at1/4/16GiB:70/72 outputs validate; both binaries time out
during the second low-budget Q12 request after a correct158.6s first request.
No cgroup OOM/max events. Isolation6280 confirms the query-budget trigger:
159.1s at1/12GiB versus1.37s at4/4GiB, both exact;310,803 one-row build batches
versus458. The production scan policy is still unchanged. Next priority is the
[bounded variable decoder contract](../../../../docs/scan-budget-batching-cliff-2026-09-08.md),
not removal of the fallback without admission. Q5/Q10 RSS rises;
concurrent-throughput gates remain required.
No full provider/resource acceptance is claimed.

[Contract and checkpoint](../../../../docs/parallel-aggregate-input-2026-09-08.md).
