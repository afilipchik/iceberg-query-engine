# Preparation admission checkpoint

Decoded IPC and explicit host-Arrow GPU benchmark preparation now preflight
immutable streams and retain a context-pool allowance before decoding. The
original SF10 setup refuses at4GiB query/12GiB process caps instead of aborting.
Two helper tests and a three-batch independent DuckDB check pass. This is a
conservative benchmark boundary, not general decoder certification. Compression
and dictionary deltas refuse; a separate1GiB startup thread failure remains open.

The borrowed-output full raw SF10 rerun finished51/66 valid pairs, with the same
five deadline failures. Q10's measured9% gain is insufficient for its time gate.
No parent task or leadership gate is complete. No release binary includes the
new preload guard yet; its validation is debug-only.

Next: classify setup refusal correctly in the benchmark supervisor, evaluate
bounded decoder/provider admission, and attribute the remaining join/aggregate
cost before another production optimization. Use the existing local DuckDB and
ClickHouse source comparison; copied layouts alone do not establish correctness
or measured benefit. Preserve required provider and GPU control gates.

[Evidence and commands](../../../../docs/ipc-preload-admission-2026-09-08.md).
