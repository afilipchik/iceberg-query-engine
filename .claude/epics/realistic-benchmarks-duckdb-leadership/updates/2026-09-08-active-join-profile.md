# Active join profiling implementation

The local upstream comparison exposed an attribution gap: old HJ_PROF counters
miss the current pull-driven inner join. Added QE_JOIN_STREAM_PROF=1 phases for
active probes and shared build initialization. It retains the existing cursor,
chunk bound, SQL semantics and cancellation. No query-specific dispatch.

32 selected streaming/preparation/ownership tests pass with profiling enabled.
A separate seven-test trace run passes and confirms22 valid records, including
two intentionally incomplete probes; hot-key output stops after12,288 rows in
three bounded batches. Source is frozen at280 Rust/Cargo hashes. Release build
67969 completed in8m43s under48GiB containment. Binary SHA256 is
0dd72323f85332a3b68e24fafbde2535d8842c4592b699b364fd88db90399ff1.
Both five-query diagnostic runs are terminal0:20/20 typed outputs pass;
372 join records in the first run and304 queue records in the second.

The prepared five-query SF10 diagnostic uses the existing4/12/32GiB budgets,
typed oracles and dataset verification. Diagnostic wall times are not acceptance
latency and do not clear current raw/native/Lance deadline failures.

[Full contract and live checkpoint](../../../../docs/active-join-profile-2026-09-08.md).

The next measured repair is the live aggregate input frontier: SelectAll polls
synchronous Parquet decode/join work within one task. Q5/Q9/Q10/Q12 use only
1.0–1.34 cores despite16 configured threads. A new four-partition test preserves
exact duplicates/NULL results and one execution per partition, but fails its
overlap assertion with peak1. It is deliberately red; production repair remains
open. Use the existing capability/admission contracts before concurrent pulls,
preserve prepared stream ownership, and stop/join tasks on errors.

[Reproducer and full implementation gates](../../../../docs/serial-aggregate-input-2026-09-08.md).
