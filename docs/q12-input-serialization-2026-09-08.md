# Q12 join-build input serialization, September 8

The strict4/4GiB Q12 failure trace from frozen557a9582 provides a completed
operator-level measurement despite the incomplete query. The join-build queue
prepared Project with16 partitions and a maximum of16 slots, but no declared
copy bound/envelope; it selected one slot. All16 producers completed, returning
310803 rows in458 batches. Their poll spans total845.053ms and copied-output
spans total2.116ms. Each producer spent792.112–802.670ms waiting for a permit;
producer wall spans were843.494–854.900ms. Overlapping wait durations are not
additive CPU time. Join build-state initialization itself reported3.829ms after
input collection. The query then reached another unknown-bound aggregate frontier
with one slot before its strict953.441ms gate/watchdog failure.

This is evidence of serialized input polling at a shared queue boundary, not
proof of a16× speedup from adding permits. The relevant live path is
`physical/operators/spillable.rs` queue preparation/producer ownership and
Project's output-capability propagation. The completed queue consumed most of
the strict gate before downstream execution. It is distinct from the previously
fixed one-row decoder fallback and from the corrected packed-block compatibility.

Next investigation after the corrected decoder rerun: establish safe admitted
output ownership through eligible scan/project paths, or another proven pre-poll
bound, before allowing more simultaneous pulls. Include the identity of the
shared query pool, output lifetimes, decoder/predicate scratch, cancellation,
refusal and metadata ownership. A marker that merely observes allocated bytes or
assumes every Arrow buffer is admitted is insufficient. Do not simply increase
queue slots, infer a bound from sampled string lengths, or remove memory safety.
The shared protocol must apply independent of query/table identity.

The current admitted scan still has footer/schema/query metadata gaps and the
ordinary4GiB path uses the legacy reader. Its variable-width output has no proven
queue-copy bound. Correctness and resource gates remain prerequisites to promotion.
Rerun the protected workload/resource cases after a bounded implementation; retain
fresh DuckDB ceilings and all failures. No complete-query speedup is established
by this timed-out trace.

Source evidence, exact extracted fields and original trace hashes are in
`benchmarks/2026-09-08-q12-input-serialization/`. The complete diagnostic remains
in `benchmarks/2026-09-08-admitted-q12/`.
