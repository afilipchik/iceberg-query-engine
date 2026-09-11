# Admission-aware queue preparation, September 8

The measured Project/join-build queue is serialized because its variable-width
source has no pool-independent copy certificate. `PreparedOutputBound` currently
promises both a copy extent and future pulls independent of the consuming pool.
The admitted decoder uses that pool, so attaching an ordinary known bound to it
would violate the existing contract. Merely increasing permits is not the fix.

## Implemented preparation changes

`ParquetSnapshot` now retains both the plain Arrow metadata view and any legacy
dictionary-coercion view. Both share the same parsed Parquet metadata and checked
file identity. The admitted row-group opener borrows the plain view rather than
reconstructing it and reinterpreting embedded Arrow schema on every group. The
legacy reader still receives its override view. No footer is reparsed per group.

Before `Schema::project` clones its metadata map and Field Arcs, the admitted
reader reserves a conservative construction allowance:4096 bytes per field,
512 base/metadata-entry allowances and twice existing metadata string capacities,
with checked overflow. It retains the lease until its schema/input owners drop.
Original plan/footer ownership remains separate; this is not exact RSS.

Scan paths, projections and static predicates are now shared immutable Arc-owned
plan data. Admitted streams retain the immutable per-partition work vector and an
index cursor, rather than cloning every work item/path/predicate into each stream.
Opening a work item only clones handles. The legacy path preserves its existing
reader routing and projection behavior. IPC lookup uses the same pathname value.

A new regression writes a Parquet schema with64KiB of metadata and requires a
32KiB query pool to refuse before projected-schema construction, with no leaked
reservation. Existing live tests cover plain/dictionary views, actual DuckDB
padded data, codecs, repeated projection, filtering, partitions and cancellation/
refusal outcomes. Full library84708 passes973/0 failures/10 existing ignores,
29.41s. After immutable-plan sharing, final library71744 also passes973/0/10,
29.34s. Both used locked/offline lance,gpu, Rayon4, one build job,48GiB scope and
repository TMPDIR. Formatting/whitespace pass. These changes postdate frozen
ea6fb42d; no new performance claim or release is warranted yet.

## Next implementation: a separate admitted-buffer protocol

1. Add a distinct prepared-input descriptor for audited streams whose output
   buffers retain admission in the supplied consuming MemoryPool. Keep its
   constructors internal. Preparation must bind the exact requested pool (or
   a constrained descendant); an unrelated pool cannot satisfy the descriptor.
   Do not encode this as `PreparedOutputBound::Bytes` or a pool-independent marker.
2. Return initialized streams without polling output or starting producers.
   Admission denial propagates; unsupported shapes decline before consumption.
   Refactor metadata-only row-group capability validation as needed to check all
   selected file/group shapes before choosing this path. No decoder replay after
   output, input consumption or hard errors.
3. Audit/admit remaining dynamic preparation and scheduler metadata, including
   boxed stream state and prepared stream/index vectors. Keep original plan/footer
   residency and query-wide metadata limitations explicit; do not infer ownership
   from Arrow payload size or from a pool high-water mark.
4. The shared join-build queue can retain these already-admitted buffers without
   another deep copy or a competing worst-case copy envelope. Keep bounded demand
   permits through pending send/consumer handoff, preserve leases on buffers after
   handoff, and retain/join or cancel all producers on error/drop. Never await a
   memory allocation while holding downstream state: exhaustion must remain a
   typed, named refusal rather than a reservation deadlock.
5. Propagate this descriptor through audited column/alias-only Project wrappers
   using reserved index/output vectors and the admitted handoff helper. The real
   Q12 build Project is column-only (`l_orderkey`, `l_shipmode`), as confirmed in
   the preserved optimized plan. General expression wrappers must decline until
   independently admitted; do not silently use the generic interpreter.
6. Use a new capability-aware prepared route for eligible ordinary raw scans as
   well as pressure scans, with the same shared query pool. Preserve IPC residency
   and unsupported legacy selection before consumption. The runtime queue trace
   must identify actual admitted preparation and slot count; static ordinary plan
   details alone cannot establish which prepared path executed.
7. Test actual overlapping pulls, wrong-pool rejection, exactly-once preparation,
   all partitions, empty prefixes, duplicate/NULL/string values, repeated output,
   low-budget denial, late errors, drop/cancellation and buffer lease lifetime.
   Compare exact results with an independent typed oracle. Unknown wrappers keep
   the old serial path. Include real admitted scans through Project and the shared
   join-build queue, not only a synthetic capability implementation.
8. Only after these gates, freeze a candidate and rerun both strict Q12 cases,
   then protected workloads/resource/provider/concurrency gates. Keep the fresh
   DuckDB ceilings and both failed prior candidates immutable.

The actual parallel scheduler change remains unimplemented. The broader goal,
including full canonical/provider benchmarks and beating DuckDB, remains open.
