# Selected scalar payload ownership component

`execution/reserved_scalar.rs` adds `ReservedScalar`, an admitted owner for
scalar payloads retained beyond their input. It is **not yet connected to
aggregate state slots or spill routing**. Existing MIN/MAX/ANY_VALUE/ARBITRARY
storage still needs integration, with inline numeric states preserved.

## Contract

`try_copy(pool, scalar)` admits the destination owner and complete retained
payload before copying. Strings and list element vectors use fallible exact
capacity allocation; mismatched allocation capacity is an error. Nested scalar
values preserve their original variants and exact bits. Timestamp timezones
and Arrow type metadata retained through shared references are included in
admission. Arrow58 type cloning shares Fields/FieldRef/timezone allocations;
its bounded dictionary boxes are copied only after admission.

The private scalar field precedes the reservation in drop order. There is no
method to detach an owned ScalarValue and no deep Clone implementation.
Callers can move the owner or share `Arc<ReservedScalar>` without duplicating
its payload. `as_scalar()` borrows the payload; cloning that borrow into an
unreserved destination would violate the caller's ownership contract and is
not the intended merge operation.

Old and replacement owners must coexist until replacement succeeds. A denied
copy leaves the old value and lease intact. Partial admission is released on
failure; partial initialized strings/list elements are destroyed before the
copy operation's lease is released. Typed pool denial remains distinct from
allocator/copy errors, so only the former can request resumable spill recovery.

Metadata accounting uses checked arithmetic and conservative allocation/header
allowances rather than Arrow's unchecked recursive `size()` summation. Shared
metadata is charged per occurrence; this can overcharge a shared graph. Value
and type traversal are limited to depth64 and refuse explicitly beyond that.
This is a bounded ownership component, not exact RSS telemetry or a complete
query-memory certificate. Future row-layout binding should retain type metadata
once, avoiding repeated per-value metadata charges.

## Verification

Five focused tests pass with zero failures/ignores and 818 filtered out. They
cover retained strings/nested lists after source destruction,
shared owner transfer and last-reference release; old-plus-new admission denial
before any copy starts; partial-copy fault injection and reuse; exact fulli128
decimal, unsigned integer, NaN payload and timestamp metadata; empty strings
and lists; retained field metadata admission; and value/type depth refusal.

Final command:

```bash
TMPDIR="$PWD/.scratch" RAYON_NUM_THREADS=4 SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --features lance,gpu --lib execution::reserved_scalar::
```

Evidence is preserved in
[the component archive](benchmarks/2026-09-07-selected-scalar-ownership/).
Earlier runs are retained, not counted as additional tests. The full library,
integration, GPU, cap and performance gates were not rerun for this unconnected
component. The preceding floating-ordering source has841 unique passes with10
ignores; those results do not certify this new source.

## Remaining work

Use this payload owner in the selected-value portion of the bound partial-state
layout, keeping primitive state inline. Admit selected-value replacement before
committing an input row, and transfer/share owners during merge without cloning
borrowed strings/lists. Finish canonical key ownership, complete state-row
framing, bounded file flush/merge and cursor/controller integration.

The two consuming-source spill-transition tests and the 256 KiB completion case
remain open. They were not rerun here. This component does not repair source
replay or establish any performance gain. Continue the existing
[implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
