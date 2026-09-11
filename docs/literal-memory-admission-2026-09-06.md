# Shared literal payload admission

Status: production implementation passes 770 selected tests with two existing
library ignores; no other skips. No optimized performance or provider claim applies to this source yet.
The preceding frozen621 measurements remain preserved separately.

The reproduced `SELECT 1 FROM t` budget violation originates in shared
`scalar_to_array`: it expands a scalar into a batch-sized Arrow payload. This
happens both for returned constants and arithmetic operands. Previously those
allocations bypassed query reservations.

`planner/reserved_literal.rs` now constructs flat scalar payloads with
`ReservedBufferBuilder` whenever the evaluator has a query allocation scope.
Primitive, date/time, interval and decimal arrays use admitted repeated values;
booleans use admitted bits. UTF-8 expansion checks the complete byte and offset
extent and admits both buffers before filling them. Escaped Arrow buffers retain
the reservation through their existing owner lifetime. NULL arrays have no payload.
Array/schema metadata and allocator overhead are outside this payload contract.

The shared scalar conversion now returns `Result`. Expression and subquery callers
propagate allocation/type errors; optional constant folding leaves failed casts
unfolded. Invalid decimal metadata returns an error instead of panicking. The
separate delimiter conversion helper is unchanged and remains an audit boundary.

Budgeted List expansion explicitly refuses because the legacy helper serializes
lists as JSON and cannot establish a typed, budgeted list contract. Unscoped legacy
List handling is unchanged. No query-memory opt-out was added. Filter, compiled,
aggregate and other evaluator entry points still need query scope coverage;
coercion and decimal arithmetic allocations are also separate open work.

Tests compare every supported scalar type at 0, 1 and 9 rows against the previous
unscoped conversion, exercise invalid decimal metadata, string offset overflow,
partial admission cleanup, the 64 KiB SQL reproducer and an escaped literal buffer.
The existing comparison, constant-folding, arithmetic and builder contracts are
included in the contained run. The first run passed720 tests (683 library,37 integration); the second passed50
integration tests, including13 spill cases. Both used `cargo test --locked` through
the 48 GiB/jobs1 wrapper. Exact selected suites and results are in
`.scratch/admitted-construction-repair/literal-contracts.log` and
`literal-integration.log`.


Optimized source623 built successfully with lance,gpu. Binary SHA256:
`478d947898202d85c3bac5a95b9581c651ffef8f8c2bb328e00dae8187471f70`.
89 float/date and ten dense float queries match DuckDB; 50/58 primitive queries
match DuckDB and all58 match the older control. Eight integer-division differences
remain preexisting. The literal budget probe now refuses; both coercion reproducers
still exceed the budget, as expected for the open boundary.

The added twenty-case literal oracle has fourteen canonical matches, one bare-NULL
value match with an explicitly different Arrow type, and five preexisting failures
on both623 and621: four unsupported unsigned SQL spellings and TIMESTAMP typed
literal silently returned as Utf8. The first strict-comparator stop and both full
oracle runs remain in `.scratch/literal-memory-repair/`. The validation driver exits
nonzero rather than concealing these limitations. No full literal correctness
claim is made. A new binder fix is under development and has not been compiled;
all optimized evidence continues to refer to frozen623.


## Frozen623 component evidence

The corrected paired component completes220/220 engine requests with exact typed
multiset validation and fresh DuckDB time ceilings. Ten alternating pairs plus
warmup per case,262144 rows,65536-row batches,four pinned threads,256MiB query
budget and24GiB outer scope:

| Component | 623 / 621 | 623 / 612 |
|---|---:|---:|
| Borrowed integer column | 0.9397 | 0.9694 |
| Integer addition | 1.0096 | 1.2342 |
| Non-null float multiplication | 1.0223 | 1.0006 |
| Integer to string | 0.9232 | 1.1725 |
| Addition plus string | 0.9118 | 1.1706 |

These are single-session component measurements. They do not establish a causal
string speedup from literal admission or broad provider performance acceptance.
The remaining integer and string cost against612 requires shared implementation
work. The binder fix is newer source and is not included in these timings.

The initial run stopped after44 validated engine requests when the general
comparator rejected a262144-row float bucket while comparing DuckDB with itself
(maximum ambiguous bucket2000). The corrected component comparator sorts whole
typed rows and checks exact equality, preserving multiplicities and legal row
reordering. It caps each input at262144 rows/16MiB. The fixture uses exactly
representable binary fractions, so no floating tolerance is needed. Both initial
failure and corrected driver/results are preserved; the global comparator's
resource bound was not raised or bypassed for general benchmarks.


Verified archive: [provenance](benchmarks/2026-09-06-literal-memory/provenance.json), 705 members, each hash-checked. Archive SHA256: `5f7db2474053c3735a4ae367b5361f18b7819d0480df6bd3d64be65e34449604`.
