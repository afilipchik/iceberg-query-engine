# Shared numeric coercion admission

Status: coercion admission passed778 selected tests (two existing library ignores).
The subsequent direct checked-arithmetic change also passes all778 selected
tests; two existing library ignores are the only skips.
No optimized performance claim applies to this increment yet. The current source
also includes the separately tested typed-literal binder fix.

Frozen623 reproduced both `CAST(i AS DOUBLE)` and `CAST(i AS DECIMAL(20,0))`
returning new128/256KiB payloads for16384 input rows with a64KiB query budget and
zero reservations. Literal admission alone did not cover these coercions.

`planner/reserved_cast.rs` handles primitive numeric source/target pairs and all
eight integer widths to Decimal128 under the existing expression memory scope.
It reuses pinned Arrow58.4's public numeric conversion helper. Decimal conversion
uses checked i128 scaling and Arrow's precision validation, including negative
scales. Invalid metadata or scale factors return errors even for TRY_CAST and
empty inputs. It does not round-trip integers through floating point.

The output payload is reserved before constructing it. Strict casts share source
validity; TRY_CAST admits its destination bitmap before converting any values.
Only value-conversion errors become NULL: buffer allocation failures propagate.
Returned buffers use the existing owner/lease mechanism. Identity casts retain
the original array. Metadata and allocator/RSS accounting remain separate.

`ReservedBufferBuilder::try_extend_reserved` adds bounded, fallible direct fill.
It checks the complete extent before iteration and restores original length on a
conversion error or short iterator. Iterator size hints cannot grow the buffer.
This avoids introducing a full Arrow temporary followed by an accounted copy.

Tests exercise all100 primitive type pairs in strict/TRY modes at empty/nonempty
lengths, plus integer->decimal precision/scale combinations, comparing values,
types and success/error outcomes with the pinned Arrow implementation. Inputs
include NULLs, signed/unsigned extrema, fractions, infinities, NaN and signed zero.
SQL regressions cover both64KiB reproducers, TRY_CAST resource errors, conversion
NULLs and escaped output leases. The direct-fill builder tests rollback and reuse.

Remaining boundaries include Float16, decimal-to-decimal/float, string-to-numeric, typed
NULL expansion, decimal arithmetic, dictionary decoding, other evaluator scopes,
array/schema metadata and result API ownership. This is not full query-wide
resource certification. DuckDB compatibility differences remain explicit evidence;
matching Arrow alone does not establish all SQL semantics.


The new fallible fill is also used by shared checked integer/nullable arithmetic.
It selects the checked operation and nullable/non-null path outside the value loop,
removing the256-value stack chunk, its reservation and the second payload copy.
Overflow and division failures still propagate through Arrow checked native ops;
NULL rows remain skipped. This targets the measured23% integer-addition overhead
against612. No speedup is inferred until the optimized component run completes.

Coercion-only logs: `.scratch/literal-memory-repair/coercion-contracts.log`
(724 passes) and `coercion-integration.log` (54 passes),48GiB/jobs1 wrapper.
Combined validation log: `direct-checked-coercion-contracts.log` (778 passes,2 existing ignores).


## Frozen628 optimized outcome

Binary SHA256: `552a91fd1349a52df8945b23c817a504535029469dd7d7e79f18758098f0c271`.
Source SHA256: `fff08baa04fa719657e1dc66cecb1b00f9217731ce7abfe661d1d1564e6ea9c6`.
Release validation:89 float/date,10 dense float and32 new coercion queries match
DuckDB. All58 primitive cases match control; eight preexisting integer division
mismatches remain. Literal oracle:15 canonical matches plus1 bare-NULL value
match; four unsigned SQL spellings remain explicitly unsupported on this binary.
The timestamp literal fix passes the optimized independent query. Literal and both
coercion64KiB reproducers refuse; borrowed input completes.

All352 component executions pass exact typed validation and fresh DuckDB ceilings.
Ten alternating pairs plus warmup,262144 rows/65536-row batches,four pinned threads,
256MiB query budget/24GiB outer scope. Candidate/control ratios:

| Component | 628 / 623 | 628 / 612 |
|---|---:|---:|
| Borrowed column | 0.9250 | 1.0557 |
| Integer addition | 0.8034 | 0.9816 |
| Float multiplication | 1.1133 | 1.0389 |
| Integer to string | 0.8794 | 1.0333 |
| Addition plus string | 0.8810 | 1.0618 |
| Integer to Double | 1.1757 | 1.2013 |
| Integer to Decimal(38,2) | 0.9069 | 0.9189 |
| Overflow-heavy TRY_CAST to TinyInt | 46.0971 | 36.6761 |

Checked integer construction recovered its component performance. The TRY_CAST
regression makes this candidate unacceptable despite passing the DuckDB ceiling:
DuckDB's own TRY_CAST is slower on this fixture. Source review identifies per-row
error formatting in a result discarded by TRY_CAST. The new follow-up uses Option
for value conversion and constructs messages only for strict-cast failure; tests
are running. No speedup is claimed for that newer source yet.

Both selected aggregate caps pass with actual spill and exact1,000,003 group
counts:1GiB cgroup peak409MiB;2048MiB RLIMIT_DATA inside8GiB scope peak414MiB.
Both account3,855,541,894 spill bytes. These are scoped aggregate gates, not global
resource certification. Full-provider/GPU/performance acceptance remains open.
