# Typed literal binding preserves the declared type

Status: reproduced on frozen621 and623; the binder fix passes712 selected tests
with two existing library ignores. Frozen623 component timings run separately.
No optimized typed-literal fix measurement is claimed yet.

The independent SQL oracle found that
`TIMESTAMP '1969-12-31 23:59:59.123456'` returned an Arrow string instead of a
microsecond timestamp. Both preserved control and current optimized candidate show
this failure. It is independent of the new literal memory construction.

The root is the `SqlExpr::TypedString` binding branch: it parses valid DATE values
specially and returns every other declared type (and invalid DATE strings) as Utf8.
The fallback also formats the AST value, rather than using the decoded literal.

The replacement binds the value through ordinary value binding and constructs a
strict `Expr::Cast` to the declared SQL type. Existing type conversion rejects
unsupported types. Existing constant folding and execution conversion now govern
both typed literals and explicit CAST, avoiding another parallel conversion path.

Three regressions cover declared Date32/TimestampMicrosecond types and independent
exact pre-epoch values (-1 day and -876544 microseconds); multiple batches and
partitions; comparison and empty output; and invalid typed date/timestamp errors.
This does not certify timezone/precision mapping or unsigned type aliases. The
oracle also reproduced unsupported UTINYINT/USMALLINT/UINTEGER/UBIGINT spellings;
those remain explicit unsupported outcomes, separate from the silent timestamp bug.

Evidence: `.scratch/literal-memory-repair/literal-oracle-02/` and
`literal-oracle-control/`. Bare NULL results have equal values but different Arrow
schemas (Null versus nullable Int32); the generic comparator rejects Null type.
Its rejection and a separately bounded null-value comparison are both preserved.


Validation log: `.scratch/literal-memory-repair/typed-literal-contracts.log`.
The 48 GiB/jobs1 wrapper ran the full library (683 passes,2 existing ignores),
cast contracts4, constant folding3, projection admission7, numeric12, and the
three new typed-literal integration tests. No other skips. The source now differs
from frozen623; no623 measurement applies to this binder increment.


## Declared type metadata is a separate reproduced binding defect

A contained five-query probe on frozen623 (`478d947898202d85c3bac5a95b9581c651ffef8f8c2bb328e00dae8187471f70`)
confirms unchecked metadata conversion in `Binder::convert_data_type`. These
predate the new typed-literal Cast routing; they must not be marked fixed by it.

| Declaration | DuckDB reference | Engine623 |
|---|---|---|
| DECIMAL(294,256) | Reject invalid width | Accept Decimal128(38,0) |
| DECIMAL(38,256) | Reject invalid scale | Accept Decimal128(38,0) |
| TIMESTAMP(3), input .123456 | Milliseconds,123 ticks | Microseconds,123456 ticks |
| TIMESTAMP(9), input .123456789 | Nanoseconds,123456789 ticks | Microseconds,123456 ticks |
| TIMESTAMP WITH TIME ZONE | UTC timezone metadata | Timezone discarded |

Decimal precision/scale are narrowed with unchecked `as` conversions; timestamps
always map to Microsecond/None regardless of declared precision/timezone. Correct
handling requires range validation before narrowing and a supported timestamp
metadata contract across binding, scalar extraction, casts and consumers. Do not
merely return a timestamp unit that downstream code will reinterpret as microseconds.
Unsupported domains must refuse explicitly until implemented correctly.

Evidence: `.scratch/coercion-memory-repair/type-metadata-probe-02/`, raw reference
Arrow and integer ticks preserved; DuckDB timezone explicitly UTC. Both probes ran
under10GiB scopes with8GiB process caps during compilation, never latency tests.
The initial probe incorrectly reported the Python nanosecond-to-datetime conversion
limitation as a reference error; it is retained and superseded by the raw Arrow
probe. The current628 source remains frozen for build/measurement; no metadata fix
has been applied yet.


Follow-up source now validates decimal metadata before narrowing, retaining the
existing signed-scale domain (-128 through declared precision), and maps the
unsigned8/16/32/64 SQL spellings to existing numeric domains. Compilation first
caught an i64/u64 mismatch in the guard; that failed build is preserved. Corrected
tests include invalid positive/negative wrapped scales, empty input and TRY_CAST,
valid negative scale, and exact UInt64 maximum. Validation is running. Timestamp
precision/timezone behavior is unchanged and remains open.


## Follow-up temporal contract source audit

The630 build wait was used for read-only source review; no temporal code changed.
The existing optimized metadata probe already reproduces precision/timezone loss.
The following boundaries must be changed together for complete support:

| Boundary | Current behavior | Required contract |
|---|---|---|
| Binder::convert_data_type | Ignores Timestamp precision/timezone and selects microseconds/None | Validate SQL metadata and preserve the intended domain |
| ScalarValue::Timestamp/data_type | Only i64 microsecond ticks and no timezone | Represent unit and timezone with ticks without converting through floating values |
| constant_folding::eval_cast | Extracts microseconds/None; guards exact output type and declines unrepresentable types | Preserve the guard; extend exact extraction only after the scalar representation supports it |
| subquery scalar extraction | Supports only microseconds/None; explicitly rejects other timestamp types | Return exact scalar metadata across every batch/partition |
| reserved_literal and scalar_to_array | Construct TimestampMicrosecondArray | Dispatch all supported units with exact timezone metadata and retain admission |

A binder-only precision fix is insufficient. Add independent typed Arrow/DuckDB
cases for seconds/milliseconds/microseconds/nanoseconds, negative epoch ticks,
rounding/truncation boundaries, offsets, timezone metadata, NULL/empty results,
scalar subqueries and grouped MIN/MAX. Verify date functions and aggregate
normalization separately: some current aggregate paths already preserve original
ticks using the output schema and should not be rewritten based on stale claims.
The current source630 freeze remains the measurement candidate; implement the
new temporal domain only after its performance gates are terminal.
