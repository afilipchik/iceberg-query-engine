# TRY_CAST input validity dispatch

Status: candidate;711 Rust tests pass with2 existing library ignores.
Optimized performance remains unproven.

Frozen630 improved integer-to-double by23.6% against629 in the first paired
component session. Its full five-provider SF10, GPU and aggregate-cap checks pass,
but protected overflow-heavy TRY_CAST repeats regress10.7% and16.1% against612.
That candidate is not performance-accepted. Its4,912-file verified archive is
linked from [the630 report](infallible-cast-construction-2026-09-06.md).

The shared fallible converter now dispatches on actual input null_count before
its TRY_CAST row loop. Non-null inputs iterate source.values directly; nullable
inputs retain the Option iterator. Conversion failure still writes NULL, and
output values/validity are pre-admitted. Allocation/metadata failures still escape
TRY_CAST as errors. The validity writer is shared and emits identical bits.
No query, benchmark identifier, value range estimate or exception is used.

Pinned Arrow58.4 unary_opt calls try_for_each_valid_idx with null_count, giving
it a separate non-null route. Our previous path used the nullable iterator for
all inputs. Removing that per-row input contract is a source-based hypothesis;
only optimized pairs can prove whether it fixes the measured regression.
The oracle adds nonzero-offset, non-null slices, extending coverage to2,000
combinations of source/target domains, metadata, mode and shape.

Next: complete full library and affected integration gates, freeze source, build
release lance+gpu, compare all eight components with630 and612, then repeat any
flagged components. Preserve infallible integer-to-float gains and shared memory
contracts. Full provider/residency/resource gates and archive follow only after
component acceptance. Timestamp metadata and broader epic requirements remain open.

Frozen632 source SHA256:
`42adf2e5e4bcd7d7d3e723a4e5fb1a5bfac5fb8b7aac76f839acf3556d21a40e`.
Release session26297 is running in64GiB/jobs1 with lance+gpu. Semantic/component
drivers are prepared under `.scratch/try-validity-repair/`; no optimized result
is claimed yet. The previous630 archive and its controls remain unchanged.


Additional prepared component coverage varies conversion outcome independently
of input validity: all values convertible to TINYINT, all overflow, half overflow,
and25% input NULLs with other values convertible. Borrowed-column and DOUBLE
controls accompany each pattern.262144 rows,65536-row batches, exact typed
multiset comparison, ten pairs plus warmup against630 and612. This matrix has
not run yet; its purpose is to avoid accepting an overflow-only speedup that
regresses common valid or nullable inputs.


## Optimized632 checks

Build26297 completed in10m42s. Frozen binary SHA256:
`605954c796e66bed90bf21e495ce622b7e105cd23d1550a6dea3ae65875c35f9`.
Optimized validation89260 passes its asserted contracts:89 float/date,10 dense
float and56 coercion queries match DuckDB. All58 primitive cases match the
preserved engine control; eight retain known integer-division differences from
DuckDB. Literal checks retain19 canonical matches plus one bounded bare-NULL
value check with its schema difference. Small-budget borrowed input succeeds;
expanded literals and DOUBLE/DECIMAL output allocations refuse by memory name.
Decimal wraparound stays fixed; three timestamp metadata probes remain mismatches.
Component session50708 is now running; no performance result yet.

## Component and input-pattern screen

All352 original component requests and528 input-pattern requests passed typed
correctness and time gates. Original TRY_CAST ratio632/630 is0.67557 and632/612
is0.69795; integer-to-double remains0.93555 of612. Float multiplication is1.11224
of630 in one screen (0.99597 of612), requiring protected repeats.

| Input pattern | TRY_CAST632/630 | TRY_CAST632/612 |
|---|---:|---:|
| all_valid | 0.69899 | 1.50559 |
| all_invalid | 0.75282 | 0.72333 |
| mixed | 0.72642 | 1.03369 |
| nullable | 0.90008 | 1.79060 |

All-valid and nullable TRY_CAST remain substantially slower than612 despite
improving against630. Protected repeats are running for both, plus the floating
control flag. No full provider/GPU/cap acceptance run is warranted unless these
component gates pass. The new matrix exposes coverage missing from the earlier
overflow-only probe; success in that probe did not establish general conversion
performance.


## Protected outcome: not accepted

All352 protected requests pass correctness/time gates. Float multiplication
ratios632/630 are0.99040 and1.02544: the initial flag did not recur. Against612,
all-valid TRY_CAST ratios are1.40002 and1.43125; nullable ratios are2.11826 and
1.97890. These confirmed regressions reject performance acceptance. No632 full
provider/GPU/aggregate-cap run is claimed; those runs would not erase this gate.

Next shared construction experiment: accumulate output validity in a register for
blocks of64 values, emit each packed bitmap block once, and consume Arrow's
slice-aware input validity chunks. Preserve output admission and conversion errors;
add63/64/65 and offset/null-pattern oracles. Per-row bitmap updates and nullable
iterator overhead are hypotheses, not yet proven causes. The next candidate must
pass all four conversion/null patterns as well as the original components.


Verified archive:2,181 files, all member/source hashes checked. Archive SHA256
`1183312b20c95d5df7cd8615093afd6d96fd48c19690332133b094c859425591`.
See [provenance](benchmarks/2026-09-06-try-validity/provenance.json). The newer
[packed-mask candidate](try-cast-bitmap-construction-2026-09-07.md) has no
optimized result yet;632 measurements do not apply to it.
