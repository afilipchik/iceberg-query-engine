# Dynamic SUBSTRING and VALUES domain failures

These are small correctness reproducers, not benchmark samples. They use the
frozen scalar-attribution release `0ebe830e84be602c940f7feae83531ea9da166e269c6e60ddcc057a2891061b2`
and independent installed DuckDB 1.4.4. Shared repairs are now implemented and
validated in the default test build; optimized release oracle validation passes; the five-query canonical SF10 release screen passes.
`substring.rs` normalizes supported Arrow string/integer encodings, propagates
NULLs, computes signed character intervals before clipping, and preserves the
positive-constant Arrow fast path. The independent fixture covers 1,024 combinations
of empty/ASCII/Unicode/NULL strings and signed/NULL arguments. Floating positions
are explicitly unsupported; extreme integer-domain parity with DuckDB is not claimed.

VALUES now infers a common column type across every row, validates widths and
propagates inference errors. Physical evaluation strictly casts cells to that
type before concatenation. Tests cover NULL placement, numeric widening, decimal
scales, conversion/shape errors and the original combined reproducer.

Validation: 739 selected library/integration tests pass, plus the dedicated IPC
test (740 executions total). The historical flatten-dependent-join test remains
ignored. An initial test exposed one-argument SUBSTRING being silently accepted;
the binder now rejects it while retaining valid `SUBSTRING(s FOR n)`. Logs are
in `.scratch/string-values-repair/{initial-tests,focused-fixed,contracts,ipc-dedicated}.log`.
The exact 604-file source snapshot has SHA-256
`f95b09d583b70671fa6e39f4ebcad9ab2760d0595cfc1df9a4d8e88d94489da8`.
These changes do not certify query-wide expression/decoder memory ownership.

The following table records the **pre-fix frozen binary**, not current source.

Explicitly typed Arrow input executes `SELECT substring(s,p,n) AS v FROM t`:

| s | p | n | DuckDB | Engine |
|---|---:|---:|---|---|
| abcd | 1 | 2 | ab | ab |
| NULL | 1 | 2 | NULL | empty string |
| abcd | NULL | 2 | NULL | ab |
| abcd | 1 | NULL | NULL | empty string |
| abcd | 0 | 2 | a | ab |
| abcd | -2 | 2 | cd | empty string |
| abcd | 3 | -2 | ab | cd |

Six of seven rows disagree. The dynamic branch in `filter.rs` reads string values
without checking validity, substitutes defaults for NULL integer arguments, and
casts signed positions/lengths to usize. Constant positive arguments use a
different Arrow kernel. A shared semantic repair must cover both paths, Unicode,
two/three arguments, slicing, dictionaries/type normalization and signed extremes;
do not special-case the seven inputs or query identifier.

The original SQL VALUES form separately fails before SUBSTRING executes:
`Arrow: It is not possible to concatenate arrays of different data types (Utf8,
Null)`. DuckDB returns the expected rows. This requires a separate VALUES type
coercion repair. The very first 1 GiB attempt failed during runtime thread startup;
it supplies no SQL evidence. The next attempts used an 8 GiB process/10 GiB scope
and completed. They did not overlap latency measurement (only a contained build).

Reproducers, SQL, typed Arrow input/output, oracle results and logs:
`.scratch/substring-domain-repro/`, `.scratch/substring-domain-repro-02/`,
`.scratch/substring-domain-repro-03/`. These reproduced failures demonstrate why canonical TPC-H correctness alone
is insufficient SQL coverage.

## Optimized release validation

The lance+gpu release completed in 8m23s under a 64 GiB scope with one build job.
Executable SHA-256: `ad506267227122efe6b67c426d113468e191fdb821f4d3ea8afe95e1fba20585`.
Its source files were verified against the frozen 604-file manifest before copying.
All 1,024 dynamic string rows, seven combined VALUES/SUBSTRING reproducer rows,
and two mixed numeric/NULL VALUES rows equal independently executed DuckDB 1.4.4
results. These are correctness checks, not performance measurements.

The validation driver initially used Arrow file input/output where the runner
requires streams; both setup/reader failures are retained separately and are not
engine correctness evidence. The corrected driver and results are in
`.scratch/string-values-repair/validate-release.py` and `release-oracle.json`.
The canonical SF10 screen against the frozen pruning release completed: all 40
requests are typed/time valid (five queries, three steady pairs plus warmup).
Suite candidate/control is 0.9710 and geometric mean 0.9683. Q16 is 6.3% slower;
no screened query exceeds a 10% median regression. This is not full acceptance. Its first
launch rejected noncanonical q1/q6 identifiers before execution; corrected q01/q06
identifiers are used in the second output directory. No measured failure is omitted.

The [verified release evidence](benchmarks/2026-09-06-string-values-repair/evidence.tar.gz)
contains 192 members: source snapshot, executable, test/build logs, red reproducers,
independent oracle results and both screen attempts. Archive SHA-256:
`d354853ffb2bc90755ee0a109bb863473da4171f5274e5efe191464755ff59a5`.
