# Packed TRY_CAST validity construction

Status: final712 Rust tests pass, with2 existing library ignores.
Optimized performance is rejected; see the terminal outcome below.

The preceding632 candidate improves overflow-heavy casts but fails the broader
input-pattern gate: protected all-valid TRY_CAST is1.40002/1.43125 of612 time,
and nullable TRY_CAST is2.11826/1.97890. Its2,181-file verified archive is at
`docs/benchmarks/2026-09-06-try-validity/`; archive SHA256
`1183312b20c95d5df7cd8615093afd6d96fd48c19690332133b094c859425591`.
It is not performance-accepted; no632 full provider/GPU/cap run is claimed.

The new shared TRY converter processes64-value blocks. Arrow BitChunks preserves
input null-mask slice offsets. A block starts with input validity, clears only
conversion failures, then writes its packed output mask once. All-valid input
blocks omit input-null checks; all-null blocks skip conversion. Nullable blocks
initialize output slots in bulk and visit only set input bits. The final byte's
padding is zero. Output values and bitmap capacity are admitted before conversion,
with no growth or extra heap scratch. Strict errors and metadata/admission behavior
are unchanged. This is a general construction change, not query-specific tuning.

The first compile exposed an iterator borrowing a temporary BitChunks view. The
view is now retained through iteration; the failed log is preserved. The first
packed version passes all712 selected Rust tests (2 existing library ignores).
Before freezing, mask initialization was changed to preserve input bits and clear
failures, avoiding per-success validity updates; those gates also pass712 tests.
The final nullable path now walks set bits after bulk initialization. Its full
library/integration run also passes712 tests (686 library plus26 integration). No performance was measured for the earlier packed variant.

The existing type-domain oracle covers2,000 combinations. A new704-combination
oracle compares integer and decimal results/errors with Arrow across0/1/7/8/9,
63/64/65 and127/128/129 rows, offsets0/1/7/63, and all-valid/all-invalid/all-null/
mixed patterns. It also verifies reservation release. Focused integration gates
cover SQL semantics, output ownership and memory refusals.

Next: freeze and build; independent optimized semantic checks; original components
and four input patterns against632 and612; protected repeats for any>10% flags.
Only then run broader provider/residency/resource gates and archive the outcome.
The goal and parent epic remain open, including timestamp metadata and broader
resource/workload requirements.


Frozen635 source SHA256:
`8bf2be7aea86e731f4689de8f3a9457e3df397e1db6689ae5b52d62d1e8e82d8`.
Release session71239 is running in64GiB/jobs1,lance+gpu. Drivers under
`.scratch/try-bitmap-repair/` compare against632 and612. If protected pattern
regressions persist, collect matched kernel/construction profiles before another
production change. Full provider/residency/resource acceptance remains unproven.


Profiling preflight: `perf stat -e cpu-clock -- true` inside a1GiB scope exits255;
host perf_event_paranoid is4. The denial is preserved in perf-capability.log.
No kernel setting was changed. objdump/nm are available; valgrind is not on PATH.
This is a profiling capability limit, not a benchmark or engine failure. If the
candidate still regresses, use supported instrumentation and static code analysis
without treating them as measured CPU-event profiles.


## Optimized635 validation

Release71239 completed in10m41s. Binary SHA256:
`38776f99a5cd84905dc5f30e3d48c79566cb92f0f2287dbdcc7a6ce7e0868ffb`.
Validation7952 completes its asserted gates:89 float/date,10 dense-float and56
coercion queries match DuckDB; all58 primitive cases match the control (eight
pre-existing integer-division differences from DuckDB remain). Literal results
retain19 canonical matches plus the bounded bare-NULL value/schema distinction.
Small-budget expanded literal/DOUBLE/DECIMAL buffers refuse by memory name while
borrowed input succeeds. Decimal metadata rejection remains fixed. The three
timestamp metadata probes still mismatch and are not counted as passing coverage.
Four-pattern performance session37381 is running; no timing result yet.

## Performance outcome: rejected

All352 original,528 pattern and660 protected component requests pass typed
correctness and time gates. Nevertheless, protected TRY_CAST ratios remain above
the10% regression limit:

| Case/control | Repeat1 | Repeat2 |
|---|---:|---:|
| original/632 | 1.31339 | 1.25319 |
| all_invalid/632 | 1.46749 | 1.23933 |
| all_valid/612 | 1.46726 | 1.30345 |
| mixed/612 | 1.14487 | 1.10525 |
| nullable/612 | 1.34308 | 1.38993 |

Source635 is not performance-accepted. No full provider/GPU/aggregate-cap run
is claimed for it. Packed validity improves the nullable path relative to632,
but does not reach612 and regresses overflow-heavy input relative to632.
Further hand-written loop changes are suspended pending isolated construction
measurements. The next diagnostic compares exact632/635 kernels and pinned Arrow
in one process, separating construction and release costs, plus admitted zero-fill
and reservation-only controls. It is not a replacement for SQL-level acceptance.

## Completed archive and diagnostic

All2,631 evidence members were hash-verified in
`docs/benchmarks/2026-09-07-try-bitmap/`. Archive SHA256:
`fc69ac151b6c04fcd8d537c72958cacd52bd819d7df463463a65ff1e6ccda097`.
Build, validation, component and archive jobs are terminal. Earlier running/next
statements above record the experiment history, not live state.
See [isolated construction results](cast-construction-costs-2026-09-07.md).
