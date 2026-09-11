# Parallel grouped aggregate candidate

The live grouped path now routes retained evaluated rows by canonical key to up
to four spillable controllers sharing the query pool. The candidate passes895
library tests and37 selected integrations. Ten library tests remain explicitly
ignored: eight dedicated CUDA cases, one dedicated IPC-cache case and the
pre-existing dependent-join case. The optimized build succeeded in8m42s and its
280 source hashes were verified before freezing the binary. The completed balanced
SF10 Q13 comparison measures31.6% lower median latency:7371.668ms control versus
5043.654ms candidate. All32 typed outputs pass; each binary has12 steady samples
and4 warmups. Four block ratios are0.6833,0.6884,0.6744,0.6857. This is a targeted
diagnostic comparison past the normal deadline, not full-suite acceptance.
The matching Q10 check validates32/32 outputs and measures2945.886ms control
versus2957.880ms candidate (1.0041×). Its block ratios are1.0050,0.9998,1.0183,
1.0051; this limited sample shows no large regression, not a broad no-regression
guarantee. Q13's median reported reserved-memory peak falls from409,736,850 to
308,975,689 bytes; these are query reservation counters, not exact RSS or proof
that all provider allocations are accounted. Separate phase attribution and
normal-deadline full raw SF10 validation follow below.

## Measured phase change

A separate two-request-per-binary Q13 diagnostic validates all four outputs.
The inner aggregate changes from4335–4433ms ingestion to1632–1642ms, including
619–621ms canonical routing and1013–1020ms scoped worker processing wall time.
It still emits1.5 million groups and reports no ingestion spill. Its output
construction stays approximately106–110ms.

The outer aggregate is a visible tradeoff: ingestion rises from128–132ms to
476–479ms. It receives23,440 small batches in the candidate, versus23,438 in the
control; routing costs151–152ms and scoped processing318–321ms. Thus the net
end-to-end improvement contains a real small-batch regression. These intervals
are instrumented wall times, not exclusive CPU; output remains included in finish.
The next experiment should investigate batching/dispatch overhead without
weakening admitted output ownership, rather than assuming more workers help
every operator equally.

The full normal-deadline raw SF10 run has completed unsuccessfully at
`.scratch/public-bench/parallel-state-sf10-raw-01/`, using fresh DuckDB calibration,
three samples, one development session,16 threads on CPUs0–15 and4/12/32GiB
query/process/cgroup limits. It records66 measured pairs:51 typed completed
comparisons, of which48 also meet the timing gate. Q5/Q9/Q10/Q12/Q13 time out
once each, then report two unavailable-worker records each. Q20 completes
correctly three times but exceeds its1644.622ms ceiling at1696.399,1679.040 and
1677.363ms. The earlier control suite completed Q20 at1579–1600ms under a
similar1646.617ms ceiling. A balanced Q20 comparison now confirms a4.4% regression:
1598.013ms control versus1668.491ms candidate, with32/32 typed outputs passing.
Four block ratios are1.0343,1.0372,1.0548,1.0489. This candidate is not accepted
as the final optimization. There is no valid full-suite score; the Q13 gain does
not satisfy the deadline or suite contract.

A separate four-output Q20 phase run passes the oracle. Its aggregate processes
98,107 rows in458 batches (about214 rows per batch), emitting58,782 groups.
Ingestion rises from32.5–32.9ms to62.0–67.2ms, including9.8ms routing and
51.9–57.1ms scoped worker processing. Output remains around6.6–8.5ms. This
locates a material part of the regression in per-batch processing overhead;
it does not attribute the entire end-to-end difference. Together with Q13's
small-batch outer aggregate, it motivates a general batching/dispatch experiment.
Do not remove Q20, loosen its ceiling, or add a query-specific selection rule.

This implements the next experiment from the
[measured serial-ingestion finding](aggregate-thread-scaling-2026-09-08.md), where
Q13 ingestion stayed near4.4 seconds at1/4/16 configured execution threads.

## Contracts

- `row_selection.rs` validates every index before ingestion. Cursor positions
  refer to selection entries, including duplicates, rather than physical row
  indices. Contiguous and selected inputs share the transactional row loop.
  Spill resumes at the first uncommitted selection entry without re-evaluation.
- `row_router.rs` builds reservation-owned index vectors before dispatching any
  row. Ownership uses complete canonical key bytes. Equal SQL NULLs, NaNs,
  signed zeros and logical dictionary values route together; dictionary codes,
  estimates and ranges do not establish ownership. Workers still compare exact
  keys, so hash collisions do not merge distinct groups.
- `parallel_controllers.rs` admits the complete controller set before opening
  input. It uses at most four Rayon workers, bounded by the existing Rayon pool;
  global input keeps one controller. If initial admission cannot create the
  requested set, it releases that attempt and tries one controller before input.
  Each controller retains the existing prepared-writer, spill, compaction and
  partial-state merge machinery. Advisory group limits are divided among workers;
  actual query-pool reservations remain authoritative.
- Each evaluated batch and its admitted routing buffers remain owned until the
  scoped Rayon call returns. There are no detached per-batch jobs or queued
  unbounded batch streams. Source errors remain terminal, and routing/worker
  failure poisons the set. A routing admission retry is allowed only over the
  already retained evaluated arrays before any worker has applied that batch.
- Equal keys have one worker owner. Each worker finishes its partial states
  before its complete groups are appended to admitted output; finalized AVG or
  variance values are never merged between workers. Output failures invalidate
  the entire query result. Scope completion waits for running tasks on errors.

The current one-batch barrier, repeated routing allocations and synchronous
per-worker finishing may limit performance, especially for small batches or
skew. These are measurement questions, not hidden acceptance exceptions.
Provider decoding and other pre-consumer allocations retain their existing
separate limitations. No dependencies changed.

## Verification

Commands use repository `.scratch` as TMPDIR, `RAYON_NUM_THREADS=4`,
`SAFE_BUILD_MEM=48G`, `SAFE_BUILD_JOBS=1` and `scripts/claude-safe-build.sh`.

- Selection component gate:113 tests pass, including sparse/reversed selections,
  duplicate indices, NULLs, exact decimal sums, empty selection, actual spill,
  invalid-index rejection before valid-prefix mutation and cleanup.
- The first routing gate has one test-fixture failure: it tries to bind an
  unsupported zero-aggregate state layout. The corrected COUNT layout passes;
  the original log is retained. This is not an engine semantic failure.
- The dedicated parallel-owner tests prove four controllers are created, execute
  real spill and produce exact COUNT/Decimal128 SUM/AVG results without duplicate
  final groups. A headroom sweep observes explicit refusal, one-worker startup
  and four-worker startup, with pool/file cleanup after abandoned attempts.
- Final command: `cargo test --locked --features lance,gpu --lib
  --test fused_aggregate_budget_transition --test fused_aggregate_input_errors
  --test live_dictionary_input --test systemic_numeric_tests
  --test aggregate_encoding_contract --test shared_prescan_errors
  --test outer_on_pushdown`. It passes895 library tests and37 integrations with
  the10 library ignores described above. Formatting and whitespace checks pass.

The release build is `cargo build --locked --release --features lance,gpu
--example benchmark_embedded`, with source frozen in
`.scratch/live-schema-boundary/parallel-source-hashes.json` (280 Rust/Cargo files).
The release build is verified terminal with exit0. Frozen binary
`parallel_benchmark_embedded` has SHA256
`459baeb88b93f9fbdddcbc10acf29632bf395b77ffdef1161e3f5f504ad491d2`.
`paired_parallel_state.py` compares it against the preserved borrowed-output
control with unchanged SF10 Q13 and typed validation. The candidate additionally
contains the shared-prescan error repair and benchmark IPC admission guard;
the control predates those changes. Raw Q13 does not use IPC preload.

The full canonical/provider/resource/concurrency gates remain required even if
the targeted comparison improves. The original DuckDB leadership objective is
unchanged. [Evidence archive](benchmarks/2026-09-08-parallel-aggregate/manifest.json).
