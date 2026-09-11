# Minimal complete GPU resident benchmark slice

2026-09-06, source593 review. Scratch design only: no production patch, builds or benchmarks. A complete correct patch spans the worker upload protocol, operator dispatch, context request state and benchmark contract; a barrier added to ready() alone is incomplete. This document specifies the first connected implementation slice instead of supplying a misleading partial opt-in.

## Existing mechanism and exact limitation

`src/physical/gpu.rs:609` Job has Upload, BuildCodes and Run. Upload/BuildCodes have no replies. GpuEngine::request (:727) deduplicates and queues columns plus codes, ignores failed sends, and returns immediately. ready (:758) checks both dependency sets plus bin count. Worker (:1251) processes jobs serially; per-dependency outcomes currently collapse into global upload_failures and absent cache entries. `GpuAggExec::execute` (:1910) fixes readiness once per operator; multi-output delegates bypass GPU, run errors increment run_fallbacks then execute CPU. Existing counters do not count ordinary not-ready CPU execution or correlate success to request/operator.

`examples/benchmark_embedded.rs:149–167` starts a timed context.sql request directly; :229 snapshots global counters. `scripts/benchmark/run.py:87–117` slices stderr and counts trace success, and :272–277 runs one warmup. This explains the previously observed columns-ready/codes-pending race. No fixed warmup count, global failure-counter delta or cache snapshot is a positive residency acknowledgment.

## Policy and first complete scope

Add manifest/request policy `mixed` (default) versus `resident_required`. Preserve existing mixed path and its CPU fallback exactly. Do not call the default mixed path a fully cold device benchmark; true cold end-to-end is a separate future declared boundary.

First resident scope: one serial benchmark process per query, one usable device, one eligible GpuAggExec with a single-output CPU delegate, immutable pinned provider identity, no other GPU requests in that process, fixed table registrations. Preparation and every measured request are single-flight. No upload/cache-mutating jobs may be submitted after successful preparation until session release. This exclusion is enforced by worker session state, not assumed from application etiquette. It avoids initially adding entry leases to the global LRU; general concurrent use remains explicitly unsupported. All ordinary GPU use outside this opt-in retains current behavior.

Unsupported extraction, decimals, nullable upload columns, Int64 out of exact f64 domain, group/bin limit, insufficient cache, multi-output delegate, zero GPU operators or more than one GPU operator return terminal structured preparation failure. Do not execute CPU SQL to discover those conditions and do not warm unsupported canonical plans indefinitely.

## Worker API and lifetime

Add serial worker commands:

- `PrepareResident { session_id, dependencies: GpuDependencySpec, reply: oneshot<Result<ResidentAck, PrepareFailure>> }`.
- `RunResident { session_id, request_id, operator_id, spec, reply }`.
- `ReleaseResident { session_id }`.

Dependency spec carries the actual pinned provider Arc/identity, required numeric columns, grouping columns/codes key, expected physical output schema, typed predicates/aggregate slots needed for exact comparison, and generation/session identity. Deduplicate columns without Debug-format hashes as identity. Derive the spec from actual planned GPU operators. CPU child output partition count is part of preflight. All size/bin math is checked.

Factor the existing worker Upload and BuildCodes arms into shared helpers returning typed outcomes, preserving ordinary retry semantics. Prepare runs these helpers directly on the same device worker for missing dependencies, preserving one attempt per missing dependency. It awaits both numeric copies AND group codes, and explicitly synchronizes the CUDA stream before acknowledging readiness. Inspect the actual cache (not mirrored atomic sets) for every required entry, consistent row counts and valid group/bin layout. A completed sequence can have evicted an earlier dependency; final all-dependency inspection must reject that case as insufficient residency capacity. Track per-dependency outcomes rather than infer cause from an absent key or process-global failure delta.

After final success the worker records the sole resident session and rejects cache-mutating jobs from other session IDs. RunResident checks active session + exact dependency/spec identity and device cache immediately before dispatch; it never uploads or evicts. Thus the acknowledgment-to-run interval cannot lose residency. Ordinary jobs already queued before Prepare are processed first because the worker is serial; jobs submitted later are rejected while the session is active. Never silently execute those rejected jobs under another identity.

Ack fields: session/preparation ID, provider identity token, expected operator ID, dependency names/kinds, rows/group count, acknowledged numeric bytes and codes bytes, cache bytes, preparation elapsed time, device identifier and synchronization completion. Failure fields: category, dependency, original error, completed attempts, bytes, deadline. Categories include unsupported/type/null/range/group limit, insufficient capacity, upload/device failure, worker gone, timeout and conflicting session. Preserve original errors for diagnostics; this is not a new claim of graceful GPU allocator safety.

The caller uses a deadline around the oneshot, never sleep/retry SQL. On timeout close the request and invalidate the session; enqueue release/cancellation token so a late preparation cannot become an orphan active session. Worker checks cancellation before activation and between dependencies. A blocking provider scan/CUDA operation may finish after timeout; process isolation plus the outer process watchdog bounds the benchmark lifetime. Do not claim cancellation preempts device work. Worker panic closes the oneshot and fails the attempt. Dropped/failed reply must immediately release session state.

## Engine/operator connection and exact counters

Add explicit request-owned resident execution state, passed by the benchmark through ExecutionContext to planning/dispatch. Avoid process-wide environment flags as mutable request policy. It carries session token, expected GPU operator spec and typed per-request events. An untimed `prepare_gpu_resident(sql, deadline)` builds/examines the actual physical plan without output pulls, extracts the sole supported GpuAggExec dependency spec and requests acknowledgment. It does not set that discarded plan's OnceLock decision as a proxy for future plans.

The measured `context.sql` remains the original timed boundary, including parse/optimize/plan/execution. Replanned GpuAggExec must match the acknowledged exact dependency spec and request session before choosing RunResident. Do not switch to timed execute-on-prepared-plan alone unless DuckDB's timing contract changes identically as a separately named experiment. Use the original SQL and typed oracle unchanged.

In resident mode the multi-partition-delegate early return, missing GPU engine, not-ready decision and device error all return explicit residency failure; none invokes CPU fallback. Record attempted, device-started, device-completed and refused events by request and operator. Completion requires exactly one expected operator with exactly one successful device run and zero fallback/refusal; zero output rows remain semantically valid if device completion is observed. Keep ordinary mixed events/counts separate. A partial device result followed by failure must remain a failure, never certify a truncated output.

The benchmark may still retain completed correctness output for any unexpected mixed response, but fails residency certification. Counters are structured response fields, not stderr matching. Retain stderr separately. Never rerun/discard a failing measured sample. Do not infer success from a global positive cumulative count.

## Harness connection

Extend versioned setup/manifest with explicit policy and preparation deadline; GPU control remains same-binary CPU with matching data/schema/threads/memory and declared warm host boundary. Before warmup/measurements, create the per-query engine process, send prepare request and persist its complete response/latency/dependency bytes separately. A failed preparation is a recorded query failure; move to the next query in another process. No repeated warmups or silent mode downgrade. Reference calibration and exact10x measured query ceiling remain unchanged. Preparation has a separate stated deadline and is reported separately; cold-end-to-end timing is not implied.

Worker protocol messages need an operation tag with backward-compatible default query. Query messages reference the resident session ID, preparation SQL hash and expected operator identity. Python verifies each response belongs to that session and original query, persists all events, and checks every resident sample's expected/completed device counters. A schema/plan mismatch terminates that query worker. Release runs after result completion; process close on all error paths is the final isolation boundary.

## First implementation checklist and tests

The first complete patch includes gpu.rs shared upload outcomes + Prepare/RunResident/Release state machine, request-owned context/physical dispatch state, operator preflight extraction, benchmark embedded protocol, and versioned harness policy/validation/tests. A stand-alone Barrier job is deliberately not proposed as the final gate: it cannot classify upload failures, prevent eviction, require device execution, or identify unsupported plans.

Deterministic no-device worker-model tests: numeric uploads finish while codes are delayed (ack remains pending); codes completion releases ack; every terminal dependency failure returns category; previous columns evicted by codes means insufficient capacity; conflicting job cannot evict active session; dropped reply/timeout/worker panic cannot leave active session; wrong session/spec fails; resident execution cannot reach CPU delegate even on not-ready/run failure; mixed keeps exact current fallback; multi-output delegate/zero GPU operators refuses preflight. Use channels/barriers, timeout only as deadlock guard.

Protocol tests: preserve mixed defaults; mandatory resident response fields and exact request matching; zero device/missing expected operator/duplicate events/fallback fail; preparation failure and all measured attempts retained; no engine restart to erase failures; preparation excluded from query timing and separately emitted. Independent typed oracle includes grouped float custom fixture and scalar filter, NULL/unsupported cases explicit.

Hardware gate under existing unchanged caps: fitting float fixture with both numeric and group-code uploads, fresh process, acknowledged preparation and all expected device executions for every sample; same-binary CPU control, exact outputs/timing rules, real device trace retained. Canonical decimals must report unsupported, not claim device coverage. Test insufficient residency capacity without increasing budgets. GPU hard admission, host collection/upload queues, kernel allocation and overall process/query ownership remain unresolved separately.

## Implementation checkpoint

The engine/adapter/runner/report prototype is now integrated, with no release acceptance. Public execution APIs prepare_gpu_resident/sql_gpu_resident use request-owned sessions, exact replanned provider/schema/aggregate/predicate-bit matching and the same parse-to-consumed SQL implementation. The first positive provider scope is immutable MemoryTable; the harness uses independently validated IPC on engine and DuckDB control sides. Columns and grouping-code acknowledgment precedes required dispatch; missing completion evidence fails the report. Preparation and measured time are separate. Default mixed behavior is preserved by policy; full host/VRAM/kernel admission is still open.

Default and GPU adapter checks pass. GPU library70034 passes690 tests, focused integrations6982 pass41, explicit IPC adds1; these732 selected passes precede the hardware test. Harness83035 passes97 with2 opt-in skips, and explicit13058 passes both skipped cases:99 total, no remaining harness skips. Actual GPU lifecycle54404 fails at positive MemoryTable planning: existing GPU identity requirements reject the provider before any candidate is registered. This is a reproduced failure, not hardware unavailability. Fake-worker protocol tests did not cover this positive planner path.

A typed GPU cache identity repair is being drafted to retain exact version/provider/group-column keys and pin immutable MemoryTable lifetime, including cleanup through eviction/mirrors/queues. A synthetic hash or removal of the planner guard alone is insufficient. The hardware test verifies preparation, session conflicts, exact grouped device results, release, and stale Run/Release isolation once the identity boundary is fixed. Release build and resident measurements must wait for this repair and its gates.

Logs and drafts: .scratch/gpu-residency-protocol/, .scratch/gpu-residency-engine/, .scratch/gpu-residency-runner/, .scratch/gpu-resident-hardware-lifecycle/. The validated float IPC fixture is .scratch/public-bench/custom-gpu-float-ipc-01/provider.json. No dependency changes.


## Exact identity repair and real-device regression

The former MemoryTable planning failure is repaired systemically. GPU column caches, grouping-code caches, queued-job deduplication and row metadata now use typed keys. Immutable MemoryTable keys strongly retain the actual provider and compare Arc allocation identity; versioned keys compare complete identity bytes and file vectors. Group-code keys retain ordered column vectors, avoiding delimiter collisions. Formatted hashes are diagnostic only. Normal mixed MemoryTable routing still declines; explicit resident planning admits the supported immutable provider.

Eviction removes exact mirror entries and releases row metadata after the last column/code entry. Failed upload commits do not leave row owners; failed enqueue removes its dedup key. Worker teardown clears mirrors and queued metadata after device-cache/session locals drop. This retains host tables while their device-cache entries live; it does not establish query-wide host or VRAM admission.

The actual CUDA lifecycle regression now passes (session9160, exit0, hardware-identity-02.log). It prepares numeric and grouped data, rejects conflicting sessions, validates distinct exact A/B results, and verifies old Run/Release messages cannot destroy a newer session. GPU library79809 passes693 tests, zero failures, three ignored; the hardware test is one of those ignored tests and was explicitly run successfully. Logs are in .scratch/gpu-residency-protocol/. The first capped attempt could not access the systemd bus; the escalated capped rerun succeeded. No bare execution was used.

Five typed-identity tests replace two obsolete string-format tests. Default adapter and full harness gates follow. The report validator also now turns malformed environment objects into explicit contract errors. Release and latency acceptance are still pending; frozen593 timings remain historical and must not be attributed to this repair.


## Source599 rejected by independent GPU semantic probes

The source599 release completed (session96980, features lance,gpu). Binary d51b27c40099df375ecb81c614bdfdcfdaefc25a6bb8ba2e8d735a76a6ef63e9, source archive0888f127f4a607241f924bfeae0bf2e14fc79ddd230ed74d14ba17d0c1407b58. Default check and all100 harness tests passed, but no performance acceptance was attempted: independent actual-device Arrow output comparisons reproduced two systemic semantic errors. `SELECT SUM(v), COUNT(v), MIN(v), MAX(v), AVG(v) FROM t WHERE v > 100` over [1,2] returned zero rows versus DuckDB's one [NULL,0,NULL,NULL,NULL]. All-NaN `MIN(v), MAX(v)` returned [+infinity,-infinity] versus [NaN,NaN]. Both GPU requests reported one completed device execution and zero dispatch failures; completion evidence is deliberately separate from correctness validation.

Reproducer results are .scratch/gpu-resident-identity/domain-red-03/{filtered,nan}/result.json; first two attempts had diagnostic script setup/event-reading errors and are preserved separately, not engine SQL failures. The scalar output repair now retains the scalar row and builds nullable non-COUNT outputs, while grouped empty output remains empty. Actual nonfinite source Float64 arrays now explicitly decline resident preparation and mixed cache upload. Four real-hardware edge tests are running in session68481. Finite fused MIN/MAX intermediates and delayed duplicate cache-upload accounting require follow-up review. Source599 is superseded and must not be certified by its narrow successful A/B lifecycle test.


## Duplicate cache jobs and computed extrema

The delayed duplicate-job hypothesis was reproduced on actual CUDA: replaying column and group-code jobs after preparation/release doubled resident byte accounting from36 to72 (hardware-duplicate-red-01.log, session16323, exit101). The shared upload helpers now check exact live cache keys when consuming jobs and reuse existing entries. Cache insertion additionally computes checked total-minus-replaced-plus-new bytes before mutation, and mirror accounting applies the net delta. The hardware regression now passes with stable byte/column/eviction/failure counters and independently expected grouped values (hardware-duplicate-green-02.log, session14200, exit0). The first green compile attempt had a test-only ambiguous float type, fixed with an explicit f64 suffix.

Nonfinite-source rejection alone did not bound fused MIN/MAX inputs: finite inputs can generate NaN through overflow followed by multiplication by zero. MIN/MAX now accept only direct column inputs in the shared supported-domain predicate. Planner construction, resident preparation/verification and actual dispatch enforce this rule, including manually constructed plans. SUM/AVG fusion retains its existing floating evaluation semantics; this is not proof of arbitrary precision or reassociation equivalence.

The scalar-empty regressions now check all five independent expected values: SUM NULL, COUNT0, MIN NULL, MAX NULL, AVG NULL. Physically empty and all-filtered inputs must execute on device and preserve the scalar row; grouped all-filtered output remains zero rows. Final library/hardware/IPC/default gates are being run sequentially. Source599 remains rejected; later production repairs require their own frozen release and measurements.


Final correctness gate23574 is terminal0: GPU library696 pass, eight ignored in ordinary invocation; six explicit CUDA cases and one explicit IPC case pass, leaving only the pre-existing flatten-dependent-join ignored test excluded. Default adapter check passes. Harness98930 previously passed100 tests with no skips and harness source has not changed since. Formatting and diff checks pass. These703 selected Rust tests and100 harness tests do not certify GPU hard admission or performance. A fresh release/benchmark must use the new source, not rejected source599.


## Exact integer SUM admission

Release4053 (binary2121b6ebd8549a91d8e5326d7c6f6d541f94971412b0bf36398e46e0e708e8fa, source7641b2cb...) independently passes the earlier scalar-empty/NaN-source reproducers, but is rejected for integer SUM. The actual device sums [2^52,1,2^52] to9007199254740992; DuckDB returns9007199254740993. Each uploaded integer is exactly representable, but their sum is not. Verified source/binary/Arrow evidence is archived in docs/benchmarks/2026-09-06-gpu-integer-sum-red/. No latency acceptance was attempted.

The shared aggregate-domain gate now admits SUM only with an exact Float64 output type; integer/decimal SUM follows ordinary CPU routing or explicitly refuses required GPU preparation. Planner, worker preparation, resident verification and dispatch all use the same gate. Direct-column MIN/MAX and COUNT remain eligible. Checks include aggregate field offsets after grouping fields and malformed schema refusal. Final gate42447 passes698 library tests, seven explicit real-CUDA tests and one explicit IPC case:706 selected Rust tests, one pre-existing ignored test excluded. Default build check passes; unchanged harness has100 passing tests with no skips. Full GPU numeric/reassociation and host/VRAM admission remain outside this acceptance.

Next shared CPU attribution is recorded in docs/ipc-scalar-subquery-attribution-2026-09-06.md: Q22's observed plan-time increase includes scalar RHS execution; trace that work before changing queue behavior or caching. This is a source-backed investigation plan, not a new performance measurement.


## COUNT(*) preflight coverage and full-fixture debug gate

Release90206 (binaryc7c1a7f05704741cbc394612c12b613a8dde7fc0622ef74f7c490d5d2e625608, sourcee104c589...) passes all three independent semantic probes, including exact CPU fallback for integer SUM. Its first measured resident run is nevertheless incomplete: CPU control40/40 comparisons pass; GPU Q6 has20/20 typed/device-valid samples, while Q1 preparation refuses and all20 requested samples remain not_run. No suite win is claimed. The full failed run is preserved in docs/benchmarks/2026-09-06-gpu-count-star-preflight-red/.

The closed-expression proof omitted wildcard syntax inside COUNT even though the aggregate planner/kernel supported COUNT(*). Preflight now admits a wildcard only as the sole argument of nondistinct COUNT; standalone wildcard, SUM(*) and DISTINCT COUNT(*) still decline. This is a general expression contract repair, with a positive SUM+COUNT(*) real-device test and negative context tests. Gate77913 passes699 library tests, eight explicit CUDA tests and one IPC case:708 selected Rust tests, one pre-existing ignored test excluded; default check passes.

Before another release build, both unchanged complete custom floating fixture queries were exercised through a debug adapter on real CUDA. Session30531 passes Q1 and Q6 with complete independent typed DuckDB comparisons and one device completion each. The first diagnostic attempt78109 had a missing comparator scratch directory after successful Q1 device execution; its error/output are preserved separately. Corrected evidence is .scratch/gpu-count-star-pilot-02/. These are correctness tests, not latency acceptance. Require this full-fixture debug check before future GPU release benchmark builds; small positive unit tests had missed actual SQL coverage.


## Completed resident development validation

Release36955 and sequential validation32255 are terminal0. Frozen sourcea8d140e1923431a6c5a211c03ef4c1e040a568166c9a28a1c9c30193ba621c45 (600 files), binary0c6f32881ad8c082dc19c0f917797066771f8cb28bd68100d395d4b65dc2a60c. All three independent semantic/fallback probes pass. Required GPU40/40 measured samples and same-binary CPU control40/40 typed comparisons pass, with zero fallback/time failures. GPU/DuckDB suite ratio0.09917, geometric mean0.09966 on the unchanged two-query600k-row custom float fixture only; preparation40.520/14.635ms is separate. Both1MiB cache preparations refuse explicitly, preserving requested not_run slots. This is not hard VRAM admission, canonical SF10 GPU support, or overall leadership.

Archive82203 verifies all243 passing-run files,87 capacity-run files, source archive and decompressed binary. Earlier wrong/refused candidates remain separately archived. Current report: docs/gpu-resident-validation-2026-09-06.md. Production/harness source remains identical to this measured snapshot; subsequent documentation updates are newer. No heavy job is live; no parent epic task or overall goal is closed. Next CPU work is the scratch-only generic scalar-subquery attribution patch/tests; GPU hard admission and full resource/provider/public/holdout/concurrency work remain required.
