# Bind UTF-8 aggregate keys once per batch — 2026-09-11

The measured generic aggregate path spends substantially more time preparing
canonical keys than dispatching rows. Frozen99e1c0dd's Q1 preparation is about
1.75s versus0.36s dispatch; all7,323batches use prepared keys. See the
[phase diagnostic](shared-aggregate-profile-2026-09-11.md) and its verified46-file
archive with800source inputs. This motivates a shared representation-binding fix,
not query-specific SQL or planner routing.

`key_rows/bound_arrays.rs` previously bound numeric representations once but sent
plain UTF-8 through generic checked traversal twice per row: size calculation and
writing. Regression36734 passes independent canonical-byte assertions, then fails:
820downcasts over512sliced rows. It coversNULL, empty strings, Unicode, embeddedNUL
and duplicates through a delegating Arrow array that counts representation access.

Current source adds a borrowed `StringArray` binding. It retains the exact marker,
little-endian u64 byte length and string bytes; NULL stays distinct from empty.
Batch/layout identity and row bounds are checked before access. Size growth uses
checked arithmetic and the existing admitted workspace. Failed encoding still
invalidates the workspace; no input, expression or selected-stream replay is added.
Dictionary/nested traversal remains checked. No dependency, ownership-policy,
key/hash format, query budget or specialized SQL rule changes.

Green54503 passes20key-row tests/1existing ignored, including the regression,
dictionary logical equivalence, admission refusal, sliced validity and stale-key
invalidation. Formatting passes. The skipped test remains a skip, not coverage.

Cycle42123 completes broad validation and strict comparison: both modes pass
1,133library tests/11ignored and128contracts. Native/IPC passes63default and62partial
with its one known numeric failure. Spill/numeric passes28with the same six failures
each. No added/removed failure names, no retired test names, executable and exit
checks pass; the new library regression adds one pass. The48GiB scope peaks at
40,124,334,080bytes through validation, swap disabled,zeroOOM/max events.

The [27-file validation archive](benchmarks/2026-09-11-bound-utf8-validation/manifest.json)
verifies every member of its800-input source snapshot. Cycle42123's release completed0 in8m49s, freezing
`59619bde34755086d5da0f469413755722b83b4f7e876f9de403f572e7d6d3cf` from800inputs.
Canonical SF10 raw/native/Iceberg/Lance is running. The cumulative validation/build
scope reaches40,216,952,832bytes,zeroOOM/max events; this is not query-only memory. Compare with
the pushedbb38784 baseline; preserve all failures. Source stays frozen. Optimized
performance is not yet measured.

Postcheck50559 is terminal0: independent typed audit, two reversed comparison
blocks and archives complete. The paired run compares99e1c0dd with59619bde, same
routing/scan instrumentation and4/12GiB budgets. All80outputs are typed-correct,
and optimized/physical plans match. Candidate/parent query-time ratios:

| Query | Block1 | Block2 |
|---|---:|---:|
| Q1 | 0.8518 | 0.8486 |
| Q6 | 1.0118 | 1.0721 |
| Q12 | 0.9304 | 1.0061 |
| Q18 | 1.0030 | 0.9948 |
| Q9 | 0.9755 | 0.8935 |

Q1 falls from6,329/6,382ms to5,391/5,416ms: about15%lower time in both orders.
Key preparation falls from1,738/1,788ms to875/903ms; dispatch remains roughly
348–363ms. This supports the targeted mechanism. Q6 is slower in both blocks;
these two instrumented blocks do not prove general regression freedom or leadership.
The16GiB postcheck scope peaks at7,076,593,664bytes,zeroOOM/max,swap disabled.
All800source inputs verify after completion. Archive and push the intermediary.
This is an intermediary cycle; generic aggregation, native admission, reference
stability, residency/resource/concurrency acceptance and overall DuckDB leadership
remain open.

## Follow-up after the intermediary push

The generic key change also reaches decoded IPC and CPU fallback in GPU mode.
After the SF10/paired checkpoint is pushed, rerun the existing five-case residency
screen on the same candidate: canonical decoded IPC, GPU control and mixed GPU,
plus the explicitly custom required-device float smoke/control. Preserve the
canonical32GiB query/48GiB process capacity-experiment label; it does not clear the
16GiB preload refusal. Request-scoped device evidence must distinguish real GPU
execution from CPU fallback. Scripts are prepared but not launched, to keep all
heavy timing sequential and publish the current intermediary first.

The ignored key-preparation microbenchmark remains historical: it calls
`KeyWorkspace::encode_arrays`, bypassing current `BoundKeyArrays`, so it cannot
measure this binding fix. The fresh paired diagnostic uses the active query path.

## Canonical provider screen completed

Cycle42123 is terminal1 with successful release and incomplete providers. The
independent audit validates337completed outputs/252of264measured pairs:
raw88/66,native77/57,Iceberg84/63,Lance88/66. Raw geomean2.409209/suite2.599317
versusDuckDB (0wins); Lance2.019243/2.985002 (1win). These ratios do not establish
leadership. The cumulative validation/build/provider scope peaks at40,571,179,008
bytes,zeroOOM/max events,swap disabled.

Native Q1 andQ6 warmups timeout. NativeQ12 now also times out on measured1; remaining
measured requests are not run. This is an additional benchmark failure relative
to8c4936d8, despite unchanged broad-test failures. Its DuckDB calibration median
fell from271.231ms to117.629ms, tightening the10x ceiling from2,712.310ms to1,176.291ms.
The candidate warmup completed in812.968ms. No causal regression conclusion follows
from these separate single-session screens alone; preserve the failed sample.

Iceberg's DuckDBQ9oracle refuses a256MiB allocation; dependent engineQ9 is not run.
This differs from the prior reference failure's stage/size and is recorded as such.
Supplemental nativeQ12 controller91767 is terminal0:16typed-correct outputs,
unchanged plans,800source inputs verified. Fresh-worker candidate/parent medians
are0.9586 and1.3515 in reversed blocks. Under the recorded current1,176.291ms ceiling,
parent exceeds it on three requests (one warmup/two measured), candidate on one
measured request. All requests are retained; the180s diagnostic watchdog does not
reclassify them as performance passes. This establishes that both binaries can
exceed the tightened ceiling, but neither reproduces earlier query history nor
proves the candidate regression-free. The original SF10 timeout remains a failure.
Native diagnostic16GiB scopepeak236,969,984bytes,zeroOOM/max,swap disabled; cgroup
charge is not process RSS or proof of complete query-wide accounting.

Verified archives (counts exclude top-level manifests): shared profile65,
routing phases46,validation27,SF101276,paired173,nativeQ1247. The checkpoint also
includes terminal controller logs. This is an intermediary candidate with known
resource/performance failures; further native stability and residency gates remain.

Intermediary commit8c8989965d242b0a9728993b16f177392bab5ad3 is pushed and its remote
branch hash verified. The1664-file allowlist includes all listed evidence and the
16-file terminal-controller archive (plus its manifest); source formatting passes, seven archived raw
log whitespace warnings are preserved. No unrelated untracked files were staged.
Residency follow-up55937 completed on the same59619bde binary:348typed outputs,
278valid pairs, with custom40required-device measurements separate from canonical
zero recorded device executions; see bound-utf8-residency-2026-09-11.md.
