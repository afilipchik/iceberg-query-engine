# IPC dictionary projection and error contracts — 2026-09-15

The candidate on parent `74a142f` has completed broad validation and canonical
provider SF10. This cycle changes shared IPC reader behavior. Native paired
attribution and residency screens are complete. Performance acceptance failed;
the intermediate candidate remains provisional.

## Reproduced failures and implementation

1. A structurally valid dictionary message without its optional `data` table
   reached Arrow58.4.0's unchecked unwrap. Red56444 reproduces a panic with full,
   numeric-only, dictionary-only and empty projections. The reader now verifies
   message extent, version, header and required record batch before decoding.
   Errors identify the file. Green1639 passes all four extent-contract tests.
2. Projection did not prevent unused dictionary payload decoding. Red75959 counts
   an actual decode while selecting only a numeric column beside a large string
   dictionary. The new bounded schema proof follows actual IDs and first schema
   definitions, including transitive nested dependencies. It skips only proven
   unnecessary payloads. All dictionary extents and envelopes remain checked.
3. Repeated projected columns caused a field/array count mismatch. Red38586
   reproduces this through the ordinary reader. The decoder now receives unique
   requested columns; the reader restores aliases afterward with shared arrays.

The dictionary proof uses fixed storage for64 IDs, declines on cycles or excessive
schema nesting, and preserves the ordinary decoder for unsupported proof shapes.
Selected dictionaries retain message order, including deltas and replacements.
Unknown IDs remain errors. Full-schema decoding is retained for all definitions;
there is no field-ordinal shortcut. No dependency, allocator, query budget or
ownership default changes. Native reader metadata, decode scratch and retained
outputs are still outside a complete query-pool admission contract.

## Validation so far

Production-feature domain11041 passes five tests, including real IPC files with
ID37 and shared ID0, verified delta messages, nested/reordered/repeated/empty
projections, NULLs, multiple batches and output retained after reader drop.
Focused native/IPC15491 passes31tests. All malformed extent/framing fixtures now
run under full, numeric, dictionary and empty projections.

Both-mode cycle38590 freezes802 source inputs. Each ownership mode passes1,140
library tests with11ignored and128contracts. Default native/IPC has64passes;
partial has63passes plus the known floating numeric mismatch. Spill/numeric has
28passes and the same six known spill failures in each mode. Strict executable
counts, exits, failure names and retained test inventory match the previous
checkpoint. Validation scope peak35,166,957,568bytes under48GiB, zeroOOM/max,
no swap; this is cumulative cgroup memory, not query-wide reservation proof.

Release completed0 in8m52s. Frozen binary
`138199d2d94606d1cac3a68f43ed876d140ea6b166b8739702240bd6c30094ae`
verifies802source inputs. Canonical provider SF10 and postchecks are terminal. All failures are preserved
in this intermediate checkpoint.


## Reproduction and interpretation

Controllers use new output directories and refuse to overwrite existing runs.
The same machine-local fixtures, compiler, locked dependencies and DuckDB reference
from the preceding cycle are used. Commands from the repository root:

```sh
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 \
  PROTOC="$PWD/.scratch/tools/protoc/bin/protoc" \
  LD_LIBRARY_PATH="$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib" \
  PYTHONPATH="$PWD/scripts" scripts/claude-safe-build.sh taskset -c 0-15 \
  .scratch/venv-lance/bin/python .scratch/parallel-aggregate-input/run_ipc_dictionary_cycle.py

TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=64G SAFE_BUILD_JOBS=1 \
  LD_LIBRARY_PATH="$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib" \
  PYTHONPATH="$PWD/scripts" scripts/claude-safe-build.sh taskset -c 0-15 \
  .scratch/venv-lance/bin/python .scratch/parallel-aggregate-input/run_ipc_dictionary_postchecks.py
```

The second controller must run only after the first is terminal. Provider screens
use16threads,4GiB query memory,12GiB process cap, three samples and one session.
Canonical decoded IPC/GPU screens use32/48GiB capacity; preload is outside timing
and this does not clear the16GiB preload gate. Custom required-GPU float smoke
remains a separate workload and requires request-scoped device evidence.

Native attribution compares the frozen parent and candidate in two reversed
blocks with a fresh worker per query, one warmup and three measured requests.
It coversQ1,Q6,Q12,Q18,Q9 under matched4/12GiB conditions and aggregate telemetry.
The180second diagnostic deadline is not the canonical ten-times-DuckDB ceiling.
All successful outputs require independent typed comparison, and unchanged plans
must be checked before attributing a timing difference to the reader change.


## Canonical provider SF10

Cycle38590 is terminal1 because native and Iceberg gates fail. Independent audit
validates340completed engine outputs and255of264planned measured pairs. No
completed output exceeded its recorded ceiling in the supplemental classification.
The entire validation/build/provider scope peaks at35.167GB under48GiB, zeroOOM/max,
no swap. A clean cgroup record does not erase process/query allocation refusals.

| Track | Typed outputs | Valid pairs | Complete | Geometric mean QE/DuckDB | Suite ratio | Wins |
|---|---:|---:|---|---:|---:|---:|
| Raw Parquet | 88 | 66 | yes | 2.392498 | 2.563807 | 0 |
| Native | 80 | 60 | no | — | — | — |
| Iceberg | 84 | 63 | no | — | — | — |
| Lance | 88 | 66 | yes | 1.995551 | 2.968537 | 1 |

NativeQ1/Q6 time out during warmup; all their measured requests are NOTRUN.
NativeQ18 completes this screen, unlike the prior completed-but-late warmup; this
single screen does not certify stable improvement. IcebergDuckDBQ9 completes,
but **Q18calibration2** refuses a262144byte allocation. EngineQ18 and both sides'
measured pairs are NOTRUN because fresh calibration is invalid. Preserve this
phase/location change from the previous Q9 refusal; it is not an engine fix.

Evidence: [provider archive](benchmarks/2026-09-15-ipc-dictionary-sf10/manifest.json)
and [validation source snapshot](benchmarks/2026-09-15-ipc-dictionary-validation/manifest.json).
The completed native comparison below separates candidate/reference behavior
without treating historical ratio changes as causation. No raw/Lance historical ratio change is attributed
to dictionary pruning.


## Native attribution: performance acceptance failed

Both reversed diagnostic blocks complete all80outputs, independently typed-correct.
Optimized and physical plans are identical for every pair. Candidate/parent median
ratios (below1is faster):

| Query | Parent then candidate | Candidate then parent |
|---|---:|---:|
| Q1 | 0.966322 | 0.995801 |
| Q6 | 0.537401 | 1.089459 |
| Q12 | 1.636227 | 1.321530 |
| Q18 | 1.000515 | 1.008473 |
| Q9 | 1.016158 | 1.004052 |

Q6's first-block gain does not repeat. Q12 regresses in both blocks:584.751→956.785ms
and1058.902→1399.371ms. The parent itself varies substantially between blocks;
planning remains about9ms, so this difference is in execution. Q1's reductions
are modest; Q18/Q9 are slightly slower. This does **not** establish a net speedup
or regression-free candidate. The intermediate branch checkpoint must remain
provisional, preserving both correctness fixes and the failed performance evidence.

[Paired evidence](benchmarks/2026-09-15-ipc-dictionary-attribution/manifest.json)
contains all requests, outputs, plans, profiles and source/binary hashes. The
180second diagnostic deadline does not clear canonical nativeQ1/Q6 timeouts.
Next work must isolate dictionary pruning from the envelope/repeated-projection
fixes using a matched execution control, investigate Q12 execution phases and
sequence variability, then retain or revise the optimization based on evidence.
Do not tune a query ID or alter the canonical ceilings to clear this gate.


## Residency and final disposition

Postcheck93724 completes0. All five cases complete; independent audit validates
348outputs/278pairs. Canonical conditions are32GiB query/48GiB process capacity,
16threads,3samples/1session; the custom float smoke uses4threads,4/8GiB and20samples.
This excludes preload and does not clear the16GiB preload refusal or full resource,
concurrency and multi-session precision gates.

| Case | Typed outputs | Valid pairs | Geometric mean QE/DuckDB | Suite ratio | Wins |
|---|---:|---:|---:|---:|---:|
| Canonical decoded IPC | 88 | 66 | 0.803013 | 1.275933 | 14/22 |
| Canonical GPU control | 88 | 66 | 0.796917 | 1.288690 | 14/22 |
| Canonical mixed GPU | 88 | 66 | 0.795504 | 1.276013 | 14/22 |
| Custom float control | 42 | 40 | 1.314668 | 1.734983 | 1/2 |
| Custom required GPU | 42 | 40 | 0.090113 | 0.084892 | 2/2 |

Canonical mixed records zero successful device executions across all88completed
outputs. The custom required case has40/40measured request-scoped device proofs,
matching prepared sessions and no request upload/new fallback/allocation failure.
Do not present the canonical mixed result as GPU acceleration. The postcheck scope
peaks at20,542,066,688bytes under64GiB, zeroOOM/max, no swap.

Decoded engine suite time is17,237.496ms versus17,370.377ms in the prior screen;
reference time is13,509.714ms versus17,164.701ms. The historical ratio change is
therefore substantially reference variation, not evidence of a pruning effect.
[Residency archive](benchmarks/2026-09-15-ipc-dictionary-residency/manifest.json)
and [controller/reproduction archive](benchmarks/2026-09-15-ipc-dictionary-controller/manifest.json)
preserve the qualifications, drivers and failures.

Correctness fixes and reproducible pruning behavior are established. A speedup
is not: nativeQ1/Q6 still fail canonical ceilings, Q12 regresses in both paired
blocks, and required resource/concurrency/precision gates remain open. Commit and
push this evidence as an intermediate **provisional** checkpoint. Continue with
Q12 queue/join phase traces and per-request process counters before changing the
implementation. A later matched control must isolate pruning from envelope and
projection correctness fixes; revise or remove the optimization if it remains
slower. Preserve the correctness fixes and avoid query-specific routing changes.
