# Proposed admitted native IPC reader — 2026-09-15

This is an implementation design, not an implemented capability or performance
claim. Current native scans still lack prepared admission. The Q12 trace on both
frozen binaries has8declared join-build partitions but1input slot. The same-binary dictionary control is now complete:96typed outputs show no
consistent pruning effect, while Q6/Q12 timing remains variable. The separate
one-slot frontier repair is under full validation. Implement this reader only
after that cycle is frozen; existing code/source/test contracts remain authoritative.

## Ownership boundary

`PreparedAdmittedInput` promises that emitted buffers retain query-pool ownership
and future decoder/scratch allocations use the supplied pool. It does not promise
pool-independent polling. Merely attaching a lease after `open_row_group` or
advertising a fixed-width logical schema cannot establish this contract.

Use a dedicated admitted reader alongside the existing correct serial route:

- Pin immutable segment files and retain the NativeTable snapshot. Reserve new
  footer/schema/descriptor/projection metadata before constructing it. The original
  plan/manifest residency remains a separate existing contract, explicitly named.
- Map bounded dictionary/record blocks instead of charging or retaining entire
  multi-column segments. Validate signed extents, framing and footer exclusion
  before mapping. Charge the page-rounded window before the mmap; its owner must
  retain both mapping and reservation until all derived buffers are gone.
- Audit the pinned Arrow58.4.0 decoder allocation sites. For an initial flat
  uncompressed capability, use `with_require_alignment(true)` and keep validation
  enabled. Do not permit hidden aligned copies. Compression and dictionary delta
  concatenation allocate payloads: either admit them explicitly or decline this
  capability before output, preserving the ordinary correct route.
- Prove supported schema/message shapes before constructing any output streams.
  Flat primitive/date/decimal/UTF8 and ordinary flat dictionaries cover the actual
  canonical workload. Nested/unknown types, unsafe alignment and unsupported
  message shapes must be explicit, conservative capability decisions, not guesses.
  Bound metadata from verified counts/lengths and the audited constructor paths;
  a broad RSS estimate is not an allocation proof.
- Keep FileDecoder and temporary batch schemas inside the reader lease. Copy
  selected rows to existing admitted flat output builders and use the declared
  plan schema for output. This avoids leaking newly constructed physical schemas
  through consumer-extracted SchemaRef values after temporary reader owners drop.
  Do not claim output admission by observing Arrow payload bytes afterward.
- Read deletion vectors through the retained snapshot rather than cloning an
  uncharged Vec. Admit selection/output construction; retain the exact global row
  cursor across batches and segments, including empty survivor batches.

Arrow's default writer uses64byte alignment, V5 metadata and no compression,
which matches the canonical native files. Eligibility still requires actual
message/layout evidence; a writer default alone is not proof about stored data.
`admitted_selection::copy_range` and admitted row selection already support plain
and checked dictionary inputs. Reuse these contracts instead of ordinary Arrow
filter/take kernels with unaccounted allocations.

## Pinned Arrow decoder allocation audit

A read-only audit of `arrow-ipc-58.4.0/src/reader.rs` confirms that alignment control
alone does not establish admission. The source hash is preserved in the scheduler
cycle's `serial-frontier-v2-arrow-reader-audit.json` evidence.

| Function | Allocation or validation boundary | Required implementation treatment |
|---|---|---|
| `create_array_from_builder` | `align_buffers(!require_alignment)` can copy payload; ArrayData/array headers still allocate | Require alignment, retain validation, and reserve separate bounded metadata |
| `read_record_batch` | Collects variadic counts, grows projected array/column vectors, creates a projected schema; asserts no unused variadic counts | Validate supported flat metadata and empty variadic counts before decode; bound field/count/projection metadata before construction |
| `get_dictionary_values` | Builds a field-match vector and a synthetic one-column schema; unwraps dictionary data | Keep the existing checked dictionary envelope contract and charge temporary schema/field metadata |
| `update_dictionaries` | Inserts dictionary entries; delta mode concatenates full old/new values | Bound dictionary map entries, own retained values, decline delta capability before consumption unless explicitly admitted |

The schema's physical shape, not only selected logical columns, controls decoder
traversal. Unsupported shapes or excess metadata must be rejected/declined before
constructing the decoder's vectors; charging after Arrow returns is too late.
This audit identifies implementation obligations, not a measured CPU bottleneck.

## Preparation, progress and cancellation

Preparation may inspect metadata but must not pre-pull output. Validate every
selected segment's eligibility, preserve its snapshot/file identity and return
exactly all declared partitions. A decline cannot consume input or silently replay
partially consumed work. I/O/corruption errors are named terminal errors, not
unsupported-capability signals.

Before choosing reader concurrency or its window size, inventory actual canonical
record/dictionary block extents and projection footprints from the pinned native
files. A whole record-body window can retain unselected columns; assuming a small
logical projection implies a small mapped/decoded working set would repeat the
previous scan-budget batching mistake. Compare the charged per-reader working
set against the4GiB query budget and downstream state needs. Keep all declared
partitions even if the budget permits fewer active readers; expose the actual
reader limit in diagnostics rather than silently claiming8way parallelism.

Reserve a bounded working-space credit before downstream join/aggregate state
expands. Metadata, live mapping windows, decoder state, pending source batches
and output construction must all compete within that credit and the query pool.
A source batch/output lifetime must not hold a semaphore that prevents progress
when consumers legitimately retain earlier results. Under insufficient budgets,
coordinate a smaller frame/working set where possible or refuse by name; do not
force8slots or bypass admission because the ordinary route was faster.

Native reads currently run off reactor threads. Preserve that scheduling contract.
If blocking tasks survive asynchronous cancellation until their current operation
finishes, they must continue owning all reservations and file/buffer state. Admit
task/stream metadata, signal cancellation before further work, and test that leases
are not released early. Do not equate dropping a JoinHandle with stopped work.

## Required implementation and evidence gates

1. Add bounded mapped-window ownership and checked metadata preparation with
   refusal/lifetime tests, including surviving Buffer/ArrayData clones and slices.
2. Add admitted flat IPC decoding/output and deletion-aware selection. Test
   duplicate/reordered/empty projection, NULL keys/values, empty/all-deleted rows,
   multiple batches/segments/partitions, shared IDs and supported encodings.
3. Test compressed/delta/nested/unaligned capability decline before output, malformed
   metadata named errors, real pool pressure, retained results, and cancellation.
   Validity or error checking must never be disabled for throughput.
4. Wire the completed reader into `NativeStreamingScanExec` preparation. Probe actual
   canonicalQ1/Q6/Q12 boundaries without output pre-pulls, then verify runtime slots
   and all partition consumption under the context pool. A successful preparation
   call alone is not evidence of concurrency or complete admission.
5. Measure matched execution with typed DuckDB oracles and unchanged SQL. Diagnose
   added copies/filter costs and scheduling before claiming speedup. Only a completed
   candidate gets both-mode gates, frozen canonical provider/residency SF10, resource
   outcomes, evidence archive, intermediate commit/push and continued work.

The implementation remains outstanding. Do not mark the existing epic complete
or describe this proposal as a resource-safe native path.
