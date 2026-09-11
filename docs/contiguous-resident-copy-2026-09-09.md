# Admitted contiguous resident copies — 2026-09-09

## Reproduced systemic cost

The admitted memory source copied contiguous batches through a generic nullable
row gather. It allocated one `Option<(usize, usize)>` per row and visited each
cell through a selection lookup even though the selection was an identity range.
At 64 MiB query memory a single Int64 source batch of 65,536 rows was split at
17,476 rows. Red25100 reproduces that result; green98115 returns all65,536 rows.
This is an operator representation issue, independent of benchmark SQL.

## Implementation and ownership

`storage/admitted_selection/range.rs` copies checked contiguous ranges directly.
Primitive values use typed admitted buffers, preserving decimal scale, timestamp
timezone and float bits. Boolean values and validity copy packed bytes, adjusting
sliced bit offsets and masking the final partial byte. UTF8 copies its byte span
and admitted normalized offsets. Schema width, column ordinals, logical types
and row extents are checked before allocation. Dictionary inputs retain the
existing checked gather fallback and its admitted row indices.

The memory source retains its prepaid input domain and child frame byte cap.
It attempts the remaining source range and halves on admission refusal; source
cursors advance only after successful construction. This does not re-evaluate
SQL or replay external input. Buffer ownership survives output slices. Source
residency and the existing query-wide admission gaps are unchanged.

## Validation

All commands use repository `.scratch` for TMPDIR and the required memory-capped
wrapper. Tests use locked/offline dependencies, lance,gpu features, four Rayon
threads, 48 GiB scope and one build job.

- Red25100: fitting contiguous batch regression fails (17,476 versus65,536).
- Green98115: fitting batch regression passes.
- Broad98963 terminal0:1,027 library tests pass,11 ignored;27 integrations pass
  across IPC extents, native streaming scans, outer streams, runtime-filter domain
  and lineage, and prepared streaming joins.
- Three independent range tests cover every sliced bitmap start/length in a
  65-bit domain including empty ranges; mixed typed values/NULLs/UTF8/float bits;
  dictionary NULL values; retained output ownership; refusal and invalid extents.
- Spill42219 terminal101:6 pass,7 fail with the same names as the preceding gate.
  This does not assert identical denial boundaries or clear spill certification.
- Formatting and whitespace checks pass.

Implementation archive: [manifest](benchmarks/2026-09-09-contiguous-copy/manifest.json), six files and503 source inputs verified.

No dependency change. Release24874 terminal0 completed in8m51s; frozen binary
`acdb8c51147c8f5ff833b72cb843f8dbf843dcd431a1359edae5c2bb461aefcd`
retains all503 verified source inputs.
The candidate also includes the separately tested runtime-filter lineage and
payload-ownership repairs. Q12/Q13 diagnostics below address remaining performance questions; full provider,
residency, concurrency and resource acceptance remain.

## Next systemic boundary to evaluate

The current planner constructs `MemoryTableExec` for cached or in-budget provider
batches (`physical/planner.rs`, scan routing). Neither that operator nor
`NativeStreamingScanExec` exposes a runtime-filter target. FilterExec also declines
by default. This corroborates the earlier Q12 probe-row discrepancy; it does not
yet measure the gain from adding those capabilities.

A follow-up must establish a shared, admitted batch-membership boundary before
widening provider targets. Preserve exact ordinal lineage and full Int64 equality;
missing/declined payloads mean pass-through. Selection masks, row indices and
copied outputs must own reservations. Apply deletion vectors first. Do not push
through computed projections, preserved outer sides, or arbitrary filters whose
evaluation can be volatile or fallible. Prefer filtering at an already evaluated
boundary if moving the predicate would change evaluation behavior.

Independent tests must cover duplicate keys, key/value NULLs in dictionaries,
multiple independently published AND filters, delayed publication, repeated query
execution, self-joins/aliases over shared immutable batches, delete vectors,
projection order, outer preserved rows, cancellation, and optional admission
refusal. Exercise ordinary and admitted consumers. First verify generic query
plans and row counts, then paired performance across providers and protected
queries. No query-name switch or benchmark-dependent estimate belongs in routing.

## Frozen diagnostic results

Primary88718 and supplemental51590 both terminal0. All24 completed outputs match
complete typed DuckDB oracles. Dataset, native/IPC provider inputs, binary and503
source hashes verify before and after both runs. The supplemental run adds
aggregate/input-frontier instrumentation. All six Q13 supplemental frontiers have
16 partitions,16 slots and admitted buffers. Resident Q12 retains916 build batches
for310,803 selected build rows.

| Mode | Primary Q12 ms | Supplemental Q12 ms | Primary Q13 ms | Supplemental Q13 ms |
|---|---|---|---|---|
| Raw Parquet |1048.213 /1052.193|1046.027 /1041.419|2606.027 /2586.930|2576.236 /2726.840|
| Native |491.994 /491.685|496.374 /494.578|2367.911 /2346.870|2331.594 /2385.396|
| Preloaded CPU32GiB |654.294 /221.466|598.437 /178.396|2852.159 /2297.657|2674.326 /2239.133|

Two entries are consecutive requests in one process, not independent sessions.
The previous dbca414a resident Q12 second requests were311.833 /331.585ms;
acdb8c51 is221.466 /178.396ms. The older collected-profile second request was
about186ms. These diagnostic samples suggest recovery
of the resident copy regression, but variability and differing source changes
prevent a causal percentage claim or non-regression certification. Q13 retains
the earlier bounded-pipeline improvement; its paired acceptance is still open.

Conditions: canonical SF10,16 threads/CPUs0–15; raw/native4GiB query and12GiB
process, preloaded CPU32GiB query and48GiB process (a separately labelled capacity
experiment). Query times exclude preparation and serialization. The180-second
diagnostic watchdog is not the10×DuckDB acceptance ceiling. No GPU execution,
Iceberg or Lance provider coverage is implied by the enabled build features.

Both64GiB scopes have swap disabled and zero max/OOM/kill events. Peaks are
19,545,763,840 bytes primary and15,190,917,120 bytes supplemental. These scope
figures do not prove query-wide admission. The immutable diagnostic archive has
261 verified files and503 source inputs:
[manifest](benchmarks/2026-09-09-contiguous-copy-diagnostics/manifest.json).
All jobs are terminal. Next run balanced protected comparisons with fresh DuckDB
ceilings before widening provider membership or claiming replacement readiness.
