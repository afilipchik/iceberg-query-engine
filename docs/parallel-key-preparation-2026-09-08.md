# Scoped parallel key preparation — September 8, 2026

PreparedKeys now owns disjoint contiguous key/hash chunks, targeting the serial
preparation span in the complete Q10 diagnostic. The completed paired comparison
shows a14.29% Q10 improvement against serial preparation. Only `physical/morsel_agg/prepared_keys.rs` differs among
the493 inputs of frozen1a0ece71.

Scheduling chooses up to four chunks, bounded by Rayon threads and one worker per
1024 input rows. Smaller batches stay serial. This is a provisional scheduling
policy. Chunk metadata, bytes/offsets, hashes and encoding scratch share the same
child pool capped at one eighth of available query memory; actual admissions and
growth overlap remain enforced. Workers use the unchanged canonical encoder.
Original row indices map directly to chunk/local offsets without concatenation.
Single-chunk lookup avoids division; incomplete metadata returns an error.

Scoped parallel iteration finishes started work before returning, including on
error. Partial owners drop before admission refusal selects ordinary routing;
no aggregate state has changed. Other errors propagate. The evaluated batch,
layout identity, full key equality, canonical owners, row ordering and exact
spill cursor remain unchanged. No source or volatile expression is replayed.

The new four-chunk regression checks8193 rows against independent Int64/NULL key
bytes across all boundaries, then forces child-budget refusal and asserts lease
cleanup. Existing encoding/identity/decimal selection/spill tests remain. The
initial new fixture incorrectly requested an unsupported empty aggregate layout;
it failed before preparation and was corrected to COUNT. Preserve that failed log.

Focused final gate:128pass,1ignored component benchmark,877filtered,2.60s. Broad
feature gate88045:995library pass/11ignored,16.78s;14 selected expression, transition,
volatile-key and error tests pass. Native remains9pass/1fail with the unchanged
383984-byte join request under262144bytes. Formatting and whitespace checks pass.

Release16406 completed in8m44s, lance/gpu features,48GiB and one build job.
Frozen binary e6a60347e771431dd882b0c9beb08d739b4a675524e85c89756a540f206ae7c4;
all493 inputs unchanged. Paired6123 is terminal1. Logs/provenance:
`.scratch/parallel-aggregate-input/parallel-key-*`. After verification, compare
against1a0ece71 using strict canonical and memory-input paired gates, then provider
and resource validation. Unit tests do not establish performance acceptance.

Completed test evidence is immutable under
`docs/benchmarks/2026-09-08-parallel-key-tests/`: eight files verified, including
before/after module snapshots and the exact patch. All493 build inputs match;
the sole changed input against1a0ece71 is prepared_keys.rs. The archive records
the release as still compiling at archival time. Canonical and custom-memory
paired drivers launched after the verified freeze; failed reference calibration
stops further actual requests while preserving not_run records.

Custom-memory comparison completed: the candidate passes Q1 warmup and six samples
in three blocks (18 measured samples); its fourth warmup is late. Serial control
fails all four warmups, so no complete paired Q1 ratio exists. All82 completed
engine outputs validate,77 meet gates, five warmups are late and30 samples are
not_run. Q6 ratio1.005722 (95%0.986009–1.031110) includes no change. This is custom
float smoke, not canonical SF10 or GPU execution.

## Completed canonical comparison

Before1a0ece71, aftere6a60347; eight canonical SF10 queries, four fresh-process
blocks, six measured pairs plus gated warmup each,16 threads,4GiB query/12GiB
process and a shared48GiB scope. Fresh DuckDB calibration/typed references and
balanced startup/execution orders are retained. Ratios are geometric means of
complete block median ratios;95% intervals resample whole paired blocks.

|Query|After / before|95% interval|
|---|---:|---:|
|Q1|0.99576|0.98851–1.00213|
|Q2|1.01655|0.91137–1.13388|
|Q5|1.01686|0.99874–1.03532|
|Q10|0.85708|0.85467–0.85949|
|Q19|0.97857|0.96214–0.99527|
|Q20|0.98964|0.97520–0.99980|

Q10 improves14.29% (interval14.05–14.53%). Q19 improves2.14% and Q20 about1.04%,
the latter with an interval barely excluding no change. Q1/Q2/Q5 intervals include
no change. Q2's upper bound exceeds1.10, so the protected regression question is
unresolved despite no confirmed10% slowdown. Four blocks are limited evidence,
not full multi-session certification. Run a prespecified wider Q2 comparison.

All347 completed canonical outputs validate;339 meet the time gate. Q12 has eight
late completed requests; Q13 has eight warmup timeouts, across both binaries.
There are93 not_run slots. Neither query has a valid complete-block ratio.
Scope peak2,802,294,784bytes; max/oom/oom_kill all zero. This scope also contains
the earlier custom-memory run, so its snapshots are cumulative, not query RSS.

Across the two distinct workloads,429 completed engine outputs validate,416 meet
gates,13 are late, eight time out and123 are not_run. These are execution counts,
not a combined performance score. Immutable evidence:
`docs/benchmarks/2026-09-08-parallel-key-pairs/`. Full provider/residency/resource
acceptance and the broader milestone remain open; this is a measured improvement
in shared code, not DuckDB leadership.

## Prespecified Q2 follow-up

Eight new process blocks,12 measured pairs/block, unchanged frozen binaries, data,
threads and budgets. All208 engine outputs (192 measured plus16 warmups) passed
typed validation and time gates; exit0. After/before ratio0.980582, whole-block
bootstrap95%0.914852–1.049988 (20000 draws, seed20260908). This study bounds the
protected slowdown below10%; it does not establish a speedup. Retain the original
wide interval above. No repeated runs until passing were used.
Evidence: `benchmarks/2026-09-08-parallel-key-q2-followup/`.
