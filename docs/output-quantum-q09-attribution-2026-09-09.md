# Current Q9 pipeline attribution — 2026-09-09

Current frozen01bb077a reproduces serial input polling in all three tested CPU
modes. Diagnostic67776 exits0; three outputs pass independent typed canonical
oracles. All180 join traces complete without unwinding. No source change was
made during residency or attribution;511 source inputs verify.

| Mode | Query ms | Declared partitions / active slots | Aggregate ingestion ms | Final output ms | Process CPU / wall |
|---|---:|---:|---:|---:|---:|
| Raw Parquet | 5947.815 | 16 / 1 | 222.143 | 0.033 | 128% |
| Native | 2737.775 | 8 / 1 | 221.639 | 0.040 | 137% |
| Resident CPU capacity | 2829.978 | 16 / 1 | 224.004 | 0.033 | 111% |

Every frontier reports `operator:Project`, `copy_bound:null`,
`admitted_buffers:false`. Each aggregate consumes3,261,613rows in916batches,
emits175rows and spills zero bytes. The raw aggregate's own evaluation takes
1.051ms; this excludes upstream computed projection evaluation. Its worker
routing takes154.602ms and processing66.582ms. Small final-output cost rules out
output construction as the dominant cost of this diagnostic.

Raw cumulative join build/candidate/gather intervals are438.550/848.669/91.132ms.
These may overlap; huge input/downstream/yield sums cannot be treated as additive
wall or exclusive CPU. Low process CPU utilization plus the one-slot frontier
supports a serialized-input bottleneck, but does not assign every remaining
millisecond to projection, decoding or joins.

Default disjoint ownership and GPU-disabled execution are explicit. Each case
runs one query in a fresh process,16threads on CPUs0–15. Raw/native use4GiB query/
12GiB process budgets; resident CPU uses32/48GiB and includes preload in process
CPU/wall accounting. Query timing excludes that preload. The180-second diagnostic
watchdog does not clear the10× DuckDB acceptance ceiling. Scope peak is
19,690,856,448bytes under48GiB, swap0, zero OOM/max events. This is attribution,
not a before/after speedup or a statistically bounded regression study.

The historical contiguous-copy diagnostic reported16 input partitions but one
aggregate-frontier slot, `copy_bound:null` and `admitted_buffers:false` for the
upstream Project. Current source still declines prepared projection capability
unless every expression is a column/alias. `InputFrontier::new` selects serial
polling when neither admitted buffers nor a reservable copy bound is available.
The canonical Q9 plan includes EXTRACT and decimal arithmetic in a projection
below aggregation. The fresh trace above confirms that this remains the active route.

The repair must be general. First reproduce the lost parallel capability with a
multi-partition computed projection, independently counted pulls/evaluations and
bounded retained output. Cover duplicates, NULLs, empty partitions, decimal
scale, dictionaries/slices, source errors and memory refusal. Compare ordinary
and specialized paths against an independent typed oracle. Then establish a
compositional input ownership contract through computed projection and inner
joins, or evaluate a semantics-preserving fused pipeline that retains the same
admission contract. Do not infer that fixed-width final output accounts for
unbounded decoder/expression/join scratch. No arbitrary slot increase, query-ID
branch or replay of expressions after partial mutation is acceptable.

Join phase timings overlap nested streams and partitions; do not sum wait phases
into exclusive query time. Process CPU time includes startup and serialization.
The useful question is whether synchronous source work remains serialized, and
which ownership boundary prevents independent admitted pulls.

The copied-output interface additionally promises **pool-independent future
pulls** (`physical/plan.rs`). Expression evaluation enters the query memory pool;
a fixed-width output-size proof alone therefore cannot authorize this interface.
A safe repair must use admitted-buffer ownership transitively, or establish an
explicitly separate bounded allocation domain. Inner joins currently expose
admitted output only through the eligible outer-probe path; computed projections
cannot simply forward an unproven child capability. Unknown prepared streams
must retain their original lifecycle and never be reconstructed/replayed.

Reproduction: `run_output_quantum_q09_profile.py` in the
[archive](benchmarks/2026-09-09-output-quantum-q09-diagnostics/manifest.json), through
the capped wrapper with repositoryTMPDIR, PYTHONPATH=scripts,48GiB, one build job,
`taskset -c 0-15` and `.scratch/venv-lance/bin/python`. Use a fresh output root.
The archive includes raw traces, plans, responses, independent oracles, source
hashes, resource records, frozen driver and versioned harness.
