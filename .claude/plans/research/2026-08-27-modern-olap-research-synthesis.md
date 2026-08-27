# Modern OLAP/Query-Engine Research vs. This Engine's Design — Synthesis (2026-08-27)

Produced from six parallel research passes: five external-literature sweeps (execution
models, columnar storage, query optimization, GPU acceleration, distributed/spill-safe
execution — 60+ primary sources total across academic papers and production-system
engineering writeups) and one from-scratch, code-verified architecture map of this
engine (not taken from `CLAUDE.md`'s own prose on faith — checked against source, with
every place `CLAUDE.md` itself had drifted flagged explicitly). Full per-thread detail
lives in this session's own transcript; this document is the distilled, actionable
synthesis: what the literature agrees and disagrees on, how this engine's actual design
compares, and a ranked list of gaps worth closing.

Read this alongside `CLAUDE.md` (current engine state) and `.claude/prds/native-tables.md`
(the most recently completed major program) — this document does not repeat what's
already correctly documented there, only what's new: research validation, contradiction,
or gap.

---

## 1. Cross-research meta-synthesis (what the five research threads agree/disagree on, independent of this engine)

**Strongest cross-thread consensus, ranked by how many independent threads/sources land
on it:**

1. **Runtime/adaptive re-optimization is the single most production-validated technique
   found across the entire research corpus.** Independently confirmed by the optimizer
   thread (Spark AQE, Snowflake Optima/Adaptive-Aggregation-Placement, generic AQP
   survey — three unrelated production systems, no cross-citation) and echoed by the
   distributed thread (DuckDB's own "dynamically managing memory of concurrent
   operators" work, VLDB'25). No technique in any thread has stronger, more repeated,
   more production-grounded evidence.
2. **Cost-gated, selective application beats "always do X" almost everywhere X is an
   expensive optimization.** This pattern repeats across three unrelated
   threads/domains: GPU offload (HyPE 2013 → "Risky Gate" 2026, cost-gate beats
   always-GPU and static shape-gating both), query optimization (Bao-style *steering*
   of an existing optimizer beats end-to-end learned replacement), and adaptive
   execution generally (Wolf et al.'s robustness-aware plan selection — prefer the
   plan that degrades gracefully, not necessarily the "optimal" one). Treat this as a
   general design principle, not just a GPU-specific one.
3. **Fine-grained, out-of-band statistics (min/max at sub-file granularity, stored near
   the footer/metadata so non-selective scans don't pay for them) is the single
   highest-confidence, lowest-risk storage technique.** 25+ year lineage (SMA 1998 →
   zone maps → BRIN → Snowflake micro-partitions → Parquet page index), directly
   measured ~100x scan reduction moving from row-group to page granularity (CERN).
4. **Learned/ML components remain, across every thread that touched them, weaker in
   production evidence than their paper volume suggests.** Learned query optimizers
   (Neo, Balsa): real practitioners cite training overhead, tail-latency risk, and
   explainability as still-unresolved blockers a decade later; the one real production
   deployment found (Microsoft QQ-Advisor) is a narrow *steering* layer over an
   unchanged classical optimizer, not a replacement. No thread found a production
   system using ML as its *primary* planner/estimator at scale.
5. **PCIe/network transfer cost dominates almost every "should we offload this"
   decision** — repeated verbatim across GPU (PCIe bandwidth ~16GB/s vs. GPU/CPU
   memory bandwidth) and distributed (network shuffle cost is *the* thing every
   disaggregated-storage design fights with a caching/tiering layer) threads. This is
   the same underlying physics showing up in two unrelated domains.

**Real, still-unresolved contradictions worth knowing about (not settling — the field
hasn't settled them either):**

- **Does join order still matter once you have robust runtime execution?** (optimizer
  thread) — Leis et al. say yes (bad order still costs real time even under runtime
  correction); "Debunking the Myth of Join Ordering" (2025) says runtime correction
  makes upfront order less consequential. Same evidence (Snowflake's 180x
  runtime-corrected query) is cited by both camps.
- **Explicit hand-managed CPU↔GPU/cache tiering vs. unified-memory/zero-copy** (GPU +
  storage threads) — explicit tiering (HeavyDB, Lancelot) is the proven approach on
  commodity PCIe hardware; unified-memory optimism is real but gated on NVLink-C2C
  superchip hardware most deployments won't have. Not applicable to this engine's
  hardware target either way — noted for completeness.
- **Lightweight bespoke compression vs. general-purpose codecs** (storage thread) — the
  "lightweight always wins" narrative from BtrBlocks/Vortex marketing is contradicted
  by DuckDB's own measured numbers (their lightweight-only encoding lost on raw ratio
  to Parquet+zstd on a real dataset). The real win of lightweight encoding is decode
  speed and random access, not smallest file size — a hybrid, not a strict either/or.
- **Late materialization: "always defer" is not the settled position it used to be.**
  Classic column-store doctrine (Abadi 2007) says defer tuple construction as long as
  possible. A 2025 VLDB paper found that once an engine is already vectorized/columnar
  (as virtually every modern engine, including this one, is), pure late materialization
  is no longer uniformly best — *selective*, per-attribute materialization timing beats
  both extremes by 8.9-14.7% on JOB. This is a dated reversal worth knowing, low
  priority to act on (single paper, execution-engine-level change).

---

## 2. Engine-vs-research comparison, by subsystem

For each subsystem: what the research validates about the current design (don't touch),
and what's a real, evidenced gap.

### 2.1 Execution model — mostly validated, one major absence

**Validated, not a gap:** vectorized `RecordBatch`-at-a-time processing, pull-based
async streams, morsel-style rayon fan-out, and — notably — the engine's own decision to
*reject* full query compilation/JIT is directly backed by the same VLDB'18 paper
(Kersten et al.) the execution-model research thread surfaced independently, plus the
engine measured this itself (`compiled_expr.rs`'s own benchmark, 0.638ms vs 0.544ms).
Radix-partitioned hash join was proposed, benchmarked, and correctly refuted on this
hardware's cache topology — an example of the "measure, don't assume" discipline the
research also validates as correct practice.

**Real gap — highest priority in this whole synthesis:** the engine's plan is **fully
fixed before execution starts**, with zero adaptive/runtime re-optimization anywhere
(confirmed by a repo-wide grep: zero matches for "adaptive"/"re-optimiz"). This is the
one technique the optimizer research thread ranks #1 by evidence strength, validated in
production by three independent, competing, non-cross-citing systems (Spark, Snowflake,
generically by the AQP survey), with concrete, large numbers (Spark: up to 8x on
TPC-DS; Snowflake: up to 180-500x on specific production query classes). The engine has
*none* of this.

### 2.2 Storage — one "wire up what already exists" quick win, one deliberate-and-defensible tradeoff, a few real additions

**Real gap, high confidence, surprisingly cheap:** the native table format already
computes per-segment `ColumnStats` (min/max/null-count) at write time and already feeds
them into the cost-based optimizer — but **never uses them for scan-time skipping**.
`NativeTable::scan()` decodes every active segment unconditionally; `scan_with_filter`
isn't even overridden, it's the trait default that just calls `scan()`. This is exactly
the #1-ranked storage technique from the research (fine-grained stats-based pruning) —
except the engine already built half of it (the stats) and never wired the other half
(the skip). `CLAUDE.md` itself already attributes a real, measured regression (Q4/Q12/Q13
join-spill pressure) to this specific gap. This is the single cheapest, best-evidenced,
highest-confidence item in this entire synthesis.

**Defensible, not a gap:** native tables ship zero compression by deliberate design
(uncompressed Arrow IPC, prioritizing mmap zero-copy reads). The storage research
thread's own contradiction (lightweight encodings don't clearly beat general compression
on ratio, they win on decode speed/random access) means "stay uncompressed for max
decode speed" is a coherent, evidence-consistent position, not simply wrong — but it
leaves real bytes on the table for large tables/cold storage.

**Real, moderate-effort additions worth considering:**
- **FSST for string columns** specifically preserves per-string random access and
  near-memory-bandwidth decode *while still compressing* — i.e., it doesn't force the
  zero-copy-random-access tradeoff the engine is currently protecting by staying
  uncompressed. This is the one storage technique that could add real compression
  without abandoning the native table format's own design philosophy.
- **Bloom filters or an equivalent low-cardinality/equality index** as a complement to
  min/max stats — min/max structurally cannot prune "value absent but in-range"
  equality predicates (a nation column ranging Argentina..Zimbabwe can't be pruned on
  `nation = 'Singapore'`); CERN's measured ~7.6x I/O reduction for exactly this case is
  the strongest single number in the storage research.
- Iceberg has no partition pruning (manifest-entry bounds unused before opening files);
  Lance has essentially no statistics at all (real min/max only via a full scan). Both
  secondary formats, lower priority than the native table's own scan-pruning gap.

### 2.3 Query optimization — genuinely strong foundation, same adaptive-execution gap as §2.1

**Validated, stronger than expected:** the engine already does real, non-trivial
cost-based join reordering — a genuine DPsize dynamic-programming enumerator for 2-12
relations driven by real footer statistics, closely matching the research's #2-ranked
technique (DP enumeration up to a size threshold, heuristic fallback beyond). This was
a genuinely pleasant surprise relative to `CLAUDE.md`'s own stale "three simple rules"
self-description — the actual optimizer has grown to 14-15 real rules including
subquery decorrelation, CSE, and functional-dependency-based group-key reduction.

**Confirmed non-gap:** zero ML/learned components anywhere. Per the research (§1, point
4), this is the evidence-backed correct position for now, not a missing feature.

**Real gaps:**
- Same adaptive-re-optimization absence as §2.1 — this is really one gap spanning both
  the optimizer and execution-model sections, not two separate ones.
- Without statistics (or above 12 relations), join ordering falls back to **hardcoded
  TPC-H table-name string matching** (`is_lineitem → score -= 5000`) — overfit to the
  benchmark this engine was built against, a real weakness on any non-TPC-H workload
  without statistics.
- `src/optimizer/cost.rs`'s `CostEstimator` is fully-implemented-looking dead code —
  never called from the actual rule pipeline. Misleading to future readers; worth
  either wiring it in or removing it, independent of the adaptive-execution question.

### 2.4 GPU acceleration — already close to where the mature literature converged

**Validated, and this is worth stating plainly:** the GPU research thread's own
conclusion, read cold, describes this engine's *existing* scope almost exactly —
"compute-bound reductions over resident data... cost/shape-gated... disabled where
correctness is at risk... not an underdeveloped MVP... structurally close to where the
most mature systems (HeavyDB, HyPE-descendants, Lancelot) converged after years of
iteration." The recent VRAM budget/LRU-eviction/failure-isolation hardening
(2026-08-26) is itself unusually well-aligned with the literature's residency-management
emphasis (Lancelot, Vortex) over raw-kernel-throughput chasing (Crystal).

**Real, well-evidenced next steps (not urgent, but genuinely validated):**
- **Formalize the offload decision as an explicit cost gate** (Risky Gate/HyPE-style:
  input size × transfer bytes × kernel cost vs. predicted CPU cost) rather than the
  current static shape recognizer. Would let genuinely favorable cases the static rule
  currently excludes (e.g. a small-group-count GROUP BY over resident data) get
  captured without reopening the whole GROUP BY question generally.
- **Test bounded low-cardinality GROUP BY on already-resident data specifically** — the
  engine currently treats all GROUP BY as flat/no-win, but the research (GFTR,
  TUM heterogeneous-aggregation) suggests *narrow*-cardinality GROUP BY on resident
  data behaves more like the ungrouped case than the general one. Directly testable
  against the existing resident-column path with minimal new machinery; the current
  "flat" finding may be leaving a real win on the table for the narrow case.
- **A narrow keys-only join-probe or Top-K primitive** — the research's strongest single
  quantitative win in this area (Risky Gate: 16.2x transfer reduction from transferring
  keys-only, deferring full-row materialization) is directly reusable without
  committing to full GPU joins, which the literature does *not* support (every
  cost-conscious production system hedges away from full join offload; GFTR's headline
  join numbers are GPU-vs-GPU only, never against a good CPU baseline).
- Explicitly **not** recommended by the research: full GPU joins, GPU scan/decode. The
  engine's current restraint here is validated, not a gap to close.

### 2.5 Distributed execution — the missing piece is exactly what the engine's own docs already name

**Validated:** the scatter/gather model's pushdown (real column projection, predicate
`RowFilter`, row-group stats pruning *within* a shard read) is a genuine, non-trivial
implementation, not naive row-slicing — reasonably aligned with the "pushdown near
storage" principle the distributed research validates, given this isn't attempting true
storage-compute disaggregation (so the "you need a caching layer" finding from
Snowflake/StarRocks/Presto doesn't directly apply — this engine's shards already own
their data locally, closer to shared-nothing than disaggregated).

**Real gap, already self-identified, now research-validated as high-value:** no
shuffle/cross-node partitioned join exists (confirmed absent — the engine's own code
comments call this "M3, not yet started"). The distributed research thread's #1-ranked
technique — typed, explicit exchange operators (broadcast / shuffle-by-key / gather)
with cost-based selection between them — is precisely this missing piece, with
concrete production numbers from ClickHouse Cloud (2-7x wins, but also a documented
regression case from choosing shuffle over broadcast under a purely rule-based
planner — worth building the cost-based selection in from the start, not bolting it on
after a rule-based version ships).

### 2.6 Memory safety / spill correctness — the highest-stakes finding in this whole synthesis

The engine has a real, currently open, low-rate (~0.34% pooled), root-cause-unconfirmed
silent wrong-answer bug in `SpillableHashJoinExec`'s spill path (`spill-join-correctness`
epic, closed 2026-08-25 with the bug still open — see `CLAUDE.md`). The distributed/
spill research thread surfaced something directly relevant that the prior epic did not
have when it closed:

**A near-exact production analog exists and suggests a concrete, untested hypothesis.**
Trino's merged bug fix (PR #25892) was caused by exactly this shape: spilling used one
hash-generator implementation, unspilling used a *different* one, causing values to be
grouped incorrectly on reload — silently wrong results, not a crash, in the same
operator class (spilling hash aggregation/join). The prior epic's own "best remaining
hypotheses" list did not include this specific mechanism (it focused on
non-idempotent-re-execution, which was disproven, and a vaguer "mid-computation
failure" theory). **Checking whether any derived/cached value used during
build/probe — the join-key hash, partition-routing hash, or any dictionary-encoding
state — is guaranteed identical between the in-memory-path computation and the
recomputed-after-reading-from-a-spill-file path is now a concrete, well-motivated next
thing to test**, not a repeat of ground the prior epic already covered.

**A second, separate, real gap, also research-motivated:** the spill path's own
documented hole — collect the *entire* build side into memory first, *then* decide
whether to spill — means an oversized build side can OOM before the spill decision
ever runs. Photon's (Databricks, SIGMOD'22) two-phase reservation discipline
(resolve all spilling in a reservation phase; a subsequent allocation phase is
guaranteed spill-free) is a structural pattern aimed exactly at this class of gap, and
also independently maps to a *second* Trino bug found in the research (#7454: a race
where a partition got spilled into existence *after* a consumer object had already
snapshotted the partition set) — the same "state changes mid-operation" root shape
shows up in two different real bugs in a comparable system.

**A concrete, cheaper-than-full-formal-methods testing upgrade:** the strongest
correctness-hardening technique found in the whole research corpus is deterministic
simulation testing (FoundationDB-style) — likely too large an investment to lead with.
A cheaper, directly actionable version exists though: property-based/fault-injection
testing specifically at the spill/unspill boundary — inject a forced spill at every
possible point during build/probe, assert row-count/checksum invariants against a
non-spilling reference execution of the identical query. This is exactly the "downsized
synthetic repro to run hundreds of trials cheaply" the prior epic named as its own
highest-leverage unattempted next step, now with a concrete methodology (differential/
fault-injection testing, SQLancer/NoREC-style) to build it around rather than ad hoc
trial repetition.

**Also still open, lower-stakes, already found and named:** three sibling bugs from the
prior epic's own characterization sweep remain unfixed — a spill-directory collision
for co-located `serve` processes (fails loudly, real availability risk), `LIMIT` not
enforced under spill for `ORDER BY...LIMIT` queries (only under an artificial extreme
memory-limit sweep), and a sort-spill run-file-not-found crash (same artificial
condition). None is the headline correctness bug, but all are real, named, and cheap
enough to fold into the same hardening effort.

---

## 3. Ranked gap list (evidence strength × expected value × the honest note on effort)

| # | Gap | Evidence strength | Effort | Notes |
|---|---|---|---|---|
| 1 | Spill-path correctness: test the hash/derived-state-consistency hypothesis (Trino-analog); fix the collect-fully-then-decide OOM hole (Photon two-phase pattern); build a fault-injection/differential testing harness at the spill/unspill boundary; fix the 3 known sibling bugs | Highest — direct production analog + open P0 correctness bug | M-L | Correctness, not performance. Should not wait behind the others. |
| 2 | Native table scan-level pruning using already-computed segment stats | Highest — 25-year technique lineage, engine already has half the mechanism built | S-M | Cheapest, most obviously-scoped win in this whole list. |
| 3 | Adaptive/runtime re-optimization (start narrow: skew/cardinality-driven join-strategy or join-order correction at a materialization boundary, not a full AQE rewrite) | Highest by production-count, but the engine has zero existing scaffolding for it | L-XL | Largest architectural lift in this list; needs its own careful, narrow-first scoping — do not attempt "build general AQE" as one epic. |
| 4 | Shuffle / cross-node partitioned join (M3) | High — validated technique, but this is a pre-existing, self-identified, large gap, not a new finding | XL | Already named by the engine's own docs; largest single item by raw size. |
| 5 | FSST for native-table string columns + a bloom-filter-class index for equality pruning | Medium-high — well-evidenced techniques, moderate implementation lift, preserves the format's own zero-copy philosophy | M | Real win, does not require abandoning the current no-compression design stance. |
| 6 | GPU: cost-gate the offload decision; test bounded low-cardinality GROUP BY; a narrow keys-only join-probe/Top-K primitive | Medium-high evidence, narrow scope, engine already close to state-of-the-art here | S-M each | Lowest risk of this list — the engine's current GPU design is already well-aligned; these are incremental, not corrective. |
| 7 | Remove or wire up the dead `CostEstimator` in `cost.rs`; replace the hardcoded TPC-H-table-name join-order fallback with a general statistics-free heuristic | Internal-consistency finding, not directly from the research | S | Cheap, low-risk cleanup; bundle into whichever epic touches the optimizer next rather than standalone. |

---

## 4. What this synthesis recommends NOT building (evidence-backed restraint, not oversight)

- **End-to-end learned/ML query optimizer or cardinality estimator.** Weakest
  production evidence in the entire corpus relative to paper volume. If any ML
  component is ever justified, the evidence points at a narrow *steering/hinting*
  layer over the existing cost-based optimizer (Bao/QQ-Advisor shape), not a
  replacement — and even that should wait until items 1-4 above are further along.
- **Full GPU joins or GPU scan/decode.** Every cost-conscious production GPU-DB paper
  across a 13-year span hedges away from this; the literature does not support it as a
  clear win, and it would compound the float-determinism/distributed-correctness
  tension the engine already manages carefully.
- **True storage-compute disaggregation for the distributed layer.** The engine's
  current shared-nothing-shard model is closer to what StarRocks/Doris/Presto actually
  run in production (disaggregation + a mandatory caching layer bolted back on) than to
  the "pure" Snowflake academic reference architecture — pursuing real disaggregation
  would mean *adding* a caching layer to solve a problem the current architecture
  doesn't have. Not a gap.
- **A general-purpose compression codec swap (zstd/gzip) for native tables.** The
  research's own contradiction here (DuckDB's own numbers show general compression can
  win on raw ratio) means this isn't wrong exactly, but it directly fights the format's
  mmap-zero-copy design goal — FSST (item 5) gets real compression without that
  conflict, so it's the better first move if compression is wanted at all.
- **Deterministic simulation testing (FoundationDB-style) as a first move.** The
  strongest correctness technique found, but a huge investment (FoundationDB built the
  simulator *before* the database). The cheaper, directly-actionable version (targeted
  fault-injection at the spill boundary, item 1) captures most of the value for a
  fraction of the cost — revisit full simulation testing only if item 1's cheaper
  version doesn't converge.
