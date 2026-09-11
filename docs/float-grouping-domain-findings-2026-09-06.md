# Float grouping and join consistency probe

On the frozen cast-only candidate, a 65,536-row typed Float64 IPC fixture cycles
through negative zero, positive zero, positive NaN, a negative NaN payload and
NULL. Sixteen 4,096-row batches are registered with four threads. Independent
DuckDB 1.4.4 evaluates the same Arrow data and SQL.

| Query | Engine | DuckDB |
|---|---:|---:|
| Count of GROUP BY a output groups | 39,322 | 3 |
| COUNT(DISTINCT a) | 2 | 2 |
| Count after id equality join and x.a <= y.a | 26,215 | 52,429 |
| Count WHERE a = a | 26,215 | 52,429 |

Binary SHA-256:
`c754cac49166fea4f1ff910a9557f90522a73ae2df4d66ce2e8214fb9aa68eb3`.
All four requests complete; three disagree. This is a small correctness probe,
not a benchmark. The outer GROUP BY count removes any ambiguity about returned
NaN payloads or output ordering. COUNT DISTINCT's agreement does not certify
GROUP BY. The saved join plan uses a Filter above an id hash join, so that sample
is evidence about the predicate path, not the specialized join residual evaluator.

The group-count physical plan is an outer SpillableHashAggregate over a project
and an inner SpillableHashAggregate. The precise failing grouping path remains
to be attributed. Source hypotheses include raw Float64 key bits in the morsel
perfect-hash dictionaries and equality/partition ownership at aggregate boundaries.
These are hypotheses, not an established explanation for 39,322 groups. Do not
patch only zero bits and infer that the reproduced cardinality failure is fixed.

Separately, `hash_join.rs::CompiledFilter` uses ordinary Rust float comparisons,
which disagree with SQL NaN equality/ordering; that specialized path needs its own
reproducer. Physical sort's spill comparison uses partial_cmp and treats unordered
pairs as equal. These paths are outside the current predicate repair's validation.

Reproducer: `.scratch/sql-float-comparison-repair/key-domain-probe.py`.
Input, SQL, results, plans and worker/cgroup evidence:
`.scratch/float-key-control-01/`. Next run the unchanged fixture on the new
predicate release, isolate any remaining GROUP BY failure across partition counts,
empty/nonempty aggregates and ordinary/morsel/spill paths, then fix the underlying
key/partition contract with independent typed oracles.

## Candidate reproduction and draft repair

The new predicate release fixes both filter counts to 52,429, while GROUP BY still
returns 39,322 and COUNT DISTINCT returns 2. This isolates the group-count failure
from the repaired predicate evaluator. The vectorized HashAggregate directly calls
`vectorized_hash::compare_row`, whose NULL behavior is intentionally join-style.
The same helper uses Rust float equality, and hashing uses raw float bits.

Current source adds a separate group-row equivalence with NULL = NULL for keys,
keeps ordinary join NULL nonmatches, and canonicalizes zero/NaN hashes across
vectorized and morsel dictionary keys. Specialized Float64 join residuals now use
the shared SQL comparator. New regressions cover small/large inputs, multiple
batches/partitions, NULL integer/string keys, float payloads, and the hash/equality
invariant. These edits are **draft/uncompiled** while the frozen candidate's
latency runs execute. Do not attribute earlier passes or performance to them.

## Implemented and tested key contract

The repair now passes the independent 3-group reproducer for 10 and 65,536 rows,
with one/four partitions and multiple batches, both with and without aggregate
states. Nullable integer/string grouping and ordinary join NULL semantics also
pass. Vectorized keys explicitly test equal SQL values producing equal hashes,
including different NaN payloads; mismatched key arity cannot compare equal.
A direct compiled join-residual test checks all non-NULL independent float-oracle
pairs so fallback execution cannot mask a specialized-path failure.

Final default-source gate: 678 library tests, 56 selected integrations (including
13 spill regressions), and the dedicated IPC test pass: **735 selected executions**.
The historical flatten-dependent-join test remains ignored. Existing spill tests
pass but are not proof that the new float grouping fixture exercised spilling.
Logs: `.scratch/group-key-equivalence-repair/{full-contracts,ipc-dedicated}.log`.

This source also replaces row-wise optional Boolean collection in the shared
float predicate evaluator with packed values and separately handled validity.
Both changes require a new optimized release and benchmarks. No current-source
performance acceptance is claimed.

## Optimized release gate

The 612-file source snapshot
`0276aa42c545a7a30570141ffbc4db394ea3688dbd387491dc487e8f2e5c1b3f`
compiled successfully with lance+gpu features. Frozen executable SHA-256:
`e8174895539343b61796925247fedebdfef45f54caa4f133d4496e70df90aae6`.
All 89 float/date oracle queries pass (701 returned rows). The independent
65,536-row grouping/join reproducer returns 3 groups, 2 distinct non-NULL values,
52,429 filtered join rows and 52,429 self-equality rows, all matching DuckDB.
The executable sources were checked against the frozen manifest before copying.
Logs and exact artifacts: `.scratch/group-key-equivalence-repair/` under
`release-oracle/`, `key-oracle/`, `release-validation.log`, and the source/binary
metadata. Matched canonical performance screens are running; success here does
not establish performance recovery or full resource/provider certification.
