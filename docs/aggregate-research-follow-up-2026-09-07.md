# Aggregate execution research follow-up — 2026-09-07

This follow-up connects the [frozen649 runtime observations](shared-aggregate-finalization-profile-2026-09-07.md)
to primary engineering and scientific sources. It adds experiment candidates,
not new engine capabilities or a performance claim. Source versions and publication
dates below are deliberate; a search crawler date is not a publication date.

## Robustness is part of the operator design

Kuiper, Boncz and Mühleisen's ICDE2024 paper combines memory management for
persistent and temporary data with a page layout that remains spillable while
serving in-memory operators. Its objective is graceful behavior beyond available
RAM rather than an abrupt switch to an unrelated slow path.
[Robust External Hash Aggregation in the Solid State Age](https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf).

Project inference: an inline accumulator optimization should fit the existing
ownership/spill contract, and a longer-term state store should make admission,
resizing, spilling and reclamation explicit. Merely reducing the bytes inside a
hash entry does not account for queued batches, rehash scratch or results. The
current two successful cap cases do not establish that broader contract.

## Partitioning is a choice to measure

Xue and Marcus's May2025 arXivv1 study separates group-key ticket assignment from
aggregate updates. Purpose-built concurrent hash tables can compete with
partitioned aggregation in their experiments. Atomic and thread-local updates
have different skew/cardinality tradeoffs; replicated thread-local state can
lose scalability at high cardinality. The authors explicitly identify efficient
spilling of non-partitioned aggregation as unclear. Their results do not establish
one universal winner.
[Global Hash Tables Strike Back!](https://arxiv.org/html/2505.04153v1).

Project inference: compare partition/scatter, local preaggregation and a possible
shared ticket/state design only after measuring which cost dominates here. A
shared-table prototype must handle exact decimal state, publication ordering,
resize ownership, cancellation and a correct memory-pressure path. It must not
replace the default with a memory-only table or assume an arbitrary concurrent
map will provide the paper's behavior. The first bounded experiment remains the
prepared finalization phase trace, followed by a measured representation/output
change if warranted.

## Runtime adaptation can avoid unnecessary intermediate work

Groß, ten Wolde and Boncz's CIDR2025 work uses linear-chained join structures for
factorized aggregation and worst-case-optimal processing, with runtime selection
based on build-time observations. The paper proposes both learned decisions and
more explainable heuristics.
[Adaptive Factorization Using Linear-Chained Hash Tables](https://mail.vldb.org/cidrdb/2025/adaptive-factorization-using-linear-chained-hash-tables.html).

Project inference: this is a later option for the nested join/aggregate shapes
where intermediate expansion is measured to dominate. Runtime statistics may
choose among correct implementations; they cannot establish uniqueness or erase
duplicates. Q18's large finalization interval alone does not prove factorization
is the right fix, and Q13's smaller finalization requires different attribution.

## Experiments that can distinguish the alternatives

Use an aggregate workload family in addition to unchanged canonical queries:

- Sweep distinct/input ratio from tiny domains to all-unique keys; test uniform,
  heavy-hitter and adversarial partition distributions.
- Vary input ordering and file/row-group fragmentation independently. Nearly
  disjoint observed groups are not a certified disjointness property.
- Include signed/unsigned integers, exact decimals, NULL keys/inputs, strings and
  dictionaries, and one versus several aggregate states per group.
- Vary HAVING selectivity across zero/some/all output rows. Separate state update,
  shard/merge, output construction, filtering and destruction measurements.
- Vary threads, query memory and concurrent queries. Include successful spill,
  named refusal, cancellation and forced allocation/resize failure.

Preserve every sample, typed independent oracle, active physical path, input/output
cardinalities and reservation/spill evidence. Run instrumented attribution apart
from latency. Protect raw Parquet, native, Iceberg and Lance, with decoded IPC and
GPU residency reported separately. A microbenchmark gain must survive complete
query timing, realistic encodings and resource gates before adoption.

These are design options for the existing epic, not permission to restart it or
replace its public-workload/holdout/leadership criteria. Current production source
remains frozen649; the balanced provider screen is still running.
