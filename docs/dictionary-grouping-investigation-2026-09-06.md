# Q1 dictionary normalization follow-up

The initial investigation below was read-only. The subsequent experiment is
implemented and accepted after matched measurements and the complete canonical
SF10 and public development gates described below.

Compared `docs/benchmarks/2026-09-06-decimal-aggregation/decimal-candidate-source.tar.gz`
with `.scratch/public-contract-final-source.tar.gz`. Preserved Q1 medians are
910.7011045 ms and 1057.7726675 ms respectively, each from 30 paired samples.
Both physical plans are `ExternalSort -> Project -> MorselAggregate`. This is a
16.15% engine-median difference across multi-change snapshots, not causal proof.

## Concrete hot-path change

`MorselAggregateExec` still requests Dictionary(Int32, Utf8) reads when every group
expression is an eligible string column not also consumed by a predicate/aggregate.
Its source constructor calls `with_dict_strings(dict_cols)` (operators/morsel_agg.rs
around 182–220). Q1 groups by returnflag/linestatus and aggregates numeric columns,
so the recorded query shape meets this eligibility test. The actual reader arrays
were not logged in the primary latency traces; runtime encoding is not directly
observed by this audit.

The decimal-only candidate's `AggregationState::process_batch` evaluates group expressions and
constructs TypedArrayAccessor directly from the resulting arrays. The final snapshot
adds `.and_then(normalize_aggregate_array)` at both group-array and aggregate-array
evaluation (physical/morsel_agg.rs around 1928/1939).

`normalize_aggregate_array` unconditionally casts every dictionary to its value type
and recursively normalizes it (around 3150–3161). For Dictionary(Int32, Utf8), this
expands strings before TypedArrayAccessor construction. The specialized DictString
accessor and combined small-dictionary tuple cache therefore cannot run for these
arrays. Processing instead uses String accessors, recomputes raw string keys and
performs perfect-index resolution per row. The decode can also allocate buffers
proportional to batch rows. This is a concrete source-level loss of an existing
specialization; its contribution to Q1 latency remains to be measured.

Correctness changes must remain: dictionary value NULLs are checked by DictString
raw_key/extract_scalar; explicit perfect-slot occupancy retains all-NULL groups;
checked integer range admission rejects overflowing packed-index domains. These
changes are in the final source and must not be reverted to recover speed.

## Minimal safe experiment

Add a private morsel-group normalization helper:

```
Dictionary(Int32, Utf8) => retain the original ArrayRef
all other types       => normalize_aggregate_array(array)
```

Use it only for `process_batch` group-array evaluation initially. Leave aggregate
input normalization and all shared HashAggregate call sites unchanged. The existing
DictString accessor supports this exact encoding and now handles both dictionary
key NULLs and selected dictionary value NULLs. Other dictionary key/value types,
LargeUtf8, temporal arrays and unsupported domains retain the checked normalization
contract. This is a provider-neutral encoding capability choice, with no query or
table name involved.

Do not simply change the shared normalizer to return all dictionaries: generic
hash aggregation relies on decoding before its scalar/key extraction. Do not
restore old NULL handling or infer occupancy from accumulator state.

## Tests and measurement

- Retain dictionary-group-versus-plain, differing codebooks, logical dictionary
  NULL/key NULL/empty string, composite dictionary+integer tuples, stride changes,
  NULL-only groups, integer extrema and forced generic fallback regressions.
- Existing dictionary tests can pass after unconditional decode, so they do not
  establish that the optimized representation is still reached. Add a focused
  preservation check for Dictionary(Int32, Utf8) group arrays, plus exact output
  comparisons against the forced generic route. Test other key widths/value types
  still use supported normalization or explicit refusal.
- Run `aggregate_encoding_contract`, `dict_accessor_tests`, systemic numeric,
  partition and spill gates. Protect temporal/unsigned/decimal behavior.
- Use same-source/features except this helper change for paired Q1 measurements.
  Separately collect existing opt-in GROUP and PROCESS worker-time counters:
  dictionary expansion belongs to group evaluation; losing the tuple cache belongs
  to processing. Compare group-array encodings/allocated bytes outside primary timing
  if available. Keep warm/cache/thread/budget conditions matched.
- Retain a change only after a demonstrated >=5% target improvement with full typed
  correctness and no protected >10% regression. Follow with the complete canonical
  and public-development suites. No claimed benefit before measured evidence.

## Implementation checkpoint

`normalize_morsel_group_array` preserves only Dictionary(Int32, Utf8) for group
evaluation. Other groups and every aggregate input still use the checked shared
normalizer. The selected gate passes 750 Rust tests with one pre-existing ignored
rule test: `.scratch/dictionary-preservation-integration.log` and
`.scratch/dictionary-remaining-gate.log`. The focused unit log separately confirms
representation preservation and dictionary correctness. Formatting and diff checks
pass. The `lance,gpu` release build passes.

The alternating ten-pair screen passes all exact result/time checks. Q1 improves
11.883% (1,032.760 to 910.036 ms). After/before median ratios for Q6, Q9, Q14 and
Q18 are 1.005592, 0.991204, 1.002908 and 0.980954. Only
`src/physical/morsel_agg.rs` differs between production source snapshots, and both
binaries use the same optional features. Evidence:
`.scratch/public-bench/dictionary-candidate-screen-01`, driven by
`.scratch/compare_dictionary_candidate.py` under the 96 GiB wrapper and CPUs 0–15.
The complete canonical SF10 gate passes all 660 pairs across three sessions,
with suite time 2.763338× DuckDB and geometric mean 2.611404×. No query regresses
more than 10% against the immediately preceding control; the earlier Q9 regression
remains open. JOB passes 339 pairs and ClickBench passes 129 pairs. The change is
accepted for this measured source snapshot. Full provider SF10 and full public
workload certification remain open; these results do not establish leadership.
See [preserved source, binaries and measurements](benchmarks/2026-09-06-dictionary-preservation/README.md).
