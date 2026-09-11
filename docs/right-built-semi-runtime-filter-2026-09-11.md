# Right-built semi-join runtime filtering — 2026-09-11

The planner excluded all right-built semi, anti and left joins from runtime filtering. That exclusion is necessary for anti and left joins, whose unmatched probe rows contribute to output. For semi-joins, unmatched probe rows never contribute. The executor already publishes keys from the actual build table; the planner failed to link its left probe scan.

The repair selects the actual probe child and join-key expression when wiring a right-built semi-join. It keeps ordinal/qualified identity resolution and the existing value-preserving `runtime_filter_target` capability. Multi-key joins may filter on one key as a superset of complete matches. No new filter allocation mechanism, dependency, ownership default or spill policy is introduced. The spill delegate still leaves an unpopulated slot when no in-memory publisher runs.

Regression20522 failed with zero linked slots where one was expected. Focused28770 passed29 integrations after the repair, including actual runtime publication and independent exact duplicate/NULL multisets for semi, anti and left joins. The fixture directly lowers bound logical joins to isolate planner wiring from optimizer transformations and the small filtered-file eager scan policy. Its Parquet probe has40 row groups. Initial fixture setup failures and a type-path compilation correction are not counted as the reproduced defect.

The expanded matrix includes duplicate build keys, a nonmatching build and an empty build. It consumes every declared output partition. Broad83080 completed successfully:1,030 library passes/3ignored and43 integrations. Formatting and whitespace checks pass. Existing lineage/domain tests also exercise aliases, computed-name collisions and extreme signed keys.

No optimized binary or canonical workload measurement includes this repair yet. It closes a planner capability gap; a speedup is not inferred from test results. Native/IPC, provider completion, resource gates and broader optimizer identity work remain open.

Reproduction: `TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --offline --lib --test runtime_filter_lineage_contract --test runtime_filter_domain_contract --test semantic_proof_tests --test outer_on_pushdown --test outer_stream_contract --test qualified_column_identity --test optimizer_convergence_contract`. See the [source/log archive](benchmarks/2026-09-11-right-semi-repair/manifest.json).

Local comparison: DuckDB checkout1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8, `src/optimizer/join_filter_pushdown_optimizer.cpp`, `GenerateJoinFilters`, excludes LEFT/OUTER/ANTI and RIGHT_SEMI while allowing ordinary SEMI. Its comments explicitly tie eligibility to preserving probe cardinality. This supports using semantic preservation and actual build/probe roles instead of treating all semi/anti orientations as identical; the engines have different orientation conventions. No external benchmark claim follows from this source comparison.

Both-mode feature/resource validation94085 is terminal with unchanged historical failures; see [full gate](right-semi-validation-2026-09-11.md). Optimized release72847 is running from the unchanged521-input snapshot. Prepared diagnostic/control/provider drivers have not run.
