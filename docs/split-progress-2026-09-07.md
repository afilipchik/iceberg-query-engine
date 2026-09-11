# Checked split progress for a completed run

`SplitPlan::scan` in `physical/morsel_agg/repartition.rs` now proves a two-way split
has two nonempty children before production repartitioning can use it. The plan
borrows the exact source run and records the separating canonical-key bit and
expected child counts. `PreparedRepartition::from_plan` is the production entry;
the arbitrary-bit constructor is private to the module. Repartition checks its
finished child counts against the plan and refuses a zero/nonmatching child pair.

The scan uses one admitted frame buffer and an admitted copy of the first key.
It compares exact canonical bytes until a different key appears, then releases
the representative buffer and counts both sides over the rest of the run. It
continues through checksum/count/EOF validation even after finding a split.
Empty and all-equal-key runs return None only after successful full validation.
A scheduler can therefore distinguish no possible key split from an IO or
admission failure, instead of blindly repeating an ineffective repartition.

A returned plan proves strict row-count reduction for both children of this
single source run. It does not select a balanced split or prove an efficient
multi-run scheduling policy. First-difference selection can be skewed. The full
scheduler still needs a consistent split across every run in a partition, global
progress checks, admitted pending-task/run metadata, oversized-group handling
and bounded merge/output working space. It must not infer that a single-key
partition is safely mergeable solely because no split exists.

Final gates: **88 morsel plus16 ownership tests pass**, zero failures/ignores;
two new tests prove a4/4 child split with scan allocations released, completed
children matching those counts, empty/all-NULL-key runs declining splits, and
late corruption refusal both before and after a split has been discovered.
The prior88-test run is retained but not added to the final total. Formatting
passes. Commands use48G containment, one build job, four Rayon threads and
lance/gpu features.

[Evidence](benchmarks/2026-09-07-split-progress/) contains seven SHA256-verified
members and653 current source-input hashes, source, isolated delta, commands
and logs. Manifest SHA256:
`17c087e8fcac604c54c6d1b41c51838c49ed908a7817ff2e8d619e05fe310b9f`.

No dependencies or live query routing changed. No full-library/integration,
end-to-end spill, dedicated cap/GPU or performance gate ran. Both consuming-source
replay failures and the unchanged256KiB query-completion case remain unresolved
and were not rerun. Next implement bounded multi-run partition scheduling and
its global progress/ownership rules, then live worker integration. Continue the
existing [implementation contract](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).
