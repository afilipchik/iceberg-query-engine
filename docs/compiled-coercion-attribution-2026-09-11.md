# Compiled comparison dispatch attribution — 2026-09-11

The frozen97e53169 candidate fails the intended performance improvement. Four balanced
binary/compiler-switch blocks show generic rawQ6 compilation on/off ratio2.232903
for the candidate, versus1.004317 for13210a20. Candidate block ratios are2.253364,
2.326176,2.118822,2.238703. All48outputs are independently typed-correct and all12
four-way logical/physical plan groups match. NativeQ6 remains noisy: the old binary's
on/off block ratios range0.746377–1.622539, so the candidate's mean1.340400 cannot be
read as a precise isolated effect. NativeQ9 on/off means1.001634old/1.003923new.
The compiler switch disables all predicate compilation; these are diagnostic controls,
not confidence-certified acceptance or a switch solely for the new conversion.

The same source-frozen generic rawQ6 profile completes one typed-correct output and
76stopped-thread snapshots. Across all sampled threads there are175evaluator leaves:
155eval_chunk and20of its closures. Decoder leaves include120HybridDecoder::value
and54PlainFixedDecoder::next_chunk. These are counts, not CPU percentages. Multiple
worker threads can contribute to one snapshot; stack depth and stopping perturb work.

The frozen executable's sql_impl symbol is at0x15eb450; the initial GDB breakpoint
at0x555556b3f450 yields runtime load base0x555555554000. Subtracting that base maps
63evaluator leaves to0x5fb57ef–0x5fb5902, the float slice/scalar loop. Its load at
0x5fb5829 is followed by operator-table dispatch at0x5fb582f–0x5fb5836; the row backedge
at0x5fb58e7 returns to0x5fb5820. This establishes operator dispatch inside the row
loop in the measured optimized binary. Another18leaves fall within the exact-decimal
operand reader0x5fbb880–0x5fbba3f. That secondary cost remains a separate candidate
for investigation; this evidence does not assign all regression time to either range.

Attribution28368 terminal0,48control outputs plus1profile output. Combined48GiB scope
peak1,404,874,752bytes,swap0,zero max/OOM events. [350-file verified archive](benchmarks/2026-09-11-compiled-coercion-attribution/manifest.json)
retains source526, binaries' manifests, all samples/results/oracles, plans, stack/PC
summaries, debugger logs and disassembly. [Full preceding paired result](compiled-coercion-measurement-2026-09-11.md).

The next source candidate changes only compiled_expr.rs: select Cmp outside the
float row loops, passing a constant BinaryOp to the existing always-inline SQL float
comparator. All slice/scalar combinations retain its NaN, infinity and signed-zero
semantics. No memory layout, reservation policy, dependency or default changes.
The exact-decimal reader is unchanged so measurements can evaluate one dispatch
repair at a time. Focused99196 terminal0:17passes/1032filtered. Both-mode feature/resource71658
is active against frozen source526. Correctness and optimized measurements
are required; this implementation is not yet a demonstrated speedup.


Dispatch validation71658 terminal1: each mode1108library passes/11ignored,
125contract passes,28spill/numeric passes/6legacy spill failures. Native/IPC63/0in
disjoint and62/1in partial. Complete executable/count/exit comparison finds no added
or removed failures; these remain failed resource gates. Peak25,737,580,544bytes,
48GiB cap,swap0,zero max/OOM events. [27-file verified validation archive](benchmarks/2026-09-11-float-dispatch-validation/manifest.json)
links the full526-input dispatch source snapshot.

Sequence92933 passed22additional float/Boolean/decimal/coercion/scalar contracts with
lance,gpu features. It is building the optimized candidate, then runs42typed-validated
requests: Q6/Q9 across raw/native/resident4 plus generic rawQ6, two reverse-order
blocks with13210a20baseline,97e53169regression control, and the new dispatch candidate.
All source remains frozen. This bounded triage must show improvement against the
pre-coercion baseline as well as the slower intermediate candidate; it does not
replace full provider/residency/resource/concurrency acceptance. Source-only tools,
formatting and whitespace checks pass. No new benchmark result exists yet.


Runtime-route correction: [the subsequent queue audit](admitted-route-cost-confound-2026-09-11.md)
shows that generic rawQE_COMPILE also switches copied to admitted decoding and expands
458producer batches to7323at the same16slots. The2.23x switch result therefore does
not isolate expression computation. The per-row operator dispatch evidence is valid,
and its repair improves generic raw against97e53169, but it does not remove the larger
route regression. Preserve this qualification when citing the earlier control.
