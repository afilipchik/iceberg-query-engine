# Optional aggregate-output validity — 2026-09-09

The shared admitted aggregate-output builder now omits a validity bitmap when
all actual output values are non-NULL. Nullable schema metadata alone does not
force a bitmap. Values containing NULL still allocate the existing admitted
bitmap and propagate denial; denial never becomes an all-valid result.
Borrowed UTF8 follows the same rule. NullArray has no physical validity buffer,
and its branch still validates every supplied value as NULL.

This changes only `src/physical/morsel_agg/admitted_output.rs` relative to frozen
0f30c946 (508 source inputs). Existing payload/type/batch owners, exact value
conversion, group finishing, HAVING placement and spill retry remain unchanged.
Buffers retain admission after array slicing and ArrayData extraction, including
zero-length primitive payloads. No dependency, memory limit or routing changes.
This is a construction/admission repair, not a measured speedup or Q1 solution.

## Reproduction and validation

The preceding debugger [located a 514-byte refusal](benchmarks/2026-09-09-output-validity-refusal/manifest.json)
in validity construction. The new focused regression exhausts a real pool after
creating the array owner, then requires actual all-valid values and empty input
to need no further admission. Actual NULL input must still fail with a typed
memory-limit error and release all ownership. Two other tests independently
check primitive values/slices, booleans, UTF8, nested list/child NULLs, NullArray
logical NULLs and ownership after extracting ArrayData.

Initial red37176 had the intended allocation failure plus a test assertion
using physical rather than logical NullArray null_count. Correcting that test
assertion leaves red95500 with exactly1 expected failure and2 passes; production
is still unchanged at that point. Green54743 passes all3 after the repair.
Broad61944 passes1,039 library tests/11ignored and42 integrations. It covers
aggregate binding/expression/output names, admitted HAVING, dictionary grouping,
actual parallel spill, native scans, outer joins, runtime-filter proofs and
streaming prepared joins. Existing ignored tests remain ignored.

Commands use `TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu`.
Focused arguments: `--lib optional_validity_regressions`.
Broad arguments: `--lib --test aggregate_expression_quantum
--test aggregate_output_name_contract --test aggregate_post_filter_admission
--test native_streaming_scan_tests --test outer_stream_contract
--test runtime_filter_domain_contract --test runtime_filter_lineage_contract
--test streaming_prepared_join_contract --test aggregate_binding_transparency
--test dictionary_group_contract_tests --test parallel_input_spill_contract`.
Full legacy spill gate1471 terminates101:6pass/7same failed names
(`--test spill_tests`). Aggregate now refuses600 bytes at262065/262144; this
is a later denial, not completion. Other page/metadata denials persist.
Formatting and whitespace checks pass. The [correctness archive](benchmarks/2026-09-09-optional-output-validity/manifest.json)
preserves all508 source inputs and red/green/broad/spill logs.

The [independent output floor](spill-test-output-floor-2026-09-09.md) proves the
legacy256KiB aggregate success assertion cannot fit its retained result. Removing
an unnecessary bitmap does not change that fact. Keep a typed refusal gate and
add a distinct positive spill test with an independent oracle and actual spill
bytes. Whole-page decompression and other legacy refusal boundaries stay open.
