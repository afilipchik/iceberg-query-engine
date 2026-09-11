# IPC file extent validation — September 8, 2026

The shared native/decoded-IPC reader now rejects malformed footer and block
extents with a file-named error before slicing the mapping or invoking Arrow's
low-level decoder. Red29372 reproduced19 panics:3 oversized footer cases and8
malformed descriptors for each of dictionary/record blocks. The positive valid
file control passed. Green70608 passes all3 tests, including25 malformed cases
and independent exact dictionary/NULL/full/projection/sliced output checks.

The repair uses checked footer subtraction, signed-to-usize conversions and
addition, limits blocks to the pre-footer region, and checks the four/eight-byte
message prefix. Both block loops use one helper. Existing Arc<Mmap> ownership,
zero-copy views and default Arrow validation remain. No dependency change,
production panic recovery or query-specific branch. This does not establish
exhaustive malformed-schema validation or query-wide memory admission.

Pinned Arrow IPC58.4.0 read_footer_length checks trailer magic/nonnegative length,
but not the containing extent; parse_message assumes a four-byte prefix and an
eight-byte continuation prefix. Footer FlatBuffer validation does not check the
signed descriptor values against the containing file. Schema projection already
uses checked Schema::project.

Validation used HEAD88849c4 plus the existing dirty tree, offline locked lance,gpu,
TMPDIR=$PWD/.scratch, RAYON_NUM_THREADS=4, SAFE_BUILD_MEM=48G,
SAFE_BUILD_JOBS=1 through scripts/claude-safe-build.sh. Commands:

```
cargo test --locked --offline --features lance,gpu --test ipc_extent_contract
cargo test --locked --offline --features lance,gpu --lib --test ipc_extent_contract --test native_streaming_scan_tests --test native_insert_tests --test native_delete_tests --test native_update_tests --test native_table_validation
cargo test --locked --offline --features lance,gpu --test native_streaming_scan_tests --test native_insert_tests --test native_update_tests --test native_table_validation
cargo test --locked --offline --features lance,gpu --no-fail-fast --test native_streaming_scan_tests --test native_update_tests --test native_table_validation
```

Broad15813 terminal101:1001 library passes/11 ignored, IPC3 passes, native delete
9pass/1fail; Cargo stopped remaining targets. Explicit follow-ups: native insert
8pass/1fail; streaming scans10pass; table validation12pass; update10pass/2fail.
All four failures are queue copy-bound errors, each actual charge187 bytes above
the declared bound. A controlled temporary removal of only this IPC repair,
mutation-control26497 terminal101, reproduces all four exact actual/bound pairs.
The IPC repair was restored byte-for-byte after the control terminated. Thus
these are independently open pre-existing queue-contract failures, not green
mutation certification. Formatting and diff whitespace checks pass.

[Immutable red/green/control source and logs](benchmarks/2026-09-08-ipc-extent-tests/sha256.json)
contain10 SHA256-verified files. Prior a023079f benchmark archives remain unchanged;
no new benchmark result is claimed for this source repair.
