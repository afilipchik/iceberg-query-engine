# Borrowed Arrow input for canonical group keys

`KeyWorkspace::encode_arrays` now reads retained evaluated Arrow arrays directly.
It validates the complete row and measures canonical bytes before admitting output
growth, then writes into reusable reserved storage. Strings and lists are borrowed;
no owned ScalarValue, String, list, sliced child array or per-row accessor vector
is constructed by this encoder. Array references remain owned by the caller,
which must account for retained evaluated batches separately.

Bound types must exactly match, including decimal scale and timestamp unit/zone.
Supported normalized types match KeyLayout; raw dictionaries, views and large
encodings need normalization before this boundary and cannot be silently accepted.
List traversal uses original child offsets, including sliced parent arrays, and
checks nested nullability and depth. Float keys canonicalize NaNs and signed zero.
NullArray needs explicit logical handling because its physical null bitmap is absent.
An encoding failure invalidates the previous key; reuse within capacity works even
when no new pool reservation is available.

Three new regressions cover scalar-wire parity for sliced string/list/decimal/float
arrays, NULL/empty keys, large exact coefficients, logical NullArray, timestamp zone
mismatch, Float32 canonical values, out-of-range rows, arity mismatch, admission
failure, reuse and reservation cleanup. Final aggregate component gate: **98 passed,
zero failed or ignored**. Formatting passes. The first test-fixture compile failure
and intermediate 97-pass run are preserved, not counted as final coverage.

[Evidence](benchmarks/2026-09-07-arrow-key-input/) contains 8 verified members and
655 current source-input hashes, snapshots, delta, commands and logs.
Manifest SHA256: `3e7541ffcc44839a920802cfec5aaa2ee07cf04f13b40b126d1372870d2a2b14`.

This removes an allocation obstacle to ingestion integration; live workers still
use the old route. Aggregate value ingestion and admitted result emission remain
unfinished. Neither original replay regressions nor the live 256KiB decimal gate
was rerun or closed. No benchmark or performance claim is made.
