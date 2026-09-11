# Fixed-width streaming candidate — validation checkpoint

General pool-independent queue/gather capabilities now cover enforced raw
fixed-width scan output and propagate through audited Filter/column Project.
Streaming remains distinct from resident input. Existing IPC routes decline
the raw capability; no metadata getter switches providers.

The normalizer checks rows/types/columns, trims exposed values and validity
independently, and rebuilds safely using the exact declared schema. Unaligned
Boolean values require a bounded bitmap repack to offset zero because Arrow
validation couples value offset with minimum validity extent. Parent owners,
decoder scratch and whole query memory are not admitted by this contract.

Tests also reproduced a pre-existing Parquet ProjectionMask mismatch: readers
emit unique roots in file order, while requested projection order may differ
or repeat columns. The raw branch now restores that order before schema wrapping;
IPC already uses ordered RecordBatch projection. Exact decimal/date/NULL tests
cover unsorted and duplicate projection with static and runtime filtering.

Build-only take bounds now omit source identity payload while retaining full
dictionary children. Their index-validity extent is explicit: Arrow may clone
an entire sliced index bitmap. Inner's fresh nonnullable indices justify zero.
The old identity-safe gather method documents its compact-index precondition.

674 selected tests pass, one pre-existing ignored; formatting passes. The first
compile found a test import error. The first runtime gate reproduced independent
bitmap validation and projection-order failures, both fixed. Real streaming join
and aggregate tests validate exact results and deferred file reads; a direct raw
queue test proves two slots are reserved before pulling and fully released.
An explicit QE_IPC_CACHE=1 gate covers preserved sidecar routing plus memory and
partition regressions. Release build is running; no new timing/cap claim yet.

Detailed logs, frozen source and proof notes: [evidence](benchmarks/2026-09-06-streaming-fixed/README.md).

The preceding measured prepared candidate recovers IPC Q14 but raw Q9/Q14 still
fail. This source must pass its own release/cap/alternating gates. Variable-width
raw output, nested/computed wrapper preparation, decoder/source admission, GPU
hard-budget admission and broader provider/public leadership remain open.

## Terminal frozen release and screening

Release5437, cap40149, raw25515 and IPC81852 all exit0. Six cap cases actually spill; 220/220 screened outputs validate exactly under fresh time ceilings. Raw five-query summed-median ratio is0.995× control, IPC1.017×; Q18 is about4% slower. Raw Q9/Q14 earlier regressions are absent. Q14 reservations are8,272,944 raw /37,861,936 IPC bytes, versus825,042,224 previous IPC. See [complete evidence](benchmarks/2026-09-06-streaming-fixed/README.md); source580 and frozen binaries preserved. Broader provider/resource validation remains open.
