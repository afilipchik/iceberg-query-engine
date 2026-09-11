# Native scan admission follow-up — 2026-09-11

On frozen candidate `1bba20b3`, the completed targeted native Q6 diagnostic reports
an aggregate-input queue with eight declared partitions and max_slots=8, but
prepared=false, admitted_buffers=false, no declared byte bound, and slots=1.
This is direct runtime evidence of serialized input admission for that workload.
It is not evidence that the engine declares only one partition or omits the others.
The trace is `admitted-coalesce-paired-01/b1-candidate-native/q06.stderr`, archived
with the current cycle after all timing finishes.

`NativeStreamingScanExec` exposes its segment-based partition count but implements
neither prepared-input factory nor the resident copied-output bound. The
`PhysicalOperator` defaults decline those capabilities. Its own source contract
states that footer metadata, dictionaries, deletion vectors, decode allocations and
retained outputs are not yet query-pool admitted. The intervening Project therefore
cannot establish the missing producer contract merely from a fixed-width logical
projection.

The current full SF10 native screen records Q1 and Q6 warmup timeouts; their
measured samples are not run. These failures remain separate from the successful
extended-deadline targeted outputs and from any noisy short-query ratio. No causal
claim that admission alone explains the entire timeout is justified without further
measurement.

A native follow-up must account for metadata, dictionary/deletion state, decoding
scratch and physical output representations, then provide an appropriate prepared
capability before enabling more concurrent owners. Verify the actual workload
boundary without output pre-pulls, then validate runtime slots, all partitions,
SQL outputs, retained-owner lifetime, memory refusal and cancellation. Do not force
queue concurrency or assert a resident bound while file decoding remains outside
its ownership contract. Keep native, raw Parquet and preloaded IPC modes distinct.

The current post-filter accumulator improves a separate admitted raw scan path.
Its batching repair does not certify native admission or native performance.
