# Stop reference failures before dependent work — September 8, 2026

The completed Lance provider screen contains a refused reference warmup followed
by three calibration allocation refusals on the same worker. No engine Q9 samples
were admitted, but the runner continued querying a process already known to have
failed. Resident orchestration also allowed measured reference requests after
invalid calibration and could attempt GPU preparation before applying that gate.

Both runners now share `query_gate.calibrate_reference`. Plan, oracle, warmup and
each calibration response are checked before the next request. Query calibration
times must be finite and positive; warmup/calibration outputs must match the typed
oracle. The first execution, timing-metadata or typed-comparison failure retires
the reference immediately. All remaining planned reference requests are traced
as not_run with executed=false. No replacement timings or within-query retry is
introduced. Invalid/overflowing calibration also closes the reference.

Resident runs apply the reference gate before GPU preparation, skip dependent
measured requests, and retire reference/engine workers after a measured reference
failure. Late or wrong resident engine results stop later engine requests, matching
ordinary runner behavior. Failed original responses and requested sample counts
remain preserved; no failed benchmark has been reclassified as successful.

Validation: a fake-worker red regression reproduces continued reference requests.
New cases cover each reference phase in both runners, wrong warmup output,
measured reference refusal, required-GPU preparation blocking, and late/wrong
resident engine results. Final full suite:125 tests run,123 pass, two existing
optional skips,2.835s in a2GiB wrapper with repository TMPDIR and pinned Python.
The fake required-GPU worker has no query method, so the test fails if preparation
is incorrectly attempted. This is control-flow coverage, not hardware validation.

No engine source, frozen binary, data or SQL changed. The four-track1a0ece71 screen
is immutable in `docs/benchmarks/2026-09-08-prepared-key-providers/` using its
original harness. New harness evidence is in
`docs/benchmarks/2026-09-08-reference-calibration-stop/`. Future runs must retain
the new harness hashes. Overall provider/residency/resource acceptance remains open.
