# Qualified identity protected performance follow-up

Status: running; no regression flag is closed yet. Session37654 owns the96GiB
heavy-job scope with affinity0–15. Do not overlap builds, profiles or archives.

The complete647/649 full-provider screen audited9680requests and identified
IPC Q22, Iceberg Q8, and Lance Q11/Q17 timing flags. This follow-up includes the
union of all those flags, with50steady pairs plus one warmup per side in each
of four startup/execution orders. It uses the original frozen647/649 binaries,
matched provider inputs, typed comparisons and fresh DuckDB10×time gates.
These are not653measurements and do not certify full workload leadership.

The plan and all outputs are preserved under
`.scratch/qualified-column-identity-repair/protected-followup-01/` and
`.scratch/public-bench/qualified-identity-protected-*/`. The driver is
`run-protected-followup.py`. Independent `audit-protected-results.py` verifies
exact query/side/iteration membership, warmup flags, all recorded comparison and
time outcomes, three reference calibrations, finitepositive timing samples,
recomputed means/medians/ratios and frozen identities. It reuses typed comparison
records; it does not rerun Arrow comparisons. Prefix audits are immutable.

The first two audited cells cover204requests. Before-first/offset0 IPC Q22 has
median ratio1.027260; Iceberg Q8 has1.000204. Neither cell has a>10%median or mean
flag. Ten cells remain at this checkpoint, including all other execution orders.
Do not infer closure or attribute a causal performance change from this prefix.

The original per-cell observations remain in
[the full identity audit](qualified-column-identity-2026-09-07.md).
The independently validated later memory correction is documented in
[the aggregate fallback audit](aggregate-fallback-pool-audit-2026-09-07.md).

The four-cell prefix now audits510requests. The first Lance cell repeats Q17's
median flag:351.037→411.321ms, ratio1.171731; mean403.036→416.553ms,
ratio1.033537. Q11 median ratio.907931 and mean1.027996 do not flag here.
The second IPC cell has median ratio.964761 and no flag. Eight cells remain.
For Q17, first-cell planning medians212.837→215.290ms and execution medians
125.599→124.817ms are similar. Parse means37.338→30.586ms and optimizer
means20.382→41.730ms show substantial intermittent wall costs; optimizer
medians are.639→1.054ms. These are stage observations, not exclusive CPU
attribution, a proved allocator cause, or permission to discard slow samples.
Evidence: `protected-followup-01/audit-04.json` and the50-pair attempt records.

## Complete follow-up and bounded diagnostics

All12cells/1632requests complete and pass the independent audit. Every typed
comparison and time gate passes. Across four execution orders, geometric means
of the per-cell median ratios are1.020540IPC Q22,1.003580Iceberg Q8,
1.026920Lance Q11, and1.164773Lance Q17. Mean-ratio geometric means are
1.009097,1.005797,1.012719 and1.034802 respectively. Q17 median ratios range
1.122805–1.231237 and flag in every order. Q11 flags once (median1.143134);
Q22/Q8 do not flag in these longer runs. Original observations remain preserved.
Q17 is a repeatable protected median regression; it prevents full acceptance.
These aggregates do not replace per-order samples or establish independent-session
confidence. `protected-followup-01/final-summary.json` preserves the full table.

The bounded main-thread syscall diagnostic completes84typed/time-correct
requests, with paired untraced and traced workers. In two649requests, optimizer
wall times143.262/127.840ms accompany3366/2748whole-request MADV_DONTNEED calls
whose syscall times sum120.413/108.946ms. Parse windows contain none in those
requests. Other slow requests discard pages during parsing, on both binaries.
This supports deferred memory maintenance as substantial request work, but
**does not prove an exact optimizer interval or allocation producer**. `plan_ms`
combines binding before optimization and physical planning afterward, so adding
reported stages cannot reconstruct an optimizer trace window. Tracing changes
cadence and slows execution; these are diagnostic timings only.

A second bounded diagnostic enables the existing allocation profiler in the
unchanged binaries, with unprofiled controls: all8requests pass typed/time gates.
The649profile's last printed large-allocation snapshot reports4591.8MB across
578allocations in a stack stopping at tokio::runtime::task::raw::poll, plus small
hash-index allocations. That stack is insufficient to assign the producer to a
specific provider or operator, or connect those addresses to discarded pages.
Process RSS already begins around2.8GiB before the first measured query and rises
near5GiB afterward; peak allocation totals are not query-budget reservations.
No allocator policy, reclamation behavior, THP setting or timing boundary changed.
Both diagnostic sessions are terminal. Drivers, records and analysis are in
`.scratch/qualified-identity-maintenance-profile/`; no source optimization is
claimed from these diagnostics.
