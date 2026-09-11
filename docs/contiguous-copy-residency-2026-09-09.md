# Frozen acdb8c51 residency screen — 2026-09-09

Runner92861 terminal1; independent typed-output/device audit45040 terminal0.
All503 source inputs and the binary verify unchanged. No source or dependency
change occurred during measurement.

| Case | Valid measured pairs | Completed warmups | Remaining outcome |
|---|---:|---:|---|
| Canonical decoded IPC |60/66|20|Q1 timeout; Q18 input-domain refusal|
| Canonical preloaded CPU control |60/66|20|Q1 timeout; Q18 input-domain refusal|
| Canonical mixed GPU |not executed|0|Incomplete CPU control rejected|
| Custom CPU control |40/40|2|Component completes|
| Custom required GPU |40/40|2|All measured requests have device evidence|

All200 completed measured outputs and44 warmups are typed-correct:244 completed
outputs total. This does not validate skipped/failed requests. Canonical Q13 now
completes; the older screen's Q13 failure has been replaced by Q18 refusal, so
an unchanged aggregate completion count does not imply unchanged behavior.

Q18 fails in `memory scan working space`: decoded IPC uses719,938,160 of
719,938,515 bytes and requests544 more; CPU control uses719,938,248 bytes and
requests544 more. The approximately687MiB input domain is smaller than the32GiB
configured query budget. Source residency also consumes admission. This is not
proof that the complete query needs more than32GiB. Attribute the retaining
consumer and compare the older frozen binary before changing the contract.
The same named domain refuses Lance Q18 in the separate provider screen.

## GPU evidence and boundaries

All40 measured required-GPU requests plus2 warmups have one attempted and one
completed device run, zero failures, matching prepared/run session IDs, unchanged
resident byte/column counts, no fallback increments and zero upload requests.
The existing repository NVRTC directory is enabled only through this process's
loader path; library hashes are retained. No host library installation/change.

Custom required-GPU medians are Q1 1.010ms and Q6 0.738ms. Separate acknowledged
preparation times are55.367ms and22.346ms. This is600,000-row custom float smoke,
not canonical SF10, a cold/upload-inclusive measurement, or full GPU acceptance.
Canonical GPU has no execution log because its required CPU control is incomplete.
Build features and a working custom device test do not certify canonical GPU.

## Conditions and evidence

Canonical IPC/control:16 threads,32GiB query and48GiB process. This remains a
separate capacity experiment and does not clear the16GiB preload refusal. Custom
CPU/GPU:4 threads,4GiB query,8GiB process;20 samples per query. One session per case.
The runner uses affinity0–15, a64GiB scope with no swap, repository TMPDIR and the
existing venv-lance Python. Each case retains setup, source/provider/data hashes,
requests, Arrow outputs, plans, failures and fresh10×DuckDB ceilings.

The shared scope high-water is20,469,223,424 bytes; every recorded scope snapshot
has zero max/OOM/kill events. The high-water spans cases and is not per-case RSS
or proof of query-wide reservation completeness.

Reproduce with `.scratch/parallel-aggregate-input/run_contiguous_copy_residency.py`
and `summarize_contiguous_copy_residency.py`, through the required memory wrapper
and PYTHONPATH=scripts. The audit independently compares completed outputs with
complete typed oracles and checks request-scoped device evidence. Full canonical,
provider, concurrency and resource acceptance remain open.

Initial matched attribution52985 completed a correct control output, then its
driver rejected normal ready/progress records as extra responses. This was a
driver assertion, not engine failure. Corrected comparison47364 is running in a
fresh directory; both runs are preserved and engine source is unchanged. See the
[provider screen](contiguous-copy-provider-screen-2026-09-09.md).

Archive: [manifest](benchmarks/2026-09-09-contiguous-copy-residency/manifest.json),
1,085 files verified.
