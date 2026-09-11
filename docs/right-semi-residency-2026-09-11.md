# Frozen68912c23 IPC/GPU residency — 2026-09-11

Follow-up92202 terminal1; audit31858 terminal0 validates252completed outputs and206/212requested measured pairs. Default disjoint ownership,canonical16threads CPU0–15,32GiB query/48GiBprocess with explicit preloaded timing; this does not clear16GiBpreload. All521 source inputs and binary/driver/harness hashes verify after execution.

Decoded IPC and canonical preloaded CPU control each have84correct outputs,63/66valid measured pairs; Q1 warmup times out. Canonical mixed GPU exits2 before execution because its CPU control is incomplete. It has zero requests and no successful device execution. A vacuously true empty-output audit is not GPU coverage.

Separate custom600k-float CPU/GPU smoke has42correct outputs per side and40valid measured pairs each. All40required-GPU measured requests pass request-scoped device evidence, including successful execution, matching prepared session, no fallback/upload failures and no query-time upload. These custom results are not canonicalSF10device certification.

The [archive](benchmarks/2026-09-11-right-semi-residency/manifest.json) preserves failed control gates, startup rejection, independent typed audits, runtime evidence and provenance. No residency leadership,resource/concurrency acceptance or canonical GPU success is certified.
