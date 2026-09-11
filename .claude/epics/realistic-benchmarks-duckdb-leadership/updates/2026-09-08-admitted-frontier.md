# Shared admitted-input follow-up

Current authoritative candidate is frozen ed721285. The join-build queue uses
PreparedAdmittedInput, but `physical/morsel_agg/input_frontier.rs` only calls the
older prepare_queue_input API. Eligible variable-width aggregate inputs therefore
retain serial polling even though the source now offers admitted buffer ownership.
This is a source-level integration gap; do not claim a measured speedup yet.

Finish the active provider screen11660 and preserve its artifacts before editing
engine source. Raw42325 validates60/66 pairs (Q9/Q13 timeouts); Native currently
validates60/66 (Q1/Q13 timeouts). Q2/Q19 raw timing increases relative to the earlier
separate screen require controlled paired confirmation. Neither incomplete screen
passes suite leadership.

## Bounded implementation task

1. InputFrontier must attempt the admitted descriptor before the copied-output
   API. Verify requested-pool ancestry and exact partition count. None may fall
   through; admission/identity/IO errors must not fall back or replay execution.
   Keep preparation exactly once and retain panic classification at this boundary.
2. Admit scheduler metadata before allocating pending jobs or spawning tasks.
   Prepared vectors/boxed streams already own construction leases. Moving their
   streams to pending jobs must preserve ownership, including cancellation.
   Keep conservative metadata allowances explicit; do not claim exact RSS.
3. In admitted mode use at most min(partitions, Rayon threads) owned tasks. Pulls
   may allocate from the supplied pool; they are not pool-independent. Retain
   demand permits and output leases through consumer processing. Do not add a
   copied-output envelope or call own_input_batch for already-owned buffers.
4. InputBatch needs an explicit ownership distinction. live_spill.rs currently
   checks envelope.is_some() to decide whether to charge received Arrow bytes.
   An admitted batch must also avoid that duplicate charge. Do not use a dummy
   zero-byte envelope as a semantic marker. Ordinary serial batches keep their
   existing consumer lease and pressure handling.
5. One-slot admitted execution must preserve the ownership distinction too.
   Otherwise low-thread configurations silently double charge identical buffers.
   Unknown/unsupported sources must retain the old serial behavior.
6. Preserve task ownership and shutdown before errors return. Test synchronous
   polls that cancellation cannot preempt, retained outputs after frontier drop,
   late errors, denied scheduler setup and pool ancestry violations.
7. Add exact regressions for actual overlapping pulls, prepare once/no ordinary
   replay, all declared partitions, duplicates/NULLs, empty prefixes, repeated
   projected columns, and admitted scan -> Project -> live aggregate. Force real
   spill with an independent oracle; count-only or engine-versus-itself checks
   cannot certify the aggregate transition.
8. Run focused frontier/live-spill tests, full locked/offline lance,gpu library,
   selected spill/prepared-wrapper/partition integrations and formatting checks,
   all builds/tests under the existing capped wrapper. Only then freeze a candidate.

## Measurement and acceptance

Use unchanged canonical queries plus a generic string-key scan/aggregate family,
varying cardinality, filtering, dictionary/plain encoding, row-group fragmentation,
threads and budgets. Trace actual preparation/slot count. Pair against frozen
ed721285 and keep fresh matched DuckDB ceilings. Include protected raw Q2/Q19 and
previously failing queries, then required providers/residencies and resource/
concurrency gates. An added code path or passing synthetic parallel test is not a
performance result. If scan decode/filter still dominates, use the separately
documented encoded-input investigation rather than changing a Q12-specific constant.
