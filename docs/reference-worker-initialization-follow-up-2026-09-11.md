# Reference-worker initialization follow-up — 2026-09-11

The active7ddbf9ce Iceberg screen records a DuckDB Q13 calibration0 refusal:
OutOfMemoryException, failed32MiB block allocation (bad allocation). Calibration1/2
and all dependent engine/reference measurements are NOTRUN. This is not the prior
1bba20b3 Q13 measured-request SIGSEGV, nor a successful comparison. Exact responses
remain in the current run's execution.jsonl and will be archived after timing.

Read-only worker source inspection establishes the initialization sequence:
RLIMIT_DATA is set, DuckDB/PyArrow are imported, a default DuckDB connection is
created, then threads, memory limit and temp directory are configured with SET.
Provider configuration follows. Query execution includes fetch_arrow_table and
excludes output serialization from the reported query interval. No active harness
or benchmark parameter was changed during this inspection.

One hypothesis is that default connection initialization creates resources before
the requested settings apply. Any retained allocator/thread state could affect
headroom under RLIMIT_DATA. This is NOT a reproduced cause of the refusal. A second
possibility is later provider/query allocation behavior; the investigation must
separate them rather than label every reference failure a setup bug.

After the frozen cycle is audited, committed and pushed, use owned diagnostic
workers to compare existing initialization with construction-time configuration,
if supported by pinned DuckDB1.4.4. Keep the same query/process caps, affinity,
provider snapshots and requested threads. Record actual settings and /proc process
Threads, VmData, RssAnon/RssFile and cgroup events at import, connection creation,
SET/configuration, provider-ready and each query boundary. Neither RSS nor VmData
alone proves the precise denied allocation; preserve both and any named errors.

Run a small setup-only control before replaying the affected query under a separate
bounded diagnostic deadline. Validate completed outputs against the existing typed
oracle. Keep default and proposed setup in independent fresh workers; do not reuse
a previously failed process. Preserve all attempts, crashes, refusals and not-run
requests. Do not raise caps, disable memory protection, or change query SQL to make
the control pass. If initialization changes, rebaseline the matched benchmark
conditions before interpreting any new engine/DuckDB performance ratio.

This follow-up is independent of engine native admission and the coordinated
Parquet reader repair. Existing incomplete provider screens remain incomplete.

## Provider-specific setup confirmed from source

The current Lance screen also fails reference calibration0 for Q9, requesting a
128MiB block. Its engine Q9 requests are NOTRUN; this cannot clear the older engine
join-index failure. Provider setup uses actual DuckDB extensions for Iceberg and
Lance and creates views over their sources. It does not eagerly convert those
providers into registered PyArrow tables. Native reference setup separately uses
CREATE TABLE AS from its reference Parquet, and decoded IPC registers preloaded
Arrow data; those are different measured modes.

Lance setup imports the Python Lance package to verify the pinned dataset version,
in addition to loading DuckDB's Lance extension. The existing compatibility setting
disables the DuckDB extension optimizer because the pinned extension's decimal AVG
pushdown truncates values. This setting is recorded for every query and is not
stock extension-optimizer performance. Preserve it in result qualifications.

The diagnostic should distinguish import/default connection/SET from extension
loading and each provider-registration step. In particular, measure before and
after Python Lance import, LOAD avro/iceberg/lance, and view creation. A provider
can allocate outside DuckDB's managed query-memory budget; no causal attribution
to such allocations has yet been established for these failures. Keep the actual
process cap and query cap unchanged during the proposed comparison.
