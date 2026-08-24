---
name: catalog-integrations
description: Extend Gravitino integration to relational catalogs and add Apache Pulsar topics-as-tables with schema-registry-driven discovery
status: completed
created: 2026-08-22T15:25:08Z
---

# PRD: catalog-integrations

## Executive Summary

The engine already speaks to one catalog: Apache Gravitino FILESET schemas
(`serve --metastore`, since 2026-08-15, gated live for parquet/iceberg/lance).
This epic (a) re-verifies that integration after five intervening epics and
extends it to Gravitino's RELATIONAL catalogs (Iceberg tables listed through
Gravitino's table API rather than modeled as filesets), and (b) adds Apache
Pulsar as a discoverable source: a Pulsar namespace acts as a catalog — the
admin API enumerates topics, the schema registry types them — and each topic
becomes a queryable table over a BOUNDED snapshot read (earliest through the
topic's last message id at query time).

*Interpretation note*: "pulsar" is taken literally as Apache Pulsar
(messaging + schema registry). If Apache Polaris (Iceberg REST catalog) was
meant instead, the Gravitino-relational work in this epic builds most of the
needed client shape anyway; Polaris would be a follow-up epic.

## Problem Statement

- Gravitino integration models everything as filesets with an engine-invented
  `format` property. Real Gravitino deployments ALSO expose relational
  catalogs whose tables carry their own provider/location metadata — the
  engine cannot see them.
- Event data in Pulsar is invisible to the engine entirely; analysts must
  export topics to files first. Pulsar's admin API + schema registry provide
  exactly the discovery/typing surface a catalog integration needs.

## User Stories

1. **Gravitino operator**: "`serve --metastore` keeps working exactly as
   gated; pointing it at a RELATIONAL catalog registers its Iceberg tables
   too." Acceptance: existing metastore_demo gate PASS; relational tables
   listed/loaded via Gravitino's table API register through the existing
   Iceberg reader (hermetic client tests; live gate if the local Gravitino
   supports a relational backend without heroics).
2. **Pulsar user**: "`serve --pulsar-admin http://…:8080 --pulsar-url
   pulsar://…:6650 --pulsar-namespace public/default` registers every
   schema'd topic as a table; `SELECT … FROM my_topic` reads a consistent
   snapshot of the topic." Acceptance: live gate against a local Pulsar
   standalone — produce known rows (JSON and Avro topics), query, compare
   values exactly; topic list matches admin API.
3. **Correctness owner**: bounded-read semantics are explicit: each query
   reads earliest -> the last message id fetched at query START (late
   arrivals excluded); topics without schemas or with unsupported schema
   types are refused BY NAME, never guessed.

## Functional Requirements

- Gravitino relational: list tables (`GET .../schemas/{s}/tables`), load
  each (`GET .../tables/{name}`), map providers: `lakehouse-iceberg` table
  with a local/file location -> `register_iceberg`; unsupported providers/
  remote URIs refused by name. Wired into the same GravitinoSource flow
  (`--metastore-catalog` of relational type), auto-detected by catalog type.
- Pulsar (feature `pulsar`, default off, like lance/gpu):
  - Discovery: admin REST `GET /admin/v2/persistent/{tenant}/{ns}` for
    topics; `GET /admin/v2/schemas/{t}/{ns}/{topic}/schema` for schemas.
  - Schema mapping v1: JSON and AVRO schema types, flat records, fields of
    string/int/long/float/double/boolean (nullable); adds `__key` (Utf8),
    `__publish_time` (Timestamp ms) metadata columns. Nested/other types
    or schemaless topics: refused by name at registration.
  - Read: bounded snapshot — fetch last message id via admin REST at scan
    start, read the topic with a pulsar-rs reader from earliest until that
    id (inclusive), decode payloads (serde_json / apache-avro, both already
    deps or feature-added), build arrow batches.
  - Registration: `ExecutionContext::register_pulsar_namespace`, `serve
    --pulsar-url --pulsar-admin --pulsar-namespace` (combinable with other
    sources), REPL `.pulsar <admin> <service> <tenant/ns>`.
- Local infra: `scripts/pulsar_local.sh` (start/stop/status a Pulsar
  standalone under .scratch using the repo's JDK17), `scripts/pulsar_demo.sh`
  end-to-end gate (produce -> query -> byte/value compare -> discovery
  check).

## Non-Functional Requirements

- Default build untouched (feature-gated dep; lock add-only check).
- Distributed: Pulsar tables are NOT shard-eligible (no
  distributed_splits) — scatter election skips them naturally; gather works.
  GPU: Pulsar provider has no parquet_files -> never offloaded. Both facts
  asserted, not assumed.
- Snapshot reads are memory-bounded by refusing topics whose backlog exceeds
  a configurable cap (`QE_PULSAR_MAX_MESSAGES`, default 10M) — refuse loudly
  rather than OOM.

## Success Criteria

1. metastore_demo.sh gate PASS (regression) + relational client tests green.
2. Pulsar live gate: two topics (JSON, Avro), 10k+ messages each, values
   exactly match what was produced; schemaless topic refused by name;
   discovery lists exactly the namespace's topics.
3. Both builds' full suites green; lock add-only for the pulsar feature.
4. Docs: README + CLAUDE.md sections; publish + merge.

## Out of Scope

- Streaming/continuous queries, subscriptions, acking, exactly-once
  (snapshot reads only); Pulsar SQL/Presto compatibility; partitioned-topic
  parallel readers (v1 reads partitions sequentially); schema evolution
  across a topic's history (latest schema is applied to all messages, decode
  failures are per-message errors that fail the query loudly); writes;
  Apache Polaris (noted follow-up); Gravitino messaging-catalog type
  (Gravitino can itself front Pulsar topics — once both halves of this epic
  exist, that composition is a small follow-up).

## Dependencies

- `pulsar` crate 6.8 (feature-gated), existing apache-avro/serde_json;
  Pulsar standalone tarball (downloaded to .scratch, like Gravitino);
  repo JDK17.
