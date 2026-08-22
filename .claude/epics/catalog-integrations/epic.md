---
name: catalog-integrations
status: completed
created: 2026-08-22T15:25:08Z
updated: 2026-08-22T15:56:33Z
progress: 100%
prd: .claude/prds/catalog-integrations.md
github: (will be set on sync)
---

# Epic: catalog-integrations

## Overview

Two catalog fronts: extend the existing Gravitino client from filesets to
relational (Iceberg) catalogs, and add Apache Pulsar namespaces as
discoverable table sources (admin API = listing, schema registry = typing,
bounded snapshot reads = data). Same discipline as always: live gates,
refusal by name, feature-gated deps, honest publication.

## Architecture Decisions

1. **Gravitino relational rides the existing flow**: GravitinoSource detects
   the catalog TYPE (`GET .../catalogs/{c}` -> type "relational" vs
   "fileset") and branches: filesets keep today's path; relational lists
   tables and maps provider `lakehouse-iceberg` + file location ->
   `register_iceberg`. Everything else refused by name. Hermetic tests mock
   the REST bodies (the client is a thin reqwest layer already).
2. **Pulsar snapshot semantics**: a scan = the topic FROZEN at the last
   message id fetched at scan start. Reads use one pulsar-rs reader from
   earliest; stop after consuming the boundary id. Backlog cap refuses
   oversized topics loudly (memory-safety rule).
3. **Latest schema applies to the whole topic** (v1): schema registry's
   current version decodes every message; a message that fails to decode
   fails the query BY NAME (topic, message id) — no silent row drops.
4. **Provider surface**: PulsarTable implements TableProvider::scan only
   (no splits, no parquet_files) — automatically invisible to scatter
   election and GPU offload; statistics() returns row-count estimate from
   admin backlog numbers when cheap, else None.
5. **Feature-gated** `pulsar` like lance/gpu; serve flags + REPL command
   compile only with the feature.

## Task Breakdown Preview

1. Regression: start local Gravitino, run metastore_demo.sh, fix drift.
2. Gravitino relational catalog support + hermetic client tests (+ live if
   the local install offers a relational backend cheaply).
3. Pulsar infra: download standalone into .scratch, pulsar_local.sh
   (start/stop/status/wipe), smoke-produce via pulsar-rs example.
4. Pulsar provider: dependency (lock check), admin/schema clients, decoder
   (JSON+Avro), bounded reader, TableProvider, register_pulsar_namespace,
   serve/REPL wiring, refusals.
5. Live gate scripts/pulsar_demo.sh (produce JSON+Avro topics, query,
   value-compare, discovery + refusal checks) + both suites + lock check.
6. Docs (README, CLAUDE.md) + publish + merge.

## Success Criteria (Technical)

Per PRD. The pulsar_demo gate is the acceptance instrument.

## Estimated Effort

6 tasks; Pulsar provider (4) dominates.
