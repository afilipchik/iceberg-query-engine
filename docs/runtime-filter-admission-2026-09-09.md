# Runtime-filter payload admission and evaluated-key reuse — 2026-09-09

## Reproduced ownership gap

Regression66681 failed: after a join and its outputs were dropped, a published
runtime filter remained alive but the query pool reported zero retained bytes.
The old publisher staged every key in an unreserved Vec, then allocated an
unreserved bitmap or hash set. A sparse Int64 domain could select a bitmap of
about125MiB for only4,000 keys. Staging also evaluated expressions independently
before hash-table construction; it could not prove identity with the table's
actual evaluated key arrays.

## Candidate contract

The new `physical/operators/runtime_filter.rs` builds optional membership over
the vectorized hash table's already-evaluated arrays. The publisher runs after
required build construction, before probe execution. It does not re-evaluate SQL
expressions or pull probe input. A missing, incompatible or empty key domain
publishes nothing. Every batch of the selected key column must be compatible;
partial domains are never published. NULLs do not contribute matching keys.

An optional child domain caps construction at one eighth of currently available
query memory. Bitmap storage uses ReservedVec. Hash-set storage reserves a checked
upper bound before try_reserve, verifies actual capacity/allocation, and retains
its lease alongside the set. Owner metadata is separately reserved. The published
payload holds these leases until its final reference drops, including references
retained by a scan after join destruction. No key-staging vector is created.

Representation choice is a costing decision: bitmaps may use modestly more bytes
(up to4x the set bound) to preserve cheap membership probes, but must fit their
budget. Sparse domains use a set when feasible. Neither representation may exceed
admission; refusal returns None and leaves the ordinary join available. Arithmetic
is checked across the full signed domain, including MIN/MAX and width overflow.

The set bound is specific to the pinned hashbrown0.17.1 i64 layout: small bucket
transitions then7/8 load, with conservative control/alignment allowance. Its
actual allocation is checked before insertion, and reserved capacity prevents
insertion growth. Dependency upgrades must review this bound and run its capacity-
transition tests. No dependency changed in this implementation.

The existing low-level Bitmap/Set fixture variants remain; the production join
publisher now constructs the admitted payload variant. This is not query-wide
allocation certification: planner/configuration metadata, other join allocations,
resident source storage and final materialization still have separate gaps.
Provider targets are not expanded by this change.

## Validation

All engine tests use the real memory wrapper, repository TMPDIR, locked/offline
`lance,gpu` features, RAYON_NUM_THREADS=4, SAFE_BUILD_MEM=48G and SAFE_BUILD_JOBS=1.

-66681 exits101: the published-filter lifetime regression is red before the repair.
-91919 exits0: focused library runtime-filter tests and two signed-domain
  integrations pass. The global name filter excluded the new ownership and
  lineage tests; do not count those as covered by this focused command.
-41137 exits0:1,023 library passes,11 ignored and73 integrations. Ownership and
  lineage regressions both execute and pass. The sparse-domain ownership case
  retains under256KiB, with exact membership and final query-pool release.
-10653 exits101: six spill tests pass and the same seven historical names fail.
-A further independent full-join regression requires optional-filter refusal while
  returning the exact typed matches under a1MiB query budget passes in3586.
-Final3586 exits101 solely at the known spill target:1,023 library passes/11ignored,
  72 selected integration passes, six spill passes and seven failures. Cargo stops
  before streaming_prepared_join_contract; a separate terminal0 supplement runs
  both of those tests. Final selected coverage is therefore74 integration passes.
-Formatting and whitespace checks pass. [Immutable evidence](benchmarks/2026-09-09-runtime-filter-admission/manifest.json)
  contains eight verified files and502 source inputs. Only four source files
  (including the new module) and the domain test differ from the lineage repair.
  All jobs are terminal.

The new component tests check full signed membership, NULL/empty domains,
transactional early refusal, selected multi-key columns, incompatible later
batches, checked extent overflow and hash-set capacity transitions. Existing
projection-lineage and qualified/retained mapping semantics remain in place.

No new release or performance result is claimed. The frozen dbca414a diagnostics
remain the preceding measurement: resident Q12 improved to312ms but was still
above186ms, and all24 outputs were typed-correct. Next address/attribute remaining resident copy cost before broader paired and
provider/residency/resource/concurrency acceptance.
