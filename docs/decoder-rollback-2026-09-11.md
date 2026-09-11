# Reject unproven packed-word optimization — 2026-09-11

Completed63cabb3a measurements do not establish a shared throughput benefit. See the complete [paired comparison and decision](packed-word-measurement-2026-09-11.md). All120outputs are correct and all60plan pairs identical; correctness does not establish performance. Preserve both gains and regressions.

After measurement56279/evidence19981 and native test repair82790 were terminal and archived, restored only the production Packed branch in `src/storage/admitted_hybrid.rs` from the240cd5e2 source archive. A byte-for-byte assertion verifies all production code before `#[cfg(test)]` now matches240cd5e2. Retained widths0–32 independent tests and the separately validated native Anti/membership test repair. No dependency, default ownership or memory-policy change.

The [three-file source archive](benchmarks/2026-09-11-decoder-rollback/manifest.json) verifies521inputs. Formatting/whitespace pass. Both-mode feature/resource validation79575 is running with locked/offline lance,gpu features under48GiB,one build job,repository TMPDIR. Do not edit source while it runs. No fresh optimized binary or full acceptance is claimed.

Validation79575 is now terminal1 and archived25files/521inputs:1095library/11ignored,125contracts and28spill/numeric passes each; native63/0 disjoint,62/1 partial and six legacy spill failures each. No added failure names. See [validation](decoder-rollback-validation-2026-09-11.md). Subsequent batch-view work is a separate source candidate.
