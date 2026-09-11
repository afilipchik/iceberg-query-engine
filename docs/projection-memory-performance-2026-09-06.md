# Projection admission candidate: optimized correctness and regression evidence

The frozen 620-file candidate passes the scoped resource behavior gates but is
**not performance-accepted**. IPC Q12 retains a protected regression; isolated
primitive projection kernels also have measurable overhead.

Binary SHA-256: `82dc15c4c595cc9fe3342ba42cdf4493277ad7440c117bf84a71dadcf35a0205`.
Source SHA-256: `dfb7ae2ab6b0c453989201c884255b08740960a919e99a63180cd4dce0d41b6a`.
Control: frozen 612-file binary `e8174895539343b61796925247fedebdfef45f54caa4f133d4496e70df90aae6`.

## Optimized validation

89 float/date queries match the independent DuckDB oracle. A broader 58-query
primitive oracle matches the control in every value and Arrow schema; 50 match
DuckDB. The eight integer division queries differ from DuckDB on both binaries:
`3 / 2` returns integer 1 rather than floating 1.5. This is a preexisting SQL
compatibility gap, not a newly passing DuckDB comparison. Preserve it explicitly.

The 64 KiB optimized probe validates borrowed input and refuses both computed
projections by memory name. This validates their new output admission, not all
query allocation: literal expansion and coercion temporaries still have unadmitted
paths. The debug gate remains 744 passing tests with two existing library ignores.

## Canonical focused paired screen

Q6/Q12/Q14/Q19/Q22, ten steady pairs plus one warmup per side, 16 threads/affinity
0–15, 40 GiB query/48 GiB process/96 GiB scope. Fresh DuckDB supplies typed answers
and 10x query ceilings. All **330 requests** pass. Ratios are candidate/control.

| Mode | Suite | Geomean | Q6 | Q12 | Q14 | Q19 | Q22 |
|---|---:|---:|---:|---:|---:|---:|---:|
| decoded_ipc | 1.0048 | 0.9856 | 0.9679 | 1.1090 | 0.9988 | 0.9915 | 0.8749 |
| lance | 0.9925 | 0.9904 | 1.0025 | 0.9827 | 0.9917 | 0.9949 | 0.9804 |
| raw_parquet | 1.0044 | 1.0009 | 0.9694 | 1.0048 | 1.0319 | 1.0002 | 0.9994 |

IPC Q12's initial 1.1090 ratio persisted in two fresh ten-pair sessions:
**1.13218 and 1.100016**. All 44 recheck requests passed correctness/time gates;
the protected performance gate remains failed. The saved Q12 projections contain
only column/alias expressions, so the new arithmetic kernels are not an established
cause. Scope overhead, other execution costs and code-generation changes need
attribution; do not infer causality from the candidate/control ratio alone.

## Separate resident projection component

262,144 rows in four 65,536-row IPC batches; four threads/affinity0–3, 256 MiB
query/8 GiB process/24 GiB scope. Same embedded timing, three fresh DuckDB
calibrations, ten steady pairs plus warmup, and typed full-table equality. All
110 engine requests validate and pass the time ceiling. No SF10 latency job ran
concurrently. This is a component experiment, not a workload leadership claim.

| Projection | Control ms | Candidate ms | Ratio |
|---|---:|---:|---:|
| `SELECT i AS v FROM t` | 0.109985 | 0.096159 | 0.8743 |
| `SELECT i + 1 AS v FROM t` | 0.388068 | 0.430317 | 1.1089 |
| `SELECT f * 1.25 AS v FROM t` | 0.251970 | 0.363801 | 1.4438 |
| `SELECT CAST(i AS VARCHAR) AS v FROM t` | 2.473494 | 4.083042 | 1.6507 |
| `SELECT i + 1 AS v, CAST(i AS VARCHAR) AS s FROM t` | 2.924672 | 4.662136 | 1.5941 |

Next optimization candidates are direct construction into admitted buffers and
Arrow's existing ArrayFormatter integer serialization. The current float kernel
uses a stack chunk followed by a buffer copy; integer strings use Rust Display
rather than the Arrow lexical formatter. These are source-level explanations to
test with component pairing, not proven attribution. Preserve admission before
allocation and old-plus-new growth accounting while optimizing.

Full 22-query/provider, GPU, concurrency and broader resource acceptance remain
open. Reproduce from `.scratch/projection-memory-repair/`; archived evidence is
in [the verified archive](benchmarks/2026-09-06-projection-memory/evidence.tar.gz).
All 1211 source/evidence members were SHA-checked. Archive SHA-256:
`1488b6ac323bef93df2f820b77c25b380a3739043abcea1ffae32face47ea435`.
