# Spill test output floor — 2026-09-09

The current `agg_spill_matches_in_memory` completion assertion cannot fit its
retained output into the specified256 KiB query budget. This is an exact fixture
finding, not an optimizer cardinality estimate or permission to ignore the test.

An independent scan of every actual `orders.o_orderkey/o_orderdate` and
`lineitem.l_orderkey` value finds14,785 distinct matching key/date groups. NULL
join keys are excluded and duplicate order rows/dates are handled as sets. The
current output builder creates separate unencoded Arrow columns; its Int64 key,
Date32 date and Int64 COUNT alone require14,785×(8+4+8)=295,700 bytes. That is
larger than262,144 bytes before SUM, validity, metadata, spill buffers or working
state. Current COUNT output is Int64 in `StateRows::output_type`; schema types
are checked against the actual fixture files. Both input hashes verify afterward.

The output-validity allocation diagnosed previously is still unnecessary for
all-valid values and deserves its own red/green admission regression. Eliminating
that waste cannot make the complete retained result fit this budget. A subsequent
refusal would not, by itself, show that the fix failed or that more input spilling
could solve the retained-output requirement.

## Required test correction

Preserve the256 KiB case as an explicit resource refusal gate: it must refuse
cleanly by name, release owners/spill files and never abort or return partial
results as success. Keep its original failure evidence. Add a separate positive
spill case with enough budget for the independently measured output plus bounded
execution overhead, and a fixture/working set that actually forces spill. Require
positive spill bytes and an independent typed oracle. The current helper checks
only `spill_metrics.is_some()` and compares engine runs using rounded strings;
neither alone proves actual spill or independent SQL correctness.

Do not simply raise the budget and call the old test fixed. First demonstrate the
negative resource contract and actual positive spill under the revised conditions.
Also preserve tests for other denial boundaries: this output-floor proof says
nothing about the join's whole-page120512-byte refusal or the8 KiB metadata cases.
A future streaming or compressed result API could have a different memory floor;
this result concerns today's collected `Vec<RecordBatch>` construction.

`ExecutionContext::sql_impl` currently stringifies partition execution/collection
errors into `QueryError::Execution`, erasing their typed cause. Preserve typed
partition context before relying on `is_memory_limit()` at the query API for the
negative gate; merely searching error text is weaker evidence. This is a separate
error-propagation issue, not proof that retrying consumed input is safe.

## Evidence

`audit_spill_fixture_output_floor.py` ran successfully in a1 GiB capped scope after
timing work ended. It uses PyArrow25.0.1 and actual fixture rows, without running
SQL or consulting engine statistics. The [archive](benchmarks/2026-09-09-spill-output-floor/manifest.json)
contains the script, result, log and fixture hashes. No production source or
legacy test has been changed by this audit.

The public `MemoryLimit.kind()` category is deliberately `Execution` today.
Preserve that existing log contract while retaining the typed root cause through
partition context; use `is_memory_limit()`/`root()` for resource classification.
Changing public categories is not required to fix erased causes.

The subsequent [typed boundary and split spill tests](partition-error-and-spill-contract-2026-09-09.md)
implement the refusal/completion distinction. The positive16MiB/2%-policy test
actually spills2,741,595 bytes and validates14,785 groups independently; retained
output admission is3,057,480 bytes. This does not retroactively certify the old
256KiB completion expectation or the other legacy spill failures.
