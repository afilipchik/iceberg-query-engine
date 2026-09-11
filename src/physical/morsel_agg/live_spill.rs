//! Production bridge: consume each partition once and retain evaluated batches
//! across partial-state spill. Parallel state workers remain a separate policy.
use super::{
    group_rows::{GroupLayout, GroupRows},
    input_frontier::InputFrontier,
    parallel_controllers::ParallelControllers,
    spill_files::RunDirectory,
};
use crate::{
    execution::{
        expression_memory::with_expression_pool, reserved_vec::ReservedVec, SharedMemoryPool,
    },
    physical::{
        operators::{evaluate_expr, spillable::AggregateExpr},
        PhysicalOperator, RecordBatchStream,
    },
    planner::{AggregateFunction, Expr, PlanSchema, ScalarValue},
    ExecutionConfig, QueryError, Result,
};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::stream;
use std::sync::Arc;

pub(super) fn panic_error(payload: Box<dyn std::any::Any + Send>) -> QueryError {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    invalid(&format!("input panicked: {message}"))
}

fn count_star(aggregate: &AggregateExpr) -> bool {
    aggregate.func == AggregateFunction::Count
        && matches!(aggregate.input, Expr::Wildcard | Expr::QualifiedWildcard(_))
}

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("streaming partial aggregate: {message}"))
}

fn partial_ownership(value: Option<&str>) -> Result<bool> {
    match value {
        None | Some("disjoint") => Ok(false),
        Some("partial") => Ok(true),
        Some(_) => Err(invalid("QE_AGG_OWNERSHIP must be disjoint or partial")),
    }
}

pub(crate) async fn execute(
    input: Arc<dyn PhysicalOperator>,
    groups: &[Expr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    pool: &SharedMemoryPool,
    config: &ExecutionConfig,
    post_filter: Option<&Expr>,
) -> Result<Option<RecordBatchStream>> {
    // Diagnostic wall intervals only: finish includes output, and upstream
    // operators may overlap. These are not additive exclusive CPU samples.
    let profile = std::env::var_os("QE_AGG_PROF").is_some();
    // Experimental algorithm selection for matched measurements. Both paths
    // retain the same query-wide admission and semantic eligibility checks.
    let partial = partial_ownership(std::env::var("QE_AGG_OWNERSHIP").ok().as_deref())?;
    let mut evaluation_time = std::time::Duration::ZERO;
    let mut ingestion_time = std::time::Duration::ZERO;
    let mut input_rows = 0usize;
    let mut input_batches = 0usize;
    // Bind optional HAVING before consumption, but apply it only to final
    // groups after partial/spilled states merge. Unknown predicates retain the
    // ordinary route; no runtime error can replay source input.
    let output_filter = match post_filter {
        Some(predicate) => match crate::physical::operators::AdmittedBatchFilter::bind(
            predicate,
            schema.clone(),
            pool,
        )? {
            Some(bound) => Some(bound),
            None => return Ok(None),
        },
        None => None,
    };
    let plan_schema = PlanSchema::from_qualified_arrow(input.schema().as_ref());
    let binding_bytes = 512usize
        .checked_add(
            (groups.len() + aggregates.len())
                .checked_mul(512)
                .ok_or_else(|| invalid("binding size overflow"))?,
        )
        .ok_or_else(|| invalid("binding size overflow"))?;
    let _binding = pool.allocate(binding_bytes)?;
    let key_types = groups
        .iter()
        .map(|expr| expr.data_type(&plan_schema))
        .collect::<Result<Vec<_>>>()?;
    let slots = aggregates
        .iter()
        .map(|agg| {
            Ok((
                agg.func,
                if count_star(agg) {
                    DataType::Int64
                } else {
                    agg.input.data_type(&plan_schema)?
                },
                agg.distinct,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let Some(layout) = GroupLayout::bind(pool, &key_types, &slots)? else {
        return Ok(None);
    };
    GroupRows::new(layout.clone())?.validate_output_schema(schema.as_ref())?;
    config.ensure_spill_dir()?;
    let directory = RunDirectory::create(layout.clone(), &config.spill_path)?;
    let per_group = 64usize
        .checked_add(
            aggregates
                .len()
                .checked_mul(48)
                .ok_or_else(|| invalid("working size overflow"))?,
        )
        .ok_or_else(|| invalid("working size overflow"))?;
    let threshold = (config.memory_limit as f64 * config.spill_threshold) as usize;
    // Honor the configured spill threshold for count-based costing. A separate
    // quarter-pool ceiling forced fitting state to spill far below that policy.
    // Counts never prove byte usage: checked reservations still cover admitted
    // state/growth and output, and prepared writers let allocation denial flush
    // resident state before retrying the exact uncommitted input row.
    let group_limit = (threshold.min(pool.max()) / per_group).max(1);
    let workers = if groups.is_empty() {
        1
    } else {
        rayon::current_num_threads().min(if partial { 16 } else { 4 })
    };
    // Preparation may consume nested join builds and admit their input/output
    // owners. Complete it before reserving optional aggregate workers, so those
    // workers cannot starve initialization. A preparation error stays terminal;
    // the prepared frontier is never reconstructed to select fewer workers.
    let mut frontier = InputFrontier::new(input, pool).await?;
    // A prepared descriptor need not cover allocations on the first pull.
    // Own that first batch (or EOF) and its admission before optional workers
    // compete for space. Keep empty batches and error ordering unchanged.
    let mut primed = Some(frontier.next().await?);
    let mut primed_lease = match primed.as_ref().and_then(Option::as_ref) {
        Some(incoming) if !incoming.is_admitted() => Some(pool.allocate(
            crate::execution::retained_batch::retained_batch_bytes(&incoming.batch, pool)?,
        )?),
        _ => None,
    };
    let mut controller = if partial {
        ParallelControllers::new_partial(layout, directory, 8, 1024, group_limit, workers)?
    } else {
        ParallelControllers::new(layout, directory, 8, 1024, group_limit, workers)?
    };
    let ingestion_result: Result<()> = async {
        while let Some(incoming) = match primed.take() {
            Some(first) => first,
            None => frontier.next().await?,
        } {
            let batch = &incoming.batch;
            // Source construction keeps its provider contract. This lease accounts
            // for the received batch retained by this consumer while state grows.
            let input_lease = if let Some(lease) = primed_lease.take() {
                Some(lease)
            } else if incoming.is_admitted() {
                None
            } else {
                let admit = || {
                    let bytes =
                        crate::execution::retained_batch::retained_batch_bytes(batch, pool)?;
                    pool.allocate(bytes)
                };
                Some(match admit() {
                    Ok(lease) => lease,
                    Err(error) if error.is_memory_limit() => {
                        if !controller.release_for_input()? {
                            return Err(error);
                        }
                        admit()?
                    }
                    Err(error) => return Err(error),
                })
            };
            if profile {
                input_rows += batch.num_rows();
                input_batches += 1;
            }
            // Bound expression temporaries independently of the provider's batch
            // quantum. The incoming owner, demand permit and full backing lease
            // stay alive until every slice has been ingested.
            // Scheduling estimate only: allow roughly 64 bytes per evaluated
            // column/row, with a 256-byte floor for intermediate values. Actual
            // expression allocations still admit against the live shared pool.
            // No retry/re-evaluation on refusal (volatile expressions may exist).
            let columns = groups
                .len()
                .checked_add(aggregates.len())
                .ok_or_else(|| invalid("evaluated column count overflow"))?;
            let row_allowance = columns
                .checked_mul(64)
                .ok_or_else(|| invalid("evaluation quantum overflow"))?
                .max(256);
            let evaluation_rows = (pool.max() / row_allowance).clamp(1, 8192);
            let view_bytes = batch.columns().iter().try_fold(512usize, |total, array| {
                let metadata = array
                    .get_array_memory_size()
                    .checked_sub(array.get_buffer_memory_size())
                    .and_then(|bytes| bytes.checked_add(512))
                    .ok_or_else(|| invalid("input view metadata overflow"))?;
                total
                    .checked_add(metadata)
                    .ok_or_else(|| invalid("input view metadata overflow"))
            })?;
            for offset in (0..batch.num_rows().max(1)).step_by(evaluation_rows) {
                let rows = (batch.num_rows() - offset).min(evaluation_rows);
                let _view_metadata = if batch.num_rows() > evaluation_rows {
                    Some(pool.allocate(view_bytes)?)
                } else {
                    None
                };
                let view = (batch.num_rows() > evaluation_rows).then(|| batch.slice(offset, rows));
                let batch = view.as_ref().unwrap_or(batch);
                let _evaluated_metadata = pool.allocate(
                    columns
                        .checked_mul(512)
                        .and_then(|n| n.checked_add(512))
                        .ok_or_else(|| invalid("evaluated metadata overflow"))?,
                )?;
                let evaluation_start = profile.then(std::time::Instant::now);
                let evaluated = with_expression_pool(pool, || {
                    let mut arrays: Vec<ArrayRef> = Vec::new();
                    arrays
                        .try_reserve_exact(columns)
                        .map_err(|_| invalid("evaluated columns allocation refused"))?;
                    for expr in groups {
                        arrays.push(evaluate_expr(&batch, expr)?);
                    }
                    // COUNT(*) has an Int64 presence input. Use the admitted literal
                    // evaluator instead of constructing an unreserved wildcard array.
                    let count_one = Expr::literal(ScalarValue::Int64(1));
                    let values = crate::physical::operators::evaluate_aggregate_inputs(
                        &batch,
                        aggregates.len(),
                        |i| {
                            if count_star(&aggregates[i]) {
                                &count_one
                            } else {
                                &aggregates[i].input
                            }
                        },
                        Ok,
                    )?;
                    arrays.extend(values);
                    let fields: Vec<_> = arrays
                        .iter()
                        .enumerate()
                        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
                        .collect();
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                        .map_err(QueryError::from)
                })?;
                if let Some(start) = evaluation_start {
                    evaluation_time += start.elapsed();
                }
                let ingestion_start = profile.then(std::time::Instant::now);
                controller.ingest(&evaluated, groups.len())?;
                if let Some(start) = ingestion_start {
                    ingestion_time += start.elapsed();
                }
                drop(evaluated);
            }
            drop(incoming);
            drop(input_lease);
        }
        Ok(())
    }
    .await;
    frontier.shutdown().await;
    drop(frontier);
    ingestion_result?;
    drop(_binding);
    let mut batches = ReservedVec::with_capacity(pool, 0)?;
    let finish_start = profile.then(std::time::Instant::now);
    let mut output_time = std::time::Duration::ZERO;
    let mut output_rows = 0usize;
    let stats = controller.finish(|groups| {
        let output_start = profile.then(std::time::Instant::now);
        // Reset at each complete group owner: sibling states may have released
        // capacity. A successful short tail does not change the target.
        let mut quantum = super::output_quantum::TARGET_ROWS;
        let mut start = 0;
        while start < groups.len() {
            // Admit collection growth before constructing the next batch.
            batches.reserve(1)?;
            let batch = super::output_quantum::build_next(groups, &schema, start, &mut quantum)?;
            let rows = batch.num_rows();
            // Filtering and publication are never retried. Advance only after
            // successful publication or a successful filter discarding this range.
            let output = match &output_filter {
                Some(filter) => filter.apply(batch, pool)?,
                None => Some(batch),
            };
            if let Some(batch) = output {
                output_rows += batch.num_rows();
                batches.extend_reserved(1, std::iter::once(batch))?;
            }
            start += rows;
        }
        if let Some(start) = output_start {
            output_time += start.elapsed();
        }
        Ok(())
    })?;
    if let Some(start) = finish_start {
        eprintln!(
            "live_aggregate_profile input_rows={input_rows} input_batches={input_batches} evaluation_ms={:.3} ingestion_ms={:.3} finish_ms={:.3} output_ms={:.3} output_rows={output_rows} output_batches={} spilled_bytes={}",
            evaluation_time.as_secs_f64() * 1000.0,
            ingestion_time.as_secs_f64() * 1000.0,
            start.elapsed().as_secs_f64() * 1000.0,
            output_time.as_secs_f64() * 1000.0,
            batches.as_slice().len(),
            stats.spilled_bytes,
        );
    }
    if stats.spilled_bytes != 0 {
        pool.record_spill(
            usize::try_from(stats.spilled_bytes).map_err(|_| invalid("spill metric overflow"))?,
        );
    }
    Ok(Some(Box::pin(stream::iter(
        batches.into_owned_iter().map(Ok),
    ))))
}

#[cfg(test)]
mod ownership_selection_tests {
    #[test]
    fn explicit_ownership_selection_rejects_unknown_modes() {
        assert!(!super::partial_ownership(None).unwrap());
        assert!(!super::partial_ownership(Some("disjoint")).unwrap());
        assert!(super::partial_ownership(Some("partial")).unwrap());
        assert!(super::partial_ownership(Some("typo")).is_err());
    }
}
