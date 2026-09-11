//! Admission-owned compiled filtering; capability refusal happens before the
//! child is prepared, never after evaluating or consuming a source batch.
use super::*;
use crate::execution::{reserved_vec::ReservedVec, SharedMemoryPool};
use crate::physical::{plan::admit_stream, PreparedAdmittedInput};
fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("admitted filter: {message}"))
}
pub(super) async fn prepare(
    filter: &FilterExec,
    pool: SharedMemoryPool,
) -> Result<Option<PreparedAdmittedInput>> {
    if filter.initialized_membership.is_some() {
        return Ok(None);
    }
    let Some(bound) = AdmittedBatchFilter::bind(&filter.predicate, filter.schema.clone(), &pool)?
    else {
        return Ok(None);
    };
    let bound = Arc::new(bound);
    let Some(prepared) = filter
        .input
        .prepare_admitted_queue_input(pool.clone())
        .await?
    else {
        return Ok(None);
    };
    if !prepared.pool.is_within(&pool)
        || prepared.streams.as_slice().len() != filter.input.output_partitions()
    {
        return Err(invalid("child pool/partition mismatch"));
    }
    let runtime_pool = prepared.pool.clone();
    let mut streams = ReservedVec::with_capacity(&pool, prepared.streams.as_slice().len())?;
    for input in prepared.streams.into_owned_iter() {
        let state = (input, bound.clone(), runtime_pool.clone());
        streams.extend_reserved(
            1,
            [admit_stream(
                futures::stream::try_unfold(state, |(mut input, bound, pool)| async move {
                    loop {
                        let Some(batch) = input.try_next().await? else {
                            return Ok(None);
                        };
                        if let Some(output) = bound.apply(batch, &pool)? {
                            return Ok(Some((output, (input, bound, pool))));
                        }
                        tokio::task::yield_now().await;
                    }
                }),
                &pool,
            )?],
        )?;
    }
    Ok(Some(PreparedAdmittedInput {
        pool: runtime_pool,
        streams,
    }))
}
