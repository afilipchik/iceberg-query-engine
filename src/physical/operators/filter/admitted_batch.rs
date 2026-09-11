//! Reusable admitted filtering of an already evaluated flat batch.
use super::*;
use crate::execution::{reserved_vec::ReservedVec, MemoryPool, MemoryReservation};
use crate::physical::compiled_expr::CompiledPredicate;
use crate::storage::admitted_selection;

pub(crate) struct AdmittedBatchFilter {
    compiled: CompiledPredicate,
    columns: ReservedVec<usize>,
    schema: SchemaRef,
    _metadata: MemoryReservation,
}
impl AdmittedBatchFilter {
    /// Optional binding only: no source is consumed and no predicate evaluated.
    pub(crate) fn bind(
        predicate: &Expr,
        schema: SchemaRef,
        pool: &MemoryPool,
    ) -> Result<Option<Self>> {
        if predicate.contains_subquery()
            || schema.fields().is_empty()
            || schema
                .fields()
                .iter()
                .any(|f| !admitted_selection::supported(f.data_type()))
        {
            return Ok(None);
        }
        let prepare = || -> Result<Option<Self>> {
            let Some(compiled) = CompiledPredicate::compile_reserved(predicate, &schema, pool)?
            else {
                return Ok(None);
            };
            let metadata = pool.allocate(512)?;
            let mut columns = ReservedVec::with_capacity(pool, schema.fields().len())?;
            columns.extend_reserved(schema.fields().len(), 0..schema.fields().len())?;
            Ok(Some(Self {
                compiled,
                columns,
                schema: schema.clone(),
                _metadata: metadata,
            }))
        };
        match prepare() {
            Err(error) if error.is_memory_limit() => Ok(None),
            result => result,
        }
    }
    /// Once input has been evaluated, refusal is terminal; never replay it.
    pub(crate) fn apply(
        &self,
        batch: RecordBatch,
        pool: &MemoryPool,
    ) -> Result<Option<RecordBatch>> {
        let mask = self
            .compiled
            .evaluate_admitted(&batch, pool)?
            .ok_or_else(|| {
                QueryError::Execution(
                    "admitted filter: runtime predicate domain differs from binding".into(),
                )
            })?;
        if mask.len() != batch.num_rows() {
            return Err(QueryError::Execution(
                "admitted filter: mask length differs from input".into(),
            ));
        }
        let count = (0..mask.len())
            .filter(|i| mask.is_valid(*i) && mask.value(*i))
            .count();
        if count == 0 {
            return Ok(None);
        }
        if count == batch.num_rows() {
            return Ok(Some(batch));
        }
        let mut rows = ReservedVec::with_capacity(pool, count)?;
        rows.extend_reserved(
            count,
            (0..mask.len())
                .filter(|i| mask.is_valid(*i) && mask.value(*i))
                .map(|i| Some((0, i))),
        )?;
        admitted_selection::gather(
            std::slice::from_ref(&batch),
            rows.as_slice(),
            self.columns.as_slice(),
            self.schema.clone(),
            pool,
        )
        .map(Some)
    }
}
