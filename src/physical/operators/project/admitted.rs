//! A closed, typed program for admitted computed projections. Unsupported
//! expressions decline before child preparation; evaluation never falls back.
use super::*;
use crate::physical::operators::filter::temporal;
use crate::{
    execution::{reserved_vec::ReservedVec, MemoryPool, SharedMemoryPool},
    planner::{BinaryOp, CastMode, ScalarFunction, ScalarValue},
    QueryError,
};
use arrow::{
    array::{ArrayRef, Date32Array},
    datatypes::DataType,
};

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("admitted projection: {message}"))
}
fn primitive(t: &DataType) -> bool {
    t.is_integer() || matches!(t, DataType::Float32 | DataType::Float64)
}
fn admitted_cast(a: &DataType, b: &DataType) -> bool {
    a == b
        || (primitive(a) && primitive(b))
        || (a.is_integer() && matches!(b, DataType::Decimal128(..)))
}
fn arithmetic_domain(op: BinaryOp, a: &DataType, b: &DataType) -> bool {
    if !matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Subtract
            | BinaryOp::Multiply
            | BinaryOp::Divide
            | BinaryOp::Modulo
    ) {
        return false;
    }
    if primitive(a) && primitive(b) {
        return true;
    }
    let exact = |t: &DataType| t.is_integer() || matches!(t, DataType::Decimal128(..));
    op != BinaryOp::Divide && exact(a) && exact(b)
}
enum Op {
    Column(usize),
    Literal(ScalarValue),
    Binary {
        left: usize,
        op: BinaryOp,
        right: usize,
    },
    Cast {
        input: usize,
        mode: CastMode,
    },
    Extract {
        input: usize,
        part: arrow::compute::DatePart,
    },
}
struct Node {
    op: Op,
    datatype: DataType,
}
struct Program {
    nodes: ReservedVec<Node>,
    roots: ReservedVec<usize>,
    input: SchemaRef,
    output: SchemaRef,
}
impl Program {
    fn bind(project: &ProjectExec, pool: &MemoryPool) -> Result<Option<Self>> {
        if project.exprs.len() != project.schema.fields().len() {
            return Err(invalid("expression/schema width mismatch"));
        }
        let build = || -> Result<Option<Self>> {
            let mut program = Self {
                nodes: ReservedVec::with_capacity(pool, 0)?,
                roots: ReservedVec::with_capacity(pool, project.exprs.len())?,
                input: project.input.schema(),
                output: project.schema.clone(),
            };
            for (expr, field) in project.exprs.iter().zip(project.schema.fields()) {
                let Some(root) = program.compile(expr, 0)? else {
                    return Ok(None);
                };
                if &program.nodes.as_slice()[root].datatype != field.data_type() {
                    return Ok(None);
                }
                program.roots.extend_reserved(1, [root])?;
            }
            Ok(Some(program))
        };
        match build() {
            Err(error) if error.is_memory_limit() => Ok(None),
            result => result,
        }
    }
    fn compile(&mut self, expr: &Expr, depth: usize) -> Result<Option<usize>> {
        if depth > 64 {
            return Ok(None);
        }
        let (op, datatype) = match expr {
            Expr::Alias { expr, .. } => return self.compile(expr, depth + 1),
            Expr::Column(column) => {
                let index =
                    crate::physical::operators::find_column_index_in_schema(&self.input, column)?;
                let datatype = self.input.field(index).data_type().clone();
                if !crate::storage::admitted_selection::supported(&datatype) {
                    return Ok(None);
                }
                (Op::Column(index), datatype)
            }
            Expr::Literal(value) => {
                let datatype = value.data_type();
                if !(primitive(&datatype)
                    || matches!(datatype, DataType::Boolean | DataType::Decimal128(..)))
                {
                    return Ok(None);
                }
                (Op::Literal(value.clone()), datatype)
            }
            Expr::BinaryExpr { left, op, right } => {
                let Some(left) = self.compile(left, depth + 1)? else {
                    return Ok(None);
                };
                let Some(right) = self.compile(right, depth + 1)? else {
                    return Ok(None);
                };
                let a = &self.nodes.as_slice()[left].datatype;
                let b = &self.nodes.as_slice()[right].datatype;
                if !arithmetic_domain(*op, a, b) {
                    return Ok(None);
                }
                let datatype = crate::planner::numeric::arithmetic_type(*op, a, b)?;
                (
                    Op::Binary {
                        left,
                        op: *op,
                        right,
                    },
                    datatype,
                )
            }
            Expr::Cast {
                expr,
                data_type,
                mode,
            } => {
                let Some(input) = self.compile(expr, depth + 1)? else {
                    return Ok(None);
                };
                if !admitted_cast(&self.nodes.as_slice()[input].datatype, data_type) {
                    return Ok(None);
                }
                (Op::Cast { input, mode: *mode }, data_type.clone())
            }
            Expr::ScalarFunc {
                func: ScalarFunction::Extract,
                args,
            } if args.len() == 2 => {
                let Expr::Literal(ScalarValue::Utf8(field)) = &args[0] else {
                    return Ok(None);
                };
                let part = match temporal::part(field) {
                    Ok(part) => part,
                    Err(_) => return Ok(None),
                };
                let Some(input) = self.compile(&args[1], depth + 1)? else {
                    return Ok(None);
                };
                if self.nodes.as_slice()[input].datatype != DataType::Date32 {
                    return Ok(None);
                }
                (Op::Extract { input, part }, DataType::Int32)
            }
            _ => return Ok(None),
        };
        self.nodes.reserve(1)?;
        let index = self.nodes.as_slice().len();
        self.nodes.extend_reserved(1, [Node { op, datatype }])?;
        Ok(Some(index))
    }
    fn apply(&self, batch: RecordBatch, pool: &SharedMemoryPool) -> Result<RecordBatch> {
        if batch.num_columns() != self.input.fields().len() {
            return Err(invalid("runtime input width mismatch"));
        }
        let count = self.nodes.as_slice().len();
        // Bounded bookkeeping for synchronous evaluation and its temporary array
        // wrappers. Output payloads reserve separately and outlive these frames.
        let _frames = pool.allocate(
            count
                .checked_mul(1024)
                .ok_or_else(|| invalid("frame extent overflow"))?,
        )?;
        let mut values = ReservedVec::<Option<ArrayRef>>::with_capacity(pool, count)?;
        fn take(values: &mut ReservedVec<Option<ArrayRef>>, index: usize) -> Result<ArrayRef> {
            values
                .as_mut_slice()
                .get_mut(index)
                .and_then(Option::take)
                .ok_or_else(|| invalid("consumed or missing expression operand"))
        }
        for node in self.nodes.as_slice() {
            let value = match &node.op {
                Op::Column(index) => {
                    let array = batch
                        .columns()
                        .get(*index)
                        .ok_or_else(|| invalid("column outside runtime input"))?;
                    if array.data_type() != &node.datatype {
                        return Err(invalid("runtime column representation changed"));
                    }
                    array.clone()
                }
                Op::Literal(value) => {
                    crate::planner::reserved_literal::expand(pool, value, batch.num_rows())?
                }
                Op::Binary { left, op, right } => {
                    let l = take(&mut values, *left)?;
                    let r = take(&mut values, *right)?;
                    crate::execution::expression_memory::with_expression_pool(pool, || {
                        crate::planner::numeric::arithmetic(*op, &l, &r)
                    })?
                }
                Op::Cast { input, mode } => {
                    let input = take(&mut values, *input)?;
                    crate::execution::expression_memory::with_expression_pool(pool, || {
                        crate::planner::numeric::cast_array(&input, &node.datatype, *mode)
                    })?
                }
                Op::Extract { input, part } => {
                    let input = take(&mut values, *input)?;
                    let array = input
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| invalid("runtime EXTRACT representation changed"))?;
                    temporal::date32(pool, array, *part)?
                }
            };
            if value.len() != batch.num_rows() || value.data_type() != &node.datatype {
                return Err(invalid("computed output differs from bound type/length"));
            }
            values.extend_reserved(1, [Some(value)])?;
        }
        let mut arrays = ReservedVec::with_capacity(pool, self.roots.as_slice().len())?;
        for &root in self.roots.as_slice() {
            arrays.extend_reserved(1, [take(&mut values, root)?])?;
        }
        crate::storage::admitted_batch::finish(self.output.clone(), batch.num_rows(), arrays, pool)
    }
}

pub(super) async fn prepare(
    project: &ProjectExec,
    pool: SharedMemoryPool,
) -> Result<Option<crate::physical::PreparedAdmittedInput>> {
    let Some(program) = Program::bind(project, &pool)? else {
        return Ok(None);
    };
    let program = Arc::new(program);
    let Some(prepared) = project
        .input
        .prepare_admitted_queue_input(pool.clone())
        .await?
    else {
        return Ok(None);
    };
    if !prepared.pool.is_within(&pool)
        || prepared.streams.as_slice().len() != project.input.output_partitions()
    {
        return Err(invalid("child pool/partition mismatch"));
    }
    let runtime = prepared.pool.clone();
    let mut streams = ReservedVec::with_capacity(&pool, prepared.streams.as_slice().len())?;
    for input in prepared.streams.into_owned_iter() {
        let state = (input, program.clone(), runtime.clone());
        let output =
            futures::stream::try_unfold(state, |(mut input, program, runtime)| async move {
                let Some(batch) = input.try_next().await? else {
                    return Ok(None);
                };
                let output = program.apply(batch, &runtime)?;
                Ok(Some((output, (input, program, runtime))))
            });
        streams.extend_reserved(1, [crate::physical::admit_stream(output, &pool)?])?;
    }
    Ok(Some(crate::physical::PreparedAdmittedInput {
        pool: runtime,
        streams,
    }))
}

#[cfg(test)]
mod tests;
