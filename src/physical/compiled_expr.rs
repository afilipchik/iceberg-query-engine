//! Closure-compiled fused expression evaluation — the form of "query
//! compilation" that survives measurement in a vectorized engine.
//!
//! `examples/expr_compile_bench.rs` priced the alternatives: arrow's SIMD
//! kernels already win standalone expression evaluation (a naive fused loop
//! LOST at 0.638ms vs 0.544ms), so LLVM/Cranelift-style codegen buys nothing
//! there. What interpretation actually costs is the TEMPORARIES: a Q6-shaped
//! predicate runs five kernel passes and materializes five intermediate
//! arrays for a mask a single pass produces 4.4x faster.
//!
//! So this module compiles an [`Expr`] tree — once, at operator construction
//! — into a flat register program evaluated chunk-at-a-time (1024 rows) over
//! chunk-local register slabs. No intermediate
//! ArrayRefs, one pass over the input columns. Anything outside the
//! supported subset (strings, CASE, functions, subqueries, dictionary
//! columns, unsupported mixed-type comparisons...) makes `compile` return `None` and the
//! caller keeps the interpreter. Runtime type/NULL checks can also decline a batch.
//!
//! ## Equivalence contract
//!
//! The compiled path must be indistinguishable from `evaluate_expr` through
//! every consumer:
//! - arithmetic and comparisons are null-strict. Boolean AND/OR (including
//!   BETWEEN lowering) use SQL three-valued logic. Programs containing these
//!   instructions in the legacy evaluate API fall back for batches with
//!   referenced NULLs. The admitted API runs a reserved per-register validity
//!   program and supports these nullable Boolean expressions directly.
//! - f64 division by zero produces ±inf/NaN in both paths (never null).
//! - supported numeric/Float64 comparisons use Arrow-compatible chunk-local casts;
//!   exact decimal comparisons require a representable shared numeric type.
//!   Other mixed domains fall back to the interpreter.
//!
//! `QE_COMPILE=0` disables compilation everywhere (the established
//! diagnostic-switch pattern): consumers ask [`compilation_enabled`].

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::planner::{BinaryOp, Column, Expr, ScalarValue, UnaryOp};

mod decimal;
mod numeric_coercion;

const CHUNK: usize = 1024;
const MAX_REGS: usize = 24;

/// Comparison operator subset.
#[derive(Clone, Copy, Debug)]
enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    #[inline(always)]
    fn apply<T: PartialOrd>(self, a: T, b: T) -> bool {
        match self {
            Cmp::Eq => a == b,
            Cmp::Ne => a != b,
            Cmp::Lt => a < b,
            Cmp::Le => a <= b,
            Cmp::Gt => a > b,
            Cmp::Ge => a >= b,
        }
    }
}

/// One value source for a typed comparison leaf.
#[derive(Clone, Debug)]
enum Src {
    /// Index into the referenced-column table.
    Col(usize),
    LitF64(f64),
    LitI64(i64),
    LitI128(i128),
    LitI32(i32),
    LitUtf8(String),
    /// An f64 register holding a computed arithmetic result.
    Reg(u8),
}

/// Flat program instructions. F-registers hold f64 chunks, M-registers hold
/// boolean (u8 0/1) chunks.
#[derive(Clone, Debug)]
enum Instr {
    LikeUtf8 {
        source: Src,
        pattern: String,
        negated: bool,
        dst: u8,
    },
    CmpUtf8 {
        a: Src,
        b: Src,
        op: Cmp,
        dst: u8,
    },
    InUtf8 {
        source: Src,
        list: Vec<Option<String>>,
        negated: bool,
        dst: u8,
    },
    /// F[dst] = column values (f64 column).
    LoadF64 {
        col: usize,
        dst: u8,
    },
    CoerceF64 {
        col: usize,
        divisor: f64,
        dst: u8,
    },
    LitF64 {
        v: f64,
        dst: u8,
    },
    Arith {
        op: BinaryOp,
        a: u8,
        b: u8,
        dst: u8,
    },
    /// M[dst] = cmp(a, b) with both sides the same arrow type.
    CmpF64 {
        a: Src,
        b: Src,
        op: Cmp,
        dst: u8,
    },
    CmpDecimal {
        a: Src,
        b: Src,
        comparison: decimal::ScaledComparison,
        op: Cmp,
        dst: u8,
    },
    CmpI64 {
        a: Src,
        b: Src,
        op: Cmp,
        dst: u8,
    },
    CmpI32 {
        a: Src,
        b: Src,
        op: Cmp,
        dst: u8,
    },
    And {
        a: u8,
        b: u8,
        dst: u8,
    },
    Or {
        a: u8,
        b: u8,
        dst: u8,
    },
    Not {
        a: u8,
        dst: u8,
    },
}

/// Which typed column array a leaf refers to.
enum ColArr<'a> {
    F64(&'a Float64Array),
    I64(&'a Int64Array),
    Decimal(&'a Decimal128Array),
    I32(&'a Int32Array),
    Date32(&'a Date32Array),
    Utf8(&'a StringArray),
}

impl<'a> ColArr<'a> {
    fn as_any_array(&self) -> &'a dyn Array {
        match self {
            ColArr::F64(a) => *a,
            ColArr::I64(a) => *a,
            ColArr::Decimal(a) => *a,
            ColArr::I32(a) => *a,
            ColArr::Date32(a) => *a,
            ColArr::Utf8(a) => *a,
        }
    }
}

fn utf8_value<'a>(source: &'a Src, arrays: &'a [ColArr], row: usize) -> &'a str {
    match source {
        Src::LitUtf8(value) => value,
        Src::Col(index) => match &arrays[*index] {
            ColArr::Utf8(array) => array.value(row),
            _ => unreachable!("validated string column"),
        },
        _ => unreachable!("compiled string operand"),
    }
}

/// A compiled predicate: `Expr` -> boolean mask in one pass.
pub struct CompiledPredicate {
    /// Unqualified names + optional relation of every referenced column, in
    /// leaf-table order.
    cols: Vec<Column>,
    /// Arrow type each column must have at evaluation time.
    col_types: Vec<DataType>,
    prog: Vec<Instr>,
    /// The M-register holding the final mask.
    out: u8,
    f_regs: usize,
    m_regs: usize,
    _compile_reservation: Option<crate::execution::MemoryReservation>,
}

/// Is compilation enabled? `QE_COMPILE=0` restores the interpreter.
pub fn compilation_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("QE_COMPILE")
            .map(|v| v != "0")
            .unwrap_or(true)
    })
}

struct Compiler {
    cols: Vec<Column>,
    col_types: Vec<DataType>,
    prog: Vec<Instr>,
    next_f: u8,
    next_m: u8,
    extended: bool,
}

impl Compiler {
    fn new() -> Self {
        Self {
            cols: Vec::new(),
            col_types: Vec::new(),
            prog: Vec::new(),
            next_f: 0,
            next_m: 0,
            extended: false,
        }
    }

    fn col_slot(&mut self, c: &Column, dt: DataType) -> Option<usize> {
        if let Some(i) = self
            .cols
            .iter()
            .position(|k| k.name == c.name && k.relation == c.relation)
        {
            // The same column may appear at two types only if the schema is
            // inconsistent; keep the first and let evaluation re-check.
            return (self.col_types[i] == dt).then_some(i);
        }
        self.cols.push(c.clone());
        self.col_types.push(dt);
        Some(self.cols.len() - 1)
    }

    fn falloc(&mut self) -> Option<u8> {
        if (self.next_f as usize) >= MAX_REGS {
            return None;
        }
        self.next_f += 1;
        Some(self.next_f - 1)
    }

    fn malloc(&mut self) -> Option<u8> {
        if (self.next_m as usize) >= MAX_REGS {
            return None;
        }
        self.next_m += 1;
        Some(self.next_m - 1)
    }

    /// Compile a numeric (f64) expression into an F-register.
    fn num_f64(&mut self, e: &Expr, schema: &arrow::datatypes::Schema) -> Option<u8> {
        match e {
            Expr::Column(c) => {
                let f = find_field(schema, c)?;
                if f.data_type() != &DataType::Float64 {
                    return None;
                }
                let slot = self.col_slot(c, DataType::Float64)?;
                let dst = self.falloc()?;
                self.prog.push(Instr::LoadF64 { col: slot, dst });
                Some(dst)
            }
            Expr::Literal(v) => {
                let x = lit_f64(v)?;
                let dst = self.falloc()?;
                self.prog.push(Instr::LitF64 { v: x, dst });
                Some(dst)
            }
            Expr::BinaryExpr { left, op, right }
                if matches!(
                    op,
                    BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide
                ) =>
            {
                let a = self.num_f64(left, schema)?;
                let b = self.num_f64(right, schema)?;
                let dst = self.falloc()?;
                self.prog.push(Instr::Arith { op: *op, a, b, dst });
                Some(dst)
            }
            Expr::Alias { expr, .. } => self.num_f64(expr, schema),
            Expr::Cast {
                expr, data_type, ..
            } if data_type == &DataType::Float64 => {
                // A no-op cast (f64 -> f64) still occurs in bound plans.
                self.num_f64(expr, schema)
            }
            _ => None,
        }
    }

    /// A comparison SIDE: a typed column, literal, or computed f64.
    fn side(&mut self, e: &Expr, schema: &arrow::datatypes::Schema) -> Option<(Src, DataType)> {
        match e {
            Expr::Column(c) => {
                let f = find_field(schema, c)?;
                let dt = f.data_type().clone();
                match dt {
                    DataType::Float64 | DataType::Int64 | DataType::Int32 | DataType::Date32 => {
                        let slot = self.col_slot(c, dt.clone())?;
                        Some((Src::Col(slot), dt))
                    }
                    DataType::Decimal128(_, _) => {
                        let slot = self.col_slot(c, dt.clone())?;
                        Some((Src::Col(slot), dt))
                    }
                    DataType::Utf8 if self.extended => {
                        let slot = self.col_slot(c, DataType::Utf8)?;
                        Some((Src::Col(slot), DataType::Utf8))
                    }
                    _ => None,
                }
            }
            Expr::Literal(v) => match v {
                ScalarValue::Float64(x) => Some((Src::LitF64((*x).into()), DataType::Float64)),
                ScalarValue::Int64(x) => Some((Src::LitI64(*x), DataType::Int64)),
                ScalarValue::Decimal128(x) => Some((
                    Src::LitI128(x.mantissa()),
                    DataType::Decimal128(38, x.scale()),
                )),
                ScalarValue::Int32(x) => Some((Src::LitI32(*x), DataType::Int32)),
                ScalarValue::Date32(x) => Some((Src::LitI32(*x), DataType::Date32)),
                ScalarValue::Utf8(x) if self.extended => {
                    Some((Src::LitUtf8(x.clone()), DataType::Utf8))
                }
                _ => None,
            },
            Expr::Alias { expr, .. } => self.side(expr, schema),
            // A computed numeric side: f64 only.
            Expr::BinaryExpr { op, .. }
                if matches!(
                    op,
                    BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide
                ) =>
            {
                let r = self.num_f64(e, schema)?;
                Some((Src::Reg(r), DataType::Float64))
            }
            _ => None,
        }
    }

    /// Compile a boolean expression into an M-register.
    fn boolean(&mut self, e: &Expr, schema: &arrow::datatypes::Schema) -> Option<u8> {
        match e {
            Expr::InList {
                expr,
                list,
                negated,
            } if self.extended && !list.is_empty() => {
                let (source, dt) = self.side(expr, schema)?;
                if dt != DataType::Utf8 {
                    return None;
                }
                let list = list
                    .iter()
                    .map(|e| match e {
                        Expr::Literal(ScalarValue::Utf8(s)) => Some(Some(s.clone())),
                        Expr::Literal(ScalarValue::Null) => Some(None),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                let dst = self.malloc()?;
                self.prog.push(Instr::InUtf8 {
                    source,
                    list,
                    negated: *negated,
                    dst,
                });
                Some(dst)
            }
            Expr::BinaryExpr { left, op, right } => match op {
                BinaryOp::Like | BinaryOp::NotLike if self.extended => {
                    let (source, dt) = self.side(left, schema)?;
                    if dt != DataType::Utf8 {
                        return None;
                    }
                    let Expr::Literal(ScalarValue::Utf8(pattern)) = right.as_ref() else {
                        return None;
                    };
                    let dst = self.malloc()?;
                    self.prog.push(Instr::LikeUtf8 {
                        source,
                        pattern: pattern.clone(),
                        negated: *op == BinaryOp::NotLike,
                        dst,
                    });
                    Some(dst)
                }
                BinaryOp::And | BinaryOp::Or => {
                    let a = self.boolean(left, schema)?;
                    let b = self.boolean(right, schema)?;
                    let dst = self.malloc()?;
                    self.prog.push(if matches!(op, BinaryOp::And) {
                        Instr::And { a, b, dst }
                    } else {
                        Instr::Or { a, b, dst }
                    });
                    Some(dst)
                }
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq => {
                    let cmp = match op {
                        BinaryOp::Eq => Cmp::Eq,
                        BinaryOp::NotEq => Cmp::Ne,
                        BinaryOp::Lt => Cmp::Lt,
                        BinaryOp::LtEq => Cmp::Le,
                        BinaryOp::Gt => Cmp::Gt,
                        BinaryOp::GtEq => Cmp::Ge,
                        _ => unreachable!(),
                    };
                    let (a, ta) = self.side(left, schema)?;
                    let (b, tb) = self.side(right, schema)?;
                    if ta != tb && (ta == DataType::Float64 || tb == DataType::Float64) {
                        let a = self.coerce_float(a, &ta)?;
                        let b = self.coerce_float(b, &tb)?;
                        let dst = self.malloc()?;
                        self.prog.push(Instr::CmpF64 { a, b, op: cmp, dst });
                        return Some(dst);
                    }
                    if matches!(ta, DataType::Decimal128(_, _))
                        || matches!(tb, DataType::Decimal128(_, _))
                    {
                        crate::planner::numeric::common_type(&ta, &tb).ok()?;
                        let scale = |dt: &DataType| match dt {
                            DataType::Decimal128(_, s) => Some(*s),
                            DataType::Int64 | DataType::Int32 => Some(0),
                            _ => None,
                        };
                        let comparison = decimal::ScaledComparison::new(scale(&ta)?, scale(&tb)?);
                        let dst = self.malloc()?;
                        self.prog.push(Instr::CmpDecimal {
                            a,
                            b,
                            comparison,
                            op: cmp,
                            dst,
                        });
                        return Some(dst);
                    }
                    // Identical arrow types only — anything the interpreter
                    // would COERCE falls back to the interpreter.
                    if ta != tb {
                        return None;
                    }
                    let dst = self.malloc()?;
                    self.prog.push(match ta {
                        DataType::Float64 => Instr::CmpF64 { a, b, op: cmp, dst },
                        DataType::Int64 => Instr::CmpI64 { a, b, op: cmp, dst },
                        DataType::Utf8 if self.extended => Instr::CmpUtf8 { a, b, op: cmp, dst },
                        DataType::Int32 | DataType::Date32 => Instr::CmpI32 { a, b, op: cmp, dst },
                        _ => return None,
                    });
                    Some(dst)
                }
                _ => None,
            },
            Expr::UnaryExpr {
                op: UnaryOp::Not,
                expr,
            } => {
                let a = self.boolean(expr, schema)?;
                let dst = self.malloc()?;
                self.prog.push(Instr::Not { a, dst });
                Some(dst)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // expr >= low AND expr <= high — the interpreter's own
                // lowering (filter.rs uses ge/le + boolean::and_kleene).
                let ge = self.boolean(
                    &Expr::BinaryExpr {
                        left: expr.clone(),
                        op: BinaryOp::GtEq,
                        right: low.clone(),
                    },
                    schema,
                )?;
                let le = self.boolean(
                    &Expr::BinaryExpr {
                        left: expr.clone(),
                        op: BinaryOp::LtEq,
                        right: high.clone(),
                    },
                    schema,
                )?;
                let dst = self.malloc()?;
                self.prog.push(Instr::And { a: ge, b: le, dst });
                if *negated {
                    let ndst = self.malloc()?;
                    self.prog.push(Instr::Not { a: dst, dst: ndst });
                    Some(ndst)
                } else {
                    Some(dst)
                }
            }
            Expr::Alias { expr, .. } => self.boolean(expr, schema),
            _ => None,
        }
    }
}

fn lit_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Float64(x) => Some((*x).into()),
        _ => None,
    }
}

fn find_field<'a>(
    schema: &'a arrow::datatypes::Schema,
    c: &Column,
) -> Option<&'a arrow::datatypes::Field> {
    crate::planner::resolve_arrow_column(schema, c).map(|index| schema.field(index))
}

/// Bounded borrowed preflight before the recursive compiler clones any AST,
/// names, literals or type metadata. These are compiler capability limits, not
/// SQL limits; unsupported shapes must be selected away before input execution.
fn compilation_envelope(expr: &Expr, schema: &arrow::datatypes::Schema) -> Option<usize> {
    fn visit(
        expr: &Expr,
        schema: &arrow::datatypes::Schema,
        depth: usize,
        nodes: &mut usize,
        bytes: &mut usize,
    ) -> Option<()> {
        if depth > 64 || *nodes >= 4096 {
            return None;
        }
        *nodes += 1;
        match expr {
            Expr::Column(column) => {
                if !matches!(
                    find_field(schema, column)?.data_type(),
                    DataType::Float64
                        | DataType::Int64
                        | DataType::Int32
                        | DataType::Date32
                        | DataType::Utf8
                        | DataType::Decimal128(_, _)
                ) {
                    return None;
                }
                *bytes = bytes
                    .checked_add(column.name.len())?
                    .checked_add(column.relation.as_ref().map_or(0, String::len))?;
            }
            Expr::Literal(ScalarValue::Utf8(value)) => *bytes = bytes.checked_add(value.len())?,
            Expr::Literal(
                ScalarValue::Float64(_)
                | ScalarValue::Int64(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Date32(_)
                | ScalarValue::Decimal128(_)
                | ScalarValue::Null,
            ) => {}
            Expr::BinaryExpr { left, right, .. } => {
                visit(left, schema, depth + 1, nodes, bytes)?;
                visit(right, schema, depth + 1, nodes, bytes)?;
            }
            Expr::UnaryExpr {
                op: UnaryOp::Not,
                expr,
            } => visit(expr, schema, depth + 1, nodes, bytes)?,
            Expr::Between {
                expr, low, high, ..
            } => {
                visit(expr, schema, depth + 1, nodes, bytes)?;
                visit(low, schema, depth + 1, nodes, bytes)?;
                visit(high, schema, depth + 1, nodes, bytes)?;
            }
            Expr::InList { expr, list, .. } => {
                visit(expr, schema, depth + 1, nodes, bytes)?;
                for item in list {
                    visit(item, schema, depth + 1, nodes, bytes)?;
                }
            }
            Expr::Alias { expr, name } => {
                *bytes = bytes.checked_add(name.len())?;
                visit(expr, schema, depth + 1, nodes, bytes)?;
            }
            Expr::Cast {
                expr, data_type, ..
            } if data_type == &DataType::Float64 => visit(expr, schema, depth + 1, nodes, bytes)?,
            _ => return None,
        }
        Some(())
    }
    let mut nodes = 0;
    let mut bytes = 0;
    visit(expr, schema, 0, &mut nodes, &mut bytes)?;
    // Fourfold typed storage covers Vec growth overlap and retained program
    // entries plus temporary BETWEEN AST clones. String payload is counted
    // separately; per-node owner allowances cover the small individual heaps.
    // The fixed allowance covers bounded recursive compiler stack/scratch.
    let typed = std::mem::size_of::<Expr>()
        + std::mem::size_of::<Instr>()
        + std::mem::size_of::<Src>()
        + std::mem::size_of::<Column>()
        + std::mem::size_of::<DataType>()
        + std::mem::size_of::<Option<String>>();
    nodes
        .checked_mul(typed.checked_mul(4)?.checked_add(8 * 512)?)?
        .checked_add(bytes.checked_mul(4)?)?
        .checked_add(64 * 4096)
}

impl CompiledPredicate {
    /// Compile `expr` against `schema`, or `None` when any part is outside
    /// the supported subset (the caller keeps the interpreter).
    pub fn compile(expr: &Expr, schema: &arrow::datatypes::Schema) -> Option<CompiledPredicate> {
        Self::compile_mode(expr, schema, false)
    }
    /// Expanded capability for the admitted evaluator only. Compilation and
    /// program/literal storage remain caller-owned, not admitted by this method.
    pub(crate) fn compile_for_admitted_evaluation(
        expr: &Expr,
        schema: &arrow::datatypes::Schema,
    ) -> Option<CompiledPredicate> {
        Self::compile_mode(expr, schema, true)
    }
    /// Reserve before compilation, then shrink to retained vector/string
    /// capacities plus owner allowances for the program's lifetime. No refusal becomes an interpreter fallback.
    /// The input Expr/schema are borrowed and remain owned by the caller.
    pub(crate) fn compile_reserved(
        expr: &Expr,
        schema: &arrow::datatypes::Schema,
        pool: &crate::execution::MemoryPool,
    ) -> crate::error::Result<Option<Self>> {
        if !compilation_enabled() {
            return Ok(None);
        }
        let Some(bytes) = compilation_envelope(expr, schema) else {
            return Ok(None);
        };
        let mut reservation = pool.allocate(bytes)?;
        let Some(mut compiled) = Self::compile_mode(expr, schema, true) else {
            return Ok(None);
        };
        let retained = compiled.retained_compilation_bytes().ok_or_else(|| {
            crate::QueryError::Execution("compiled storage extent overflow".into())
        })?;
        if retained > reservation.size() {
            return Err(crate::QueryError::Execution(
                "compiled storage exceeds construction envelope".into(),
            ));
        }
        reservation.resize(retained)?;
        compiled._compile_reservation = Some(reservation);
        Ok(Some(compiled))
    }
    fn retained_compilation_bytes(&self) -> Option<usize> {
        fn vector(bytes: &mut usize, capacity: usize, size: usize) -> Option<()> {
            *bytes = bytes
                .checked_add(capacity.checked_mul(size)?)?
                .checked_add(512)?;
            Some(())
        }
        fn string(bytes: &mut usize, value: &String) -> Option<()> {
            vector(bytes, value.capacity(), 1)
        }
        fn source(bytes: &mut usize, value: &Src) -> Option<()> {
            if let Src::LitUtf8(value) = value {
                string(bytes, value)?;
            }
            Some(())
        }
        let mut bytes = std::mem::size_of::<Self>().checked_add(512)?;
        vector(
            &mut bytes,
            self.cols.capacity(),
            std::mem::size_of::<Column>(),
        )?;
        vector(
            &mut bytes,
            self.col_types.capacity(),
            std::mem::size_of::<DataType>(),
        )?;
        vector(
            &mut bytes,
            self.prog.capacity(),
            std::mem::size_of::<Instr>(),
        )?;
        for column in &self.cols {
            string(&mut bytes, &column.name)?;
            if let Some(relation) = &column.relation {
                string(&mut bytes, relation)?;
            }
        }
        for instruction in &self.prog {
            match instruction {
                Instr::CmpF64 { a, b, .. }
                | Instr::CmpI64 { a, b, .. }
                | Instr::CmpI32 { a, b, .. }
                | Instr::CmpUtf8 { a, b, .. } => {
                    source(&mut bytes, a)?;
                    source(&mut bytes, b)?;
                }
                Instr::LikeUtf8 {
                    source: value,
                    pattern,
                    ..
                } => {
                    source(&mut bytes, value)?;
                    string(&mut bytes, pattern)?;
                }
                Instr::InUtf8 {
                    source: value,
                    list,
                    ..
                } => {
                    source(&mut bytes, value)?;
                    vector(
                        &mut bytes,
                        list.capacity(),
                        std::mem::size_of::<Option<String>>(),
                    )?;
                    for value in list.iter().flatten() {
                        string(&mut bytes, value)?;
                    }
                }
                _ => {}
            }
        }
        Some(bytes)
    }
    fn compile_mode(
        expr: &Expr,
        schema: &arrow::datatypes::Schema,
        extended: bool,
    ) -> Option<CompiledPredicate> {
        if !compilation_enabled() {
            return None;
        }
        let mut c = Compiler::new();
        c.extended = extended;
        let out = c.boolean(expr, schema)?;
        Some(CompiledPredicate {
            cols: c.cols,
            col_types: c.col_types,
            prog: c.prog,
            out,
            f_regs: c.next_f as usize,
            m_regs: c.next_m as usize,
            _compile_reservation: None,
        })
    }

    /// Evaluate to the same mask the interpreter's kernel chain produces.
    /// Returns `None` when the batch's actual column types diverge from the
    /// compiled assumptions (dictionary-encoded batches, schema drift), or
    /// nullable Boolean programs require per-register validity —
    /// the caller falls back for THIS batch.
    pub fn evaluate(&self, batch: &RecordBatch) -> Option<BooleanArray> {
        if self.prog.iter().any(|i| {
            matches!(
                i,
                Instr::CmpUtf8 { .. } | Instr::InUtf8 { .. } | Instr::LikeUtf8 { .. }
            )
        }) {
            return None;
        }
        let n = batch.num_rows();
        // Resolve + type-check every referenced column.
        let mut arrays: Vec<ColArr> = Vec::with_capacity(self.cols.len());
        for (c, dt) in self.cols.iter().zip(self.col_types.iter()) {
            let idx = find_batch_column(batch, c)?;
            let arr = batch.column(idx);
            if arr.data_type() != dt {
                return None;
            }
            arrays.push(match dt {
                DataType::Float64 => ColArr::F64(arr.as_any().downcast_ref()?),
                DataType::Int64 => ColArr::I64(arr.as_any().downcast_ref()?),
                DataType::Decimal128(_, _) => ColArr::Decimal(arr.as_any().downcast_ref()?),
                DataType::Int32 => ColArr::I32(arr.as_any().downcast_ref()?),
                DataType::Date32 => ColArr::Date32(arr.as_any().downcast_ref()?),
                _ => return None,
            });
        }

        let any_nulls = arrays.iter().any(|a| a.as_any_array().null_count() > 0);
        // A global leaf-validity intersection cannot represent false AND
        // unknown or true OR unknown. Keep the null-free fused path, and let
        // the interpreter apply Kleene logic to nullable Boolean programs.
        if any_nulls
            && self
                .prog
                .iter()
                .any(|i| matches!(i, Instr::And { .. } | Instr::Or { .. }))
        {
            return None;
        }

        let mut f_slabs = vec![[0f64; CHUNK]; self.f_regs.max(1)];
        let mut m_slabs = vec![[0u8; CHUNK]; self.m_regs.max(1)];
        let mut out_builder = arrow::array::builder::BooleanBufferBuilder::new(n);
        let mut valid_bits: Option<Vec<bool>> = any_nulls.then(|| Vec::with_capacity(n));

        let mut start = 0usize;
        while start < n {
            let len = (n - start).min(CHUNK);
            self.eval_chunk(&arrays, start, len, &mut f_slabs, &mut m_slabs);
            let out = &m_slabs[self.out as usize];
            // Pack the 0/1 chunk into bytes, append as a range — the per-bit
            // append call was measurably the hot spot.
            let mut packed = [0u8; CHUNK / 8];
            let full = len / 8;
            for bi in 0..full {
                let o = bi * 8;
                packed[bi] = out[o]
                    | (out[o + 1] << 1)
                    | (out[o + 2] << 2)
                    | (out[o + 3] << 3)
                    | (out[o + 4] << 4)
                    | (out[o + 5] << 5)
                    | (out[o + 6] << 6)
                    | (out[o + 7] << 7);
            }
            for i in (full * 8)..len {
                if out[i] != 0 {
                    packed[i / 8] |= 1 << (i % 8);
                }
            }
            out_builder.append_packed_range(0..len, &packed);
            if let Some(vb) = valid_bits.as_mut() {
                // Null-strict subset: a row is valid iff every referenced
                // column is valid — identical to kernel-chain propagation.
                for i in 0..len {
                    let row = start + i;
                    vb.push(arrays.iter().all(|a| a.as_any_array().is_valid(row)));
                }
            }
            start += len;
        }

        let values = arrow::buffer::BooleanBuffer::new(out_builder.finish().into_inner(), 0, n);
        Some(match valid_bits {
            None => BooleanArray::new(values, None),
            Some(vb) => {
                let nulls = arrow::buffer::NullBuffer::from(vb);
                BooleanArray::new(values, Some(nulls))
            }
        })
    }

    /// Evaluate the existing register program with admitted column handles,
    /// register slabs and output bitmaps. None indicates a runtime type decline; nullable Boolean programs use per-register validity.
    /// Allocation refusal is an error, never a fallback signal.
    /// Compilation/program/schema ownership remains with the caller.
    pub(crate) fn evaluate_admitted(
        &self,
        batch: &RecordBatch,
        pool: &crate::execution::MemoryPool,
    ) -> crate::error::Result<Option<BooleanArray>> {
        use crate::execution::{reserved_vec::ReservedVec, ReservedBufferBuilder};
        use arrow::buffer::{BooleanBuffer, NullBuffer};
        let mut arrays = ReservedVec::with_capacity(pool, self.cols.len())?;
        for (c, dt) in self.cols.iter().zip(self.col_types.iter()) {
            let Some(index) = find_batch_column(batch, c) else {
                return Ok(None);
            };
            let array = batch.column(index);
            if array.data_type() != dt {
                return Ok(None);
            }
            let resolved = match dt {
                DataType::Float64 => array.as_any().downcast_ref().map(ColArr::F64),
                DataType::Int64 => array.as_any().downcast_ref().map(ColArr::I64),
                DataType::Decimal128(_, _) => array.as_any().downcast_ref().map(ColArr::Decimal),
                DataType::Int32 => array.as_any().downcast_ref().map(ColArr::I32),
                DataType::Date32 => array.as_any().downcast_ref().map(ColArr::Date32),
                DataType::Utf8 => array.as_any().downcast_ref().map(ColArr::Utf8),
                _ => None,
            };
            let Some(resolved) = resolved else {
                return Ok(None);
            };
            arrays.extend_reserved(1, [resolved])?;
        }
        let any_nulls = arrays
            .as_slice()
            .iter()
            .any(|a| a.as_any_array().null_count() > 0)
            || self
                .prog
                .iter()
                .any(|i| matches!(i,Instr::InUtf8 { list,.. } if list.iter().any(Option::is_none)));
        let mut f = ReservedVec::with_capacity(pool, self.f_regs.max(1))?;
        f.extend_reserved(self.f_regs.max(1), std::iter::repeat([0f64; CHUNK]))?;
        let mut m = ReservedVec::with_capacity(pool, self.m_regs.max(1))?;
        m.extend_reserved(self.m_regs.max(1), std::iter::repeat([0u8; CHUNK]))?;
        let mut register_validity = if any_nulls {
            let mut f = ReservedVec::with_capacity(pool, self.f_regs.max(1))?;
            f.extend_reserved(self.f_regs.max(1), std::iter::repeat([0u8; CHUNK]))?;
            let mut m = ReservedVec::with_capacity(pool, self.m_regs.max(1))?;
            m.extend_reserved(self.m_regs.max(1), std::iter::repeat([0u8; CHUNK]))?;
            Some((f, m))
        } else {
            None
        };
        let rows = batch.num_rows();
        let bytes = rows.div_ceil(8);
        let mut output = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
        output.extend_reserved(bytes, std::iter::repeat(0))?;
        let mut validity = if any_nulls {
            let mut bitmap = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
            bitmap.extend_reserved(bytes, std::iter::repeat(0))?;
            Some(bitmap)
        } else {
            None
        };
        for start in (0..rows).step_by(CHUNK) {
            let len = (rows - start).min(CHUNK);
            self.eval_chunk(
                arrays.as_slice(),
                start,
                len,
                f.as_mut_slice(),
                m.as_mut_slice(),
            );
            let values = &m.as_slice()[self.out as usize];
            for (byte_index, chunk) in values[..len].chunks(8).enumerate() {
                let mut packed = 0;
                for (bit, value) in chunk.iter().enumerate() {
                    packed |= *value << bit;
                }
                output.as_mut_slice()[start / 8 + byte_index] = packed;
            }
            if let (Some(validity), Some((fv, mv))) =
                (validity.as_mut(), register_validity.as_mut())
            {
                self.validity_chunk(
                    arrays.as_slice(),
                    start,
                    len,
                    m.as_slice(),
                    fv.as_mut_slice(),
                    mv.as_mut_slice(),
                );
                for i in 0..len {
                    if mv.as_slice()[self.out as usize][i] != 0 {
                        let row = start + i;
                        validity.as_mut_slice()[row / 8] |= 1 << (row % 8);
                    }
                }
            }
        }
        Ok(Some(BooleanArray::new(
            BooleanBuffer::new(output.finish(), 0, rows),
            validity.map(|bits| NullBuffer::new(BooleanBuffer::new(bits.finish(), 0, rows))),
        )))
    }

    /// Parallel validity program for the admitted evaluator. Boolean values
    /// already computed by eval_chunk are meaningful whenever this program
    /// marks them valid, including false AND unknown and true OR unknown.
    fn validity_chunk(
        &self,
        arrays: &[ColArr],
        start: usize,
        len: usize,
        values: &[[u8; CHUNK]],
        f: &mut [[u8; CHUNK]],
        m: &mut [[u8; CHUNK]],
    ) {
        fn source(src: &Src, arrays: &[ColArr], row: usize, i: usize, f: &[[u8; CHUNK]]) -> u8 {
            match src {
                Src::Col(c) => arrays[*c].as_any_array().is_valid(row) as u8,
                Src::Reg(r) => f[*r as usize][i],
                _ => 1,
            }
        }
        for instr in &self.prog {
            for i in 0..len {
                match instr {
                    Instr::LikeUtf8 {
                        source: src, dst, ..
                    } => m[*dst as usize][i] = source(src, arrays, start + i, i, f),
                    Instr::CmpUtf8 { a, b, dst, .. } => {
                        m[*dst as usize][i] =
                            source(a, arrays, start + i, i, f) & source(b, arrays, start + i, i, f)
                    }
                    Instr::InUtf8 {
                        source: src,
                        list,
                        dst,
                        ..
                    } => {
                        let valid = source(src, arrays, start + i, i, f) != 0;
                        let found = list
                            .iter()
                            .flatten()
                            .any(|s| s == utf8_value(src, arrays, start + i));
                        m[*dst as usize][i] =
                            (valid && (found || !list.iter().any(Option::is_none))) as u8;
                    }
                    Instr::LoadF64 { col, dst } | Instr::CoerceF64 { col, dst, .. } => {
                        f[*dst as usize][i] = arrays[*col].as_any_array().is_valid(start + i) as u8
                    }
                    Instr::LitF64 { dst, .. } => f[*dst as usize][i] = 1,
                    Instr::Arith { a, b, dst, .. } => {
                        f[*dst as usize][i] = f[*a as usize][i] & f[*b as usize][i]
                    }
                    Instr::CmpF64 { a, b, dst, .. }
                    | Instr::CmpDecimal { a, b, dst, .. }
                    | Instr::CmpI64 { a, b, dst, .. }
                    | Instr::CmpI32 { a, b, dst, .. } => {
                        m[*dst as usize][i] =
                            source(a, arrays, start + i, i, f) & source(b, arrays, start + i, i, f)
                    }
                    Instr::Not { a, dst } => m[*dst as usize][i] = m[*a as usize][i],
                    Instr::And { a, b, dst } => {
                        let av = m[*a as usize][i];
                        let bv = m[*b as usize][i];
                        m[*dst as usize][i] = (av & bv)
                            | (av & (1 - values[*a as usize][i]))
                            | (bv & (1 - values[*b as usize][i]));
                    }
                    Instr::Or { a, b, dst } => {
                        let av = m[*a as usize][i];
                        let bv = m[*b as usize][i];
                        m[*dst as usize][i] = (av & bv)
                            | (av & values[*a as usize][i])
                            | (bv & values[*b as usize][i]);
                    }
                }
            }
        }
    }

    fn eval_chunk(
        &self,
        arrays: &[ColArr],
        start: usize,
        len: usize,
        f: &mut [[f64; CHUNK]],
        m: &mut [[u8; CHUNK]],
    ) {
        // Register allocation is SSA-shaped: a destination register is always
        // freshly allocated, so dst > every operand register — which is what
        // makes the split_at_mut borrows below safe AND branch-free inside
        // the row loops. Operands are hoisted to slices/scalars per chunk;
        // the inner loops compile to straight-line vectorizable code.
        enum FOp<'s> {
            Slice(&'s [f64]),
            Scalar(f64),
        }
        enum IOp<'s> {
            Slice(&'s [i64]),
            Scalar(i64),
        }
        enum I32Op<'s> {
            Slice(&'s [i32]),
            Scalar(i32),
        }

        macro_rules! cmp_shapes {
            ($a:expr, $b:expr, $dst:expr, $OpTy:ident, $cmp:tt) => {{
                let d = &mut m[$dst as usize];
                match ($a, $b) {
                    ($OpTy::Slice(x), $OpTy::Slice(y)) => {
                        for i in 0..len {
                            d[i] = (x[i] $cmp y[i]) as u8;
                        }
                    }
                    ($OpTy::Slice(x), $OpTy::Scalar(y)) => {
                        for i in 0..len {
                            d[i] = (x[i] $cmp y) as u8;
                        }
                    }
                    ($OpTy::Scalar(x), $OpTy::Slice(y)) => {
                        for i in 0..len {
                            d[i] = (x $cmp y[i]) as u8;
                        }
                    }
                    ($OpTy::Scalar(x), $OpTy::Scalar(y)) => {
                        let v = (x $cmp y) as u8;
                        d[..len].fill(v);
                    }
                }
            }};
        }
        // The operator match happens ONCE per chunk; every inner loop is
        // monomorphic and vectorizes.
        macro_rules! cmp_loop {
            ($a:expr, $b:expr, $op:expr, $dst:expr, $OpTy:ident) => {{
                match $op {
                    Cmp::Eq => cmp_shapes!($a, $b, $dst, $OpTy, ==),
                    Cmp::Ne => cmp_shapes!($a, $b, $dst, $OpTy, !=),
                    Cmp::Lt => cmp_shapes!($a, $b, $dst, $OpTy, <),
                    Cmp::Le => cmp_shapes!($a, $b, $dst, $OpTy, <=),
                    Cmp::Gt => cmp_shapes!($a, $b, $dst, $OpTy, >),
                    Cmp::Ge => cmp_shapes!($a, $b, $dst, $OpTy, >=),
                }
            }};
        }

        for ins in &self.prog {
            match ins {
                Instr::LoadF64 { col, dst } => {
                    if let ColArr::F64(a) = &arrays[*col] {
                        f[*dst as usize][..len].copy_from_slice(&a.values()[start..start + len]);
                    }
                }
                Instr::CoerceF64 { col, divisor, dst } => {
                    let output = &mut f[*dst as usize][..len];
                    match &arrays[*col] {
                        ColArr::I32(a) => {
                            for (out, value) in
                                output.iter_mut().zip(&a.values()[start..start + len])
                            {
                                *out = *value as f64;
                            }
                        }
                        ColArr::I64(a) => {
                            for (out, value) in
                                output.iter_mut().zip(&a.values()[start..start + len])
                            {
                                *out = *value as f64;
                            }
                        }
                        ColArr::Decimal(a) => {
                            for (out, value) in
                                output.iter_mut().zip(&a.values()[start..start + len])
                            {
                                *out = *value as f64 / divisor;
                            }
                        }
                        _ => unreachable!("typed conversion bound before evaluation"),
                    }
                }
                Instr::LitF64 { v, dst } => {
                    f[*dst as usize][..len].fill(*v);
                }
                Instr::Arith { op, a, b, dst } => {
                    let (ops, dsts) = f.split_at_mut(*dst as usize);
                    let d = &mut dsts[0];
                    let x = &ops[*a as usize];
                    let y = &ops[*b as usize];
                    match op {
                        BinaryOp::Add => {
                            for i in 0..len {
                                d[i] = x[i] + y[i];
                            }
                        }
                        BinaryOp::Subtract => {
                            for i in 0..len {
                                d[i] = x[i] - y[i];
                            }
                        }
                        BinaryOp::Multiply => {
                            for i in 0..len {
                                d[i] = x[i] * y[i];
                            }
                        }
                        BinaryOp::Divide => {
                            for i in 0..len {
                                d[i] = x[i] / y[i];
                            }
                        }
                        _ => unreachable!("typed at compile"),
                    }
                }
                Instr::CmpF64 { a, b, op, dst } => {
                    let resolve = |src: &Src| -> FOp<'_> {
                        match src {
                            Src::Col(c) => match &arrays[*c] {
                                ColArr::F64(arr) => FOp::Slice(&arr.values()[start..start + len]),
                                _ => unreachable!("typed at compile"),
                            },
                            Src::LitF64(v) => FOp::Scalar(*v),
                            // Registers are read-only here (masks are written,
                            // f is not), so the borrow is fine.
                            Src::Reg(_) => unreachable!("resolved below"),
                            _ => unreachable!("typed at compile"),
                        }
                    };
                    // Reg sources borrow `f` immutably while `m` is written —
                    // disjoint arrays, resolved without the closure.
                    let a_op = match a {
                        Src::Reg(r) => FOp::Slice(&f[*r as usize][..len]),
                        other => resolve(other),
                    };
                    let b_op = match b {
                        Src::Reg(r) => FOp::Slice(&f[*r as usize][..len]),
                        other => resolve(other),
                    };
                    let d = &mut m[*dst as usize];
                    // Bind the SQL operator outside the row loop. Passing a
                    // runtime BinaryOp through the inlined comparator leaves
                    // an operator jump table in every row of the release loop.
                    macro_rules! compare_shapes {
                        ($operator:ident) => {
                            match (a_op, b_op) {
                                (FOp::Slice(x), FOp::Slice(y)) => {
                                    for i in 0..len {
                                        d[i] = crate::planner::numeric::sql_float_compare(
                                            x[i],
                                            BinaryOp::$operator,
                                            y[i],
                                        ) as u8;
                                    }
                                }
                                (FOp::Slice(x), FOp::Scalar(y)) => {
                                    for i in 0..len {
                                        d[i] = crate::planner::numeric::sql_float_compare(
                                            x[i],
                                            BinaryOp::$operator,
                                            y,
                                        ) as u8;
                                    }
                                }
                                (FOp::Scalar(x), FOp::Slice(y)) => {
                                    for i in 0..len {
                                        d[i] = crate::planner::numeric::sql_float_compare(
                                            x,
                                            BinaryOp::$operator,
                                            y[i],
                                        ) as u8;
                                    }
                                }
                                (FOp::Scalar(x), FOp::Scalar(y)) => {
                                    d[..len].fill(crate::planner::numeric::sql_float_compare(
                                        x,
                                        BinaryOp::$operator,
                                        y,
                                    ) as u8);
                                }
                            }
                        };
                    }
                    match op {
                        Cmp::Eq => compare_shapes!(Eq),
                        Cmp::Ne => compare_shapes!(NotEq),
                        Cmp::Lt => compare_shapes!(Lt),
                        Cmp::Le => compare_shapes!(LtEq),
                        Cmp::Gt => compare_shapes!(Gt),
                        Cmp::Ge => compare_shapes!(GtEq),
                    }
                }

                Instr::CmpDecimal {
                    a,
                    b,
                    comparison,
                    op,
                    dst,
                } => {
                    let value = |source: &Src, row: usize| -> i128 {
                        match source {
                            Src::LitI128(v) => *v,
                            Src::LitI64(v) => i128::from(*v),
                            Src::LitI32(v) => i128::from(*v),
                            Src::Col(c) => match &arrays[*c] {
                                ColArr::Decimal(a) => a.value(row),
                                ColArr::I64(a) => i128::from(a.value(row)),
                                ColArr::I32(a) => i128::from(a.value(row)),
                                _ => unreachable!("bound decimal comparison column"),
                            },
                            _ => unreachable!("bound decimal comparison source"),
                        }
                    };
                    for i in 0..len {
                        m[*dst as usize][i] = op.apply(
                            comparison.compare(value(a, start + i), value(b, start + i)),
                            std::cmp::Ordering::Equal,
                        ) as u8;
                    }
                }
                Instr::CmpI64 { a, b, op, dst } => {
                    let resolve = |src: &Src| -> IOp<'_> {
                        match src {
                            Src::Col(c) => match &arrays[*c] {
                                ColArr::I64(arr) => IOp::Slice(&arr.values()[start..start + len]),
                                _ => unreachable!("typed at compile"),
                            },
                            Src::LitI64(v) => IOp::Scalar(*v),
                            _ => unreachable!("typed at compile"),
                        }
                    };
                    let (a_op, b_op) = (resolve(a), resolve(b));
                    cmp_loop!(a_op, b_op, op, *dst, IOp);
                }
                Instr::CmpI32 { a, b, op, dst } => {
                    let resolve = |src: &Src| -> I32Op<'_> {
                        match src {
                            Src::Col(c) => match &arrays[*c] {
                                ColArr::I32(arr) => I32Op::Slice(&arr.values()[start..start + len]),
                                ColArr::Date32(arr) => {
                                    I32Op::Slice(&arr.values()[start..start + len])
                                }
                                _ => unreachable!("typed at compile"),
                            },
                            Src::LitI32(v) => I32Op::Scalar(*v),
                            _ => unreachable!("typed at compile"),
                        }
                    };
                    let (a_op, b_op) = (resolve(a), resolve(b));
                    cmp_loop!(a_op, b_op, op, *dst, I32Op);
                }
                Instr::LikeUtf8 {
                    source,
                    pattern,
                    negated,
                    dst,
                } => {
                    let matcher = crate::physical::operators::classify_like(pattern);
                    for i in 0..len {
                        m[*dst as usize][i] =
                            (matcher.matches(utf8_value(source, arrays, start + i)) ^ *negated)
                                as u8;
                    }
                }
                Instr::CmpUtf8 { a, b, op, dst } => {
                    for i in 0..len {
                        m[*dst as usize][i] = op.apply(
                            utf8_value(a, arrays, start + i),
                            utf8_value(b, arrays, start + i),
                        ) as u8;
                    }
                }
                Instr::InUtf8 {
                    source,
                    list,
                    negated,
                    dst,
                } => {
                    for i in 0..len {
                        let found = list
                            .iter()
                            .flatten()
                            .any(|s| s == utf8_value(source, arrays, start + i));
                        m[*dst as usize][i] = (found ^ *negated) as u8;
                    }
                }
                Instr::And { a, b, dst } => {
                    let (ops, dsts) = m.split_at_mut(*dst as usize);
                    let d = &mut dsts[0];
                    let x = &ops[*a as usize];
                    let y = &ops[*b as usize];
                    for i in 0..len {
                        d[i] = x[i] & y[i];
                    }
                }
                Instr::Or { a, b, dst } => {
                    let (ops, dsts) = m.split_at_mut(*dst as usize);
                    let d = &mut dsts[0];
                    let x = &ops[*a as usize];
                    let y = &ops[*b as usize];
                    for i in 0..len {
                        d[i] = x[i] | y[i];
                    }
                }
                Instr::Not { a, dst } => {
                    let (ops, dsts) = m.split_at_mut(*dst as usize);
                    let d = &mut dsts[0];
                    let x = &ops[*a as usize];
                    for i in 0..len {
                        d[i] = 1 - x[i];
                    }
                }
            }
        }
    }
}

/// A per-call-site predicate evaluator: compiles against the first batch's
/// schema, evaluates fused when the expression is in the subset, falls back
/// to the interpreter otherwise (or on per-batch type drift, e.g.
/// dictionary-encoded columns arriving mid-stream).
pub struct PredicateEvaluator {
    expr: Expr,
    compiled: std::sync::OnceLock<Option<CompiledPredicate>>,
}

impl PredicateEvaluator {
    pub fn new(expr: Expr) -> Self {
        Self {
            expr,
            compiled: std::sync::OnceLock::new(),
        }
    }

    pub fn expr(&self) -> &Expr {
        &self.expr
    }

    /// The boolean mask, by the fused path when possible.
    pub fn evaluate(&self, batch: &RecordBatch) -> crate::error::Result<BooleanArray> {
        let compiled = self
            .compiled
            .get_or_init(|| CompiledPredicate::compile(&self.expr, &batch.schema()));
        if let Some(c) = compiled {
            if let Some(mask) = c.evaluate(batch) {
                return Ok(mask);
            }
        }
        let arr = crate::physical::operators::evaluate_expr(batch, &self.expr)?;
        arr.as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| {
                crate::error::QueryError::Execution(
                    "filter predicate must evaluate to boolean".into(),
                )
            })
    }
}

/// Same column resolution the interpreter's `find_column_index` performs,
/// reduced to the subset the compiler admits.
fn find_batch_column(batch: &RecordBatch, c: &Column) -> Option<usize> {
    crate::planner::resolve_arrow_column(batch.schema().as_ref(), c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::evaluate_expr;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch(nulls: bool) -> RecordBatch {
        let n = 4000usize;
        let f: Float64Array = (0..n)
            .map(|i| {
                if nulls && i % 7 == 0 {
                    None
                } else {
                    Some((i % 100) as f64 * 0.13 - 3.0)
                }
            })
            .collect();
        let g: Float64Array = (0..n)
            .map(|i| {
                if nulls && i % 11 == 0 {
                    None
                } else {
                    Some((i % 53) as f64 * 0.37)
                }
            })
            .collect();
        let k: Int64Array = (0..n)
            .map(|i| {
                if nulls && i % 13 == 0 {
                    None
                } else {
                    Some((i % 29) as i64 - 5)
                }
            })
            .collect();
        let d: Date32Array = (0..n)
            .map(|i| {
                if nulls && i % 17 == 0 {
                    None
                } else {
                    Some(9000 + (i % 900) as i32)
                }
            })
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("f", DataType::Float64, true),
                Field::new("g", DataType::Float64, true),
                Field::new("k", DataType::Int64, true),
                Field::new("d", DataType::Date32, true),
            ])),
            vec![Arc::new(f), Arc::new(g), Arc::new(k), Arc::new(d)],
        )
        .unwrap()
    }

    fn col(n: &str) -> Expr {
        Expr::Column(Column::new(n))
    }
    fn litf(v: f64) -> Expr {
        Expr::Literal(ScalarValue::Float64(v.into()))
    }
    fn liti(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(v))
    }
    fn litd(v: i32) -> Expr {
        Expr::Literal(ScalarValue::Date32(v))
    }
    fn bin(l: Expr, op: BinaryOp, r: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(l),
            op,
            right: Box::new(r),
        }
    }

    /// Every predicate in the subset must produce the interpreter's mask,
    /// null for null, on batches with and without nulls.
    #[test]
    fn compiled_masks_equal_interpreted_masks() {
        let preds = vec![
            bin(col("f"), BinaryOp::Gt, litf(1.0)),
            bin(col("f"), BinaryOp::LtEq, col("g")),
            bin(col("k"), BinaryOp::Eq, liti(3)),
            bin(col("d"), BinaryOp::GtEq, litd(9400)),
            bin(
                bin(col("f"), BinaryOp::GtEq, litf(0.05)),
                BinaryOp::And,
                bin(
                    bin(col("f"), BinaryOp::LtEq, litf(7.0)),
                    BinaryOp::And,
                    bin(col("g"), BinaryOp::Lt, litf(12.0)),
                ),
            ),
            bin(
                bin(col("k"), BinaryOp::Lt, liti(0)),
                BinaryOp::Or,
                bin(col("d"), BinaryOp::Lt, litd(9100)),
            ),
            Expr::UnaryExpr {
                op: UnaryOp::Not,
                expr: Box::new(bin(col("f"), BinaryOp::Gt, litf(0.0))),
            },
            Expr::Between {
                expr: Box::new(col("g")),
                low: Box::new(litf(2.0)),
                high: Box::new(litf(9.0)),
                negated: false,
            },
            // arithmetic inside a comparison
            bin(
                bin(
                    col("f"),
                    BinaryOp::Multiply,
                    bin(litf(1.0), BinaryOp::Subtract, col("g")),
                ),
                BinaryOp::Gt,
                litf(-2.5),
            ),
        ];
        for nulls in [false, true] {
            let b = batch(nulls);
            for p in &preds {
                let compiled = CompiledPredicate::compile(p, &b.schema())
                    .unwrap_or_else(|| panic!("must compile: {p}"));
                let pool = crate::execution::MemoryPool::new(1 << 20);
                let admitted = compiled.evaluate_admitted(&b, &pool).unwrap();
                assert!(
                    admitted.is_some(),
                    "typed nullable programs must execute with admitted validity"
                );
                if let Some(mask) = admitted.as_ref() {
                    let expected = evaluate_expr(&b, p).unwrap();
                    assert_eq!(
                        mask,
                        expected.as_any().downcast_ref::<BooleanArray>().unwrap()
                    );
                }
                drop(admitted);
                assert_eq!(pool.used(), 0);
                let got = compiled.evaluate(&b).unwrap_or_else(|| {
                    assert!(nulls, "null-free typed programs must stay fused");
                    PredicateEvaluator::new(p.clone()).evaluate(&b).unwrap()
                });
                let want = evaluate_expr(&b, p).unwrap();
                let want = want.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(&got, want, "pred {p} nulls={nulls}");
            }
        }
    }

    /// Shapes outside the subset must refuse to compile, not miscompile.
    #[test]
    fn out_of_subset_shapes_decline() {
        let b = batch(false);
        let cases = vec![
            // strings
            bin(
                col("f"),
                BinaryOp::Eq,
                Expr::Literal(ScalarValue::Utf8("x".into())),
            ),
            // functions
            Expr::ScalarFunc {
                func: crate::planner::ScalarFunction::Abs,
                args: vec![col("f")],
            },
        ];
        for p in cases {
            assert!(
                CompiledPredicate::compile(&p, &b.schema()).is_none(),
                "must decline: {p}"
            );
        }
    }
    #[test]
    fn admitted_masks_refuse_cleanly_and_retain_extracted_owners() {
        let b = batch(true).slice(3, 3073);
        let predicate = bin(col("k"), BinaryOp::Gt, liti(3));
        let compiled = CompiledPredicate::compile(&predicate, &b.schema()).unwrap();
        let small = crate::execution::MemoryPool::new(1024);
        assert!(compiled
            .evaluate_admitted(&b, &small)
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(small.used(), 0);
        let pool = crate::execution::MemoryPool::new(131072);
        let output = compiled.evaluate_admitted(&b, &pool).unwrap().unwrap();
        let oracle = evaluate_expr(&b, &predicate).unwrap();
        assert_eq!(
            &output,
            oracle.as_any().downcast_ref::<BooleanArray>().unwrap()
        );
        let slice = output.slice(7, 1025);
        drop(output);
        assert!(pool.used() > 0);
        let data = slice.to_data();
        drop(slice);
        assert!(pool.used() > 0);
        drop(data);
        assert_eq!(pool.used(), 0);
        let empty = compiled
            .evaluate_admitted(&b.slice(0, 0), &pool)
            .unwrap()
            .unwrap();
        assert!(empty.is_empty());
        drop(empty);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn admitted_schema_decline_is_distinct_from_memory_refusal() {
        let b = batch(false);
        let compiled =
            CompiledPredicate::compile(&bin(col("k"), BinaryOp::Eq, liti(3)), &b.schema()).unwrap();
        let drifted = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Float64, false)])),
            vec![Arc::new(Float64Array::from(vec![3.0]))],
        )
        .unwrap();
        let pool = crate::execution::MemoryPool::new(131072);
        assert!(compiled
            .evaluate_admitted(&drifted, &pool)
            .unwrap()
            .is_none());
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn admitted_boolean_truth_tables_and_survivors_are_exact() {
        let a = [Some(0i64), Some(1), None]
            .into_iter()
            .flat_map(|v| [v; 3])
            .collect::<Vec<_>>();
        let b = [Some(0i64), Some(1), None].repeat(3);
        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(Int64Array::from(a)) as arrow::array::ArrayRef),
            ("b", Arc::new(Int64Array::from(b)) as arrow::array::ArrayRef),
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..9)) as arrow::array::ArrayRef,
            ),
        ])
        .unwrap();
        let and = bin(
            bin(col("a"), BinaryOp::Gt, liti(0)),
            BinaryOp::And,
            bin(col("b"), BinaryOp::Gt, liti(0)),
        );
        let or = bin(
            bin(col("a"), BinaryOp::Gt, liti(0)),
            BinaryOp::Or,
            bin(col("b"), BinaryOp::Gt, liti(0)),
        );
        let not = Expr::UnaryExpr {
            op: UnaryOp::Not,
            expr: Box::new(and.clone()),
        };
        for (predicate, expected) in [
            (
                and,
                vec![
                    Some(false),
                    Some(false),
                    Some(false),
                    Some(false),
                    Some(true),
                    None,
                    Some(false),
                    None,
                    None,
                ],
            ),
            (
                or,
                vec![
                    Some(false),
                    Some(true),
                    None,
                    Some(true),
                    Some(true),
                    Some(true),
                    None,
                    Some(true),
                    None,
                ],
            ),
            (
                not,
                vec![
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                    None,
                    None,
                ],
            ),
        ] {
            let pool = crate::execution::MemoryPool::new(131072);
            let compiled = CompiledPredicate::compile(&predicate, &batch.schema()).unwrap();
            let mask = compiled.evaluate_admitted(&batch, &pool).unwrap().unwrap();
            assert_eq!(mask.iter().collect::<Vec<_>>(), expected);
            let output = crate::storage::admitted_gather::filter(&batch, &mask, &pool).unwrap();
            let ids = output
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(
                ids.values().as_ref(),
                expected
                    .iter()
                    .enumerate()
                    .filter_map(|(i, v)| (*v == Some(true)).then_some(i as i64))
                    .collect::<Vec<_>>()
            );
            drop(output);
            drop(mask);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn admitted_string_comparisons_membership_and_nulls_match_oracles() {
        let strings = [
            Some("MAIL"),
            Some("SHIP"),
            Some(""),
            None,
            Some("é"),
            Some("e\u{301}"),
            Some("🙂"),
            Some("other"),
        ];
        let values = (0..2053)
            .map(|i| strings[i % strings.len()])
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "s",
                Arc::new(StringArray::from(values.clone())) as arrow::array::ArrayRef,
            ),
            (
                "t",
                Arc::new(StringArray::from(
                    values.iter().rev().copied().collect::<Vec<_>>(),
                )) as arrow::array::ArrayRef,
            ),
            (
                "k",
                Arc::new(Int64Array::from_iter_values(0..2053)) as arrow::array::ArrayRef,
            ),
        ])
        .unwrap()
        .slice(3, 2049);
        let literal = |s: &str| Expr::Literal(ScalarValue::Utf8(s.into()));
        for op in [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            for rhs in [literal("é"), col("t")] {
                let predicate = bin(col("s"), op, rhs);
                assert!(CompiledPredicate::compile(&predicate, &batch.schema()).is_none());
                let compiled =
                    CompiledPredicate::compile_for_admitted_evaluation(&predicate, &batch.schema())
                        .unwrap();
                assert!(compiled.evaluate(&batch).is_none());
                let pool = crate::execution::MemoryPool::new(131072);
                let actual = compiled.evaluate_admitted(&batch, &pool).unwrap().unwrap();
                let oracle = evaluate_expr(&batch, &predicate).unwrap();
                assert_eq!(
                    &actual,
                    oracle.as_any().downcast_ref::<BooleanArray>().unwrap()
                );
                drop(actual);
                assert_eq!(pool.used(), 0);
            }
        }
        for negated in [false, true] {
            for has_null in [false, true] {
                let mut list = vec![literal("MAIL"), literal("SHIP"), literal("MAIL")];
                if has_null {
                    list.push(Expr::Literal(ScalarValue::Null));
                }
                let predicate = Expr::InList {
                    expr: Box::new(col("s")),
                    list,
                    negated,
                };
                let compiled =
                    CompiledPredicate::compile_for_admitted_evaluation(&predicate, &batch.schema())
                        .unwrap();
                let pool = crate::execution::MemoryPool::new(131072);
                let actual = compiled.evaluate_admitted(&batch, &pool).unwrap().unwrap();
                let expected = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| {
                        v.and_then(|s| {
                            if s == "MAIL" || s == "SHIP" {
                                Some(!negated)
                            } else if has_null {
                                None
                            } else {
                                Some(negated)
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual.iter().collect::<Vec<_>>(), expected);
                let oracle = evaluate_expr(&batch, &predicate).unwrap();
                assert_eq!(
                    &actual,
                    oracle.as_any().downcast_ref::<BooleanArray>().unwrap()
                );
                drop(actual);
                assert_eq!(pool.used(), 0);
            }
        }
    }
    #[test]
    fn admitted_null_list_and_mixed_boolean_survivors_are_exact() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "s",
                Arc::new(StringArray::from(vec!["MAIL", "other", "SHIP", "other"]))
                    as arrow::array::ArrayRef,
            ),
            (
                "k",
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as arrow::array::ArrayRef,
            ),
        ])
        .unwrap();
        let membership = Expr::InList {
            expr: Box::new(col("s")),
            list: vec![
                Expr::Literal(ScalarValue::Utf8("MAIL".into())),
                Expr::Literal(ScalarValue::Null),
            ],
            negated: false,
        };
        let predicate = bin(
            membership,
            BinaryOp::Or,
            bin(col("k"), BinaryOp::Gt, liti(1)),
        );
        let compiled =
            CompiledPredicate::compile_for_admitted_evaluation(&predicate, &batch.schema())
                .unwrap();
        let tiny = crate::execution::MemoryPool::new(1024);
        assert!(compiled
            .evaluate_admitted(&batch, &tiny)
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(tiny.used(), 0);
        let pool = crate::execution::MemoryPool::new(131072);
        let mask = compiled.evaluate_admitted(&batch, &pool).unwrap().unwrap();
        assert_eq!(
            mask.iter().collect::<Vec<_>>(),
            vec![Some(true), None, Some(true), Some(true)]
        );
        let result = crate::storage::admitted_gather::filter(&batch, &mask, &pool).unwrap();
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 2, 3]
        );
        drop(result);
        drop(mask);
        assert_eq!(pool.used(), 0);
        let bad = Expr::InList {
            expr: Box::new(col("s")),
            list: vec![liti(1)],
            negated: false,
        };
        assert!(
            CompiledPredicate::compile_for_admitted_evaluation(&bad, &batch.schema()).is_none()
        );
    }

    #[test]
    fn admitted_like_reuses_allocation_free_matching_with_unicode_null_semantics() {
        let strings = vec![
            Some(""),
            Some("é"),
            Some("e\u{301}"),
            Some("🙂"),
            Some("MAIL"),
            Some("special xx requests"),
            Some("requests special"),
            None,
        ];
        let batch = RecordBatch::try_from_iter(vec![(
            "s",
            Arc::new(StringArray::from(strings.clone())) as arrow::array::ArrayRef,
        )])
        .unwrap();
        for pattern in ["%", "M%", "%IL", "%é%", "_", "__", "%special%requests%", ""] {
            for negated in [false, true] {
                let predicate = bin(
                    col("s"),
                    if negated {
                        BinaryOp::NotLike
                    } else {
                        BinaryOp::Like
                    },
                    Expr::Literal(ScalarValue::Utf8(pattern.into())),
                );
                assert!(CompiledPredicate::compile(&predicate, &batch.schema()).is_none());
                let compiled =
                    CompiledPredicate::compile_for_admitted_evaluation(&predicate, &batch.schema())
                        .unwrap();
                let pool = crate::execution::MemoryPool::new(131072);
                let actual = compiled.evaluate_admitted(&batch, &pool).unwrap().unwrap();
                let oracle = evaluate_expr(&batch, &predicate).unwrap();
                assert_eq!(
                    &actual,
                    oracle.as_any().downcast_ref::<BooleanArray>().unwrap()
                );
                if pattern == "_" {
                    assert_eq!(
                        actual.iter().collect::<Vec<_>>(),
                        strings
                            .iter()
                            .map(|s| s.map(|s| (s.chars().count() == 1) ^ negated))
                            .collect::<Vec<_>>()
                    );
                }
                if pattern == "%special%requests%" {
                    assert_eq!(
                        actual.iter().collect::<Vec<_>>(),
                        strings
                            .iter()
                            .enumerate()
                            .map(|(i, s)| s.map(|_| (i == 5) ^ negated))
                            .collect::<Vec<_>>()
                    );
                }
                drop(actual);
                assert_eq!(pool.used(), 0);
            }
        }
    }

    #[test]
    fn reserved_compilation_refuses_before_construction_and_retains_program_charge() {
        let batch = RecordBatch::try_from_iter(vec![(
            "s",
            Arc::new(StringArray::from(vec![Some("abc"), None])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let predicate = bin(
            col("s"),
            BinaryOp::Like,
            Expr::Literal(ScalarValue::Utf8(format!("%{}%", "x".repeat(4096)))),
        );
        let small = crate::execution::MemoryPool::new(1024);
        assert!(
            matches!(CompiledPredicate::compile_reserved(&predicate,&batch.schema(),&small), Err(e) if e.is_memory_limit())
        );
        assert_eq!(small.used(), 0);
        let pool = crate::execution::MemoryPool::new(4 << 20);
        let compiled = CompiledPredicate::compile_reserved(&predicate, &batch.schema(), &pool)
            .unwrap()
            .unwrap();
        let charge = pool.used();
        assert!(charge >= 4096);
        assert!(
            pool.reserved_peak() > charge,
            "construction allowance must shrink"
        );
        let held = pool.allocate(pool.max() - charge).unwrap();
        assert!(compiled
            .evaluate_admitted(&batch, &pool)
            .unwrap_err()
            .is_memory_limit());
        drop(held);
        assert_eq!(pool.used(), charge);
        let mask = compiled.evaluate_admitted(&batch, &pool).unwrap().unwrap();
        assert_eq!(mask.iter().collect::<Vec<_>>(), vec![Some(false), None]);
        drop(compiled);
        assert!(pool.used() > 0 && pool.used() < charge);
        drop(mask);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn reserved_compile_depth_and_node_limits_decline_without_admission() {
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let mut deep = bin(
            col("s"),
            BinaryOp::Eq,
            Expr::Literal(ScalarValue::Utf8("x".into())),
        );
        for _ in 0..80 {
            deep = Expr::Alias {
                expr: Box::new(deep),
                name: "alias".into(),
            };
        }
        let small = crate::execution::MemoryPool::new(1024);
        assert!(CompiledPredicate::compile_reserved(&deep, &schema, &small)
            .unwrap()
            .is_none());
        let large = Expr::InList {
            expr: Box::new(col("s")),
            list: vec![Expr::Literal(ScalarValue::Utf8("x".into())); 4096],
            negated: false,
        };
        assert!(CompiledPredicate::compile_reserved(&large, &schema, &small)
            .unwrap()
            .is_none());
        assert_eq!(small.used(), 0);
        let pool = crate::execution::MemoryPool::new(4 << 20);
        let mixed = bin(col("s"), BinaryOp::Eq, liti(3));
        assert!(CompiledPredicate::compile_reserved(&mixed, &schema, &pool)
            .unwrap()
            .is_none());
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod compiled_qualified_identity_contract {
    use super::*;
    use crate::planner::{BinaryOp, Column, SchemaField};
    use std::sync::Arc;
    #[test]
    fn compiled_loader_preserves_distinct_columns_with_colliding_display() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            SchemaField::new("b.c", DataType::Float64)
                .with_relation("a")
                .to_arrow_field(),
            SchemaField::new("c", DataType::Float64)
                .with_relation("a.b")
                .to_arrow_field(),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.0), Some(7.0), None])),
                Arc::new(Float64Array::from(vec![Some(2.0), Some(3.0), Some(2.0)])),
            ],
        )
        .unwrap();
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new_qualified("a", "b.c"))),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Column(Column::new_qualified("a.b", "c"))),
        };
        let compiled =
            CompiledPredicate::compile(&expr, schema.as_ref()).expect("exercise compiled path");
        let actual = compiled
            .evaluate(&batch)
            .expect("compiled batch must execute");
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), None]
        );
    }
}

#[cfg(test)]
mod numeric_coercion_tests;
