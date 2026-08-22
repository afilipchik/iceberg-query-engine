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
//! preallocated L1-resident slabs. No allocation per batch, no intermediate
//! ArrayRefs, one pass over the input columns. Anything outside the
//! supported subset (strings, CASE, functions, subqueries, dictionary
//! columns, mixed-type comparisons...) makes `compile` return `None` and the
//! caller keeps the interpreter — the fallback is decided once per operator,
//! never per batch.
//!
//! ## Equivalence contract
//!
//! The compiled path must be indistinguishable from `evaluate_expr` through
//! every consumer:
//! - arithmetic and comparisons are null-strict, matching the interpreter's
//!   arrow kernels (`boolean::and`, not Kleene). Because every operator in
//!   the subset is null-strict, a result row is valid iff EVERY referenced
//!   column is valid at that row — so validity is computed once as the AND
//!   of leaf validities, exactly what kernel-by-kernel propagation yields.
//! - f64 division by zero produces ±inf/NaN in both paths (never null).
//! - numeric comparisons require identical arrow types on both sides;
//!   anything the interpreter would coerce falls back to the interpreter.
//!
//! `QE_COMPILE=0` disables compilation everywhere (the established
//! diagnostic-switch pattern): consumers ask [`compilation_enabled`].

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::planner::{BinaryOp, Column, Expr, ScalarValue, UnaryOp};

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
    LitI32(i32),
    /// An f64 register holding a computed arithmetic result.
    Reg(u8),
}

/// Flat program instructions. F-registers hold f64 chunks, M-registers hold
/// boolean (u8 0/1) chunks.
#[derive(Clone, Debug)]
enum Instr {
    /// F[dst] = column values (f64 column).
    LoadF64 {
        col: usize,
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
    I32(&'a Int32Array),
    Date32(&'a Date32Array),
}

impl<'a> ColArr<'a> {
    fn as_any_array(&self) -> &'a dyn Array {
        match self {
            ColArr::F64(a) => *a,
            ColArr::I64(a) => *a,
            ColArr::I32(a) => *a,
            ColArr::Date32(a) => *a,
        }
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
}

impl Compiler {
    fn new() -> Self {
        Self {
            cols: Vec::new(),
            col_types: Vec::new(),
            prog: Vec::new(),
            next_f: 0,
            next_m: 0,
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
            Expr::Cast { expr, data_type } if data_type == &DataType::Float64 => {
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
                    _ => None,
                }
            }
            Expr::Literal(v) => match v {
                ScalarValue::Float64(x) => Some((Src::LitF64((*x).into()), DataType::Float64)),
                ScalarValue::Int64(x) => Some((Src::LitI64(*x), DataType::Int64)),
                ScalarValue::Int32(x) => Some((Src::LitI32(*x), DataType::Int32)),
                ScalarValue::Date32(x) => Some((Src::LitI32(*x), DataType::Date32)),
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
            Expr::BinaryExpr { left, op, right } => match op {
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
                    // Identical arrow types only — anything the interpreter
                    // would COERCE falls back to the interpreter.
                    if ta != tb {
                        return None;
                    }
                    let dst = self.malloc()?;
                    self.prog.push(match ta {
                        DataType::Float64 => Instr::CmpF64 { a, b, op: cmp, dst },
                        DataType::Int64 => Instr::CmpI64 { a, b, op: cmp, dst },
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
                // lowering (filter.rs uses ge/le + boolean::and).
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
    // Same resolution the interpreter uses: qualified name first, then bare.
    if let Some(rel) = &c.relation {
        let q = format!("{rel}.{}", c.name);
        if let Ok(f) = schema.field_with_name(&q) {
            return Some(f);
        }
    }
    schema.field_with_name(&c.name).ok().or_else(|| {
        // A bare reference may resolve to a uniquely-qualified field.
        let mut hit = None;
        for f in schema.fields() {
            if f.name().rsplit('.').next() == Some(c.name.as_str()) {
                if hit.is_some() {
                    return None;
                }
                hit = Some(f.as_ref());
            }
        }
        hit
    })
}

impl CompiledPredicate {
    /// Compile `expr` against `schema`, or `None` when any part is outside
    /// the supported subset (the caller keeps the interpreter).
    pub fn compile(expr: &Expr, schema: &arrow::datatypes::Schema) -> Option<CompiledPredicate> {
        if !compilation_enabled() {
            return None;
        }
        let mut c = Compiler::new();
        let out = c.boolean(expr, schema)?;
        Some(CompiledPredicate {
            cols: c.cols,
            col_types: c.col_types,
            prog: c.prog,
            out,
            f_regs: c.next_f as usize,
            m_regs: c.next_m as usize,
        })
    }

    /// Evaluate to the same mask the interpreter's kernel chain produces.
    /// Returns `None` when the batch's actual column types diverge from the
    /// compiled assumptions (dictionary-encoded batches, schema drift) —
    /// the caller falls back for THIS batch.
    pub fn evaluate(&self, batch: &RecordBatch) -> Option<BooleanArray> {
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
                DataType::Int32 => ColArr::I32(arr.as_any().downcast_ref()?),
                DataType::Date32 => ColArr::Date32(arr.as_any().downcast_ref()?),
                _ => return None,
            });
        }

        let any_nulls = arrays.iter().any(|a| a.as_any_array().null_count() > 0);

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
                    cmp_loop!(a_op, b_op, op, *dst, FOp);
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
    let schema = batch.schema();
    if let Some(rel) = &c.relation {
        let q = format!("{rel}.{}", c.name);
        if let Some((i, _)) = schema.column_with_name(&q) {
            return Some(i);
        }
    }
    if let Some((i, _)) = schema.column_with_name(&c.name) {
        return Some(i);
    }
    let mut hit = None;
    for (i, f) in schema.fields().iter().enumerate() {
        if f.name().rsplit('.').next() == Some(c.name.as_str()) {
            if hit.is_some() {
                return None;
            }
            hit = Some(i);
        }
    }
    hit
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
                let got = compiled.evaluate(&b).expect("types match");
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
            // mixed types the interpreter would coerce
            bin(col("k"), BinaryOp::Gt, litf(1.5)),
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
}
