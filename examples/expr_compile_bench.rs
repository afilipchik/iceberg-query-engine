//! Prices expression COMPILATION before anything is built (the QE_MORSEL=0 /
//! radix_bench discipline): how much does the tree-walking, temporary-
//! materializing interpreter leave on the table vs a perfectly fused
//! single-pass loop? The fused loop is the CEILING for any compilation
//! scheme (closure compilation, Cranelift, LLVM) — no JIT can beat it.

use arrow::array::{Array, BooleanArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::planner::{BinaryOp, Column, Expr, ScalarValue};
use std::sync::Arc;
use std::time::Instant;

fn batch(rows: usize) -> RecordBatch {
    let price: Float64Array = (0..rows).map(|i| 900.0 + (i % 1000) as f64).collect();
    let disc: Float64Array = (0..rows).map(|i| (i % 11) as f64 * 0.01).collect();
    let tax: Float64Array = (0..rows).map(|i| (i % 9) as f64 * 0.01).collect();
    let qty: Float64Array = (0..rows).map(|i| (i % 50) as f64 + 1.0).collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_quantity", DataType::Float64, false),
        ])),
        vec![
            Arc::new(price),
            Arc::new(disc),
            Arc::new(tax),
            Arc::new(qty),
        ],
    )
    .unwrap()
}

fn col(n: &str) -> Expr {
    Expr::Column(Column::new(n))
}
fn lit(v: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(v.into()))
}
fn bin(l: Expr, op: BinaryOp, r: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

fn time(label: &str, iters: usize, mut f: impl FnMut() -> f64) {
    // warmup
    for _ in 0..3 {
        std::hint::black_box(f());
    }
    let t0 = Instant::now();
    let mut sink = 0.0;
    for _ in 0..iters {
        sink += std::hint::black_box(f());
    }
    let per = t0.elapsed().as_secs_f64() / iters as f64 * 1e3;
    println!("{label:44} {per:8.3} ms/iter   (sink {sink:.1})");
}

fn main() {
    let rows = 8192usize;
    let nbatches = 64usize;
    let batches: Vec<RecordBatch> = (0..nbatches).map(|_| batch(rows)).collect();
    println!(
        "rows/batch={rows}, batches={nbatches} (~{}k rows/iter)\n",
        rows * nbatches / 1000
    );

    // ---- Shape 1: Q1's aggregate input  price * (1-disc) * (1+tax) ----
    let q1 = bin(
        bin(
            col("l_extendedprice"),
            BinaryOp::Multiply,
            bin(lit(1.0), BinaryOp::Subtract, col("l_discount")),
        ),
        BinaryOp::Multiply,
        bin(lit(1.0), BinaryOp::Add, col("l_tax")),
    );
    let iters = 100;
    time("Q1 arith chain: interpreted", iters, || {
        let mut s = 0.0;
        for b in &batches {
            let a = query_engine::physical::operators::evaluate_expr(b, &q1).unwrap();
            let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
            s += a.value(0);
        }
        s
    });
    time("Q1 arith chain: fused loop (ceiling)", iters, || {
        let mut s = 0.0;
        for b in &batches {
            let p = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            let d = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            let t = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            let pv = p.values();
            let dv = d.values();
            let tv = t.values();
            let mut out = Vec::with_capacity(pv.len());
            for i in 0..pv.len() {
                out.push(pv[i] * (1.0 - dv[i]) * (1.0 + tv[i]));
            }
            s += out[0];
            std::hint::black_box(&out);
        }
        s
    });

    // ---- Shape 2: fused into the SUM (no output vector at all) ----
    time("Q1 chain + SUM: interpreted then arrow sum", iters, || {
        let mut s = 0.0;
        for b in &batches {
            let a = query_engine::physical::operators::evaluate_expr(b, &q1).unwrap();
            let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
            s += arrow::compute::sum(a).unwrap();
        }
        s
    });
    time("Q1 chain + SUM: fused accumulate (ceiling)", iters, || {
        let mut s = 0.0;
        for b in &batches {
            let p = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            let d = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            let t = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            let pv = p.values();
            let dv = d.values();
            let tv = t.values();
            let mut acc = 0.0;
            for i in 0..pv.len() {
                acc += pv[i] * (1.0 - dv[i]) * (1.0 + tv[i]);
            }
            s += acc;
        }
        s
    });

    // ---- Shape 3: Q6-style predicate ----
    let q6 = bin(
        bin(
            bin(col("l_discount"), BinaryOp::GtEq, lit(0.05)),
            BinaryOp::And,
            bin(col("l_discount"), BinaryOp::LtEq, lit(0.07)),
        ),
        BinaryOp::And,
        bin(col("l_quantity"), BinaryOp::Lt, lit(24.0)),
    );
    time("Q6 predicate: interpreted", iters, || {
        let mut s = 0.0;
        for b in &batches {
            let a = query_engine::physical::operators::evaluate_expr(b, &q6).unwrap();
            let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
            s += a.true_count() as f64;
        }
        s
    });
    let q6_compiled = query_engine::physical::compiled_expr::CompiledPredicate::compile(
        &q6,
        &batches[0].schema(),
    )
    .expect("q6 compiles");
    time("Q6 predicate: COMPILED (this epic)", iters, || {
        let mut s = 0.0;
        for b in &batches {
            let m = q6_compiled.evaluate(b).unwrap();
            s += m.true_count() as f64;
        }
        s
    });
    time("Q6 predicate: fused loop (ceiling)", iters, || {
        let mut s = 0.0;
        for b in &batches {
            let d = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            let q = b.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
            let dv = d.values();
            let qv = q.values();
            let mut n = 0usize;
            for i in 0..dv.len() {
                n += ((dv[i] >= 0.05) & (dv[i] <= 0.07) & (qv[i] < 24.0)) as usize;
            }
            s += n as f64;
        }
        s
    });
}
