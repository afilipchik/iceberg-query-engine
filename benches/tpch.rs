//! TPC-H benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use query_engine::execution::ExecutionContext;
use query_engine::tpch::{self, TpchGenerator};

fn create_context(sf: f64) -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    let mut gen = TpchGenerator::new(sf);
    gen.generate_all(&mut ctx);
    ctx
}

fn benchmark_queries(c: &mut Criterion) {
    let sf = 0.01;
    let ctx = create_context(sf);

    let mut group = c.benchmark_group("tpch");
    group.sample_size(10);

    // Benchmark Q1 and Q6 as they are simpler aggregation queries
    for q in [1, 6] {
        if let Some(sql) = tpch::get_query(q) {
            group.bench_with_input(
                BenchmarkId::new("query", q),
                &sql,
                |b, sql| {
                    b.iter(|| {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async {
                            let result = ctx.sql(sql).await.unwrap();
                            black_box(result.row_count);
                        })
                    });
                },
            );
        }
    }

    group.finish();
}

fn benchmark_full_suite(c: &mut Criterion) {
    let sf = 0.01;
    let ctx = create_context(sf);

    c.bench_function("tpch_full_suite", |b| {
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut total = 0;
                for q in tpch::ALL_QUERIES {
                    if let Some(sql) = tpch::get_query(q) {
                        if let Ok(result) = ctx.sql(sql).await {
                            total += result.row_count;
                        }
                    }
                }
                black_box(total)
            })
        });
    });
}

criterion_group!(benches, benchmark_queries, benchmark_full_suite);
criterion_main!(benches);
