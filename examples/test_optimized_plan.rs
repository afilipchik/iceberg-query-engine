use query_engine::ExecutionContext;
use query_engine::tpch;

#[tokio::main]
async fn main() {
    let sql = tpch::get_query(2).unwrap();
    println!("Query:\n{}\n", sql);
    
    let mut ctx = ExecutionContext::new();
    let mut gen = query_engine::tpch::TpchGenerator::new(0.001);
    gen.generate_all(&mut ctx);
    
    println!("=== LOGICAL PLAN ===\n");
    let logical = ctx.logical_plan(sql).unwrap();
    println!("{}\n", logical);
    
    println!("=== OPTIMIZED PLAN ===\n");
    let optimized = ctx.optimized_plan(sql).unwrap();
    println!("{}\n", optimized);
}
