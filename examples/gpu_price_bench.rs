//! GPU pricing bench — the GO/NO-GO gate for the gpu-acceleration epic.
//!
//! Measures on THIS box: H2D bandwidth over PCIe, then the Q6 shape
//! (3-predicate filter + SUM) and the Q1-flat shape (3-column arithmetic
//! chain + 4 SUMs) as fused CUDA kernels over device-resident columns,
//! against the CPU doing the same over host-resident arrow buffers.
//! Cold = includes upload; warm = data already resident (the column-cache
//! regime). Gate: warm >= 3x CPU, cold >= 0.8x.
//!
//! Run: cargo run --release --features gpu --example gpu_price_bench

#[cfg(not(feature = "gpu"))]
fn main() {
    eprintln!("build with --features gpu");
}

#[cfg(feature = "gpu")]
fn main() {
    use cudarc::driver::{CudaContext, LaunchConfig, PushKernelArg};
    use std::time::Instant;

    let n: usize = 59_986_052; // SF=10 lineitem row count
    println!("rows = {n} (~SF=10 lineitem)");

    // Host columns, TPC-H-flavored values.
    let price: Vec<f64> = (0..n).map(|i| 900.0 + (i % 100_000) as f64 * 0.01).collect();
    let disc: Vec<f64> = (0..n).map(|i| (i % 11) as f64 * 0.01).collect();
    let tax: Vec<f64> = (0..n).map(|i| (i % 9) as f64 * 0.01).collect();
    let qty: Vec<f64> = (0..n).map(|i| (i % 50) as f64 + 1.0).collect();
    let ship: Vec<i32> = (0..n).map(|i| 8766 + (i % 2557) as i32).collect(); // ~7 years of days

    let ctx = CudaContext::new(0).expect("cuda device 0");
    let stream = ctx.default_stream();
    println!("device: {}", ctx.name().unwrap_or_default());

    // ---- H2D bandwidth ----
    let t0 = Instant::now();
    let d_price = stream.memcpy_stod(&price).unwrap();
    let d_disc = stream.memcpy_stod(&disc).unwrap();
    let d_tax = stream.memcpy_stod(&tax).unwrap();
    let d_qty = stream.memcpy_stod(&qty).unwrap();
    let d_ship = stream.memcpy_stod(&ship).unwrap();
    stream.synchronize().unwrap();
    let up = t0.elapsed().as_secs_f64();
    let bytes = n * (8 * 4 + 4);
    println!(
        "H2D: {:.1} GB in {:.3}s = {:.1} GB/s",
        bytes as f64 / 1e9,
        up,
        bytes as f64 / up / 1e9
    );

    // ---- kernels ----
    const SRC: &str = r#"
extern "C" __global__ void q6_kernel(
    const double* __restrict__ price,
    const double* __restrict__ disc,
    const double* __restrict__ qty,
    const int* __restrict__ ship,
    long long n,
    double* __restrict__ block_sums)
{
    __shared__ double sh[256];
    double acc = 0.0;
    for (long long i = blockIdx.x * blockDim.x + threadIdx.x; i < n;
         i += (long long)gridDim.x * blockDim.x) {
        bool keep = ship[i] >= 9131 && ship[i] < 9496 &&
                    disc[i] >= 0.05 && disc[i] <= 0.07 && qty[i] < 24.0;
        if (keep) acc += price[i] * disc[i];
    }
    sh[threadIdx.x] = acc;
    __syncthreads();
    for (int s = blockDim.x / 2; s > 0; s >>= 1) {
        if (threadIdx.x < s) sh[threadIdx.x] += sh[threadIdx.x + s];
        __syncthreads();
    }
    if (threadIdx.x == 0) block_sums[blockIdx.x] = sh[0];
}

extern "C" __global__ void q1_kernel(
    const double* __restrict__ price,
    const double* __restrict__ disc,
    const double* __restrict__ tax,
    const double* __restrict__ qty,
    long long n,
    double* __restrict__ block_out) // 4 sums per block
{
    __shared__ double sh[4][256];
    double s_qty = 0.0, s_base = 0.0, s_disc = 0.0, s_charge = 0.0;
    for (long long i = blockIdx.x * blockDim.x + threadIdx.x; i < n;
         i += (long long)gridDim.x * blockDim.x) {
        double p = price[i], d = disc[i];
        double dp = p * (1.0 - d);
        s_qty += qty[i];
        s_base += p;
        s_disc += dp;
        s_charge += dp * (1.0 + tax[i]);
    }
    sh[0][threadIdx.x] = s_qty;
    sh[1][threadIdx.x] = s_base;
    sh[2][threadIdx.x] = s_disc;
    sh[3][threadIdx.x] = s_charge;
    __syncthreads();
    for (int s = blockDim.x / 2; s > 0; s >>= 1) {
        if (threadIdx.x < s)
            for (int k = 0; k < 4; k++) sh[k][threadIdx.x] += sh[k][threadIdx.x + s];
        __syncthreads();
    }
    if (threadIdx.x == 0)
        for (int k = 0; k < 4; k++) block_out[blockIdx.x * 4 + k] = sh[k][0];
}
"#;

    let ptx = cudarc::nvrtc::compile_ptx(SRC).expect("nvrtc");
    let module = ctx.load_module(ptx).expect("load");
    let q6 = module.load_function("q6_kernel").unwrap();
    let q1 = module.load_function("q1_kernel").unwrap();

    let blocks = 512u32;
    let cfg = LaunchConfig {
        grid_dim: (blocks, 1, 1),
        block_dim: (256, 1, 1),
        shared_mem_bytes: 0,
    };
    let mut d_bs = stream.alloc_zeros::<f64>(blocks as usize * 4).unwrap();
    let nn = n as i64;

    // warmup + timed GPU Q6
    for _ in 0..3 {
        let mut b = stream.launch_builder(&q6);
        b.arg(&d_price).arg(&d_disc).arg(&d_qty).arg(&d_ship).arg(&nn).arg(&mut d_bs);
        unsafe { b.launch(cfg) }.unwrap();
    }
    stream.synchronize().unwrap();
    let iters = 50;
    let t0 = Instant::now();
    for _ in 0..iters {
        let mut b = stream.launch_builder(&q6);
        b.arg(&d_price).arg(&d_disc).arg(&d_qty).arg(&d_ship).arg(&nn).arg(&mut d_bs);
        unsafe { b.launch(cfg) }.unwrap();
    }
    stream.synchronize().unwrap();
    let g6 = t0.elapsed().as_secs_f64() / iters as f64;
    let partials: Vec<f64> = stream.memcpy_dtov(&d_bs).unwrap();
    let g6_sum: f64 = partials[..blocks as usize].iter().sum();

    // timed GPU Q1
    for _ in 0..3 {
        let mut b = stream.launch_builder(&q1);
        b.arg(&d_price).arg(&d_disc).arg(&d_tax).arg(&d_qty).arg(&nn).arg(&mut d_bs);
        unsafe { b.launch(cfg) }.unwrap();
    }
    stream.synchronize().unwrap();
    let t0 = Instant::now();
    for _ in 0..iters {
        let mut b = stream.launch_builder(&q1);
        b.arg(&d_price).arg(&d_disc).arg(&d_tax).arg(&d_qty).arg(&nn).arg(&mut d_bs);
        unsafe { b.launch(cfg) }.unwrap();
    }
    stream.synchronize().unwrap();
    let g1 = t0.elapsed().as_secs_f64() / iters as f64;

    // ---- CPU reference (rayon parallel, what the engine effectively does) ----
    use rayon::prelude::*;
    let cpu_iters = 10;
    let t0 = Instant::now();
    let mut c6_sum = 0.0;
    for _ in 0..cpu_iters {
        c6_sum = (0..n)
            .into_par_iter()
            .with_min_len(65536)
            .map(|i| {
                let keep = ship[i] >= 9131
                    && ship[i] < 9496
                    && disc[i] >= 0.05
                    && disc[i] <= 0.07
                    && qty[i] < 24.0;
                if keep {
                    price[i] * disc[i]
                } else {
                    0.0
                }
            })
            .sum();
    }
    let c6 = t0.elapsed().as_secs_f64() / cpu_iters as f64;

    let t0 = Instant::now();
    let mut c1 = (0.0, 0.0, 0.0, 0.0);
    for _ in 0..cpu_iters {
        c1 = (0..n)
            .into_par_iter()
            .with_min_len(65536)
            .fold(
                || (0.0, 0.0, 0.0, 0.0),
                |mut a, i| {
                    let p = price[i];
                    let d = disc[i];
                    let dp = p * (1.0 - d);
                    a.0 += qty[i];
                    a.1 += p;
                    a.2 += dp;
                    a.3 += dp * (1.0 + tax[i]);
                    a
                },
            )
            .reduce(|| (0.0, 0.0, 0.0, 0.0), |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2, a.3 + b.3));
    }
    let c1t = t0.elapsed().as_secs_f64() / cpu_iters as f64;

    println!("\nQ6 shape  (filter+sum, 3.5 cols):");
    println!("  GPU warm: {:8.3} ms   CPU: {:8.3} ms   speedup {:5.1}x", g6 * 1e3, c6 * 1e3, c6 / g6);
    println!("  GPU cold (incl. its share of upload): {:8.3} ms", g6 * 1e3 + up * 1e3 * (28.0 / 36.0));
    println!("  checksum gpu={g6_sum:.3} cpu={c6_sum:.3}");
    println!("Q1 shape  (4 sums over arith chain, 4 cols):");
    println!("  GPU warm: {:8.3} ms   CPU: {:8.3} ms   speedup {:5.1}x", g1 * 1e3, c1t * 1e3, c1t / g1);
    println!("  (cpu checksums {:.1} {:.1} {:.1} {:.1})", c1.0, c1.1, c1.2, c1.3);
}
