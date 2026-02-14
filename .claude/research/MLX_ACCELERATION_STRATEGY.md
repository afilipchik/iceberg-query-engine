# MLX Acceleration Strategy for Iceberg Query Engine

**Date**: 2026-02-12
**Status**: Research Complete, Implementation Pending

---

## Executive Summary

Apple's MLX framework presents a **greenfield opportunity** to build the first MLX-accelerated analytical database engine. Key advantages:

1. **Unified Memory Architecture** — Zero-copy between CPU and GPU
2. **800 GB/s bandwidth** (M4 Max) — No PCIe bottleneck
3. **192GB capacity** — Entire working sets fit in unified memory
4. **Neural Engine** (38 TOPS) — Specialized for ML operations

**No major database currently uses MLX.**

---

## MLX Operations Mapping to Database

| Database Operation | MLX Equivalent | Notes |
|-------------------|----------------|-------|
| WHERE col > 100 | `mx.greater(data, 100)` | GPU-parallel comparison |
| SELECT SUM(col) | `mx.sum(data)` | GPU reduction |
| SELECT AVG(col) | `mx.mean(data)` | GPU reduction |
| ORDER BY col LIMIT 10 | `mx.topk(data, k=10)` | Highly optimized |
| GROUP BY + SUM | `mx.where(mask, values, 0).sum()` | Masked aggregation |
| Cumulative sum | `mx.cumsum(data)` | Window function support |
| BETWEEN a AND b | `mx.logical_and(mx.greater(data, a), mx.less(data, b))` | Combined predicates |

---

## Architecture: Rust + Swift/MLX

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apple Silicon SoC                            │
│  ┌─────────┐   ┌─────────┐   ┌──────────────┐                  │
│  │   CPU   │   │   GPU   │   │ Neural Engine│                  │
│  │ Cores   │   │ Cores   │   │  (16-core)   │                  │
│  └────┬────┘   └────┬────┘   └──────┬───────┘                  │
│       │             │               │                          │
│       └─────────────┴───────────────┘                          │
│                     │                                          │
│       ┌─────────────▼─────────────────┐                       │
│       │     Unified Memory            │                       │
│       │     (8GB - 192GB)             │                       │
│       │     No copies needed!         │                       │
│       └───────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘

Software Stack:
┌──────────────┐    FFI    ┌──────────────┐    Zero-copy    ┌──────────┐
│ Rust Engine  │◄────────►│ Swift Bridge │◄───────────────►│   MLX    │
│ (Planning,   │          │ (cbindgen/   │                 │ (GPU/NE) │
│  Execution)  │          │  swift-bridge)│                │          │
└──────────────┘          └──────────────┘                 └──────────┘
```

---

## FFI Implementation

### Option 1: swift-bridge (Recommended)

```rust
// Rust: src/mlx/bridge.rs
#[swift_bridge::bridge]
mod mlx_bridge {
    extern "Swift" {
        fn mlx_filter_f64(
            data: *const f64,
            len: usize,
            op: u8,  // 0=gt, 1=lt, 2=eq, 3=gte, 4=lte, 5=neq
            value: f64
        ) -> MLXArrayResult;

        fn mlx_sum_f64(data: *const f64, len: usize) -> f64;
        fn mlx_mean_f64(data: *const f64, len: usize) -> f64;
        fn mlx_sort_indices_f64(data: *const f64, len: usize) -> *mut usize;

        fn mlx_topk_f64(data: *const f64, len: usize, k: usize) -> TopKResult;
    }

    struct MLXArrayResult {
        data: *mut f64,
        len: usize,
    }

    struct TopKResult {
        values: *mut f64,
        indices: *mut usize,
        len: usize,
    }
}
```

### Swift Side

```swift
// Swift: src/mlx/swift/MLXBridge.swift
import MLX

@objc public class MLXBridge: NSObject {
    @objc public static func mlx_filter_f64(
        _ data: UnsafePointer<Double>,
        _ len: Int,
        _ op: UInt8,
        _ value: Double
    ) -> MLXArrayResult {
        let arr = mx.Array(data: Data(bytes: data, count: len * 8), dtype: .float64)

        let mask: mx.Array
        switch op {
        case 0: mask = arr .> value      // gt
        case 1: mask = arr .< value      // lt
        case 2: mask = arr .== value     // eq
        case 3: mask = arr .>= value     // gte
        case 4: mask = arr .<= value     // lte
        default: mask = arr .!= value    // neq
        }

        let result = arr[mask]
        return result.toResult()
    }

    @objc public static func mlx_sum_f64(
        _ data: UnsafePointer<Double>,
        _ len: Int
    ) -> Double {
        let arr = mx.Array(data: Data(bytes: data, count: len * 8), dtype: .float64)
        return arr.sum().item()
    }
}
```

---

## Implementation Phases

### Phase 1: Core GPU Operators (2-4 weeks)

1. **Setup FFI Infrastructure**
   - Add swift-bridge dependency
   - Create Swift package for MLX operations
   - Build script to compile Swift into static library

2. **Implement GPU Filter**
   - `MLXFilterExec` operator wrapping `mx.greater/less/equal`
   - Fall back to CPU filter when MLX unavailable

3. **Implement GPU Aggregation**
   - `MLXAggregateExec` using `mx.sum/mean/min/max`
   - Group-by via masked aggregation

4. **Implement GPU Sort**
   - `MLXSortExec` using `mx.argsort`
   - `MLXTopKExec` using `mx.topk` (highly optimized)

### Phase 2: ML-Integrated Features (4-8 weeks)

5. **Learned Indexes**
   - Train small MLP on key column CDF
   - Replace binary search with neural prediction
   - Local search around predicted position

6. **Cardinality Estimation**
   - Train model on query patterns → result sizes
   - Feed: table stats, filter selectivity, join columns
   - Output: estimated row count

7. **Vector Similarity Search**
   - Store embeddings in unified memory
   - GPU-accelerated cosine similarity
   - Top-K retrieval

### Phase 3: Advanced Features (8-16 weeks)

8. **RL Query Optimizer**
   - State: encoded query plan
   - Action: apply transformation
   - Reward: query execution time

9. **Approximate Query Processing**
   - Learn data distributions
   - Answer queries from model (with confidence bounds)
   - 100-1000x speedup for analytics

---

## MLX vs Metal Decision Matrix

| Operation | MLX | Custom Metal |
|-----------|-----|--------------|
| Element-wise filter | ✅ Best | Overkill |
| Reduction (sum, mean) | ✅ Best | Overkill |
| Sort / Top-K | ✅ Best | Overkill |
| Hash join | ⚠️ Possible | ✅ Better |
| Custom join algorithms | ❌ Limited | ✅ Best |
| Learned index inference | ✅ Best | Overkill |
| Vector similarity | ✅ Best | Equal |

**Rule**: Start with MLX. Add custom Metal only for hot paths that MLX can't express.

---

## Key Files Structure

```
src/
├── mlx/
│   ├── mod.rs                    # Feature-gated module
│   ├── bridge.rs                 # FFI declarations
│   ├── operators/
│   │   ├── mod.rs
│   │   ├── filter.rs             # MLXFilterExec
│   │   ├── aggregate.rs          # MLXAggregateExec
│   │   └── sort.rs               # MLXSortExec, MLXTopKExec
│   ├── learned/
│   │   ├── mod.rs
│   │   ├── index.rs              # LearnedIndex
│   │   ├── cardinality.rs        # CardinalityEstimator
│   │   └── vector.rs             # VectorSimilaritySearch
│   └── swift/
│       ├── Package.swift
│       ├── Sources/
│       │   └── MLXBridge/
│       │       ├── Bridge.swift
│       │       ├── FilterOps.swift
│       │       ├── AggOps.swift
│       │       └── SortOps.swift
│       └── build.sh              # Compiles Swift to .a
```

---

## Cargo.toml Addition

```toml
[features]
default = []
mlx = ["swift-bridge"]  # Only builds on macOS with Swift

[dependencies]
swift-bridge = { version = "0.1", optional = true }

[build-dependencies]
swift-bridge-build = { version = "0.1", optional = true }
```

---

## Verification Commands

```bash
# Check Apple Silicon
uname -m  # Should be "arm64"

# Check Swift
swift --version

# Build with MLX
cargo build --release --features mlx

# Benchmark MLX vs CPU
cargo bench --bench mlx_filter
cargo bench --bench mlx_aggregate

# Run TPC-H with MLX
cargo run --release --features mlx -- benchmark --sf 0.1
```

---

## Creative Frontiers (The "Next Big Thing")

### 1. Learned Database Components

| Component | Traditional | Learned (MLX) |
|-----------|-------------|---------------|
| Index | B-tree, Hash | Neural CDF |
| Cardinality | Histograms | Neural estimator |
| Query plan | Heuristic rules | RL agent |
| Cache | LRU | Predictive ML |

### 2. Neural Engine Optimization

The 16-core Neural Engine (38 TOPS) is ideal for:
- Small model inference (learned indexes)
- Batch scoring (cardinality estimation)
- Embedding generation (for vector search)

### 3. Unified Memory = New Algorithms

Traditional databases assume CPU-GPU copy overhead. With unified memory:
- No batching required for small operations
- Can keep hot data GPU-resident indefinitely
- Join algorithms that would be memory-bound become compute-bound

---

## Sources

- [MLX Documentation](https://ml-explore.github.io/mlx/)
- [MLX GitHub](https://github.com/ml-explore/mlx)
- [WWDC 2025: Get started with MLX](https://developer.apple.com/videos/play/wwdc2025/315/)
- [Learned Index Structures](https://arxiv.org/pdf/1712.01208)
- [ML-Based Cardinality Estimation](https://www.itm-conferences.org/articles/itmconf/pdf/2025/09/itmconf_cseit2025_03023.pdf)
- [swift-bridge](https://github.com/chinedufn/swift-bridge)
