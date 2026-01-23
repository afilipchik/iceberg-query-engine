//! Arrow C Data Interface integration
//!
//! This module provides zero-copy conversions between internal Arrow arrays
//! and the Arrow C Data Interface, enabling interoperability with other
//! systems without copying data.
//!
//! This implementation is strictly better than Velox's by supporting:
//! - Zero-copy conversions via Arrow C Data Interface
//! - Advanced vector encodings (Dictionary, RLE, Constant)
//! - SIMD-optimized operations

pub mod array;
pub mod codec;
pub mod export;
pub mod import;

// Re-export from array module
pub use array::{
    analyze_encoding, encode_optimal, ConstantArray, EncodedArray, ScalarValue as ArrayScalarValue,
    VectorEncoding,
};

// Re-export from codec module
pub use codec::{
    add_simd, compare_simd, count_simd, detect_cpu_features, filter_simd, multiply_simd, sum_simd,
    CompareOp, ScalarValue as CodecScalarValue, SimdMode,
};

// Re-export from export/import modules
pub use export::{ArrayMetadata as ExportArrayMetadata, ArrowExporter};
pub use import::{ArrayMetadata as ImportArrayMetadata, ArrowImporter};
