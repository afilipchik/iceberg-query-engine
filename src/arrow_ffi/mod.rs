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

pub use array::*;
pub use codec::*;
pub use export::*;
pub use import::*;
