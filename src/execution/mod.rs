//! Query execution module

pub mod alloc_profile;
mod context;
pub(crate) mod expression_memory;
mod memory;
mod reserved_buffer;
pub(crate) mod reserved_scalar;
pub(crate) mod reserved_vec;
pub(crate) mod retained_batch;
pub mod topology;

pub use context::*;
pub use memory::*;
pub use reserved_buffer::ReservedBufferBuilder;
pub use topology::Topology;

#[cfg(feature = "gpu")]
pub use crate::physical::gpu::{
    GpuResidentEvidence, GpuResidentPreparation, GpuResidentQueryOutcome, PreparedGpuSession,
};
