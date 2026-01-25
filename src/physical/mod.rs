//! Physical execution module
//!
//! Converts logical plans to physical plans and executes them

pub mod morsel;
pub mod morsel_agg;
pub mod operators;
mod plan;
mod planner;

pub use morsel::*;
pub use morsel_agg::*;
pub use operators::*;
pub use plan::*;
pub use planner::*;
