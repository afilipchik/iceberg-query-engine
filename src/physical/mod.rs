//! Physical execution module
//!
//! Converts logical plans to physical plans and executes them

pub mod operators;
mod plan;
mod planner;

pub use operators::*;
pub use plan::*;
pub use planner::*;
