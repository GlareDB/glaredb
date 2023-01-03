//! SQL execution.
pub mod catalog;
pub mod context;
pub mod engine;
pub mod errors;
pub mod logical_plan;
pub mod parser;
pub mod session;

mod dispatch;
mod functions;
mod planner;
mod vars;
