//! Shell utilities and wrappers.

pub mod highlighter;
pub mod lineedit;
pub mod raw;
#[allow(clippy::module_inception)]
pub mod shell; // TODO: Naming

mod debug;
mod vt100;
