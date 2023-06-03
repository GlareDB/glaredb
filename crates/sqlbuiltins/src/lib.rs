//! Builtin sql objects.
//!
//! This crate provides the implementation of various builtin sql objects
//! (particularly functions). These live outside the `sqlexec` crate to allow
//! it to be imported into both `sqlexec` and `metastore`.

pub mod errors;
pub mod functions;
