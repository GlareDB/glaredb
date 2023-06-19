//! Client and type abstractions over metastore.
//!
//! Crates should try to import this crate instead of `metastore` directly to
//! avoid dependency cycles.
pub mod proto;
pub mod session;
pub mod types;
