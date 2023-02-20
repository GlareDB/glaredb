//! The metastore crate defines the service for managing database catalogs.
pub mod builtins;
pub mod errors;
pub mod local;
pub mod proto;
pub mod session;
pub mod srv;
pub mod types;

mod database;
mod storage;
