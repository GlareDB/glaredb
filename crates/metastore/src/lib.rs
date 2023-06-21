//! The metastore crate defines the service for managing database catalogs.
pub mod builtins;
pub mod errors;
pub mod local;
pub mod srv;
pub mod types;
pub mod validation;

mod database;
mod storage;
