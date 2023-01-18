//! The metastore crate defines the service for managing database catalogs.
pub mod errors;
pub mod proto;
pub mod session;
pub mod types;

mod database;
mod srv;
