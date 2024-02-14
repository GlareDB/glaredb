//! The metastore crate defines the service for managing database catalogs.
pub mod errors;
pub mod local;
pub mod srv;
pub mod util;

mod database;
mod storage;
