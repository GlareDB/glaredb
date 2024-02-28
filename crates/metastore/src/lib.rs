//! The metastore crate defines the service for managing database catalogs.
pub mod client;
mod database;
pub mod errors;
pub mod local;
pub mod srv;
mod storage;
pub mod util;
