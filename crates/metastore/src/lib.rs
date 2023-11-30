//! The metastore crate defines the service for managing database catalogs.
pub mod errors;
pub mod local;
pub mod srv;

mod database;
mod entries;
mod storage;
pub mod util;
