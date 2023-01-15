//! Module for handling the catalog for a single database.
use crate::proto::catalog;
use parking_lot::Mutex;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct DatabaseCatalog {}

struct State {
    /// Version incremented on every update.
    version: u64,
    // entries: BTreeMap<u32, >
}
