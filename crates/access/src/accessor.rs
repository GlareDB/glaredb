use crate::errors::Result;
use crate::keys::TableKey;
use persistence::file::DiskCache;
use scc::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DatabaseAccessor {
    disk: Arc<DiskCache>,
    tables: HashMap<TableKey, TableAccessor>,
}

#[derive(Debug, Clone)]
pub struct TableAccessor {
    disk: Arc<DiskCache>,
}

pub trait PartitionReader: Send + Sync {}

impl TableAccessor {}

pub struct LocalPartitionAccessor {
    table: TableAccessor,
}
