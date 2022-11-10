//! Built-in system catalog tables.
pub mod builtin_types;

use crate::errors::Result;
use access::table::PartitionedTable;
use catalog_types::keys::SchemaId;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::MemTable;
use std::collections::HashMap;
use std::sync::Arc;

use builtin_types::BuiltinTypesTable;

pub const SYSTEM_SCHEMA_NAME: &str = "system_catalog";
pub const SYSTEM_SCHEMA_ID: SchemaId = 0;

pub enum Table {
    View(MemTable),
    Base(PartitionedTable),
}

impl Table {
    pub fn is_view(&self) -> bool {
        matches!(self, Table::View(_))
    }
}

impl From<MemTable> for Table {
    fn from(table: MemTable) -> Self {
        Table::View(table)
    }
}

impl From<PartitionedTable> for Table {
    fn from(table: PartitionedTable) -> Self {
        Table::Base(table)
    }
}

pub trait SystemTable {
    /// Return the schema of the table.
    fn schema(&self) -> Schema;

    fn name(&self) -> &'static str;

    fn is_readonly(&self) -> bool;

    // TODO: Pass in access stuff.
    fn get_table(&self) -> Table;
}

pub struct SystemSchema {
    tables: HashMap<&'static str, Arc<dyn SystemTable>>,
}

impl SystemSchema {
    pub fn bootstrap() -> Result<SystemSchema> {
        let tables: &[Arc<dyn SystemTable>] = &[Arc::new(BuiltinTypesTable::new())];

        let tables: HashMap<_, _> = tables
            .into_iter()
            .map(|table| (table.name(), table.clone()))
            .collect();

        Ok(SystemSchema { tables })
    }
}
