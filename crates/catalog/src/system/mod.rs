//! Built-in system catalog tables.
pub mod builtin_types;
pub mod relations;
pub mod schemas;
pub mod sequences;

use crate::errors::{CatalogError, Result};
use access::runtime::AccessRuntime;
use access::table::PartitionedTable;
use catalog_types::keys::SchemaId;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use std::collections::HashMap;
use std::sync::Arc;

use builtin_types::BuiltinTypesTable;
use relations::RelationsTable;
use schemas::SchemasTable;
use sequences::SequencesTable;

/// Maximum reserved schema id. All user schemas are required to be greater than
/// this.
pub const MAX_RESERVED_SCHEMA_ID: SchemaId = 100;

pub const SYSTEM_SCHEMA_NAME: &str = "system_catalog";
pub const SYSTEM_SCHEMA_ID: SchemaId = 0;

/// The types of table that a "system" table can be.
///
/// NOTE: This will eventually included a "cloud" table type for interactive
/// with the Cloud service.
pub enum SystemTable {
    /// Completely in-memory and nothing is persisted. Presented a view to the
    /// user.
    View(MemTable),
    /// A persistent table backed by a partitioned table.
    Base(PartitionedTable),
}

impl SystemTable {
    // TODO: Change this to return a "MutableTableProvider".
    pub fn get_partitioned_table(&self) -> Result<&PartitionedTable> {
        match self {
            SystemTable::View(_) => Err(CatalogError::TableReadonly),
            SystemTable::Base(table) => Ok(table),
        }
    }

    fn as_table_provider(self) -> Arc<dyn TableProvider> {
        match self {
            SystemTable::View(table) => Arc::new(table),
            SystemTable::Base(table) => Arc::new(table),
        }
    }
}

impl From<MemTable> for SystemTable {
    fn from(table: MemTable) -> Self {
        SystemTable::View(table)
    }
}

impl From<PartitionedTable> for SystemTable {
    fn from(table: PartitionedTable) -> Self {
        SystemTable::Base(table)
    }
}

pub trait SystemTableAccessor: Sync + Send {
    /// Return the schema of the table.
    fn schema(&self) -> &Schema;

    /// Return the constant name for the table.
    fn name(&self) -> &'static str;

    /// Returns whether or not this table is read-only.
    fn is_readonly(&self) -> bool;

    /// Get the underlying table type.
    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable;
}

pub struct SystemSchema {
    tables: HashMap<&'static str, Arc<dyn SystemTableAccessor>>,
}

impl SystemSchema {
    pub fn bootstrap() -> Result<SystemSchema> {
        let tables: &[Arc<dyn SystemTableAccessor>] = &[
            Arc::new(BuiltinTypesTable::new()),
            Arc::new(RelationsTable::new()),
            Arc::new(SchemasTable::new()),
            Arc::new(SequencesTable::new()),
        ];

        let tables: HashMap<_, _> = tables
            .into_iter()
            .map(|table| (table.name(), table.clone()))
            .collect();

        Ok(SystemSchema { tables })
    }

    pub fn get_system_table_accessor(&self, name: &str) -> Option<Arc<dyn SystemTableAccessor>> {
        self.tables.get(name).cloned()
    }

    /// Provider returns the system schema provider.
    pub fn provider(&self, runtime: Arc<AccessRuntime>) -> SystemSchemaProvider {
        SystemSchemaProvider {
            runtime,
            tables: self.tables.clone(), // Cheap, accessors are behind an arc and all table names are static.
        }
    }
}

/// Provides the system schema. Contains a reference to the access runtime to
/// allow reading from persistent storage.
pub struct SystemSchemaProvider {
    runtime: Arc<AccessRuntime>,
    tables: HashMap<&'static str, Arc<dyn SystemTableAccessor>>,
}

impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(|s| s.to_string()).collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        Some(
            self.tables
                .get(name)?
                .get_table(self.runtime.clone())
                .as_table_provider(),
        )
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
