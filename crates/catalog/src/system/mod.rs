//! Built-in system catalog tables.
pub mod attributes;
pub mod builtin_types;
pub mod constants;
pub mod relations;
pub mod schemas;
pub mod sequences;

use crate::errors::{internal, CatalogError, Result};
use access::runtime::AccessRuntime;
use access::table::PartitionedTable;
use catalog_types::context::SessionContext;
use catalog_types::keys::SchemaId;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use attributes::AttributesTable;
use builtin_types::BuiltinTypesTable;
use relations::{RelationRow, RelationsTable};
use schemas::{SchemaRow, SchemasTable};
use sequences::{SequenceRow, SequencesTable};

/// Maximum reserved schema id. All user schemas are required to be greater than
/// this.
pub const MAX_RESERVED_SCHEMA_ID: SchemaId = 100;

pub const SYSTEM_SCHEMA_NAME: &str = "system_catalog";
pub const SYSTEM_SCHEMA_ID: SchemaId = 0;

// TODO: This ties every "id" in the system to the same sequence. Probably not
// fantastic. Kinda like oid?
pub const ID_SEQUENCE_NAME: &str = "id_seq";

/// Name of the public schema.
pub const PUBLIC_SCHEMA_NAME: &str = "public";
pub const PUBLIC_SCHEMA_ID: SchemaId = MAX_RESERVED_SCHEMA_ID + 1;

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

    pub fn into_table_provider_ref(self) -> Arc<dyn TableProvider> {
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
    fn schema(&self) -> SchemaRef;

    /// Return the constant name for the table.
    fn name(&self) -> &'static str;

    /// Returns whether or not this table is read-only.
    fn is_readonly(&self) -> bool;

    /// Get the underlying table type.
    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable;
}

/// An in-memory system schema backed by both persistent and transient tables.
pub struct SystemSchema {
    tables: HashMap<&'static str, Arc<dyn SystemTableAccessor>>,
}

impl SystemSchema {
    /// Create a new system schema.
    pub fn new() -> Result<SystemSchema> {
        let tables: &[Arc<dyn SystemTableAccessor>] = &[
            Arc::new(BuiltinTypesTable::new()),
            Arc::new(RelationsTable::new()),
            Arc::new(SchemasTable::new()),
            Arc::new(SequencesTable::new()),
            Arc::new(AttributesTable::new()),
        ];

        let tables: HashMap<_, _> = tables
            .iter()
            .map(|table| (table.name(), table.clone()))
            .collect();

        Ok(SystemSchema { tables })
    }

    /// Bootstrap the system schema.
    pub async fn bootstrap(&self, runtime: &Arc<AccessRuntime>) -> Result<()> {
        // TODO: This will eventually hold a global (cross-node) lock.
        // TODO: Everything should be broken up into "bootstrap steps". We'll
        // persist which steps have been ran, and only run newer bootstrap
        // steps. This will allow us to evolve the system schema.

        let sess_ctx = SessionContext::new(); // TODO: Have a "system" session context?

        // Ensure we have the id sequence table.
        let rel = RelationRow::scan_one_by_name(
            &sess_ctx,
            runtime,
            self,
            SYSTEM_SCHEMA_ID,
            ID_SEQUENCE_NAME,
        )
        .await?;
        if rel.is_none() {
            // Create it.
            let rel = RelationRow {
                schema_id: SYSTEM_SCHEMA_ID,
                table_id: constants::SEQUENCES_TABLE_ID,
                table_name: constants::SEQUENCES_TABLE_NAME.to_string(),
            };
            rel.insert(&sess_ctx, runtime, self).await?;
        }

        // Ensure we have the sequence entry.
        // TODO: Don't trigger a sequence increment.
        let next = SequenceRow::next(
            &sess_ctx,
            runtime,
            self,
            SYSTEM_SCHEMA_ID,
            constants::SEQUENCES_TABLE_ID,
        )
        .await?;
        if next.is_none() {
            // Insert the sequence row.
            let seq = SequenceRow {
                schema: SYSTEM_SCHEMA_ID,
                table: constants::SEQUENCES_TABLE_ID,
                // TODO: This is jank. Currently doing this avoid specifying an
                // id that would actually belong to a reserved schema.
                next: (PUBLIC_SCHEMA_ID + 1) as i64,
                inc: 1,
            };
            seq.insert(&sess_ctx, runtime, self).await?;
        }

        // Ensure that we have the "public" schema available. Note that without
        // discrete bootstrap steps, this would recreate the public schema even
        // if it's deleted by the user.
        let schema = SchemaRow::scan_by_name(&sess_ctx, runtime, self, PUBLIC_SCHEMA_NAME).await?;
        if schema.is_none() {
            let schema = SchemaRow {
                id: PUBLIC_SCHEMA_ID,
                name: PUBLIC_SCHEMA_NAME.to_string(),
            };
            schema.insert(&sess_ctx, runtime, self).await?;
        }

        Ok(())
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

    /// Get the next id to use.
    // TODO: Likely will remove.
    pub async fn next_id(&self, ctx: &SessionContext, runtime: &Arc<AccessRuntime>) -> Result<i64> {
        SequenceRow::next(
            ctx,
            runtime,
            self,
            SYSTEM_SCHEMA_ID,
            constants::SEQUENCES_TABLE_ID,
        )
        .await?
        .ok_or_else(|| {
            internal!(
                "missing sequence; schema id: {}, table id: {}, table name: {}",
                SYSTEM_SCHEMA_ID,
                constants::SEQUENCES_TABLE_ID,
                constants::SEQUENCES_TABLE_NAME
            )
        })
    }
}

impl fmt::Debug for SystemSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SystemSchema({:?})",
            self.tables.keys().collect::<Vec<_>>()
        )
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
                .into_table_provider_ref(),
        )
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
