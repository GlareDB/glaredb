use crate::system::{SystemTable, SystemTableAccessor, SYSTEM_SCHEMA_ID};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::keys::{PartitionId, SchemaId, TableId, TableKey};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub const SCHEMAS_TABLE_ID: TableId = 0;
pub const SCHEMAS_TABLE_NAME: &str = "schemas";

pub struct SchemasTable {
    schema: SchemaRef,
}

impl SchemasTable {
    pub fn new() -> SchemasTable {
        SchemasTable {
            schema: Arc::new(Schema::new(vec![
                Field::new("schema_id", DataType::UInt32, false),
                Field::new("schema_name", DataType::Utf8, false),
            ])),
        }
    }
}

impl SystemTableAccessor for SchemasTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn name(&self) -> &'static str {
        SCHEMAS_TABLE_NAME
    }

    fn is_readonly(&self) -> bool {
        false
    }

    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable {
        let key = TableKey {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: SCHEMAS_TABLE_ID,
        };

        SystemTable::Base(PartitionedTable::new(
            key,
            Box::new(SinglePartitionStrategy),
            runtime,
            self.schema.clone(),
        ))
    }
}
