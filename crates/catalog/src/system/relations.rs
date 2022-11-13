use crate::system::{SystemTable, SystemTableAccessor, SYSTEM_SCHEMA_ID};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::datatypes::type_id_for_arrow_type;
use catalog_types::keys::{SchemaId, TableKey};
use datafusion::arrow::array::{StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::sync::Arc;

pub const RELATIONS_TABLE_ID: SchemaId = 3;
pub const RELATIONS_TABLE_NAME: &str = "relations";

pub struct RelationsTable {
    schema: SchemaRef,
}

impl RelationsTable {
    pub fn new() -> RelationsTable {
        RelationsTable {
            schema: Arc::new(Schema::new(vec![
                Field::new("schema_id", DataType::UInt32, false),
                Field::new("table_id", DataType::UInt32, false),
                Field::new("table_name", DataType::Utf8, false),
            ])),
        }
    }
}

impl SystemTableAccessor for RelationsTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn name(&self) -> &'static str {
        RELATIONS_TABLE_NAME
    }

    fn is_readonly(&self) -> bool {
        false
    }

    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable {
        let key = TableKey {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: RELATIONS_TABLE_ID,
        };

        SystemTable::Base(PartitionedTable::new(
            key,
            Box::new(SinglePartitionStrategy),
            runtime,
            self.schema.clone(),
        ))
    }
}
