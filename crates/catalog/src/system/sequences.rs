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

pub const SEQUENCES_TABLE_ID: SchemaId = 2;
pub const SEQUENCES_TABLE_NAME: &str = "builtin_types";

pub struct SequencesTable {
    schema: SchemaRef,
}

impl SequencesTable {
    pub fn new() -> SequencesTable {
        SequencesTable {
            schema: Arc::new(Schema::new(vec![
                Field::new("seq_schema_id", DataType::UInt32, false),
                Field::new("seq_rel_id", DataType::UInt32, false),
                Field::new("seq_data_type", DataType::UInt32, false), // Type ID
                Field::new("seq_start", DataType::Int64, false),
                Field::new("seq_min", DataType::Int64, false),
                Field::new("seq_max", DataType::Int64, false),
                Field::new("seq_inc", DataType::Int64, false),
            ])),
        }
    }
}

impl SystemTableAccessor for SequencesTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn name(&self) -> &'static str {
        SEQUENCES_TABLE_NAME
    }

    fn is_readonly(&self) -> bool {
        false
    }

    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable {
        let key = TableKey {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: SEQUENCES_TABLE_ID,
        };

        SystemTable::Base(PartitionedTable::new(
            key,
            Box::new(SinglePartitionStrategy),
            runtime,
            self.schema.clone(),
        ))
    }
}
