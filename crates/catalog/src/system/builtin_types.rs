use crate::system::{SystemTable, Table};
use catalog_types::datatypes::type_id_for_arrow_type;
use datafusion::arrow::array::{StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::sync::Arc;

/// Table decribing the builtin types for the database.
pub struct BuiltinTypesTable {}

impl BuiltinTypesTable {
    pub fn new() -> BuiltinTypesTable {
        BuiltinTypesTable {}
    }
}

impl SystemTable for BuiltinTypesTable {
    fn schema(&self) -> Schema {
        Schema::new(vec![
            Field::new("type_id", DataType::UInt32, false),
            Field::new("type_name", DataType::Utf8, false),
        ])
    }

    fn name(&self) -> &'static str {
        "builtin_types"
    }

    fn is_readonly(&self) -> bool {
        true
    }

    fn get_table(&self) -> Table {
        // TODO: This could all be generated once.

        let schema = Arc::new(self.schema());
        let types = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
        ];

        let mut type_ids = UInt32Builder::with_capacity(types.len());
        let mut type_names = StringBuilder::with_capacity(types.len(), 10);
        for typ in types {
            let id = type_id_for_arrow_type(&typ).unwrap();
            let name = format!("{:?}", typ);
            type_ids.append_value(id);
            type_names.append_value(name);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(type_ids.finish()), Arc::new(type_names.finish())],
        )
        .unwrap();

        MemTable::try_new(schema, vec![vec![batch]]).unwrap().into()
    }
}
