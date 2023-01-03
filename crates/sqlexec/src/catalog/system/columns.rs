use crate::catalog::transaction::Context;
use crate::catalog::Catalog;
use datafusion::arrow::array::{BooleanBuilder, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::sync::Arc;

pub fn columns_memory_table<C: Context>(ctx: &C, catalog: &Catalog) -> MemTable {
    let arrow_schema = Arc::new(columns_arrow_schema());

    let mut schema_names = StringBuilder::new();
    let mut table_names = StringBuilder::new();
    let mut column_names = StringBuilder::new();
    let mut column_indexes = UInt32Builder::new();
    let mut data_types = StringBuilder::new();
    let mut is_nullables = BooleanBuilder::new();

    for schema in catalog.schemas.iter(ctx) {
        for table in schema.tables.iter(ctx) {
            for (i, col) in table.columns.iter().enumerate() {
                schema_names.append_value(&table.schema);
                table_names.append_value(&table.name);
                column_names.append_value(&col.name);
                column_indexes.append_value(i as u32);
                data_types.append_value(col.datatype.to_string());
                is_nullables.append_value(col.nullable);
            }
        }
    }

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(schema_names.finish()),
            Arc::new(table_names.finish()),
            Arc::new(column_names.finish()),
            Arc::new(column_indexes.finish()),
            Arc::new(data_types.finish()),
            Arc::new(is_nullables.finish()),
        ],
    )
    .unwrap();

    MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
}

pub fn columns_arrow_schema() -> ArrowSchema {
    let fields = vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("column_index", DataType::UInt32, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Boolean, false),
    ];
    ArrowSchema::new(fields)
}
