use crate::catalog::transaction::Context;
use crate::catalog::Catalog;
use datafusion::arrow::array::{StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::sync::Arc;

pub fn tables_memory_table<C: Context>(ctx: &C, catalog: &Catalog) -> MemTable {
    let arrow_schema = Arc::new(tables_arrow_schema());

    let mut schema_names = StringBuilder::new();
    let mut table_names = StringBuilder::new();
    let mut column_counts = UInt32Builder::new();
    let mut accesses = StringBuilder::new();

    for schema in catalog.schemas.iter(ctx) {
        for table in schema.tables.iter(ctx) {
            schema_names.append_value(&table.schema);
            table_names.append_value(&table.name);
            column_counts.append_value(table.columns.len() as u32);
            accesses.append_value(table.access.to_string());
        }
    }

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(schema_names.finish()),
            Arc::new(table_names.finish()),
            Arc::new(column_counts.finish()),
            Arc::new(accesses.finish()),
        ],
    )
    .unwrap();

    MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
}

pub fn tables_arrow_schema() -> ArrowSchema {
    let fields = vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_count", DataType::UInt32, false),
        Field::new("access", DataType::Utf8, false),
    ];
    ArrowSchema::new(fields)
}
