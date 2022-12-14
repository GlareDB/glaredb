use crate::catalog::Catalog;
use crate::transaction::Context;
use datafusion::arrow::array::{StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::sync::Arc;

pub fn views_memory_table<C: Context>(ctx: &C, catalog: &Catalog) -> MemTable {
    let arrow_schema = Arc::new(views_arrow_schema());

    let mut schema_names = StringBuilder::new();
    let mut view_names = StringBuilder::new();
    let mut column_counts = UInt32Builder::new();
    let mut sqls = StringBuilder::new();

    for schema in catalog.schemas.iter(ctx) {
        for view in schema.views.iter(ctx) {
            schema_names.append_value(&view.schema);
            view_names.append_value(&view.name);
            column_counts.append_value(view.column_count);
            sqls.append_value(&view.sql);
        }
    }

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(schema_names.finish()),
            Arc::new(view_names.finish()),
            Arc::new(column_counts.finish()),
            Arc::new(sqls.finish()),
        ],
    )
    .unwrap();

    MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
}

pub fn views_arrow_schema() -> ArrowSchema {
    let fields = vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("view_name", DataType::Utf8, false),
        Field::new("column_count", DataType::UInt32, false),
        Field::new("sql", DataType::Utf8, false),
    ];
    ArrowSchema::new(fields)
}
