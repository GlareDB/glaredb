use crate::catalog::{Catalog, Context};
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::sync::Arc;

pub fn schemas_memory_table<C: Context>(ctx: &C, catalog: &Catalog) -> MemTable {
    let arrow_schema = Arc::new(schema_arrow_schema());

    let mut schema_names = StringBuilder::new();

    for schema in catalog.schemas.iter(ctx) {
        schema_names.append_value(&schema.name);
    }

    let batch =
        RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(schema_names.finish())]).unwrap();

    MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
}

pub fn schema_arrow_schema() -> ArrowSchema {
    let fields = vec![Field::new("schema_name", DataType::Utf8, false)];
    ArrowSchema::new(fields)
}
