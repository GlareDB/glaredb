use crate::errors::{internal, CatalogError, Result};
use crate::system::{SystemSchema, SystemTable, SystemTableAccessor, SYSTEM_SCHEMA_ID};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::context::SessionContext;
use catalog_types::keys::{PartitionId, SchemaId, TableId, TableKey};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
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

/// Scan all schema names in the database.
pub async fn scan_schema_names(
    ctx: &SessionContext,
    runtime: &Arc<AccessRuntime>,
    system: &SystemSchema,
) -> Result<Vec<String>> {
    let schemas_table = system
        .get_system_table_accessor(SCHEMAS_TABLE_NAME)
        .ok_or(CatalogError::MissingSystemTable(
            SCHEMAS_TABLE_NAME.to_string(),
        ))?
        .get_table(runtime.clone());
    let partitioned_table = schemas_table.get_partitioned_table()?;

    // Only care about the name.
    let projection = Some(vec![1]);
    // No filters since we only have one "database".
    let plan = partitioned_table
        .scan_inner(ctx.get_df_state(), &projection, &[], None)
        .await?;
    let stream = plan.execute(0, ctx.task_context())?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;

    let mut names = Vec::with_capacity(batches.iter().fold(0, |acc, batch| acc + batch.num_rows()));
    for batch in batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal!("failed to downcast to strings array"))?;

        for val in col.into_iter() {
            match val {
                Some(val) => names.push(val.to_string()),
                None => return Err(internal!("unexpected null value for schema name")),
            }
        }
    }

    Ok(names)
}

/// Insert a a new schema into the database.
pub async fn insert_schema(
    ctx: &SessionContext,
    runtime: &Arc<AccessRuntime>,
    system: &SystemSchema,
) -> Result<()> {
    unimplemented!()
}
