use crate::dbg::dbg_table;
use crate::errors::{internal, CatalogError, Result};
use crate::filter::filter_scan;
use crate::system::{SystemSchema, SystemTable, SystemTableAccessor, SYSTEM_SCHEMA_ID};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::context::SessionContext;
use catalog_types::interfaces::MutableTableProvider;
use catalog_types::keys::{SchemaId, TableId, TableKey};
use datafusion::arrow::array::{StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::Expr;
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;
use tracing::error;

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
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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

/// Represents a single row in the schemas system table.
#[derive(Debug)]
pub struct SchemaRow {
    pub id: SchemaId,
    pub name: String,
}

impl SchemaRow {
    /// Scan all schemas.
    pub async fn scan_many(
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<Vec<Self>> {
        let schemas_table = system
            .get_system_table_accessor(SCHEMAS_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(SCHEMAS_TABLE_NAME.to_string()))?
            .get_table(runtime.clone());
        let partitioned_table = schemas_table.get_partitioned_table()?;

        let plan = partitioned_table
            .scan_inner(ctx.get_df_state(), &None, &[], None)
            .await?;
        let stream = plan.execute(0, ctx.task_context())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let mut schemas: Vec<SchemaRow> =
            Vec::with_capacity(batches.iter().fold(0, |acc, batch| acc + batch.num_rows()));
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| internal!("failed to downcast schema ids to uint32 array"))?;
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal!("failed to downcast schema names to string array"))?;

            for (id, name) in ids.into_iter().zip(names.into_iter()) {
                let id = match id {
                    Some(id) => id,
                    None => return Err(internal!("schema id unexpectedly null")),
                };
                let name = match name {
                    Some(name) => name,
                    None => return Err(internal!("schema name unexpectedly null")),
                };
                schemas.push(SchemaRow {
                    id,
                    name: name.to_string(),
                })
            }
        }

        Ok(schemas)
    }

    pub async fn scan_by_name(
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
        name: &str,
    ) -> Result<Option<SchemaRow>> {
        let schemas_table = system
            .get_system_table_accessor(SCHEMAS_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(SCHEMAS_TABLE_NAME.to_string()))?
            .get_table(runtime.clone());
        let table = schemas_table.into_table_provider_ref();

        let filter = Expr::Column(Column::from_name("schema_name"))
            .eq(Expr::Literal(ScalarValue::Utf8(Some(name.to_string()))));
        let plan = filter_scan(&table, ctx.get_df_state(), &[filter], None).await?;
        let stream = plan.execute(0, ctx.task_context())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_num_rows = batches.iter().fold(0, |acc, batch| acc + batch.num_rows());
        if total_num_rows > 1 {
            error!(%name, "expected a single schema row for name");
            // Continue on. Eventually this will return an error when we check
            // for duplicate schema names.
        }

        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| internal!("failed to downcast schema ids to uint32 array"))?;
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal!("failed to downcast schema names to string array"))?;

            for (id, name) in ids.into_iter().zip(names.into_iter()) {
                let id = match id {
                    Some(id) => id,
                    None => return Err(internal!("schema id unexpectedly null")),
                };
                let name = match name {
                    Some(name) => name,
                    None => return Err(internal!("schema name unexpectedly null")),
                };
                // Return the first schema we get.
                return Ok(Some(SchemaRow {
                    id,
                    name: name.to_string(),
                }));
            }
        }

        // No schema with the given name found.
        Ok(None)
    }

    /// Insert a schema into the schema system table.
    // TODO: Check for duplicates.
    pub async fn insert(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        let accessor = system
            .get_system_table_accessor(SCHEMAS_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(SCHEMAS_TABLE_NAME.to_string()))?;
        let schemas_table = accessor.get_table(runtime.clone());
        let partitioned_table = schemas_table.get_partitioned_table()?;

        let batch = RecordBatch::try_new(
            accessor.schema(),
            vec![
                Arc::new(UInt32Array::from_value(self.id, 1)),
                Arc::new(StringArray::from_iter_values(&[&self.name])),
            ],
        )?;

        partitioned_table.insert(ctx, batch).await?;

        Ok(())
    }
}
