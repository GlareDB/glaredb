use crate::errors::{internal, CatalogError, Result};
use crate::filter::filter_scan;
use crate::system::constants::{RELATIONS_TABLE_ID, RELATIONS_TABLE_NAME};
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
use futures::TryStreamExt;
use itertools::izip;
use std::sync::Arc;
use tracing::error;

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
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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

/// Represents a single row in the relations system table.
#[derive(Debug)]
pub struct RelationRow {
    pub schema_id: SchemaId,
    pub table_id: TableId,
    pub table_name: String,
}

impl RelationRow {
    /// Scan all relations in a schema.
    pub async fn scan_many_in_schema(
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
        schema: SchemaId,
    ) -> Result<Vec<RelationRow>> {
        let schemas_table = system
            .get_system_table_accessor(RELATIONS_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(RELATIONS_TABLE_NAME.to_string()))?
            .get_table(runtime.clone());
        let partitioned_table = schemas_table.into_table_provider_ref();

        // Filter for only this schema.
        let filter = Expr::Column(Column::from_name("schema_id"))
            .eq(Expr::Literal(ScalarValue::UInt32(Some(schema))));

        let plan = filter_scan(partitioned_table, ctx.get_df_state(), &[filter], None).await?;
        let stream = plan.execute(0, ctx.task_context())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let relations = Self::read_from_batches(batches)?;

        Ok(relations)
    }

    /// Scan a single relation in a schema by its name.
    pub async fn scan_one_by_name(
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
        schema: SchemaId,
        name: &str,
    ) -> Result<Option<RelationRow>> {
        let schemas_table = system
            .get_system_table_accessor(RELATIONS_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(RELATIONS_TABLE_NAME.to_string()))?
            .get_table(runtime.clone());
        let partitioned_table = schemas_table.into_table_provider_ref();

        // Filter for only this schema.
        let filter = Expr::Column(Column::from_name("schema_id"))
            .eq(Expr::Literal(ScalarValue::UInt32(Some(schema))))
            .and(
                Expr::Column(Column::from_name("table_name"))
                    .eq(Expr::Literal(ScalarValue::new_utf8(name))),
            );

        let plan = filter_scan(partitioned_table, ctx.get_df_state(), &[filter], None).await?;
        let stream = plan.execute(0, ctx.task_context())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let mut relations = Self::read_from_batches(batches)?;

        match relations.pop() {
            Some(relation) => {
                if relations.is_empty() {
                    error!(
                        ?name,
                        ?schema,
                        "multiple relations returned when scanning by name"
                    );
                    // Return error in the future.
                }
                Ok(Some(relation))
            }
            None => Ok(None),
        }
    }

    /// Insert a table into the relations system table.
    // TODO: Check for dupes.
    pub async fn insert(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        let accessor = system
            .get_system_table_accessor(RELATIONS_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(RELATIONS_TABLE_NAME.to_string()))?;
        let schemas_table = accessor.get_table(runtime.clone());
        let partitioned_table = schemas_table.get_partitioned_table()?;

        let batch = RecordBatch::try_new(
            accessor.schema(),
            vec![
                Arc::new(UInt32Array::from_value(self.schema_id, 1)),
                Arc::new(UInt32Array::from_value(self.table_id, 1)),
                Arc::new(StringArray::from_iter_values([&self.table_name])),
            ],
        )?;

        partitioned_table.insert(ctx, batch).await?;

        Ok(())
    }

    fn read_from_batches(batches: Vec<RecordBatch>) -> Result<Vec<Self>> {
        let mut relations: Vec<RelationRow> =
            Vec::with_capacity(batches.iter().fold(0, |acc, batch| acc + batch.num_rows()));
        for batch in batches {
            let schema_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| internal!("failed to downcast schema ids to uint32 array"))?;
            let table_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| internal!("failed to downcast table ids to uint32 array"))?;
            let table_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal!("failed to downcast table names to string array"))?;

            for (schema_id, table_id, table_name) in izip!(schema_ids, table_ids, table_names) {
                let schema_id = match schema_id {
                    Some(schema_id) => schema_id,
                    None => return Err(internal!("unexpected null value for schema id")),
                };
                let table_id = match table_id {
                    Some(table_id) => table_id,
                    None => return Err(internal!("unexpected null value for table id")),
                };
                let table_name = match table_name {
                    Some(table_name) => table_name,
                    None => return Err(internal!("unexpected null value for table name")),
                };

                relations.push(RelationRow {
                    schema_id,
                    table_id,
                    table_name: table_name.to_string(),
                });
            }
        }

        Ok(relations)
    }
}
