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
use futures::TryStreamExt;
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

/// Scan all relation names in a schema.
pub async fn scan_relation_names(
    ctx: &SessionContext,
    runtime: &Arc<AccessRuntime>,
    system: &SystemSchema,
    schema: SchemaId,
) -> Result<Vec<String>> {
    unimplemented!()
    // let schemas_table = system
    //     .get_system_table_accessor(RELATIONS_TABLE_NAME)
    //     .ok_or_else(|| CatalogError::MissingSystemTable(RELATIONS_TABLE_NAME.to_string()))?
    //     .get_table(runtime.clone());
    // let partitioned_table = schemas_table.into_table_provider_ref();

    // // Only care about the name.
    // let projection = Some(vec![2]);
    // // Filter for only this schema.
    // let filter = Expr::Column(Column::from_name("schema_id"))
    //     .eq(Expr::Literal(ScalarValue::UInt32(Some(schema))));

    // let plan = filter_scan(
    //     partitioned_table,
    //     ctx.get_df_state(),
    //     &projection,
    //     &[filter],
    //     None,
    // )
    // .await?;
    // let stream = plan.execute(0, ctx.task_context())?;
    // let batches: Vec<RecordBatch> = stream.try_collect().await?;

    // let mut names = Vec::with_capacity(batches.iter().fold(0, |acc, batch| acc + batch.num_rows()));
    // for batch in batches {
    //     let col = batch
    //         .column(0)
    //         .as_any()
    //         .downcast_ref::<StringArray>()
    //         .ok_or_else(|| internal!("failed to downcast to strings array"))?;

    //     for val in col.into_iter() {
    //         match val {
    //             Some(val) => names.push(val.to_string()),
    //             None => return Err(internal!("unexpected null value for schema name")),
    //         }
    //     }
    // }

    // Ok(names)
}

pub async fn insert_relation(
    ctx: &SessionContext,
    runtime: &Arc<AccessRuntime>,
    system: &SystemSchema,
    schema: SchemaId,
    id: TableId,
    name: &str,
) -> Result<()> {
    let accessor = system
        .get_system_table_accessor(RELATIONS_TABLE_NAME)
        .ok_or_else(|| CatalogError::MissingSystemTable(RELATIONS_TABLE_NAME.to_string()))?;
    let schemas_table = accessor.get_table(runtime.clone());
    let partitioned_table = schemas_table.get_partitioned_table()?;

    let batch = RecordBatch::try_new(
        accessor.schema(),
        vec![
            Arc::new(UInt32Array::from(vec![schema])),
            Arc::new(UInt32Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name])),
        ],
    )?;

    partitioned_table.insert(ctx, batch).await?;

    Ok(())
}
