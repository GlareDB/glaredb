use crate::errors::{internal, CatalogError, Result};
use crate::filter::filter_scan;
use crate::system::constants::{BOOTSTRAP_TABLE_ID, BOOTSTRAP_TABLE_NAME};
use crate::system::{SystemSchema, SystemTable, SystemTableAccessor, SYSTEM_SCHEMA_ID};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::context::SessionContext;
use catalog_types::interfaces::MutableTableProvider;
use catalog_types::keys::{SchemaId, TableKey};
use datafusion::arrow::array::{StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use itertools::izip;
use std::sync::Arc;

pub struct BootstrapTable {
    schema: SchemaRef,
}

impl BootstrapTable {
    pub fn new() -> BootstrapTable {
        BootstrapTable {
            schema: Arc::new(Schema::new(vec![
                Field::new("bootstrap_step", DataType::UInt32, false),
                Field::new("bootstrap_name", DataType::Utf8, false),
            ])),
        }
    }
}

impl SystemTableAccessor for BootstrapTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn name(&self) -> &'static str {
        BOOTSTRAP_TABLE_NAME
    }

    fn is_readonly(&self) -> bool {
        false
    }

    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable {
        let key = TableKey {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: BOOTSTRAP_TABLE_ID,
        };

        SystemTable::Base(PartitionedTable::new(
            key,
            Box::new(SinglePartitionStrategy),
            runtime,
            self.schema.clone(),
        ))
    }
}

#[derive(Debug)]
pub struct BoostrapRow {
    pub bootstrap_step: u32,
    pub bootstrap_name: String,
}

impl BoostrapRow {
    /// Scan the latest bootstrap step ordered by `bootstrap_step`.
    ///
    /// Returns `None` if no rows exist yet.
    pub async fn scan_latest(
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<Option<BoostrapRow>> {
        let bootstrap_table = system
            .get_system_table_accessor(BOOTSTRAP_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(BOOTSTRAP_TABLE_NAME.to_string()))?
            .get_table(runtime.clone());
        let table = bootstrap_table.into_table_provider_ref();

        // We _could_ add a sort plan node here, but it's easier to just
        // manually sort the rows returned.
        let plan = filter_scan(table, ctx.get_df_state(), &[], None).await?;
        let stream = plan.execute(0, ctx.task_context())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let mut rows = Self::read_from_batches(batches)?;
        // Latest last.
        rows.sort_by(|a, b| a.bootstrap_step.cmp(&b.bootstrap_step));

        Ok(rows.pop())
    }

    /// Insert a bootstrap row indicating that that step successfully ran.
    pub async fn insert(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        let accessor = system
            .get_system_table_accessor(BOOTSTRAP_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(BOOTSTRAP_TABLE_NAME.to_string()))?;
        let bootstrap_table = accessor.get_table(runtime.clone());
        let table = bootstrap_table.get_partitioned_table()?;

        let batch = RecordBatch::try_new(
            accessor.schema(),
            vec![
                Arc::new(UInt32Array::from_value(self.bootstrap_step, 1)),
                Arc::new(StringArray::from_iter_values([&self.bootstrap_name])),
            ],
        )?;

        table.insert(ctx, batch).await?;

        Ok(())
    }

    fn read_from_batches(batches: Vec<RecordBatch>) -> Result<Vec<Self>> {
        let mut rows: Vec<BoostrapRow> =
            Vec::with_capacity(batches.iter().fold(0, |acc, batch| acc + batch.num_rows()));

        for batch in batches {
            let bootstrap_steps = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| internal!("failed to downcast bootstrap steps to uint32 array"))?;
            let bootstrap_names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal!("failed to downcast bootstrap name to string array"))?;
            for (bootstrap_step, bootstrap_name) in izip!(bootstrap_steps, bootstrap_names) {
                let bootstrap_step = match bootstrap_step {
                    Some(bootstrap_step) => bootstrap_step,
                    None => return Err(internal!("unexpected null value for bootstrap step")),
                };
                let bootstrap_name = match bootstrap_name {
                    Some(bootstrap_name) => bootstrap_name,
                    None => return Err(internal!("unexpected null value for bootstrap name")),
                };

                rows.push(BoostrapRow {
                    bootstrap_step,
                    bootstrap_name: bootstrap_name.to_string(),
                })
            }
        }

        Ok(rows)
    }
}
