use crate::errors::{internal, CatalogError, Result};
use crate::filter::filter_scan;
use crate::system::constants::{SEQUENCES_TABLE_ID, SEQUENCES_TABLE_NAME};
use crate::system::{SystemSchema, SystemTable, SystemTableAccessor, SYSTEM_SCHEMA_ID};
use access::runtime::AccessRuntime;
use access::strategy::SinglePartitionStrategy;
use access::table::PartitionedTable;
use catalog_types::context::SessionContext;
use catalog_types::interfaces::MutableTableProvider;
use catalog_types::keys::{SchemaId, TableId, TableKey};
use datafusion::arrow::array::{Int64Array, UInt32Array};
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::Expr;
use futures::TryStreamExt;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::Mutex;

const NEXT_COL_IDX: usize = 2;
const INC_COL_IDX: usize = 3;

/// Holds information about sequences within the db.
///
/// All sequences **must** have an associated record in the "relations" table.
pub struct SequencesTable {
    schema: SchemaRef,
}

impl SequencesTable {
    pub fn new() -> SequencesTable {
        SequencesTable {
            schema: Arc::new(Schema::new(vec![
                Field::new("schema_id", DataType::UInt32, false),
                Field::new("table_id", DataType::UInt32, false),
                Field::new("next", DataType::Int64, false),
                Field::new("inc", DataType::Int64, false),
            ])),
        }
    }
}

impl SystemTableAccessor for SequencesTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn name(&self) -> &'static str {
        SEQUENCES_TABLE_NAME
    }

    fn is_readonly(&self) -> bool {
        false
    }

    fn get_table(&self, runtime: Arc<AccessRuntime>) -> SystemTable {
        let key = TableKey {
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: SEQUENCES_TABLE_ID,
        };

        SystemTable::Base(PartitionedTable::new(
            key,
            Box::new(SinglePartitionStrategy),
            runtime,
            self.schema.clone(),
        ))
    }
}

/// A global mutex for _all_ sequences.
///
/// No, this isn't good. But we don't have anything yet to lock db resources.
static SEQUENCE_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::const_new(()));

#[derive(Debug)]
pub struct SequenceRow {
    pub schema: SchemaId,
    pub table: TableId,
    pub next: i64,
    pub inc: i64,
}

impl SequenceRow {
    /// Increments a given sequence, returning the next value to use.
    // TODO: Jank because we don't have in-place updates.
    // TODO: All sequence increments acquire a global lock.
    pub async fn next(
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
        schema: SchemaId,
        table: TableId,
    ) -> Result<Option<i64>> {
        let schemas_table = system
            .get_system_table_accessor(SEQUENCES_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(SEQUENCES_TABLE_NAME.to_string()))?
            .get_table(runtime.clone());
        let partitioned_table = schemas_table.into_table_provider_ref();

        // Filter for this table.
        let filter = Expr::Column(Column::from_name("schema_id"))
            .eq(Expr::Literal(ScalarValue::UInt32(Some(schema))))
            .and(
                Expr::Column(Column::from_name("table_id"))
                    .eq(Expr::Literal(ScalarValue::UInt32(Some(table)))),
            );

        // So...
        //
        // Since we don't have updates, each update to the sequence adds a new
        // row to the table. To get the "next" value to use, we scan all rows
        // for a given sequence and find the max value.

        let _g = SEQUENCE_MUTEX.lock().await;

        let plan = filter_scan(partitioned_table, ctx.get_df_state(), &[filter], None).await?;
        let stream = plan.execute(0, ctx.task_context())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let mut inc = None;
        let mut max_next = None;

        for batch in batches {
            let nexts = batch
                .column(NEXT_COL_IDX)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| internal!("failed to downcast next value to int64 array"))?;
            let incs = batch
                .column(INC_COL_IDX)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| internal!("failed to downcast inc value to int64 array"))?;

            if let Some(batch_max) = aggregate::max(nexts) {
                inc = Some(incs.value(0)); // Increment values should be constant across all rows for this sequence.

                // Update the "global" max we've seen across all batches.
                match max_next {
                    Some(val) if batch_max > val => max_next = Some(batch_max),
                    None => max_next = Some(batch_max),
                    _ => (),
                }
            }
        }

        let (inc, max_next) = match (inc, max_next) {
            (Some(inc), Some(max_next)) => (inc, max_next),
            _ => return Ok(None), // Sequence doesn't exist.
        };

        // Insert incremented sequence value.
        let seq = SequenceRow {
            schema,
            table,
            next: max_next + inc,
            inc,
        };
        seq.insert(ctx, runtime, system).await?;

        Ok(Some(max_next))
    }

    /// Insert a new sequence value for a sequence.
    pub async fn insert(
        &self,
        ctx: &SessionContext,
        runtime: &Arc<AccessRuntime>,
        system: &SystemSchema,
    ) -> Result<()> {
        let accessor = system
            .get_system_table_accessor(SEQUENCES_TABLE_NAME)
            .ok_or_else(|| CatalogError::MissingSystemTable(SEQUENCES_TABLE_NAME.to_string()))?;
        let schemas_table = accessor.get_table(runtime.clone());
        let partitioned_table = schemas_table.get_partitioned_table()?;

        let batch = RecordBatch::try_new(
            accessor.schema(),
            vec![
                Arc::new(UInt32Array::from_value(self.schema, 1)),
                Arc::new(UInt32Array::from_value(self.table, 1)),
                Arc::new(Int64Array::from_value(self.next, 1)),
                Arc::new(Int64Array::from_value(self.inc, 1)),
            ],
        )?;

        partitioned_table.insert(ctx, batch).await?;

        Ok(())
    }
}
