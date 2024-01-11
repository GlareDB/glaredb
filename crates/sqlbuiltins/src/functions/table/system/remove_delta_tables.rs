use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datasources::native::access::NativeTableStorage;
use protogen::metastore::types::catalog::TableEntry;

#[derive(Clone)]
pub struct DeleteDeltaTablesOperation {
    pub entries: Vec<TableEntry>,
}

impl DeleteDeltaTablesOperation {
    pub const NAME: &'static str = "delete_delta_tables";
    pub async fn execute(&self, ctx: Arc<TaskContext>) -> DataFusionResult<()> {
        let storage = ctx
            .session_config()
            .get_extension::<NativeTableStorage>()
            .expect("Native table storage to be on context");

        for entry in &self.entries {
            tracing::debug!("Removing table: {:?}", entry);
            storage.delete_table(entry).await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to remove table: {:?}, error: {:?}",
                    entry, e
                ))
            })?;
        }

        Ok(())
    }
}
