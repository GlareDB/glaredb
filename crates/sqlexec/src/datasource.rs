use crate::errors::{internal, Result};
use arrowstore::proto::arrow_store_service_server::ArrowStoreService;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan};
use parking_lot::RwLock;
use std::any::Any;
use std::sync::Arc;

const DEFAULT_BUFFER_SIZE: usize = 128;

struct MemTableInner {
    latest_buffer: usize,
    /// The most recent batches we've received.
    latest: Vec<RecordBatch>,
    /// All the previous record batches.
    rest: Vec<RecordBatch>,
}

#[derive(Clone)]
pub struct MemTable {
    schema: SchemaRef,
    inner: Arc<RwLock<MemTableInner>>,
}

impl MemTable {
    pub fn new(schema: SchemaRef) -> MemTable {
        MemTable {
            schema: schema.clone(),
            inner: Arc::new(RwLock::new(MemTableInner {
                latest_buffer: DEFAULT_BUFFER_SIZE,
                latest: Vec::new(),
                rest: Vec::new(),
            })),
        }
    }

    pub fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        let mut inner = self.inner.write();
        inner.latest.push(batch);

        if inner.latest.len() > inner.latest_buffer {
            let batch = RecordBatch::concat(&self.schema, &inner.latest[..])?;
            inner.rest.push(batch);
            inner.latest.clear();
        }

        Ok(())
    }
}

#[async_trait]
impl TableProvider for MemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.read();
        let mut partitions = inner.rest.clone();
        partitions.append(&mut inner.latest.clone());
        let exec = MemoryExec::try_new(&[partitions], self.schema.clone(), projection.clone())?;
        Ok(Arc::new(exec))
    }
}
