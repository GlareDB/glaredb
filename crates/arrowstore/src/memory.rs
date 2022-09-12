use crate::proto::{self, arrow_store_service_server::ArrowStoreService};
use crate::serialize::*;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use futures::Stream;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::{Request, Response, Status};
use tracing::trace;

#[derive(Default)]
pub struct MemoryService {
    source: Arc<MemorySource>,
}

#[async_trait]
impl ArrowStoreService for MemoryService {
    type ScanStream =
        Pin<Box<dyn Stream<Item = Result<proto::ArrowBatch, Status>> + Send + 'static>>;

    async fn begin_transaction(
        &self,
        request: Request<proto::BeginTransactionRequest>,
    ) -> Result<Response<proto::BeginTransactionResponse>, Status> {
        Ok(Response::new(proto::BeginTransactionResponse {
            tx_ctx: Some(proto::TransactionContext {
                timestamp: request.into_inner().timestamp,
            }),
        }))
    }

    async fn end_transaction(
        &self,
        _request: Request<proto::EndTransactionRequest>,
    ) -> Result<Response<proto::EndTransactionResponse>, Status> {
        Ok(Response::new(proto::EndTransactionResponse {}))
    }

    async fn allocate_table(
        &self,
        request: Request<proto::AllocateTableRequest>,
    ) -> Result<Response<proto::AllocateTableResponse>, Status> {
        let req = request.into_inner();
        self.source
            .allocate_table(req.table, req.schema, req.pk_idxs)?;
        Ok(Response::new(proto::AllocateTableResponse {}))
    }

    async fn insert_batch(
        &self,
        request: Request<proto::InsertBatchRequest>,
    ) -> Result<Response<proto::InsertBatchResponse>, Status> {
        let req = request.into_inner();
        if let Some(batch) = req.batch {
            self.source.insert_batch(&req.table, batch.ipc_raw)?;
        }
        Ok(Response::new(proto::InsertBatchResponse {}))
    }

    async fn get_row(
        &self,
        _request: Request<proto::GetRowRequest>,
    ) -> Result<Response<proto::ArrowBatch>, Status> {
        unimplemented!()
    }

    async fn create_index(
        &self,
        _request: Request<proto::CreateIndexRequest>,
    ) -> Result<Response<proto::CreateIndexResponse>, Status> {
        unimplemented!()
    }

    async fn prepare_scan(
        &self,
        request: Request<proto::PrepareScanRequest>,
    ) -> Result<Response<proto::PrepareScanResponse>, Status> {
        let req = request.into_inner();
        let id = self.source.prepare_scan(req.table, req.projection)?;
        Ok(Response::new(proto::PrepareScanResponse {
            node_id: None,
            cursor_id: Some(proto::CursorId {
                id: id.to_le_bytes().to_vec(),
            }),
        }))
    }

    async fn scan(
        &self,
        request: Request<proto::ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        let req = request.into_inner();
        let cursor_id = req
            .cursor_id
            .ok_or_else(|| Status::internal("missing cursor id"))?
            .id;
        let id = u64::from_le_bytes(
            cursor_id
                .try_into()
                .map_err(|_| Status::internal("unable to convert to cursor id"))?,
        );
        let cursor = self.source.remove_cursor(id)?;
        Ok(Response::new(Box::pin(cursor)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    #[error("missing cursor with id: {0}")]
    MissingCursor(u64),
    #[error("duplicate primary key")]
    DuplicatePrimaryKey,
    #[error("missing column")]
    MissingColumn,
    #[error("duplicate table: {0}")]
    DuplicateTable(String),
    #[error("missing table: {0}")]
    MissingTable(String),
    #[error("schema mismatch")]
    SchemaMismatch,
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

impl From<MemoryError> for Status {
    fn from(e: MemoryError) -> Self {
        match e {
            e @ MemoryError::DuplicateTable(_) => Status::already_exists(e.to_string()),
            e @ MemoryError::DuplicatePrimaryKey => Status::already_exists(e.to_string()),
            other => Status::unknown(other.to_string()),
        }
    }
}

/// An in-memory data source.
#[derive(Default)]
pub struct MemorySource {
    tables: RwLock<HashMap<String, Arc<RwLock<MemoryTable>>>>,
    cursors: Mutex<HashMap<u64, MemoryCursor>>,
}

impl MemorySource {
    fn allocate_table(
        &self,
        table: String,
        schema: Vec<u8>,
        pk_idxs: Vec<u64>,
    ) -> Result<(), MemoryError> {
        use std::collections::hash_map::Entry;

        let schema = deserialize_schema(&schema[..])?;
        let pk_idxs = pk_idxs.into_iter().map(|idx| idx as usize).collect();

        let mut tables = self.tables.write();
        match tables.entry(table) {
            Entry::Vacant(ent) => {
                // ent.insert(value)
                let table = MemoryTable::new(schema, pk_idxs);
                ent.insert(Arc::new(RwLock::new(table)));
                Ok(())
            }
            Entry::Occupied(ent) => Err(MemoryError::DuplicateTable(ent.key().clone())),
        }
    }

    fn insert_batch(&self, table: &str, ipc_raw: Vec<u8>) -> Result<(), MemoryError> {
        let batches = deserialize_batches_from_ipc(&ipc_raw[..])?;
        let tables = self.tables.read();
        let mut table = tables
            .get(table)
            .ok_or_else(|| MemoryError::MissingTable(table.to_string()))?
            .write();

        table.insert_batches(batches)?;

        Ok(())
    }

    fn prepare_scan(&self, table: String, projection: Vec<u64>) -> Result<u64, MemoryError> {
        static CURSOR_ID: AtomicU64 = AtomicU64::new(0);

        let table = {
            let tables = self.tables.read();
            let table = tables.get(&table).ok_or(MemoryError::MissingTable(table))?;
            table.clone()
        };
        let projection: Vec<_> = projection.into_iter().map(|idx| idx as usize).collect();
        let id = CURSOR_ID.fetch_add(1, Ordering::Relaxed);

        let mut cursors = self.cursors.lock();
        cursors.insert(
            id,
            MemoryCursor {
                table,
                projection,
                batch_idx: 0,
            },
        );

        Ok(id)
    }

    fn remove_cursor(&self, cursor_id: u64) -> Result<MemoryCursor, MemoryError> {
        let mut cursors = self.cursors.lock();
        cursors
            .remove(&cursor_id)
            .ok_or(MemoryError::MissingCursor(cursor_id))
    }
}

struct MemoryTable {
    schema: Arc<Schema>,
    // Column indexes used for the primary key.
    pk_idxs: Vec<usize>,
    // All batches.
    batches: Vec<(Index, RecordBatch)>,
}

impl MemoryTable {
    fn new(schema: Schema, pk_idxs: Vec<usize>) -> Self {
        MemoryTable {
            schema: Arc::new(schema),
            pk_idxs,
            batches: Vec::new(),
        }
    }

    fn insert_batches(&mut self, batches: Vec<RecordBatch>) -> Result<(), MemoryError> {
        let indexes = batches
            .iter()
            .map(|batch| Index::new(batch, &self.pk_idxs))
            .collect::<Result<Vec<_>, _>>()?;

        // TODO: Check for overlaps in primary keys across all batches.

        let mut batches: Vec<_> = indexes.into_iter().zip(batches.into_iter()).collect();
        self.batches.append(&mut batches);

        Ok(())
    }

    fn get_batch(&self, idx: usize) -> Option<&RecordBatch> {
        self.batches.get(idx).map(|(_, batch)| batch)
    }
}

struct MemoryCursor {
    table: Arc<RwLock<MemoryTable>>,
    projection: Vec<usize>,
    batch_idx: usize,
}

impl Stream for MemoryCursor {
    type Item = Result<proto::ArrowBatch, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        trace!("polling");
        let table = self.table.read();
        let schema = table.schema.clone();

        match table.get_batch(self.batch_idx) {
            Some(batch) => {
                let mut batch = batch.clone(); // Uses arcs under the hood, allows us to release the lock.
                std::mem::drop(table);

                if !self.projection.is_empty() {
                    match batch.project(&self.projection) {
                        Ok(projected) => batch = projected,
                        Err(e) => return Poll::Ready(Some(Err(Status::internal(e.to_string())))),
                    }
                }

                let mut buf = Vec::new();
                match serialize_batch_to_ipc(&mut buf, &schema, &batch) {
                    Ok(_) => {
                        self.batch_idx += 1;
                        Poll::Ready(Some(Ok(proto::ArrowBatch { ipc_raw: buf })))
                    }
                    Err(e) => Poll::Ready(Some(Err(Status::internal(e.to_string())))),
                }
            }
            None => Poll::Ready(None),
        }
    }
}

/// Index for a single record batch.
#[derive(Default)]
#[allow(dead_code)]
struct Index {
    pk: HashMap<Vec<ScalarValue>, usize>,
}

impl Index {
    fn new(batch: &RecordBatch, pk_idxs: &[usize]) -> Result<Self, MemoryError> {
        let pk_cols = pk_idxs
            .iter()
            .map(|idx| {
                if *idx < batch.num_columns() {
                    Some(batch.column(*idx))
                } else {
                    None
                }
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(MemoryError::MissingColumn)?;

        let mut pk = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let key = pk_cols
                .iter()
                .map(|col| ScalarValue::try_from_array(*col, row_idx))
                .collect::<Result<Vec<_>, _>>()?;
            if pk.insert(key, row_idx).is_some() {
                return Err(MemoryError::DuplicatePrimaryKey);
            }
        }

        Ok(Index { pk })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field};
    use futures::StreamExt;

    #[tokio::test]
    async fn sanity() {
        logutil::init_test();
        let service = MemoryService::default();

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let mut buf = Vec::new();
        serialize_schema(&mut buf, schema.as_ref()).unwrap();
        service
            .allocate_table(Request::new(proto::AllocateTableRequest {
                tx_ctx: None,
                table: "test_table".to_string(),
                schema: buf,
                pk_idxs: vec![0, 1],
            }))
            .await
            .unwrap();

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap();

        let insert_fut = |batch: &RecordBatch| {
            let mut buf = Vec::new();
            serialize_batch_to_ipc(&mut buf, schema.clone(), batch).unwrap();
            service.insert_batch(Request::new(proto::InsertBatchRequest {
                tx_ctx: None,
                table: "test_table".to_string(),
                batch: Some(proto::ArrowBatch { ipc_raw: buf }),
            }))
        };

        insert_fut(&batch1).await.unwrap();
        insert_fut(&batch2).await.unwrap();

        let prepare = service
            .prepare_scan(Request::new(proto::PrepareScanRequest {
                tx_ctx: None,
                scan_type: proto::ScanType::Base as i32,
                table: "test_table".to_string(),
                projection: Vec::new(),
            }))
            .await
            .unwrap();

        let cursor_id = prepare.into_inner().cursor_id.unwrap();

        let mut stream = service
            .scan(Request::new(proto::ScanRequest {
                cursor_id: Some(cursor_id),
            }))
            .await
            .unwrap()
            .into_inner();

        let stream_batch1 = stream
            .next()
            .await
            .expect("not empty")
            .expect("no rpc error");
        let stream_batch2 = stream
            .next()
            .await
            .expect("not empty")
            .expect("no rpc error");

        // Should only have one record batch per message.
        let stream_batch1 = deserialize_batches_from_ipc(&stream_batch1.ipc_raw[..])
            .unwrap()
            .pop()
            .unwrap();
        let stream_batch2 = deserialize_batches_from_ipc(&stream_batch2.ipc_raw[..])
            .unwrap()
            .pop()
            .unwrap();

        assert_eq!(batch1, stream_batch1);
        assert_eq!(batch2, stream_batch2);

        // No more to stream.
        assert!(stream.next().await.is_none());
    }
}
