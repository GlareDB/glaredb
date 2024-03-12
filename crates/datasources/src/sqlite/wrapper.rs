use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_sqlite::rusqlite;
use async_sqlite::rusqlite::types::Value;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Future, FutureExt, Stream};
use tempfile;
use tokio::sync::mpsc;

use super::convert::Converter;
use crate::sqlite::errors::Result;

#[derive(Clone)]
pub struct SqliteAsyncClient {
    path: PathBuf,
    inner: async_sqlite::Client,
    // we're just tying the lifetime of the tempdir to this connection
    #[allow(dead_code)]
    cache: Option<Arc<tempfile::TempDir>>,
}

impl fmt::Debug for SqliteAsyncClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqliteAsyncClient({})", self.path.to_string_lossy())
    }
}

impl SqliteAsyncClient {
    pub async fn new(path: PathBuf, cache: Option<Arc<tempfile::TempDir>>) -> Result<Self> {
        let inner = async_sqlite::ClientBuilder::new()
            .path(&path)
            .open()
            .await?;

        Ok(Self { path, inner, cache })
    }

    /// Query and return a RecordBatchStream for sqlite data.
    pub fn query(&self, schema: SchemaRef, s: impl Into<String>) -> SqliteRecordBatchStream {
        let s = s.into();

        let (tx, rx) = mpsc::channel(1);

        let client = self.inner.clone();
        let conv = Converter::new(schema.clone());

        let handle = Box::pin(async move {
            client
                .conn(move |conn| {
                    let mut stmt = conn.prepare(&s)?;
                    let mut rows = stmt.query([])?;
                    loop {
                        let batch = conv
                            .create_record_batch(&mut rows)
                            .map_err(|e| DataFusionError::Execution(format!("{e}")))
                            .transpose();

                        if let Some(batch) = batch {
                            if tx.blocking_send(batch).is_err() {
                                // Receiver is dropped so we can exit.
                                break;
                            }
                        } else {
                            // No more rows to process, we can end here.
                            break;
                        }
                    }
                    Ok(())
                })
                .await?;
            Ok(())
        });

        SqliteRecordBatchStream {
            rx,
            handle,
            handle_finished: false,
            schema,
        }
    }

    // Collects and returns all the rows from the query.
    pub async fn query_all(&self, s: impl Into<String>) -> Result<SqliteBatch> {
        let s = s.into();

        Ok(self
            .inner
            .conn(move |conn| {
                let mut stmt = conn.prepare(&s)?;

                let cols = stmt
                    .column_names()
                    .into_iter()
                    .map(|c| Column {
                        name: c.to_string(),
                    })
                    .collect::<Vec<_>>();

                let num_cols = cols.len();

                let data = stmt
                    .query([])?
                    .mapped(|r| {
                        (0..num_cols)
                            .map(|idx| {
                                let v = r.get_ref(idx)?;
                                Ok(Value::from(v))
                            })
                            .collect::<Result<Vec<_>, rusqlite::Error>>()
                    })
                    .collect::<Result<Vec<_>, rusqlite::Error>>()?;

                Ok(SqliteBatch {
                    cols: cols.clone(),
                    data,
                })
            })
            .await?)
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct SqliteBatch {
    pub cols: Vec<Column>,
    pub data: Vec<Vec<Value>>,
}

impl SqliteBatch {
    pub fn get_val_by_col_name(&self, row: usize, name: impl AsRef<str>) -> Option<&Value> {
        let col_name = name.as_ref();
        let col_idx = self.cols.iter().enumerate().find_map(|(idx, col)| {
            if col.name == col_name {
                Some(idx)
            } else {
                None
            }
        })?;

        let row = self.data.get(row)?;
        row.get(col_idx)
    }
}

pub struct SqliteRecordBatchStream {
    rx: mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
    handle: Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    handle_finished: bool,
    schema: SchemaRef,
}

impl Stream for SqliteRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.handle_finished {
            // Poll only if the handle hasn't yielded yet.
            match self.handle.poll_unpin(cx) {
                Poll::Pending => (),
                Poll::Ready(Ok(())) => {
                    self.handle_finished = true;
                }
                Poll::Ready(Err(e)) => {
                    self.handle_finished = true;
                    return Poll::Ready(Some(Err(DataFusionError::Execution(format!("{e}")))));
                }
            }
        }
        self.rx.poll_recv(cx)
    }
}

impl RecordBatchStream for SqliteRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
