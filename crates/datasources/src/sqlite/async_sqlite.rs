use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::DataType;
use futures::Stream;
use rusqlite::types::Value;
use rusqlite::{Connection, Params};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;

use crate::sqlite::errors::{Result, SqliteError};

#[derive(Clone, Debug)]
pub struct SqliteAsyncClient {
    path: PathBuf,
}

impl SqliteAsyncClient {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn execute(&self, s: impl Into<String>, p: impl Params) -> Result<usize> {
        let s = s.into();
        let path = self.path.clone();

        tokio::task::block_in_place(move || {
            let conn = Connection::open(path)?;
            let n = conn.execute(&s, p)?;
            Ok(n)
        })
    }

    pub fn query(
        &self,
        s: impl Into<String>,
        p: impl Params + Send + 'static,
    ) -> SqliteQueryStream {
        let s = s.into();

        let (req_tx, mut req_rx) = mpsc::channel(1);
        let (resp_tx, resp_rx) = mpsc::channel(1);

        let path = self.path.clone();
        let handle: JoinHandle<Result<()>> = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(path)?;

            let mut stmt = conn.prepare(&s)?;

            let cols = stmt
                .columns()
                .into_iter()
                .map(Column::from)
                .collect::<Vec<_>>();

            let num_cols = cols.len();

            let mut rows = stmt.query(p)?.mapped(|r| {
                (0..num_cols)
                    .map(|idx| {
                        let v = r.get_ref(idx)?;
                        Ok(Value::from(v))
                    })
                    .collect::<Result<Vec<_>, rusqlite::Error>>()
            });

            while let Some(()) = req_rx.blocking_recv() {
                eprintln!("processing a batch");
                // Collect the next "n" rows for the batch.
                const BATCH_SIZE: usize = 1000;

                let data = (0..BATCH_SIZE)
                    .filter_map(|_| rows.next())
                    .collect::<Result<Vec<_>, _>>();

                let batch = match data {
                    Ok(data) => {
                        if data.is_empty() {
                            // Exit when there's nothing more to process. This
                            // should drop the only response sender and hence,
                            // end the stream.
                            break;
                        } else {
                            Ok(SqliteBatch {
                                cols: cols.clone(),
                                data,
                            })
                        }
                    }
                    Err(e) => Err(SqliteError::from(e)),
                };

                resp_tx
                    .blocking_send(batch)
                    .map_err(|e| SqliteError::MpscSendError(e.to_string()))?;
            }

            Ok(())
        });

        SqliteQueryStream::new(req_tx, resp_rx, handle)
    }

    // Collects and returns all the rows from the query.
    pub fn query_all(&self, s: impl Into<String>, p: impl Params) -> Result<SqliteBatch> {
        let s = s.into();
        let path = self.path.clone();

        tokio::task::block_in_place(move || {
            let conn = Connection::open(path)?;

            let mut stmt = conn.prepare(&s)?;

            let cols = stmt
                .columns()
                .into_iter()
                .map(Column::from)
                .collect::<Vec<_>>();

            let num_cols = cols.len();

            let data = stmt
                .query(p)?
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
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub decl_type: Option<DataType>,
}

impl<'a> From<rusqlite::Column<'a>> for Column {
    fn from(col: rusqlite::Column<'a>) -> Self {
        let decl_type = col.decl_type().and_then(|decl_type| match decl_type {
            "boolean" | "bool" => Some(DataType::Boolean),
            // TODO: Support dates and times?
            // "date" => DataType::Date,
            // "time" => DataType::Time,
            // "datetime" | "timestamp" => DataType::Datetime,
            s if s.contains("int") => Some(DataType::Int64),
            s if s.contains("char") || s.contains("clob") || s.contains("text") => {
                Some(DataType::Utf8)
            }
            s if s.contains("real") || s.contains("floa") || s.contains("doub") => {
                Some(DataType::Float64)
            }
            s if s.contains("blob") => Some(DataType::Binary),
            _ => None,
        });

        Self {
            name: col.name().to_owned(),
            decl_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqliteBatch {
    pub cols: Vec<Column>,
    pub data: Vec<Vec<Value>>,
}

pub struct SqliteQueryStream {
    waiting_for_response: bool,
    req_tx: mpsc::Sender<()>,
    resp_rx: mpsc::Receiver<Result<SqliteBatch>>,
    _handle: JoinHandle<Result<()>>,
}

impl SqliteQueryStream {
    fn new(
        req_tx: mpsc::Sender<()>,
        resp_rx: mpsc::Receiver<Result<SqliteBatch>>,
        handle: JoinHandle<Result<()>>,
    ) -> Self {
        Self {
            waiting_for_response: false,
            req_tx,
            resp_rx,
            _handle: handle,
        }
    }
}

impl Stream for SqliteQueryStream {
    type Item = Result<SqliteBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If we're already waiting for a response, don't try to send another
        // message to the handle.
        if !self.waiting_for_response {
            match self.req_tx.try_send(()) {
                Ok(_) => {
                    self.waiting_for_response = true;
                }
                Err(TrySendError::Full(_)) => return Poll::Pending,
                Err(TrySendError::Closed(_)) => {
                    // When the channel is closed, we are done.
                    return Poll::Ready(None);
                }
            };
        }

        // Expect response
        match self.resp_rx.poll_recv(cx) {
            Poll::Ready(r) => {
                self.waiting_for_response = false;
                Poll::Ready(r)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
