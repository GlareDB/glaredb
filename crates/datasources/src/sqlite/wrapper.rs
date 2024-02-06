use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_sqlite::rusqlite::types::Value;
use async_sqlite::rusqlite::{self, OpenFlags};
use datafusion::arrow::datatypes::DataType;
use futures::Stream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::sqlite::errors::{Result, SqliteError};

#[derive(Clone)]
pub struct SqliteAsyncClient {
    path: PathBuf,
    inner: async_sqlite::Client,
}

impl fmt::Debug for SqliteAsyncClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqliteAsyncClient({})", self.path.to_string_lossy())
    }
}

impl SqliteAsyncClient {
    pub async fn new(path: PathBuf) -> Result<Self> {
        let inner = async_sqlite::ClientBuilder::new()
            .flags(OpenFlags::SQLITE_OPEN_READ_ONLY)
            .path(&path)
            .open()
            .await?;
        Ok(Self { path, inner })
    }

    pub async fn execute(&self, s: impl Into<String>) -> Result<usize> {
        let s = s.into();
        Ok(self.inner.conn(move |conn| conn.execute(&s, [])).await?)
    }

    pub fn query(&self, s: impl Into<String>) -> SqliteQueryStream {
        let s = s.into();

        let (tx, rx) = mpsc::channel(1);

        let client = self.inner.clone();
        let handle = tokio::spawn(async move {
            client
                .conn(move |conn| {
                    let mut stmt = conn.prepare(&s)?;

                    let cols = stmt
                        .columns()
                        .into_iter()
                        .map(Column::from)
                        .collect::<Vec<_>>();

                    let num_cols = cols.len();

                    let mut rows = stmt.query([])?.mapped(|r| {
                        (0..num_cols)
                            .map(|idx| {
                                let v = r.get_ref(idx)?;
                                Ok(Value::from(v))
                            })
                            .collect::<Result<Vec<_>, rusqlite::Error>>()
                    });

                    loop {
                        // Collect the next "n" rows for the batch.
                        const BATCH_SIZE: usize = 1000;

                        let data = (0..BATCH_SIZE)
                            .filter_map(|_| rows.next())
                            .collect::<Result<Vec<_>, _>>();

                        let batch = match data {
                            Ok(data) if data.is_empty() => {
                                // Exit when there's nothing more to process. This
                                // should drop the only response sender and hence,
                                // end the stream.
                                break;
                            }
                            Ok(data) => Ok(SqliteBatch {
                                cols: cols.clone(),
                                data,
                            }),
                            Err(e) => Err(SqliteError::from(e)),
                        };

                        if tx.blocking_send(batch).is_err() {
                            // Receiver is dropped so we can exit.
                            break;
                        }
                    }

                    Ok(())
                })
                .await?;
            Ok(())
        });

        SqliteQueryStream {
            rx,
            _handle: handle,
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
                    .columns()
                    .into_iter()
                    .map(Column::from)
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
    rx: mpsc::Receiver<Result<SqliteBatch>>,
    _handle: JoinHandle<Result<()>>,
}

impl Stream for SqliteQueryStream {
    type Item = Result<SqliteBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
