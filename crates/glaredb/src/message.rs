use coretypes::batch::Batch;
use coretypes::vec::ColumnVec;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_serde::formats::{Bincode, SymmetricalBincode};
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub duration: Duration,
    pub inner: ResponseInner,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseInner {
    Other,
    QueryResult { batches: Vec<SerializableBatch> },
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            ResponseInner::Other => writeln!(f, "other")?,
            ResponseInner::QueryResult { batches } => {
                for batch in batches.iter() {
                    let num_rows = batch
                        .columns
                        .get(0)
                        .and_then(|col| Some(col.len()))
                        .unwrap_or(0);
                    for row in 0..num_rows {
                        for col in batch.columns.iter() {
                            write!(f, "{} ", col.get_value(row).unwrap())?;
                        }
                        writeln!(f)?;
                    }
                }
            }
        }
        write!(f, "took: {:?}", self.duration)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SerializableBatch {
    columns: Vec<ColumnVec>,
}

impl From<Batch> for SerializableBatch {
    fn from(batch: Batch) -> Self {
        let mut cols = Vec::with_capacity(batch.arity());
        for col in batch.columns.into_iter() {
            match Arc::try_unwrap(col) {
                Ok(col) => cols.push(col),
                Err(col) => cols.push((*col).clone()),
            }
        }
        SerializableBatch { columns: cols }
    }
}

type ClientStream = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Response,
    Request,
    Bincode<Response, Request>,
>;

pub fn new_client_stream(socket: TcpStream) -> ClientStream {
    tokio_serde::Framed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        Bincode::default(),
    )
}

type ServerStream = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Request,
    Response,
    Bincode<Request, Response>,
>;

pub fn new_server_stream(socket: TcpStream) -> ServerStream {
    tokio_serde::Framed::new(
        Framed::new(socket, LengthDelimitedCodec::new()),
        Bincode::default(),
    )
}
