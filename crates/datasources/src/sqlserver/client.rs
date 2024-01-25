//! Implementation of a wrapper around a tiberius client.
//!
//! The raw tiberius client ties the response streams for queries to the
//! lifetime of the mutable reference to the client. This makes it very
//! difficult wrap in a custom async stream for datafusion (since we can't
//! actually store both the mutable reference and the actual owned object on
//! that same struct).
//!
//! To get around this, the below implementation wraps the client to provide a
//! more ergnomic interface that decouples stream lifetimes from the client
//! lifetime.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncWrite, Stream, StreamExt};
use tiberius::{Column, QueryItem, ResultMetadata, Row};
use tokio::sync::mpsc;
use tracing::debug;

use super::errors::{Result, SqlServerError};

/// Connect to a SQL Server database using the provided tiberius config and stream.
///
/// This will return a client and a connection. The Client is used for querying,
/// while the Connection is what's actually used to communicate with the server.
/// The Connection should be handled in a background thread, and should run to
/// completion.
pub async fn connect<S>(config: tiberius::Config, stream: S) -> Result<(Client, Connection<S>)>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (sender, receiver) = mpsc::unbounded_channel();
    let client = tiberius::Client::connect(config, stream).await?;

    Ok((Client::new(sender), Connection::new(client, receiver)))
}

/// Wrapper around the tiberius client.
#[derive(Debug)]
pub struct Connection<S: AsyncRead + AsyncWrite + Unpin + Send> {
    /// The actual client to the server.
    client: tiberius::Client<S>,
    /// Channel for requests from client.
    receiver: mpsc::UnboundedReceiver<Request>,
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn new(client: tiberius::Client<S>, receiver: mpsc::UnboundedReceiver<Request>) -> Self {
        Connection { client, receiver }
    }

    /// Runs the connection to completion.
    ///
    /// Incoming messages from the client will be processed to completion before
    /// moving on to the next message.
    pub async fn run(mut self) -> Result<()> {
        while let Some(req) = self.receiver.recv().await {
            match req {
                Request::Query { query, response } => {
                    let mut stream = match self.client.query(query, &[]).await {
                        Ok(stream) => stream,
                        Err(e) => {
                            // We don't care if this errors, just means that the
                            // client is no longer listening.
                            let _ = response.send(Err(e.into())).await;
                            continue;
                        }
                    };

                    while let Some(item) = stream.next().await {
                        if response.send(item.map_err(|e| e.into())).await.is_err() {
                            // Client no longer listening, stop reading from the
                            // stream and just move to the next message.
                            break;
                        }
                    }
                }
                Request::Drop => {
                    debug!("closing SQL Server connection");
                    self.client.close().await?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

/// Client side of the connection.
#[derive(Debug)]
pub struct Client {
    sender: mpsc::UnboundedSender<Request>,
}

impl Client {
    fn new(sender: mpsc::UnboundedSender<Request>) -> Self {
        Client { sender }
    }

    // TODO: Zero clue how we'd want to handle params. Might as well just fork
    // tiberius to make it more reasonable.
    pub async fn query<'a>(&self, query: impl Into<Cow<'a, str>>) -> Result<QueryStream> {
        let query = query.into().to_string();

        let (sender, receiver) = mpsc::channel(1);
        let req = Request::Query {
            query,
            response: sender,
        };

        if self.sender.send(req).is_err() {
            return Err(SqlServerError::String(
                "connection to SQL Server closed".to_string(),
            ));
        }

        Ok(QueryStream {
            receiver,
            metadata: None,
            buffered_rows: VecDeque::new(),
        })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.sender.send(Request::Drop);
    }
}

#[derive(Debug)]
enum Request {
    /// Run a query, sending response items to the channel.
    Query {
        query: String,
        response: mpsc::Sender<Result<QueryItem>>,
    },
    /// Client was dropped, drop the connection.
    Drop,
}

/// Streaming result of a query.
///
/// Note that the stream implementation for this returns _only_ rows. The
/// `QueryStream` in tiberius will return `QueryItem`, however that's not useful
/// in our case, we just care about the rows.
#[derive(Debug)]
pub struct QueryStream {
    receiver: mpsc::Receiver<Result<QueryItem>>,
    metadata: Option<ResultMetadata>,
    buffered_rows: VecDeque<Row>,
}

impl QueryStream {
    /// Returns column data for this stream if it's available.
    pub async fn columns(&mut self) -> Result<Option<&[Column]>> {
        if let Some(ref metadata) = self.metadata {
            return Ok(Some(metadata.columns()));
        }

        if let Some(item) = self.receiver.recv().await {
            let item = item?;
            match item {
                QueryItem::Row(row) => {
                    self.buffered_rows.push_back(row);
                    return Ok(None);
                }
                QueryItem::Metadata(metadata) => {
                    self.metadata = Some(metadata);
                    return Ok(Some(self.metadata.as_ref().unwrap().columns()));
                }
            }
        }

        Ok(None)
    }
}

impl Stream for QueryStream {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Pull buffered rows first.
        if let Some(row) = self.buffered_rows.pop_front() {
            return Poll::Ready(Some(Ok(row)));
        }

        // Try to read from mpsc.
        loop {
            match self.receiver.poll_recv(cx) {
                Poll::Ready(Some(item)) => match item {
                    Ok(QueryItem::Row(row)) => return Poll::Ready(Some(Ok(row))),
                    Ok(QueryItem::Metadata(metadata)) => {
                        self.metadata = Some(metadata);
                        // We received some metadata, continue loop to get the
                        // rest of the rows.
                        continue;
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
