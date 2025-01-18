use std::sync::Arc;
use std::task::Context;

use futures::future::BoxFuture;
use rayexec_error::Result;
use rayexec_io::http::HttpClient;

use super::client::{HybridClient, PullStatus};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::intermediate::pipeline::StreamId;
use crate::execution::operators::sink::operation::{PartitionSink, PollPush, SinkOperation};
use crate::execution::operators::source::operation::{PartitionSource, PollPull, SourceOperation};
use crate::execution::operators::PollFinalize;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Client-side stream for sending batches from the client to the server (push).
///
/// All the ipc client/server streams have a partitioning requirement of one.
/// However it should be pretty easy to extend to support multiple partition
/// with very little change. The remote side (server) could just set it to its
/// target partitioning value, and the for clients, we can just send that
/// information over along with the plan+bind data. Then the remote side would
/// have everything it needs for wiring up the streams for correct partitioning.
#[derive(Debug)]
pub struct ClientToServerStream<C: HttpClient> {
    stream_id: StreamId,
    client: Arc<HybridClient<C>>,
}

impl<C: HttpClient + 'static> ClientToServerStream<C> {
    pub fn new(stream_id: StreamId, client: Arc<HybridClient<C>>) -> Self {
        ClientToServerStream { stream_id, client }
    }
}

impl<C: HttpClient + 'static> SinkOperation for ClientToServerStream<C> {
    fn create_partition_sinks(
        &mut self,
        _context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        assert_eq!(1, num_sinks);

        Ok(vec![Box::new(ClientToServerPartitionSink {
            stream_id: self.stream_id,
            client: self.client.clone(),
        })])
    }

    fn partitioning_requirement(&self) -> Option<usize> {
        Some(1)
    }
}

impl<C: HttpClient> Explainable for ClientToServerStream<C> {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ClientToServerStream")
    }
}

#[derive(Debug)]
pub struct ClientToServerPartitionSink<C: HttpClient> {
    stream_id: StreamId,
    client: Arc<HybridClient<C>>,
}

impl<C: HttpClient> PartitionSink for ClientToServerPartitionSink<C> {
    fn poll_push(&mut self, cx: &mut Context, input: &mut Batch) -> Result<PollPush> {
        unimplemented!()
    }

    fn poll_finalize(&mut self, cx: &mut Context) -> Result<PollFinalize> {
        unimplemented!()
    }

    // fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
    //     // TODO: Figure out backpressure
    //     Box::pin(async { self.client.push(self.stream_id, 0, batch).await })
    // }

    // fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
    //     Box::pin(async { self.client.finalize(self.stream_id, 0).await })
    // }
}

/// Client-side stream for receiving batches from the server to the client
/// (pull).
#[derive(Debug)]
pub struct ServerToClientStream<C: HttpClient> {
    stream_id: StreamId,
    client: Arc<HybridClient<C>>,
}

impl<C: HttpClient> ServerToClientStream<C> {
    pub fn new(stream_id: StreamId, client: Arc<HybridClient<C>>) -> Self {
        ServerToClientStream { stream_id, client }
    }
}

impl<C: HttpClient + 'static> SourceOperation for ServerToClientStream<C> {
    fn create_partition_sources(
        &mut self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSource>>> {
        unimplemented!()
        // assert_eq!(1, num_sources);

        // vec![Box::new(ServerToClientPartitionSource {
        //     stream_id: self.stream_id,
        //     client: self.client.clone(),
        // })]
    }
}

impl<C: HttpClient> Explainable for ServerToClientStream<C> {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("ServerToClientStream")
    }
}

#[derive(Debug)]
pub struct ServerToClientPartitionSource<C: HttpClient> {
    stream_id: StreamId,
    client: Arc<HybridClient<C>>,
}

impl<C: HttpClient> PartitionSource for ServerToClientPartitionSource<C> {
    fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        unimplemented!()
    }

    // fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
    //     Box::pin(async {
    //         // TODO: Backoff + hint somehow
    //         loop {
    //             let status = self.client.pull(self.stream_id, 0).await?;
    //             match status {
    //                 PullStatus::Batch(batch) => return Ok(Some(batch.0)),
    //                 PullStatus::Pending => continue,
    //                 PullStatus::Finished => return Ok(None),
    //             }
    //         }
    //     })
    // }
}
