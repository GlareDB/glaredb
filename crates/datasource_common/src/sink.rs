use std::collections::HashMap;

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;

#[derive(thiserror::Error, Debug)]
pub enum SinkError {
    #[error(transparent)]
    Boxed(Box<dyn std::error::Error>),

    #[error("Sink error: {0}")]
    String(String),
}

/// Allow writing a record batch steam to some source.
#[async_trait]
pub trait Sink: Sync + Send {
    /// Stream a record batch into a sink, returning the number of rows written.
    async fn stream_into(&self, stream: SendableRecordBatchStream) -> Result<usize, SinkError>;
}
