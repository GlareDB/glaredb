pub mod operation;

use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::FutureExt;
use operation::{PartitionSource, SourceOperation};
use rayexec_error::{RayexecError, Result};

use super::util::futures::make_static;
use super::{
    ExecutableOperator,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

pub struct SourcePartitionState {
    source: Box<dyn PartitionSource>,
    /// In progress pull we're working on.
    future: Option<BoxFuture<'static, Result<Option<Batch>>>>,
}

impl fmt::Debug for SourcePartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuerySourcePartitionState")
            .finish_non_exhaustive()
    }
}

/// Operater for reading batches from a source.
// TODO: Deduplicate with table scan and table function scan.
#[derive(Debug)]
pub struct PhysicalSource<S: SourceOperation> {
    pub(crate) source: S,
}

impl<S: SourceOperation> PhysicalSource<S> {
    pub fn new(source: S) -> Self {
        PhysicalSource { source }
    }
}

impl<S: SourceOperation> ExecutableOperator for PhysicalSource<S> {}

impl<S: SourceOperation> Explainable for PhysicalSource<S> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.source.explain_entry(conf)
    }
}
