use dyn_clone::DynClone;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_io::FileLocation;
use std::sync::Arc;
use std::{fmt::Debug, task::Context};

use crate::{
    execution::operators::{PollFinalize, PollPush},
    runtime::ExecutionRuntime,
};

pub trait CopyToFunction: Debug + Sync + Send + DynClone {
    /// Name of the copy to function.
    fn name(&self) -> &'static str;

    /// Create a COPY TO destination that will write to the given location.
    // TODO: Additional COPY TO args once we have them.
    fn create_sinks(
        &self,
        runtime: &Arc<dyn ExecutionRuntime>,
        schema: Schema,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn CopyToSink>>>;
}

impl Clone for Box<dyn CopyToFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn CopyToFunction> for Box<dyn CopyToFunction + '_> {
    fn eq(&self, other: &dyn CopyToFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn CopyToFunction + '_ {
    fn eq(&self, other: &dyn CopyToFunction) -> bool {
        self.name() == other.name()
    }
}

pub trait CopyToSink: Debug + Send {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush>;
    fn poll_finalize(&mut self, cx: &mut Context) -> Result<PollFinalize>;
}
