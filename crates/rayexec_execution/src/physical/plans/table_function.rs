use super::{GlobalSourceState, LocalSourceState, PollPull, SourceOperator};
use crate::functions::table::{self, TableFunctionStream};
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use futures::StreamExt;
use rayexec_error::Result;
use std::fmt;
use std::task::Context;

pub struct TableFunctionLocalState {
    stream: TableFunctionStream,
}

impl fmt::Debug for TableFunctionLocalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunctionLocalState")
            .finish_non_exhaustive()
    }
}

/// Source implementation for reading from a table function stream.
#[derive(Debug)]
pub struct PhysicalTableFunction {
    func: Box<dyn table::BoundTableFunction>,
}

impl PhysicalTableFunction {
    pub fn new(func: Box<dyn table::BoundTableFunction>) -> Self {
        PhysicalTableFunction { func }
    }
}

// "Source" (new)
impl SourceOperator for PhysicalTableFunction {
    fn output_partitions(&self) -> usize {
        self.func.partitions()
    }

    fn init_local_state(&self, partition: usize) -> Result<LocalSourceState> {
        let stream = self.func.execute(partition)?;
        let state = TableFunctionLocalState { stream };
        Ok(LocalSourceState::TableFunction(state))
    }

    fn init_global_state(&self) -> Result<GlobalSourceState> {
        Ok(GlobalSourceState::TableFunction(()))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        local: &mut LocalSourceState,
        _global: &GlobalSourceState,
    ) -> Result<PollPull> {
        let local = match local {
            LocalSourceState::TableFunction(local) => local,
            other => panic!("invalid local state: {:?}", other),
        };

        let poll = local.stream.poll_next_unpin(cx);
        PollPull::from_poll(poll)
    }
}

impl Explainable for PhysicalTableFunction {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
    }
}
