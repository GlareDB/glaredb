use std::{collections::BTreeMap, sync::Arc};

use parking_lot::Mutex;
use rayexec_execution::runtime::{
    dump::{PartitionPipelineDump, PipelineDump, QueryDump},
    QueryHandle,
};

use super::task::{PartitionPipelineTask, TaskState};

/// Query handle for queries being executed on the threaded runtime.
#[derive(Debug)]
pub struct ThreadedQueryHandle {
    /// Registered task states for all pipelines in a query.
    pub(crate) states: Mutex<Vec<Arc<TaskState>>>,
}

impl QueryHandle for ThreadedQueryHandle {
    /// Cancel the query.
    fn cancel(&self) {
        let states = self.states.lock();

        for state in states.iter() {
            let mut pipeline = state.pipeline.lock();
            pipeline.query_canceled = true;
            std::mem::drop(pipeline);

            // Re-execute the pipeline so it picks up the set bool. This lets us
            // cancel the pipeline regardless of if it's pending.
            let task = PartitionPipelineTask::from_task_state(state.clone());
            task.execute()
        }
    }

    fn dump(&self) -> QueryDump {
        use std::collections::btree_map::Entry;

        let mut dump = QueryDump {
            pipelines: BTreeMap::new(),
        };

        let states = self.states.lock();

        for state in states.iter() {
            let pipeline_state = state.pipeline.lock();
            match dump.pipelines.entry(pipeline_state.pipeline.pipeline_id()) {
                Entry::Occupied(mut ent) => {
                    ent.get_mut().partitions.insert(
                        pipeline_state.pipeline.partition(),
                        PartitionPipelineDump {
                            state: pipeline_state.pipeline.state().clone(),
                            timings: pipeline_state.pipeline.timings().clone(),
                        },
                    );
                }
                Entry::Vacant(ent) => {
                    let partition_dump = PartitionPipelineDump {
                        state: pipeline_state.pipeline.state().clone(),
                        timings: pipeline_state.pipeline.timings().clone(),
                    };
                    let pipeline_dump = PipelineDump {
                        operators: pipeline_state.pipeline.iter_operators().cloned().collect(),
                        partitions: [(pipeline_state.pipeline.partition(), partition_dump)]
                            .into_iter()
                            .collect(),
                    };
                    ent.insert(pipeline_dump);
                }
            }
        }

        dump
    }
}
