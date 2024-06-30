use crate::execution::operators::PhysicalOperator;
use crate::execution::pipeline::PartitionPipelineTimings;
use crate::execution::pipeline::PipelineId;
use crate::execution::pipeline::PipelinePartitionState;
use crate::logical::explainable::ExplainConfig;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use super::query::{PartitionPipelineTask, TaskState};

/// A handle for all pipelines in a query.
#[derive(Debug)]
pub struct QueryHandle {
    /// Registered task states for all pipelines in a query.
    pub(crate) states: Mutex<Vec<Arc<TaskState>>>,
}

impl QueryHandle {
    /// Cancel the query.
    pub fn cancel(&self) {
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

    pub fn query_dump(&self) -> QueryDump {
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

#[derive(Debug)]
pub struct QueryDump {
    pipelines: BTreeMap<PipelineId, PipelineDump>,
}

impl fmt::Display for QueryDump {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (id, pipeline) in &self.pipelines {
            writeln!(f, "Pipeline: {}", id.0)?;
            writeln!(f, "{pipeline}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PipelineDump {
    operators: Vec<Arc<dyn PhysicalOperator>>,
    partitions: BTreeMap<usize, PartitionPipelineDump>,
}

#[derive(Debug)]
pub struct PartitionPipelineDump {
    state: PipelinePartitionState,
    timings: PartitionPipelineTimings,
}

impl fmt::Display for PipelineDump {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "OPERATORS")?;
        for (idx, operator) in self.operators.iter().enumerate() {
            writeln!(
                f,
                "[{idx:>2}] {}",
                operator.explain_entry(ExplainConfig { verbose: true })
            )?;
        }

        writeln!(f, "PARTITIONS")?;
        for (partition, dump) in &self.partitions {
            write!(f, "[{partition:>2}] ")?;
            match &dump.timings.completed {
                Some(completed) => {
                    let dur =
                        completed.duration_since(dump.timings.start.expect("start to be set"));
                    writeln!(f, "completed: {}ms", dur.as_millis())?;
                }
                None => writeln!(f, "incomplete: {:?}", dump.state)?,
            }
        }

        Ok(())
    }
}
