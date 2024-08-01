use crate::execution::executable::pipeline::PartitionPipelineTimings;
use crate::execution::executable::pipeline::PipelineId;
use crate::execution::executable::pipeline::PipelinePartitionState;
use crate::execution::operators::PhysicalOperator;
use crate::logical::explainable::ExplainConfig;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
pub struct QueryDump {
    pub pipelines: BTreeMap<PipelineId, PipelineDump>,
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
    pub operators: Vec<Arc<dyn PhysicalOperator>>,
    pub partitions: BTreeMap<usize, PartitionPipelineDump>,
}

#[derive(Debug)]
pub struct PartitionPipelineDump {
    pub state: PipelinePartitionState,
    pub timings: PartitionPipelineTimings,
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
            writeln!(f, "[{partition:>2}] {:?}", dump.state)?;
        }

        Ok(())
    }
}
