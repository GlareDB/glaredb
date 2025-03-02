use std::task::Context;

use rayexec_error::Result;

use super::{BaseOperator, ExecutionProperties, PollPull, PullOperator};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

/// A dummy operator that produces a single batch containing no columns and a
/// single row for each partition.
#[derive(Debug)]
pub struct PhysicalEmpty;

impl BaseOperator for PhysicalEmpty {
    type OperatorState = ();

    fn create_operator_state(
        &self,
        _context: &DatabaseContext,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &[]
    }
}

impl PullOperator for PhysicalEmpty {
    type PartitionPullState = ();

    fn create_partition_pull_states(
        &self,
        _operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        Ok(vec![(); partitions])
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        _state: &mut Self::PartitionPullState,
        _operator_state: &Self::OperatorState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        output.set_num_rows(1)?;
        Ok(PollPull::Exhausted)
    }
}

impl Explainable for PhysicalEmpty {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Empty")
    }
}

impl DatabaseProtoConv for PhysicalEmpty {
    type ProtoType = rayexec_proto::generated::execution::PhysicalEmpty;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {})
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self)
    }
}
