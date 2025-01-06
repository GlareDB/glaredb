use std::sync::Arc;
use std::task::Context;

use rayexec_error::{RayexecError, Result};

use super::{
    ExecutableOperator,
    ExecutionStates2,
    InputOutputStates2,
    OperatorState,
    PartitionState,
    PollFinalize2,
    PollPull2,
    PollPush2,
};
use crate::arrays::batch::Batch2;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

#[derive(Debug)]
pub struct ValuesPartitionState {
    batches: Vec<Batch2>,
}

#[derive(Debug)]
pub struct PhysicalValues {
    batches: Vec<Batch2>,
}

impl PhysicalValues {
    pub fn new(batches: Vec<Batch2>) -> Self {
        PhysicalValues { batches }
    }
}

impl ExecutableOperator for PhysicalValues {
    fn create_states2(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates2> {
        let num_partitions = partitions[0];

        let mut states: Vec<_> = (0..num_partitions)
            .map(|_| ValuesPartitionState {
                batches: Vec::new(),
            })
            .collect();

        for (idx, batch) in self.batches.iter().enumerate() {
            states[idx % num_partitions].batches.push(batch.clone());
        }

        Ok(ExecutionStates2 {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates2::OneToOne {
                partition_states: states.into_iter().map(PartitionState::Values).collect(),
            },
        })
    }

    fn poll_push2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch2,
    ) -> Result<PollPush2> {
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn poll_finalize_push2(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize2> {
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn poll_pull2(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull2> {
        match partition_state {
            PartitionState::Values(state) => match state.batches.pop() {
                Some(batch) => Ok(PollPull2::Computed(batch.into())),
                None => Ok(PollPull2::Exhausted),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalValues {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Values")
    }
}

impl DatabaseProtoConv for PhysicalValues {
    type ProtoType = rayexec_proto::generated::execution::PhysicalValues;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // use rayexec_proto::generated::array::IpcStreamBatch;

        // // TODO: Should empty values even be allowed? Is it allowed?
        // let schema = match self.batches.first() {
        //     Some(batch) => Schema::new(
        //         batch
        //             .columns()
        //             .iter()
        //             .map(|c| Field::new("", c.datatype().clone(), true)),
        //     ),
        //     None => {
        //         return Ok(Self::ProtoType {
        //             batches: Some(IpcStreamBatch { ipc: Vec::new() }),
        //         })
        //     }
        // };

        // let buf = Vec::new();
        // let mut writer = StreamWriter::try_new(buf, &schema, IpcConfig {})?;

        // for batch in &self.batches {
        //     writer.write_batch(batch)?
        // }

        // let buf = writer.into_writer();

        // Ok(Self::ProtoType {
        //     batches: Some(IpcStreamBatch { ipc: buf }),
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // let ipc = proto.batches.required("batches")?.ipc;

        // let mut reader = StreamReader::try_new(Cursor::new(ipc), IpcConfig {})?;

        // let mut batches = Vec::new();
        // while let Some(batch) = reader.try_next_batch()? {
        //     batches.push(batch);
        // }

        // Ok(Self { batches })
    }
}
