use std::io::Cursor;
use std::sync::Arc;
use std::task::Context;

use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::field::{Field, Schema};
use rayexec_bullet::ipc::stream::{StreamReader, StreamWriter};
use rayexec_bullet::ipc::IpcConfig;
use rayexec_error::{OptionExt, RayexecError, Result};

use super::{
    ExecutableOperator,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PollFinalizeOld,
    PollPullOld,
    PollPushOld,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

#[derive(Debug)]
pub struct ValuesPartitionState {
    batches: Vec<BatchOld>,
}

#[derive(Debug)]
pub struct PhysicalValues {
    batches: Vec<BatchOld>,
}

impl PhysicalValues {
    pub fn new(batches: Vec<BatchOld>) -> Self {
        PhysicalValues { batches }
    }
}

impl ExecutableOperator for PhysicalValues {
    fn create_states_old(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let num_partitions = partitions[0];

        let mut states: Vec<_> = (0..num_partitions)
            .map(|_| ValuesPartitionState {
                batches: Vec::new(),
            })
            .collect();

        for (idx, batch) in self.batches.iter().enumerate() {
            states[idx % num_partitions].batches.push(batch.clone());
        }

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: states.into_iter().map(PartitionState::Values).collect(),
            },
        })
    }

    fn poll_push_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: BatchOld,
    ) -> Result<PollPushOld> {
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn poll_finalize_push_old(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalizeOld> {
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn poll_pull_old(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPullOld> {
        match partition_state {
            PartitionState::Values(state) => match state.batches.pop() {
                Some(batch) => Ok(PollPullOld::Computed(batch.into())),
                None => Ok(PollPullOld::Exhausted),
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
        use rayexec_proto::generated::array::IpcStreamBatch;

        // TODO: Should empty values even be allowed? Is it allowed?
        let schema = match self.batches.first() {
            Some(batch) => Schema::new(
                batch
                    .columns()
                    .iter()
                    .map(|c| Field::new("", c.datatype().clone(), true)),
            ),
            None => {
                return Ok(Self::ProtoType {
                    batches: Some(IpcStreamBatch { ipc: Vec::new() }),
                })
            }
        };

        let buf = Vec::new();
        let mut writer = StreamWriter::try_new(buf, &schema, IpcConfig {})?;

        for batch in &self.batches {
            writer.write_batch(batch)?
        }

        let buf = writer.into_writer();

        Ok(Self::ProtoType {
            batches: Some(IpcStreamBatch { ipc: buf }),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        let ipc = proto.batches.required("batches")?.ipc;

        let mut reader = StreamReader::try_new(Cursor::new(ipc), IpcConfig {})?;

        let mut batches = Vec::new();
        while let Some(batch) = reader.try_next_batch()? {
            batches.push(batch);
        }

        Ok(Self { batches })
    }
}
