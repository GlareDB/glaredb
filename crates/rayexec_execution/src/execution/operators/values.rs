use crate::{
    database::DatabaseContext,
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
    proto::DatabaseProtoConv,
};
use rayexec_bullet::{
    batch::Batch,
    field::{Field, Schema},
    ipc::{
        stream::{StreamReader, StreamWriter},
        IpcConfig,
    },
};
use rayexec_error::{OptionExt, RayexecError, Result};
use std::{io::Cursor, sync::Arc, task::Context};

use super::{
    ExecutableOperator, ExecutionStates, InputOutputStates, OperatorState, PartitionState,
    PollFinalize, PollPull, PollPush,
};

#[derive(Debug)]
pub struct ValuesPartitionState {
    batches: Vec<Batch>,
}

#[derive(Debug)]
pub struct PhysicalValues {
    batches: Vec<Batch>,
}

impl PhysicalValues {
    pub fn new(batches: Vec<Batch>) -> Self {
        PhysicalValues { batches }
    }
}

impl ExecutableOperator for PhysicalValues {
    fn create_states(
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

    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to Values operator"))
    }

    fn poll_pull(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Values(state) => match state.batches.pop() {
                Some(batch) => Ok(PollPull::Batch(batch)),
                None => Ok(PollPull::Exhausted),
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
                    .map(|c| Field::new("", c.datatype(), true)),
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
