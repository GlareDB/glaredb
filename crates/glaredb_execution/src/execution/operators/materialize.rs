use std::task::Context;

use glaredb_error::Result;
use parking_lot::Mutex;

use super::{
    BaseOperator,
    ExecutionProperties,
    MaterializingOperator,
    PollFinalize,
    PollPull,
    PollPush,
    PullOperator,
    PushOperator,
};
use crate::arrays::batch::Batch;
use crate::arrays::collection::concurrent::{
    ColumnCollectionAppendState,
    ConcurrentColumnCollection,
    ParallelColumnCollectionScanState,
};
use crate::arrays::datatype::DataType;
use crate::execution::partition_wakers::PartitionWakers;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::logical::binder::bind_context::MaterializationRef;
use crate::storage::projections::Projections;

#[derive(Debug)]
pub struct MaterializeOperatorState {
    /// The collection we're materializing into.
    collection: ConcurrentColumnCollection,
    inner: Mutex<OperatorStateInner>,
}

#[derive(Debug)]
struct OperatorStateInner {
    /// Remaining number of partitions for the singular input.
    ///
    /// Initialized when push partition states are created, decremented as
    /// partitions are finished.
    remaining_input_partitions: usize,
    /// Wakers for all pull outputs.
    ///
    /// Appended to when create partition states for the pull side.
    pull_wakers: Vec<PartitionWakers>,
}

#[derive(Debug)]
pub struct MaterializePushPartitionState {
    append_state: ColumnCollectionAppendState,
}

#[derive(Debug)]
pub struct MaterializePullPartitionState {
    /// The output pipeline index.
    ///
    /// Used to index into the correct set of wakers.
    output_idx: usize,
    /// The output partition index.
    partition_idx: usize,
    /// Projections to use for the scan.
    projections: Projections,
    /// Parallel scan state, coordinates with other scan states for this output.
    scan_state: ParallelColumnCollectionScanState,
}

#[derive(Debug)]
pub struct PhysicalMaterialize {
    /// Datatypes materialized batches.
    pub(crate) datatypes: Vec<DataType>,
    /// The materialization reference associated with this node.
    pub(crate) materialization_ref: MaterializationRef,
}

impl BaseOperator for PhysicalMaterialize {
    const OPERATOR_NAME: &str = "Materialize";

    type OperatorState = MaterializeOperatorState;

    fn create_operator_state(&self, props: ExecutionProperties) -> Result<Self::OperatorState> {
        // TODO: Configurable segment size.
        let collection =
            ConcurrentColumnCollection::new(self.datatypes.clone(), 4, props.batch_size);

        Ok(MaterializeOperatorState {
            collection,
            inner: Mutex::new(OperatorStateInner {
                remaining_input_partitions: 0,
                pull_wakers: Vec::new(),
            }),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.datatypes
    }
}

impl MaterializingOperator for PhysicalMaterialize {
    fn materialization_ref(&self) -> MaterializationRef {
        self.materialization_ref
    }
}

impl PullOperator for PhysicalMaterialize {
    type PartitionPullState = MaterializePullPartitionState;

    fn create_partition_pull_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPullState>> {
        let mut wakers = PartitionWakers::empty();
        wakers.init_for_partitions(partitions);

        let mut inner = operator_state.inner.lock();
        let output_idx = inner.pull_wakers.len();
        inner.pull_wakers.push(wakers);

        let states = operator_state
            .collection
            .init_parallel_scan_states(partitions)
            .enumerate()
            .map(
                |(partition_idx, scan_state)| MaterializePullPartitionState {
                    output_idx,
                    partition_idx,
                    projections: Projections::new(0..self.datatypes.len()), // Currently project all output.
                    scan_state,
                },
            )
            .collect();

        Ok(states)
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPullState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        output.reset_for_write()?;

        let count = operator_state.collection.parallel_scan(
            &state.projections,
            &mut state.scan_state,
            output,
        )?;
        debug_assert_eq!(count, output.num_rows());

        if count == 0 {
            // Check if input is finished.
            let mut inner = operator_state.inner.lock();

            if inner.remaining_input_partitions > 0 {
                // Still have inputs, need to come back later for more batches.
                inner.pull_wakers[state.output_idx].store(cx.waker(), state.partition_idx);
                return Ok(PollPull::Pending);
            } else {
                // We don't need the lock for the scan.
                std::mem::drop(inner);
                // Do a second scan here to verify count==0.
                //
                // We do this since there's a race-condition where:
                // 1. This partition scans, gets zero rows.
                // 2. Final input partition flushes its rows, sets remaining to 0.
                // 3. This partition does not know about the final set of rows.
                //
                // This second scan addresses step 3, ensuring we see the final
                // set of rows.
                let count = operator_state.collection.parallel_scan(
                    &state.projections,
                    &mut state.scan_state,
                    output,
                )?;
                if count != 0 {
                    // There was a race, but we now know about the final set of
                    // rows.
                    return Ok(PollPull::HasMore);
                }

                // Inputs are finished, we're exhausted.
                return Ok(PollPull::Exhausted);
            }
        }

        Ok(PollPull::HasMore)
    }
}

impl PushOperator for PhysicalMaterialize {
    type PartitionPushState = MaterializePushPartitionState;

    fn create_partition_push_states(
        &self,
        operator_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionPushState>> {
        let mut inner = operator_state.inner.lock();
        assert_eq!(
            0, inner.remaining_input_partitions,
            "Multiple inputs to physical materialize"
        );
        inner.remaining_input_partitions = partitions;

        let states = (0..partitions)
            .map(|_| MaterializePushPartitionState {
                append_state: operator_state.collection.init_append_state(),
            })
            .collect();

        Ok(states)
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
        input: &mut Batch,
    ) -> Result<PollPush> {
        operator_state
            .collection
            .append_batch(&mut state.append_state, input)?;
        operator_state.collection.flush(&mut state.append_state)?;

        // TODO: Do we want to call this every time? Maybe have a bool returned
        // from `append_batch` indicating if we flushed, and only wake then.

        let mut inner = operator_state.inner.lock();
        inner
            .pull_wakers
            .iter_mut()
            .for_each(|wakers| wakers.wake_all());

        Ok(PollPush::NeedsMore)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionPushState,
    ) -> Result<PollFinalize> {
        operator_state.collection.flush(&mut state.append_state)?;

        let mut inner = operator_state.inner.lock();
        inner.remaining_input_partitions -= 1;

        inner
            .pull_wakers
            .iter_mut()
            .for_each(|wakers| wakers.wake_all());

        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalMaterialize {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::operator::OperatorWrapper;

    // TODO: Wake tests.

    #[test]
    fn single_input_two_outputs() {
        let wrapper = OperatorWrapper::new(PhysicalMaterialize {
            datatypes: vec![DataType::Int32],
            materialization_ref: MaterializationRef {
                materialization_idx: 0,
            },
        });

        let props = ExecutionProperties { batch_size: 4 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();

        // Single input.
        let mut input_states = wrapper
            .operator
            .create_partition_push_states(&op_state, props, 1)
            .unwrap();

        // Two outputs, each receiving the same batches.
        let mut out_states1 = wrapper
            .operator
            .create_partition_pull_states(&op_state, props, 1)
            .unwrap();
        let mut out_states2 = wrapper
            .operator
            .create_partition_pull_states(&op_state, props, 1)
            .unwrap();

        let mut input = generate_batch!([4, 5, 6, 7]);
        wrapper
            .poll_push(&op_state, &mut input_states[0], &mut input)
            .unwrap();

        let expected = generate_batch!([4, 5, 6, 7]);

        // Output 1
        let mut out1 = Batch::new([DataType::Int32], 4).unwrap();
        let poll = wrapper
            .poll_pull(&op_state, &mut out_states1[0], &mut out1)
            .unwrap();
        assert_eq!(PollPull::HasMore, poll);
        assert_batches_eq(&expected, &out1);

        // Output 2
        let mut out2 = Batch::new([DataType::Int32], 4).unwrap();
        let poll = wrapper
            .poll_pull(&op_state, &mut out_states2[0], &mut out2)
            .unwrap();
        assert_eq!(PollPull::HasMore, poll);
        assert_batches_eq(&expected, &out2);

        // Finish input.
        let poll = wrapper
            .poll_finalize_push(&op_state, &mut input_states[0])
            .unwrap();
        assert_eq!(PollFinalize::Finalized, poll);

        // Outputs should be exhausted.
        let poll = wrapper
            .poll_pull(&op_state, &mut out_states1[0], &mut out1)
            .unwrap();
        assert_eq!(PollPull::Exhausted, poll);
        assert_eq!(0, out1.num_rows());

        let poll = wrapper
            .poll_pull(&op_state, &mut out_states2[0], &mut out2)
            .unwrap();
        assert_eq!(PollPull::Exhausted, poll);
        assert_eq!(0, out2.num_rows());
    }
}
