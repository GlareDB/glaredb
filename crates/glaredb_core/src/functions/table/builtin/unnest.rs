use std::task::Context;

use glaredb_error::Result;

use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::compute::copy::copy_rows_raw;
use crate::arrays::datatype::DataTypeId;
use crate::arrays::field::{ColumnSchema, Field};
use crate::execution::operators::{ExecutionProperties, PollExecute, PollFinalize};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::execute::TableExecuteFunction;
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::logical::statistics::StatisticsValue;

// TODO: Unit test when create list arrays is more fun.

pub const FUNCTION_SET_UNNEST: TableFunctionSet = TableFunctionSet {
    name: "unnest",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::List,
        description: "Converts a list into a table with one row per list element.",
        arguments: &["list"],
        example: None,
    }],
    functions: &[
        // unnest(list)
        RawTableFunction::new_execute(
            &Signature::new(&[DataTypeId::List(&DataTypeId::Any)], DataTypeId::Table),
            &UnnestList,
        ),
    ],
};

#[derive(Debug)]
pub struct UnnestListBindState {}

#[derive(Debug)]
pub struct UnnestListOperatorState {}

#[derive(Debug)]
pub struct UnnestListPartitionState {
    /// Current row in the input.
    current_row: usize,
    /// Current position in the list.
    current_list_pos: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct UnnestList;

impl TableExecuteFunction for UnnestList {
    type BindState = UnnestListBindState;
    type OperatorState = UnnestListOperatorState;
    type PartitionState = UnnestListPartitionState;

    fn bind(&self, input: TableFunctionInput) -> Result<TableFunctionBindState<Self::BindState>> {
        let datatype = input.positional[0].datatype()?;
        let meta = datatype.try_get_list_type_meta()?;
        let out_type = meta.datatype.as_ref().clone();

        Ok(TableFunctionBindState {
            state: UnnestListBindState {},
            input,
            schema: ColumnSchema::new([Field::new("unnest", out_type, true)]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_execute_operator_state(
        _bind_state: &Self::BindState,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(UnnestListOperatorState {})
    }

    fn create_execute_partition_states(
        _op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let states = (0..partitions)
            .map(|_| UnnestListPartitionState {
                current_row: 0,
                current_list_pos: 0,
            })
            .collect();

        Ok(states)
    }

    fn poll_execute(
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        // TODO: This could ensure the child buffers are shared from the input.
        // Currently we just copy them.

        let input = input.arrays[0].flatten()?;

        let cap = output.write_capacity()?;
        let out_arr = &mut output.arrays[0];

        let mut output_offset = 0;
        let mut total_count = 0;

        let list_buf = input.array_buffer.get_list_buffer()?;

        loop {
            if total_count == cap {
                // We filled up this batch, need to emit but keep the current
                // input batch.
                output.set_num_rows(total_count)?;
                return Ok(PollExecute::HasMore);
            }

            if state.current_row >= input.logical_len() {
                // We need more input. Reset states to accomadate that.
                state.current_row = 0;
                state.current_list_pos = 0;

                output.set_num_rows(total_count)?;

                // Return ready here to emit the current batch. This may waste
                // space, we may want to store the output offset on state, and
                // emit NeedsMore instead.
                //
                // That change would also require that we track when this gets
                // finalized, as we'd need to emit any remaining rows.
                return Ok(PollExecute::Ready);
            }

            let row_valid = input.validity.is_valid(state.current_row);
            if !row_valid {
                // Skip
                state.current_row += 1;
                state.current_list_pos = 0;
                continue;
            }

            let sel_idx = input.selection.get(state.current_row).unwrap();
            let list_meta = list_buf.metadata.as_slice()[sel_idx];

            let src_offset = list_meta.offset as usize + state.current_list_pos;
            let src_rem_len = list_meta.len as usize - src_offset;

            if src_rem_len == 0 {
                // Move to next list.
                state.current_row += 1;
                state.current_list_pos = 0;
                continue;
            }

            // Figure out how much we can fit in the output.
            let count = usize::min(cap - output_offset, src_rem_len);
            // Select the part of the list we care about.
            let src_sel = Selection::linear(src_offset, src_rem_len);

            // Map index from list (after src_sel) to output idx.
            let mapping = (0..count).map(|idx| (idx, idx + output_offset));

            copy_rows_raw(
                list_buf.child_physical_type,
                &list_buf.child_buffer,
                &list_buf.child_validity,
                Some(src_sel),
                mapping,
                &mut out_arr.data,
                &mut out_arr.validity,
            )?;

            // Update states.
            output_offset += count;
            total_count += count;
            state.current_list_pos += count;
        }
    }

    fn poll_finalize_execute(
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionState,
    ) -> Result<PollFinalize> {
        Ok(PollFinalize::Finalized)
    }
}
