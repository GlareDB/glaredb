use std::task::Context;

use glaredb_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalI64};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::field::{ColumnSchema, Field};
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::{ScanContext, TableScanFunction};
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::statistics::value::StatisticsValue;
use crate::storage::projections::{ProjectedColumn, Projections};
use crate::storage::scan_filter::PhysicalScanFilter;

pub const FUNCTION_SET_REPEAT: TableFunctionSet = TableFunctionSet {
    name: "repeat",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Repeat a value the specified number of times.",
        arguments: &["value", "count"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Table),
        &Repeat,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Repeat;

#[derive(Debug)]
pub struct RepeatBindState {
    value: i64,
    count: i64,
}

#[derive(Debug)]
pub struct RepeatOperatorState {
    value: i64,
    count: i64,
    projections: Projections,
}

#[derive(Debug)]
pub struct RepeatPartitionState {
    remaining: usize,
}

impl TableScanFunction for Repeat {
    type BindState = RepeatBindState;
    type OperatorState = RepeatOperatorState;
    type PartitionState = RepeatPartitionState;

    async fn bind(
        _scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let value = input.positional[0]
            .clone()
            .try_into_scalar()?
            .try_as_i64()?;
        let count = input.positional[1]
            .clone()
            .try_into_scalar()?
            .try_as_i64()?;

        Ok(TableFunctionBindState {
            state: RepeatBindState { value, count },
            input,
            data_schema: ColumnSchema::new([Field::new("repeat", DataType::int64(), false)]),
            meta_schema: None,
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _filters: &[PhysicalScanFilter],
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(RepeatOperatorState {
            value: bind_state.value,
            count: bind_state.count,
            projections,
        })
    }

    fn create_pull_partition_states(
        _bind_state: &Self::BindState,
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert!(partitions > 0);

        let total_count = op_state.count.max(0) as usize;
        let states = (0..partitions)
            .map(|partition_idx| {
                let start = (total_count * partition_idx) / partitions;
                let end = (total_count * (partition_idx + 1)) / partitions;
                RepeatPartitionState {
                    remaining: end - start,
                }
            })
            .collect();

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        _bind_state: &Self::BindState,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let out_cap = output.write_capacity()?;
        let count = usize::min(state.remaining, out_cap);

        if count == 0 {
            return Ok(PollPull::Exhausted);
        }

        op_state
            .projections
            .for_each_column(output, &mut |col, arr| match col {
                ProjectedColumn::Data(0) => {
                    let mut data = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                    for idx in 0..count {
                        data.put(idx, &op_state.value);
                    }
                    Ok(())
                }
                other => panic!("invalid projection: {other:?}"),
            })?;

        state.remaining -= count;
        output.set_num_rows(count)?;

        if state.remaining > 0 {
            Ok(PollPull::HasMore)
        } else {
            Ok(PollPull::Exhausted)
        }
    }
}
