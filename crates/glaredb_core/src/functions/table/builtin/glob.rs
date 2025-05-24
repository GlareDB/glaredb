use std::task::Context;

use glaredb_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::field::{ColumnSchema, Field};
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::{ScanContext, TableScanFunction};
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::runtime::filesystem::file_provider::{MultiFileData, MultiFileProvider};
use crate::statistics::value::StatisticsValue;
use crate::storage::projections::{ProjectedColumn, Projections};
use crate::storage::scan_filter::PhysicalScanFilter;

pub const FUNCTION_SET_GLOB: TableFunctionSet = TableFunctionSet {
    name: "glob",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "List file names that match the provided glob.",
        arguments: &["glob"],
        example: None,
    }],
    functions: &[
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
            &Glob,
        ),
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::List], DataTypeId::Table),
            &Glob,
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Glob;

#[derive(Debug)]
pub struct GlobBindState {
    mf_data: MultiFileData,
}

#[derive(Debug)]
pub struct GlobOperatorState {
    mf_data: MultiFileData,
    projections: Projections,
}

#[derive(Debug)]
pub struct GlobPartitionState {
    /// Path indices this partition will be emitting.
    path_indices: Vec<usize>,
}

impl TableScanFunction for Glob {
    type BindState = GlobBindState;
    type OperatorState = GlobOperatorState;
    type PartitionState = GlobPartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let (mut provider, _) =
            MultiFileProvider::try_new_from_inputs(scan_context, &input).await?;

        let mut mf_data = MultiFileData::empty();
        provider.expand_all(&mut mf_data).await?;

        Ok(TableFunctionBindState {
            state: GlobBindState { mf_data },
            input,
            data_schema: ColumnSchema::new([Field::new("filename", DataType::utf8(), false)]),
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
        Ok(GlobOperatorState {
            mf_data: bind_state.mf_data.clone(),
            projections,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert!(partitions > 0);

        let states = (0..partitions)
            .map(|partition_idx| {
                let path_indices = (0..op_state.mf_data.expanded_count())
                    .skip(partition_idx)
                    .step_by(partitions)
                    .collect();
                GlobPartitionState { path_indices }
            })
            .collect();

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let out_cap = output.write_capacity()?;
        let count = usize::min(state.path_indices.len(), out_cap);

        if count == 0 {
            return Ok(PollPull::Exhausted);
        }

        op_state
            .projections
            .for_each_column(output, &mut |col, arr| {
                match col {
                    ProjectedColumn::Data(0) => {
                        let mut data = PhysicalUtf8::get_addressable_mut(arr.data_mut())?;
                        let expanded = op_state.mf_data.expanded();

                        // Treat paths as a stack, start from the back first.
                        for (idx, &path_idx) in
                            state.path_indices.iter().rev().take(count).enumerate()
                        {
                            let path = &expanded[path_idx];
                            data.put(idx, path);
                        }

                        Ok(())
                    }
                    other => panic!("invalid projection: {other:?}"),
                }
            })?;

        // Truncate remaining paths.
        let rem = state.path_indices.len() - count;
        state.path_indices.truncate(rem);

        output.set_num_rows(count)?;
        Ok(PollPull::HasMore)
    }
}
