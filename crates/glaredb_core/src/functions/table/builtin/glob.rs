use std::task::{Context, Poll};

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
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::runtime::filesystem::file_provider::{MultiFileData, MultiFileProvider};
use crate::runtime::filesystem::{FileOpenContext, FileSystemWithState};
use crate::statistics::value::StatisticsValue;
use crate::storage::projections::Projections;
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
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &Glob,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct Glob;

#[derive(Debug)]
pub struct GlobBindState {
    fs: FileSystemWithState,
    glob: String,
}

#[derive(Debug)]
pub struct GlobOperatorState {
    fs: FileSystemWithState,
    glob: String,
    projections: Projections,
}

#[derive(Debug)]
pub enum GlobPartitionState {
    /// This partition is globbing.
    Globbing {
        /// The file provider.
        provider: MultiFileProvider,
        mf_data: MultiFileData,
        /// The 'nth' file we're on.
        n: usize,
        /// Current count we've written to the batch.
        curr_count: usize,
    },
    /// This partition isn't doing anything.
    Exhausted,
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
        let glob = ConstFold::rewrite(input.positional[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        let fs = scan_context.dispatch.filesystem_for_path(&glob)?;
        let context = FileOpenContext::new(scan_context.database_context, &input.named);
        let fs = fs.load_state(context).await?;

        Ok(TableFunctionBindState {
            state: GlobBindState { fs, glob },
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
            fs: bind_state.fs.clone(),
            glob: bind_state.glob.clone(),
            projections,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert!(partitions > 0);

        let mut states = vec![GlobPartitionState::Globbing {
            provider: MultiFileProvider::try_new_from_path(&op_state.fs, &op_state.glob)?,
            mf_data: MultiFileData::empty(),
            n: 0,
            curr_count: 0,
        }];
        states.resize_with(partitions, || GlobPartitionState::Exhausted);

        Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        match state {
            GlobPartitionState::Globbing {
                provider,
                mf_data,
                n,
                curr_count,
            } => {
                let cap = output.write_capacity()?;
                let mut is_exhausted = false;

                let mut filename_buf = if op_state.projections.has_data_column(0) {
                    Some(PhysicalUtf8::get_addressable_mut(
                        &mut output.arrays[0].data,
                    )?)
                } else {
                    None
                };

                while *curr_count < cap {
                    match provider.poll_expand_n(cx, mf_data, *n) {
                        Poll::Ready(Ok(_)) => {
                            match mf_data.get(*n) {
                                Some(path) => {
                                    if let Some(buf) = &mut filename_buf {
                                        buf.put(*curr_count, path);
                                    }
                                    *curr_count += 1;
                                    *n += 1;
                                }
                                None => {
                                    // No more files.
                                    is_exhausted = true;
                                    break;
                                }
                            }
                        }
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(PollPull::Pending),
                    }
                }

                output.set_num_rows(*curr_count)?;
                *curr_count = 0;

                if is_exhausted {
                    Ok(PollPull::Exhausted)
                } else {
                    Ok(PollPull::HasMore)
                }
            }
            GlobPartitionState::Exhausted => {
                output.set_num_rows(0)?;
                Ok(PollPull::Exhausted)
            }
        }
    }
}
