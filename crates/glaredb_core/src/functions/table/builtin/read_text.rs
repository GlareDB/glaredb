use std::collections::VecDeque;
use std::task::{Context, Poll};

use futures::FutureExt;
use glaredb_error::{DbError, Result, ResultExt};

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
use crate::runtime::filesystem::{
    AnyFile,
    FileOpenContext,
    FileSystemFuture,
    FileSystemWithState,
    OpenFlags,
};
use crate::statistics::value::StatisticsValue;
use crate::storage::projections::{ProjectedColumn, Projections};
use crate::storage::scan_filter::PhysicalScanFilter;

pub const FUNCTION_SET_READ_TEXT: TableFunctionSet = TableFunctionSet {
    name: "read_text",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Read the content of a file.",
        arguments: &["path"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &ReadText,
    )],
};

/// Test function for checking interface sanity for the filesystem stuff. Also
/// for other things like virtual columns.
#[derive(Debug, Clone, Copy)]
pub struct ReadText;

#[derive(Debug)]
pub struct ReadTextBindState {
    fs: FileSystemWithState,
    mf_data: MultiFileData,
}

#[derive(Debug)]
pub struct ReadTextOperatorState {
    fs: FileSystemWithState,
    mf_data: MultiFileData,
    projections: Projections,
}

pub struct ReadTextPartitionState {
    /// Reusable buffer for reading the data.
    buf: Vec<u8>, // TODO: Buffer managed, also not really a big deal here.
    /// Current read state.
    state: ReadState,
    /// Queue of files this partition will be handling.
    queue: VecDeque<String>,
}

enum ReadState {
    /// Initialize the next file to read.
    Init,
    /// Currently opening a file.
    Opening {
        open_fut: FileSystemFuture<'static, Result<AnyFile>>,
    },
    /// Currently scanning a file.
    Scanning { file: AnyFile, buf_offset: usize },
}

impl TableScanFunction for ReadText {
    type BindState = ReadTextBindState;
    type OperatorState = ReadTextOperatorState;
    type PartitionState = ReadTextPartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let path = ConstFold::rewrite(input.positional[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        let fs = scan_context.dispatch.filesystem_for_path(&path)?;
        let context = FileOpenContext::new(scan_context.database_context, &input.named);
        let fs = fs.load_state(context).await?;
        let mut provider = MultiFileProvider::try_new_from_path(&fs, &path)?;

        let mut mf_data = MultiFileData::empty();
        // TODO: This is implicitly single threaded. It may make sense to
        // parallelize by pushing continued expanded into the poll_pull. This
        // will matter more for reading parquet, csv than this function.
        provider.expand_all(&mut mf_data).await?;

        Ok(TableFunctionBindState {
            state: ReadTextBindState {
                fs: fs.clone(),
                mf_data,
            },
            input,
            data_schema: ColumnSchema::new([Field::new("content", DataType::utf8(), false)]),
            meta_schema: Some(provider.meta_schema()),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _filters: &[PhysicalScanFilter],
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ReadTextOperatorState {
            fs: bind_state.fs.clone(),
            mf_data: bind_state.mf_data.clone(), // TODO
            projections,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let expanded = op_state.mf_data.expanded();

        // Split files to read across all partitions.
        let states = (0..partitions)
            .map(|partition_idx| {
                let queue: VecDeque<_> = expanded[partition_idx..]
                    .iter()
                    .step_by(partitions)
                    .map(|path| path.to_string())
                    .collect();

                ReadTextPartitionState {
                    buf: Vec::new(),
                    state: ReadState::Init,
                    queue,
                }
            })
            .collect();

        Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        loop {
            match state {
                ReadTextPartitionState::Opening { buf, open_fut } => {
                    let file = match open_fut.poll_unpin(cx) {
                        Poll::Ready(result) => result?,
                        Poll::Pending => return Ok(PollPull::Pending),
                    };

                    if op_state.projections.has_data_column(0) {
                        buf.resize(file.call_size(), 0);
                    }

                    *state = ReadTextPartitionState::Scanning {
                        file,
                        buf_offset: 0,
                        buf: std::mem::take(buf),
                    };
                    continue;
                }
                ReadTextPartitionState::Scanning {
                    file,
                    buf_offset,
                    buf,
                } => {
                    let mut is_pending = false;

                    // TODO: So clean...
                    op_state
                        .projections
                        .for_each_column(output, &mut |col, arr| match col {
                            ProjectedColumn::Data(0) => {
                                // TODO: needs to happen outside.
                                loop {
                                    let read_buf = &mut buf[*buf_offset..];
                                    match file.call_poll_read(cx, read_buf)? {
                                        Poll::Ready(n) => {
                                            if n == 0 {
                                                // Read complete, write it to
                                                // the array.
                                                let mut data = PhysicalUtf8::get_addressable_mut(
                                                    &mut arr.data,
                                                )?;
                                                let s = std::str::from_utf8(buf)
                                                    .context("Invalid UTF8")?;
                                                data.put(0, s);
                                                return Ok(());
                                            } else {
                                                // Still reading, come back for
                                                // more.
                                                *buf_offset += n;
                                            }
                                        }
                                        Poll::Pending => {
                                            is_pending = true;
                                            return Ok(());
                                        }
                                    };
                                }
                            }
                            ProjectedColumn::Metadata(
                                MultiFileProvider::META_PROJECTION_FILENAME,
                            ) => {
                                println!("FILENAME");
                                Ok(())
                            }
                            ProjectedColumn::Metadata(MultiFileProvider::META_PROJECTION_ROWID) => {
                                println!("ROWID");
                                Ok(())
                            }
                            other => panic!("invalid projection: {other:?}"),
                        })?;

                    if is_pending {
                        return Ok(PollPull::Pending);
                    }

                    *state = ReadTextPartitionState::Exhausted;

                    output.set_num_rows(1)?;
                    return Ok(PollPull::Exhausted);
                }
                ReadTextPartitionState::Exhausted => {
                    output.set_num_rows(0)?;
                    return Ok(PollPull::Exhausted);
                }
            }
        }
    }
}
