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
use crate::logical::statistics::StatisticsValue;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::runtime::filesystem::{AnyFile, AnyFileSystem, FileSystemFuture, OpenFlags};
use crate::storage::projections::{ProjectedColumn, Projections};

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

/// Test function for checking interface sanity for the filesystem stuff.
#[derive(Debug, Clone, Copy)]
pub struct ReadText;

#[derive(Debug)]
pub struct ReadTextBindState {
    fs: AnyFileSystem,
    path: String,
}

#[derive(Debug)]
pub struct ReadTextOperatorState {
    fs: AnyFileSystem,
    path: String,
    projections: Projections,
}

pub enum ReadTextPartitionState {
    Opening {
        buf: Vec<u8>,
        open_fut: FileSystemFuture<'static, Result<AnyFile>>,
    },
    Scanning {
        file: AnyFile,
        buf_offset: usize,
        buf: Vec<u8>, // TODO: Buffer managed, also not really a big deal here.
    },
    Exhausted,
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
        match fs.call_stat(&path).await? {
            Some(stat) if stat.file_type.is_file() => (), // We have a file.
            Some(_) => return Err(DbError::new("Cannot read lines from a directory")), // TODO: Globbing and stuff
            None => return Err(DbError::new(format!("Missing file for path '{path}'"))),
        }

        Ok(TableFunctionBindState {
            state: ReadTextBindState {
                fs: fs.clone(),
                path,
            },
            input,
            schema: ColumnSchema::new([Field::new("content", DataType::Utf8, false)]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ReadTextOperatorState {
            fs: bind_state.fs.clone(),
            path: bind_state.path.clone(),
            projections,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        // Single partition reads for now.
        let mut states = vec![ReadTextPartitionState::Opening {
            buf: Vec::new(),
            open_fut: op_state
                .fs
                .call_open_static(OpenFlags::READ, op_state.path.clone()),
        }];
        states.resize_with(partitions, || ReadTextPartitionState::Exhausted);

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
                                let read_buf = &mut buf[*buf_offset..];
                                loop {
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
