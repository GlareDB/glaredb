use std::task::Context;

use glaredb_error::{DbError, Result};

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataTypeId;
use crate::catalog::context::DatabaseContext;
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::{ScanContext, TableScanFunction};
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::storage::projections::Projections;

pub const FUNCTION_SET_READ_LINES: TableFunctionSet = TableFunctionSet {
    name: "read_lines",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Read lines from a file.",
        arguments: &["path"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
        &ReadLines,
    )],
};

/// Test function for checking interface sanity for the filesystem stuff.
#[derive(Debug, Clone, Copy)]
pub struct ReadLines;

#[derive(Debug)]
pub struct ReadLinesBindState {}

#[derive(Debug)]
pub struct ReadLinesOperatorState {}

#[derive(Debug)]
pub struct ReadLinesPartitionState {}

impl TableScanFunction for ReadLines {
    type BindState = ReadLinesBindState;
    type OperatorState = ReadLinesOperatorState;
    type PartitionState = ReadLinesPartitionState;

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

        unimplemented!()
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        unimplemented!()
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        unimplemented!()
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}
