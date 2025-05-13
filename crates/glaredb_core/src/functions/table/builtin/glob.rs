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

pub const FUNCTION_SET_GLOB: TableFunctionSet = TableFunctionSet {
    name: "glob",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "List file names that match the provided glob.",
        arguments: &["glob"],
        example: None,
    }],
    functions: &[],
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
pub enum GlobPartitionState {}

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
        let fs = fs.try_with_context(context)?;

        Ok(TableFunctionBindState {
            state: GlobBindState { fs, glob },
            input,
            schema: ColumnSchema::new([Field::new("filename", DataType::utf8(), false)]),
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
