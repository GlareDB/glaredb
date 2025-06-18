use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::Context;

use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI32,
    PhysicalI64,
    PhysicalUtf8,
};
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::{DataType, DataTypeId};
use glaredb_core::arrays::field::{ColumnSchema, Field};
use glaredb_core::execution::operators::{ExecutionProperties, PollPull};
use glaredb_core::functions::Signature;
use glaredb_core::functions::documentation::{Category, Documentation};
use glaredb_core::functions::function_set::TableFunctionSet;
use glaredb_core::functions::table::scan::{ScanContext, TableScanFunction};
use glaredb_core::functions::table::{
    RawTableFunction,
    TableFunctionBindState,
    TableFunctionInput,
};
use glaredb_core::optimizer::expr_rewrite::ExpressionRewriteRule;
use glaredb_core::optimizer::expr_rewrite::const_fold::ConstFold;
use glaredb_core::runtime::filesystem::FileOpenContext;
use glaredb_core::statistics::value::StatisticsValue;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::{OptionExt, Result};

pub const FUNCTION_SET_OPENAI_CREATE_RESPONSE: TableFunctionSet = TableFunctionSet {
    name: "openai_create_response",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: r#"
Create an OpenAI model response.

Uses the [Create Model Response](https://platform.openai.com/docs/api-reference/responses/create) API.
"#,
        arguments: &["input"],
        example: None,
    }],
    functions: &[],
};

#[derive(Debug, Clone, Copy)]
pub struct OpenAICreateResponse;

pub struct OpenAICreateResponseBindState {
    input: String,
    model: String,
    api_key: String,
}

pub struct OpenAICreateResponseOperatorState {
    projections: Projections,
    input: String,
    model: String,
    api_key: String,
}

pub enum OpenAICreateResponsePartitionState {
    /// Need to initialized the request.
    Init,
}

impl TableScanFunction for OpenAICreateResponse {
    type BindState = OpenAICreateResponseBindState;
    type OperatorState = OpenAICreateResponseOperatorState;
    type PartitionState = OpenAICreateResponsePartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let context = FileOpenContext::new(scan_context.database_context, &input.named);
        let model = context.require_value("model")?.try_into_string()?;
        let api_key = context.require_value("api_key")?.try_into_string()?;

        assert_eq!(1, input.positional.len());
        let text_input = ConstFold::rewrite(input.positional[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        Ok(TableFunctionBindState {
            state: OpenAICreateResponseBindState {
                input: text_input,
                model,
                api_key,
            },
            input,
            data_schema: ColumnSchema::new([
                Field::new("input", DataType::utf8(), false),
                Field::new("response_id", DataType::utf8(), false),
                Field::new("output_text", DataType::utf8(), false),
            ]),
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
        Ok(OpenAICreateResponseOperatorState {
            projections,
            input: bind_state.input.clone(),     // TODO
            model: bind_state.model.clone(),     // TODO
            api_key: bind_state.api_key.clone(), // TODO
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
