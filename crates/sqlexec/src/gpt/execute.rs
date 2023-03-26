use crate::gpt::errors::{GptError, Result};
use crate::parser::{ExplainGptStmt, StatementWithExtensions};
use crate::planner::logical_plan::*;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{memory::MemoryStream, ExecutionPlan, SendableRecordBatchStream};
use gpt::client::{ChatModel, CompletionRequest, GptClient};
use std::sync::Arc;

pub struct GptExecutor<'a> {
    client: Option<&'a GptClient>,
}

impl<'a> GptExecutor<'a> {
    pub fn new(client: Option<&'a GptClient>) -> Self {
        GptExecutor { client }
    }

    pub async fn execute_gpt_explain(&self, plan: GptExplain) -> Result<SendableRecordBatchStream> {
        let client = self.client.ok_or(GptError::GptClientNotConfigured)?;
        let resp = client.make_completion_request(&plan.req).await?;

        let choice = resp
            .choices
            .into_iter()
            .last()
            .ok_or(GptError::NoCompletions)?;

        let contents = textwrap::wrap(&choice.message.content, 80).join("\n");
        let arr = StringArray::from(vec![Some(contents)]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "GPT Explain",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let stream = MemoryStream::try_new(vec![batch], schema, None).unwrap();
        Ok(Box::pin(stream))
    }
}
