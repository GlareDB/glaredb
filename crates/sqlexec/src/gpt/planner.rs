use crate::gpt::errors::{GptError, Result};
use crate::parser::{ExplainGptStmt, StatementWithExtensions};
use crate::planner::logical_plan::*;
use gpt::client::{ChatModel, CompletionRequest, GptClient, Message};

#[derive(Debug, Clone)]
pub struct GptPlanner;

impl GptPlanner {
    pub fn plan_gpt_explain(&self, stmt: ExplainGptStmt) -> Result<LogicalPlan> {
        let messages = vec![
            Message{
                role: "system".to_string(),
                content: "You are a SQL expert capable of explaining what a SQL statement is doing. Be concise in your responses.".to_string(),
            },
            Message{
                role: "user".to_string(),
                content: format!("Explain the following SQL statement:\n{}", stmt.stmt),
            },
        ];

        let req = CompletionRequest {
            model: ChatModel::Gpt35Turbo.as_str(),
            messages,
            ..Default::default()
        };

        Ok(LogicalPlan::Gpt(GptPlan::Explain(GptExplain { req })))
    }

    pub fn plan_from_gpt_function(&self, stmt)
}
