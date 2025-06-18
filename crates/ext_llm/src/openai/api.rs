//! <https://platform.openai.com/docs/api-reference/responses>

use serde::{Deserialize, Serialize};

// TODO: Additional fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateResponseRequest<'a> {
    /// Model ID.
    pub model: &'a str,
    /// Input string.
    // TODO: This should be extended to allow for response ids for stateful
    // requests.
    pub input: &'a str,
}

// TODO: Additional fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateResponseResponse<'a> {
    /// Response id.
    pub id: &'a str,
    /// Outputs for the model.
    pub output: Vec<ResponseOutput<'a>>,
}

impl<'a> CreateResponseResponse<'a> {
    /// Get either the first "output_text" message or refusal.
    ///
    /// Returns None if we don't have any usable messages.
    // TODO: Create an `output_text` utility similar to the openai sdk.
    pub fn usable_output_text(&self) -> Option<&ResponseOutputContent<'a>> {
        for output in &self.output {
            if let ResponseOutput::Message { content, .. } = output {
                if let Some(content) = content.first() {
                    return Some(content);
                }
            }
        }
        None
    }
}

/// An individual output message tagged by its "type" field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseOutput<'a> {
    Message {
        id: &'a str,
        role: &'a str,
        content: Vec<ResponseOutputContent<'a>>,
        status: &'a str,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseOutputContent<'a> {
    OutputText { text: &'a str },
    Refusal { refusal: &'a str },
}
