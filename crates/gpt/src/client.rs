use crate::errors::Result;
use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    HeaderMap,
};
use serde::{Deserialize, Serialize};

const OPENAI_API_ROOT: &str = "https://api.openai.com/v1";

pub enum ChatModel {
    Gpt4,
    Gpt35Turbo,
}

impl ChatModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChatModel::Gpt4 => "gpt-4",
            ChatModel::Gpt35Turbo => "gpt-3.5-turbo",
        }
    }
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct CompletionRequest {
    pub model: &'static str,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<&'static [&'static str]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    // Note omitting logits.
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Message {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompletionResponse {
    pub choices: Vec<CompletionChoice>,
    /// If the 'stream' parameter is activated.
    pub id: Option<String>,
    /// If the 'stream' parameter is activated.
    pub object: Option<String>,
    pub model: Option<String>,
    pub created: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompletionChoice {
    pub message: Message,
    pub index: usize,
    pub finish_reason: String,
    // Note logprobs omitted.
}

#[derive(Debug, Clone)]
pub struct GptClient {
    client: reqwest::Client,
}

impl GptClient {
    pub fn new(api_key: &str) -> Result<GptClient> {
        let mut def_headers = HeaderMap::new();
        def_headers.insert(AUTHORIZATION, format!("Bearer {api_key}").parse().unwrap());
        def_headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        let client = reqwest::Client::builder()
            .default_headers(def_headers)
            .build()?;

        Ok(GptClient { client })
    }

    pub async fn make_completion_request(
        &self,
        req: &CompletionRequest,
    ) -> Result<CompletionResponse> {
        let url = format!("{OPENAI_API_ROOT}/chat/completions");
        let resp = self
            .client
            .post(url)
            .json(req)
            .send()
            .await?
            .json::<CompletionResponse>()
            .await?;

        Ok(resp)
    }
}
