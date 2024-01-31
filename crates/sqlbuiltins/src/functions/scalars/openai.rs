use std::str::FromStr;
use std::sync::Arc;

use async_openai::config::OpenAIConfig;
use async_openai::types::{CreateEmbeddingRequest, Embedding, EmbeddingInput, EncodingFormat};
use async_openai::Client;
use datafusion::arrow::array::{
    ArrayRef,
    AsArray,
    FixedSizeListArray,
    FixedSizeListBuilder,
    Float32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr,
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    Signature,
    TypeSignature,
    Volatility,
};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use datafusion::variable::VarProvider;
use datafusion_ext::vars::CredentialsVarProvider;
use once_cell::sync::Lazy;
use protogen::metastore::types::catalog::FunctionType;
use tokio::runtime::Handle;
use tokio::task;

use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction};
static DEFAULT_CREDENTIAL_LOCATION: Lazy<&[&str]> =
    Lazy::new(|| &["@creds", "openai", "openai_default_credential", "api_key"]);
/// This is a placeholder for empty values in the input array
/// The openai API does not accept empty strings, so we use this to represent NULL/"" values
const EMPTY_PLACEHOLDER: &str = "NULL";

pub struct OpenAIEmbed;

impl ConstBuiltinFunction for OpenAIEmbed {
    const NAME: &'static str = "openai_embed";
    const DESCRIPTION: &'static str = "Embeds text using OpenAI's API. 
    WARNING: This function makes an external API call and may be slow. It is recommended to use it with small datasets.
    Available models: 'text-embedding-3-small', 'text-embedding-ada-002', 'text-embedding-3-large' default: 'text-embedding-3-small'
    Note: This function requires an API key. You can pass it as the first argument or set it as a stored credential.
    If no API key is provided, the function will attempt to use the stored credential 'openai_default_credential'";
    const EXAMPLE: &'static str =
        "openai_embed(@creds.openai.my_openai, 'text-embedding-3-small', 'Hello, world!')";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            // args: <API_KEY>, <MODEL>, <TEXT>
            TypeSignature::OneOf(vec![
                // openai_embed('<text>')  -- uses default model and credential
                TypeSignature::Exact(vec![DataType::Utf8]),
                // openai_embed('<model>', '<text>') -- uses default credential
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                // openai_embed('api_key', '<model>', '<text>')  --uses provided api key and model
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            ]),
            // This is a volatile function because it makes an external API call.
            // Additionally, the openai API key may change or expire at any time.
            Volatility::Volatile,
        ))
    }
}
#[derive(Debug)]
enum EmbeddingModel {
    TextEmbedding3Small,
    TextEmbeddingAda002,
    TextEmbedding3Large,
}
impl EmbeddingModel {
    fn len(&self) -> i32 {
        match self {
            EmbeddingModel::TextEmbedding3Small => 1536,
            EmbeddingModel::TextEmbeddingAda002 => 1536,
            EmbeddingModel::TextEmbedding3Large => 3072,
        }
    }
}
impl FromStr for EmbeddingModel {
    type Err = datafusion::error::DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text-embedding-3-small" => Ok(EmbeddingModel::TextEmbedding3Small),
            "text-embedding-ada-002" => Ok(EmbeddingModel::TextEmbeddingAda002),
            "text-embedding-3-large" => Ok(EmbeddingModel::TextEmbedding3Large),
            _ => Err(DataFusionError::Execution("Invalid model name".to_string())),
        }
    }
}

impl ToString for EmbeddingModel {
    fn to_string(&self) -> String {
        match self {
            EmbeddingModel::TextEmbedding3Small => "text-embedding-3-small".to_string(),
            EmbeddingModel::TextEmbeddingAda002 => "text-embedding-ada-002".to_string(),
            EmbeddingModel::TextEmbedding3Large => "text-embedding-3-large".to_string(),
        }
    }
}

fn model_from_arg(arg: &Expr) -> datafusion::error::Result<EmbeddingModel> {
    match arg {
        Expr::Literal(ScalarValue::Utf8(v)) => v.clone().unwrap().parse(),
        _ => Err(DataFusionError::Plan("Invalid argument. Available models are: 'text-embedding-3-small', 'text-embedding-ada-002', 'text-embedding-3-large' ".to_string())),
    }
}

impl BuiltinScalarUDF for OpenAIEmbed {
    fn namespace(&self) -> crate::functions::FunctionNamespace {
        crate::functions::FunctionNamespace::Optional("openai")
    }
    fn try_as_expr(
        &self,
        catalog: &catalog::session_catalog::SessionCatalog,
        mut args: Vec<Expr>,
    ) -> datafusion::error::Result<Expr> {
        let creds_from_arg = |values: Vec<String>| -> Option<OpenAIConfig> {
            let prov = CredentialsVarProvider::new(catalog);
            let scalar = prov.get_value(values);

            match scalar.ok()? {
                ScalarValue::Utf8(v) => Some(OpenAIConfig::new().with_api_key(v.unwrap())),
                ScalarValue::Struct(Some(values), _) => {
                    let api_key = values.first()?;
                    let api_base = values.get(1);
                    let org_id = values.get(2);
                    let api_key = match api_key {
                        ScalarValue::Utf8(v) => v.clone().unwrap(),
                        _ => return None,
                    };
                    let mut config = OpenAIConfig::new().with_api_key(api_key);
                    config = match api_base {
                        Some(ScalarValue::Utf8(Some(v))) => config.with_api_base(v.clone()),
                        _ => config,
                    };
                    config = match org_id {
                        Some(ScalarValue::Utf8(Some(v))) => config.with_org_id(v.clone()),
                        _ => config,
                    };
                    Some(config)
                }
                _ => None,
            }
        };

        let (creds, model, idx) = match args.len() {
            // openai_embed(<expr>)
            1 => {
                let model = EmbeddingModel::TextEmbedding3Small;
                let scalar = creds_from_arg(
                    DEFAULT_CREDENTIAL_LOCATION
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                );
                (scalar, model, 0)
            }
            // openai_embed(<model>, <expr>)
            2 => {
                let model = model_from_arg(&args[0])?;
                let scalar = creds_from_arg(
                    DEFAULT_CREDENTIAL_LOCATION
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                );
                (scalar, model, 1)
            }
            // openai_embed('api_key', '<model>', '<expr>')
            3 => {
                let creds = match args.first() {
                    Some(Expr::Literal(ScalarValue::Utf8(v))) => {
                        Some(OpenAIConfig::new().with_api_key(v.clone().unwrap()))
                    }
                    Some(Expr::ScalarVariable(_, values)) => creds_from_arg(values.clone()),
                    _ => return Err(DataFusionError::Plan("Invalid argument".to_string())),
                };

                let model = model_from_arg(&args[1])?;
                (creds, model, 2)
            }
            _ => return Err(DataFusionError::Plan("Invalid argument count".to_string())),
        };
        let Some(creds) = creds else {
            return Err(DataFusionError::Plan(
                "No API key or credential provided".to_string(),
            ));
        };

        let model_len = model.len();
        let return_type_fn: ReturnTypeFunction = Arc::new(move |_| {
            let f = Field::new("item", DataType::Float32, true);
            let dtype = DataType::FixedSizeList(Arc::new(f), model_len);
            Ok(Arc::new(dtype))
        });

        let scalar_fn_impl: ScalarFunctionImplementation = Arc::new(move |args| {
            let input_chunks = match &args[0] {
                ColumnarValue::Array(arr) => match arr.data_type() {
                    DataType::Utf8 => arr
                        .as_string::<i32>()
                        .into_iter()
                        .map(|s| s.unwrap_or(EMPTY_PLACEHOLDER).to_string())
                        .collect::<Vec<_>>()
                        .chunks(2000)
                        .map(|chunk| EmbeddingInput::StringArray(chunk.to_vec()))
                        .collect::<Vec<_>>(),
                    DataType::LargeUtf8 => arr
                        .as_string::<i64>()
                        .into_iter()
                        .map(|s| s.unwrap_or(EMPTY_PLACEHOLDER).to_string())
                        .collect::<Vec<_>>()
                        .chunks(2000)
                        .map(|chunk| EmbeddingInput::StringArray(chunk.to_vec()))
                        .collect::<Vec<_>>(),
                    _ => return Err(DataFusionError::Plan("Invalid argument".to_string())),
                },
                ColumnarValue::Scalar(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v)) => {
                    vec![EmbeddingInput::StringArray(vec![v
                        .clone()
                        .unwrap_or_default()])]
                }
                _ => return Err(DataFusionError::Plan("Invalid argument".to_string())),
            };

            let reqwest_client = reqwest::ClientBuilder::new()
                // Set a hard timeout of 10 seconds
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap();

            let client = Client::with_config(creds.clone()).with_http_client(reqwest_client);
            let embed = client.embeddings();
            // We chunk the input into 2000 items per request to avoid hitting token limits
            let reqs = input_chunks
                .into_iter()
                .map(|input| CreateEmbeddingRequest {
                    model: model.to_string(),
                    input,
                    encoding_format: Some(EncodingFormat::Float),
                    user: None,
                    dimensions: None,
                });

            // no way around blocking here. Expressions are not async
            let res: datafusion::error::Result<FixedSizeListArray> =
                task::block_in_place(move || {
                    Handle::current().block_on(async move {
                        let values_builder = Float32Builder::new();
                        let mut builder = FixedSizeListBuilder::new(values_builder, model_len);

                        // We need to do them sequentially to maintain the order
                        // This should also help us abort early if there's an error
                        for req in reqs {
                            let res = embed.create(req).await;
                            let res = res.map_err(|e| DataFusionError::Execution(e.to_string()))?;
                            for Embedding { embedding, .. } in res.data.iter() {
                                builder.values().append_slice(embedding);
                                builder.append(true);
                            }
                        }
                        Ok(builder.finish())
                    })
                });

            let a: ArrayRef = Arc::new(res?);
            Ok(ColumnarValue::Array(a))
        });
        let signature = Signature::exact(vec![DataType::Utf8], Volatility::Volatile);
        let udf = ScalarUDF::new(Self::NAME, &signature, &return_type_fn, &scalar_fn_impl);

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            vec![args.remove(idx)],
        )))
    }
}
