use std::str::FromStr;
use std::sync::Arc;

use async_openai::config::OpenAIConfig;
use async_openai::types::{CreateEmbeddingRequest, Embedding, EmbeddingInput, EncodingFormat};
use async_openai::Client;
use datafusion::arrow::array::{
    ArrayRef, AsArray, FixedSizeListArray, FixedSizeListBuilder, Float32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature,
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
const DEFAULT_CREDENTIAL_LOCATION: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "@creds".to_string(),
        "openai".to_string(),
        "openai_default_credential".to_string(),
        "api_key".to_string(),
    ]
});

pub struct OpenAIEmbed;

impl ConstBuiltinFunction for OpenAIEmbed {
    const NAME: &'static str = "openai_embed";
    const DESCRIPTION: &'static str = "Embeds text using OpenAI's API. 
    Available models: 'text-embedding-3-small', 'text-embedding-ada-002', 'text-embedding-3-large' default: 'text-embedding-3-small'
    Note: This function requires an API key. You can pass it as the first argument or set it as a stored credential.
    If no API key is provided, the function will attempt to use the stored credential 'openai_default_credential'";
    const EXAMPLE: &'static str =
        "openai_embed(@creds.openai.my_openai.api_key, 'text-embedding-3-small', 'Hello, world!')";
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
        _ => Err(DataFusionError::Plan("Invalid argument".to_string())),
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
        let creds_from_arg = |values: Vec<String>| -> Option<String> {
            let prov = CredentialsVarProvider::new(catalog);
            let scalar = prov.get_value(values);

            match scalar.ok()? {
                ScalarValue::Utf8(scalar) => scalar,
                _ => None,
            }
        };

        let (creds, model, idx) = match args.len() {
            // openai_embed(<expr>)
            1 => {
                let model = EmbeddingModel::TextEmbedding3Small;
                let scalar = creds_from_arg(DEFAULT_CREDENTIAL_LOCATION.clone());
                (scalar, model, 0)
            }
            // openai_embed(<model>, <expr>)
            2 => {
                let model = model_from_arg(&args[0])?;
                let scalar = creds_from_arg(DEFAULT_CREDENTIAL_LOCATION.clone());
                (scalar, model, 1)
            }
            // openai_embed('api_key', '<model>', '<expr>')
            3 => {
                let creds = match args.get(0) {
                    Some(Expr::Literal(ScalarValue::Utf8(v))) => {
                        Some(v.clone().unwrap().to_string())
                    }
                    Some(Expr::ScalarVariable(_, values)) => {
                        let scalar = creds_from_arg(values.clone());
                        scalar
                    }
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
            let input = match &args[0] {
                ColumnarValue::Array(arr) => match arr.data_type() {
                    DataType::Utf8 => EmbeddingInput::StringArray(
                        arr.as_string::<i32>()
                            .into_iter()
                            .map(|s| s.unwrap_or_default().to_string())
                            .collect(),
                    ),
                    DataType::LargeUtf8 => EmbeddingInput::StringArray(
                        arr.as_string::<i64>()
                            .into_iter()
                            .map(|s| s.unwrap_or_default().to_string())
                            .collect(),
                    ),
                    _ => return Err(DataFusionError::Plan("Invalid argument".to_string())),
                },
                ColumnarValue::Scalar(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v)) => {
                    EmbeddingInput::StringArray(vec![v.clone().unwrap_or_default()])
                }
                _ => return Err(DataFusionError::Plan("Invalid argument".to_string())),
            };

            let client = Client::with_config(OpenAIConfig::new().with_api_key(creds.clone()));
            let embed = client.embeddings();
            let res = embed.create(CreateEmbeddingRequest {
                model: model.to_string(),
                input: input,
                encoding_format: Some(EncodingFormat::Float),
                user: None,
                dimensions: None,
            });

            // no way around blocking here. Expressions are not async
            let res: datafusion::error::Result<FixedSizeListArray> =
                task::block_in_place(move || {
                    Handle::current().block_on(async move {
                        let res = res
                            .await
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                        let values_builder = Float32Builder::new();

                        let mut builder = FixedSizeListBuilder::new(values_builder, model_len);

                        for Embedding { embedding, .. } in res.data.iter() {
                            builder.values().append_slice(embedding);
                            builder.append(true);
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
