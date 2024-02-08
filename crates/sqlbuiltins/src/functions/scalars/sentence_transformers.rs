use std::sync::Arc;

use candle_core::{CpuStorage, Device, Storage, Tensor, WithDType};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config, DTYPE};
use datafusion::arrow::array::{Array, AsArray, FixedSizeListBuilder, Float32Builder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr,
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    ScalarUDFImpl,
    Signature,
    Volatility,
};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use hf_hub::api::sync::Api;
use hf_hub::{Repo, RepoType};
use protogen::metastore::types::catalog::FunctionType;
use tokenizers::{PaddingParams, Tokenizer};

use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction, FunctionNamespace};

#[derive(Clone)]
pub struct BertUdf {
    model: Arc<BertModel>,
    signature: Signature,
    tokenizer: Tokenizer,
    device: Device,
    output_dim: usize,
}

impl std::fmt::Debug for BertUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BertUdf").finish()
    }
}

impl BertUdf {
    pub fn new() -> Self {
        let device = Device::Cpu;
        let model_id = "sentence-transformers/all-MiniLM-L6-v2".to_string();
        let revision = "refs/pr/21".to_string();

        let repo = Repo::with_revision(model_id, RepoType::Model, revision);

        let (config_filename, tokenizer_filename, weights_filename) = {
            let api = Api::new().expect("failed to create api");
            let api = api.repo(repo);
            let config = api.get("config.json").expect("failed to get config");
            let tokenizer = api.get("tokenizer.json").expect("failed to get tokenizer");
            let weights = api.get("model.safetensors").expect("failed to get weights");

            (config, tokenizer, weights)
        };
        let config = std::fs::read_to_string(config_filename).expect("failed to read config");


        let config: Config = serde_json::from_str(&config).expect("failed to parse config");
        let mut tokenizer =
            Tokenizer::from_file(tokenizer_filename).expect("failed to load tokenizer");

        let pp = PaddingParams {
            strategy: tokenizers::PaddingStrategy::BatchLongest,
            ..Default::default()
        };
        tokenizer.with_padding(Some(pp));
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&[weights_filename], DTYPE, &device)
                .expect("failed to load weights")
        };
        let model = BertModel::load(vb, &config).expect("failed to load model");
        let signature = Signature::exact(vec![DataType::Utf8], Volatility::Stable);

        let output_dim = 384;

        Self {
            model: Arc::new(model),
            signature,
            tokenizer,
            device,
            output_dim,
        }
    }
}

impl ConstBuiltinFunction for BertUdf {
    const NAME: &'static str = "minilm_l6_v2";

    const DESCRIPTION: &'static str = "Transforms a sentence into a vector using a BERT model";

    const EXAMPLE: &'static str = "sentence_transformers.minilm_l6_v2('This is a sentence')";

    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(self.signature.clone())
    }
}

impl ScalarUDFImpl for BertUdf {
    fn aliases(&self) -> &[String] {
        &[]
    }

    fn monotonicity(
        &self,
    ) -> datafusion::error::Result<Option<datafusion::logical_expr::FuncMonotonicity>> {
        Ok(None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "minilm_l6_v2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        let f = Field::new("item", DataType::Float32, true);
        let dtype = DataType::FixedSizeList(Arc::new(f), self.output_dim as i32);
        Ok(dtype)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let values = match &args[0] {
            datafusion::physical_plan::ColumnarValue::Array(arr) => {
                let values = match arr.data_type() {
                    DataType::Utf8 => {
                        if arr.null_count() > 0 {
                            return Err(DataFusionError::Execution(
                                "null values not supported".to_string(),
                            ));
                        }
                        arr.as_string::<i32>()
                            .into_iter()
                            .map(|v| v.unwrap())
                            .collect::<Vec<_>>()
                    }
                    _ => return Err(DataFusionError::Execution("invalid type".to_string())),
                };
                values
            }
            datafusion::physical_plan::ColumnarValue::Scalar(ScalarValue::Utf8(ref v)) => {
                vec![v.as_deref().unwrap()]
            }
            _ => return Err(DataFusionError::Execution("invalid type".to_string())),
        };

        let tokens = self.tokenizer.encode_batch(values, true).unwrap();

        let token_ids = tokens
            .iter()
            .map(|tokens| {
                let tokens = tokens.get_ids().to_vec();
                Ok(Tensor::new(tokens.as_slice(), &self.device)?)
            })
            .collect::<Result<Vec<_>, candle_core::Error>>()
            .unwrap();
        let token_ids = Tensor::stack(&token_ids, 0).unwrap();
        let token_type_ids = token_ids.zeros_like().unwrap();
        let embeddings = self.model.forward(&token_ids, &token_type_ids).unwrap();

        let (_n_sentence, n_tokens, _hidden_size) = embeddings.dims3().unwrap();
        let embeddings = (embeddings.sum(1).unwrap() / (n_tokens as f64)).unwrap();
        let n_dims = embeddings.shape().dims().len();
        let arr: Arc<dyn Array> = match n_dims {
            2 => {
                let (dim1, dim2) = embeddings.dims2().unwrap();
                let (storage, layout) = embeddings.storage_and_layout();

                let from_cpu_storage = |cpu_storage: &CpuStorage| {
                    let data = f32::cpu_storage_as_slice(cpu_storage).unwrap();
                    let values_builder = Float32Builder::new();
                    let mut builder =
                        FixedSizeListBuilder::new(values_builder, self.output_dim as i32);

                    let arr = match layout.contiguous_offsets() {
                        Some((o1, o2)) => {
                            let data = &data[o1..o2];
                            for idx_row in 0..dim1 {
                                builder
                                    .values()
                                    .append_slice(&data[idx_row * dim2..(idx_row + 1) * dim2]);
                                builder.append(true);
                            }
                            builder.finish()
                        }
                        None => {
                            let mut src_index = embeddings.strided_index();
                            for _idx_row in 0..dim1 {
                                let row = (0..dim2)
                                    .map(|_| data[src_index.next().unwrap()])
                                    .collect::<Vec<_>>();
                                builder.values().append_slice(&row);
                                builder.append(true);
                            }
                            builder.finish()
                        }
                    };

                    arr
                };
                match &*storage {
                    Storage::Cpu(storage) => Arc::new(from_cpu_storage(storage)),
                    _ => unreachable!("Only CPU storage supported"),
                }
            }
            n_dims => todo!("Only 2 dimensions supported, got {}", n_dims),
        };

        Ok(ColumnarValue::Array(arr))
    }
}

impl BuiltinScalarUDF for BertUdf {
    fn try_as_expr(
        &self,
        _: &catalog::session_catalog::SessionCatalog,
        args: Vec<datafusion::prelude::Expr>,
    ) -> datafusion::error::Result<datafusion::prelude::Expr> {
        let this = self.clone();
        let out_dim = self.output_dim;
        let scalar_f: ScalarFunctionImplementation = Arc::new(move |args| this.invoke(args));


        let return_type_fn: ReturnTypeFunction = Arc::new(move |_| {
            let f = Field::new("item", DataType::Float32, true);
            let dtype = DataType::FixedSizeList(Arc::new(f), out_dim as i32);
            Ok(Arc::new(dtype))
        });

        let udf = ScalarUDF::new(Self::NAME, &self.signature, &return_type_fn, &scalar_f);

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn namespace(&self) -> FunctionNamespace {
        FunctionNamespace::Required("sentence_transformers")
    }
}
