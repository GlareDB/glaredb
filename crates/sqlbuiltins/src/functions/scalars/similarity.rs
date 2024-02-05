use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{Array, FixedSizeListArray, ListArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr,
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    Signature,
    Volatility,
};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use protogen::metastore::types::catalog::FunctionType;

use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction};

pub struct CosineSimilarity;

impl ConstBuiltinFunction for CosineSimilarity {
    const NAME: &'static str = "cosine_similarity";
    const DESCRIPTION: &'static str =
        "Returns the cosine similarity between two floating point vectors";
    const EXAMPLE: &'static str = "cosine_similarity([1.0, 2.0, 3.0], [4.0, 5.0, 6.0])";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        let sig = Signature::any(2, Volatility::Immutable);
        Some(sig)
    }
}

fn arr_to_query_vec(arr: &dyn Array) -> datafusion::error::Result<Arc<dyn Array>> {
    Ok(match arr.data_type() {
        DataType::List(fld) => match fld.data_type() {
            DataType::Float64 | DataType::Float16 | DataType::Float32 => {
                arr.as_any().downcast_ref::<ListArray>().unwrap().value(0)
            }
            dtype => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported data type for cosine_similarity query vector: {:?}",
                    dtype
                )))
            }
        },
        DataType::FixedSizeList(fld, _) => match fld.data_type() {
            DataType::Float64 | DataType::Float16 | DataType::Float32 => arr
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap()
                .value(0),
            dtype => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported data type for cosine_similarity query vector: {:?}",
                    dtype
                )))
            }
        },
        dtype => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported data type for cosine_similarity query vector: {:?}",
                dtype
            )))
        }
    })
}

fn arr_to_target_vec(arr: &dyn Array) -> datafusion::error::Result<Cow<FixedSizeListArray>> {
    Ok(match arr.data_type() {
        DataType::FixedSizeList(fld, _) => match fld.data_type() {
            DataType::Float64 | DataType::Float16 | DataType::Float32 => {
                Cow::Borrowed(arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap())
            }

            dtype => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported data type for cosine_similarity target vector: {:?}",
                    dtype
                )))
            }
        },
        DataType::List(fld) => match fld.data_type() {
            DataType::Float64 | DataType::Float16 | DataType::Float32 => {
                let tv = arr.as_any().downcast_ref::<ListArray>().unwrap();
                // arrow_cast for some reason doesnt support list -> fixed size list so we just do it manually
                let values = tv.values().clone();
                let nulls = tv.nulls().map(|x| x.to_owned());
                let len = tv.len() as i32;
                let fld = fld.clone();
                let fsl = FixedSizeListArray::new(fld, len, values, nulls);
                Cow::Owned(fsl)
            }

            dtype => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported data type for cosine_similarity target vector inner type: {:?}",
                    dtype
                )))
            }
        },
        dtype => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported data type for cosine_similarity: {:?}",
                dtype
            )))
        }
    })
}


impl BuiltinScalarUDF for CosineSimilarity {
    fn try_as_expr(
        &self,
        _: &catalog::session_catalog::SessionCatalog,
        args: Vec<Expr>,
    ) -> datafusion::error::Result<Expr> {
        let scalar_f: ScalarFunctionImplementation = Arc::new(move |args| {
            let query_vec = match &args[0] {
                ColumnarValue::Array(arr) => arr_to_query_vec(arr),
                ColumnarValue::Scalar(ScalarValue::List(arr)) => arr_to_query_vec(arr.as_ref()),
                ColumnarValue::Scalar(ScalarValue::FixedSizeList(arr)) => {
                    arr_to_query_vec(arr.as_ref())
                }
                _ => {
                    return Err(DataFusionError::Execution(
                        "Invalid argument type".to_string(),
                    ))
                }
            }?;
            let target_vec = match &args[1] {
                ColumnarValue::Array(arr) => arr_to_target_vec(arr),
                ColumnarValue::Scalar(ScalarValue::List(arr)) => arr_to_target_vec(arr.as_ref()),
                ColumnarValue::Scalar(ScalarValue::FixedSizeList(arr)) => {
                    arr_to_target_vec(arr.as_ref())
                }
                _ => {
                    return Err(DataFusionError::Execution(
                        "Invalid argument type".to_string(),
                    ))
                }
            }?;
            let result: Arc<dyn Array> = lance_linalg::distance::cosine_distance_arrow_batch(
                query_vec.as_ref(),
                &target_vec,
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            Ok(ColumnarValue::Array(result))
        });

        let return_type_fn: ReturnTypeFunction = Arc::new(move |_| {
            let dtype = DataType::Float32;
            Ok(Arc::new(dtype))
        });

        let udf = ScalarUDF::new(
            Self::NAME,
            &self.signature().unwrap(),
            &return_type_fn,
            &scalar_f,
        );

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }
}
