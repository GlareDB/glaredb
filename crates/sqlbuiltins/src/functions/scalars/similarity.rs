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

use crate::functions::{BuiltinScalarUDF, ConstBuiltinFunction, FunctionNamespace};

pub struct CosineSimilarity;

impl ConstBuiltinFunction for CosineSimilarity {
    const NAME: &'static str = "cosine_similarity";
    const DESCRIPTION: &'static str = "Returns the cosine similarity between two vectors";
    const EXAMPLE: &'static str = "cosine_similarity([1, 2, 3], [4, 5, 6])";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::any(2, Volatility::Stable))
    }
}

impl BuiltinScalarUDF for CosineSimilarity {
    fn try_as_expr(
        &self,
        _: &catalog::session_catalog::SessionCatalog,
        args: Vec<Expr>,
    ) -> datafusion::error::Result<Expr> {
        let scalar_f: ScalarFunctionImplementation = Arc::new(move |args| {
            let query_vec = match &args[0] {
                ColumnarValue::Array(arr) => arr,
                ColumnarValue::Scalar(ScalarValue::List(arr)) => arr,
                ColumnarValue::Scalar(ScalarValue::FixedSizeList(arr)) => arr,
                _ => todo!(),
            };
            let target_vec = match &args[1] {
                ColumnarValue::Array(arr) => arr,
                ColumnarValue::Scalar(ScalarValue::List(arr)) => arr,
                ColumnarValue::Scalar(ScalarValue::FixedSizeList(arr)) => arr,
                _ => todo!(),
            };

            let query_vec = match query_vec.data_type() {
                DataType::FixedSizeList(fld, _) | DataType::List(fld) => match fld.data_type() {
                    DataType::Float64 | DataType::Float16 | DataType::Float32 => query_vec
                        .as_any()
                        .downcast_ref::<ListArray>()
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
            };

            let target_vec = match target_vec.data_type() {
                DataType::FixedSizeList(fld, _) => match fld.data_type() {
                    DataType::Float64 | DataType::Float16 | DataType::Float32 => Cow::Borrowed(
                        target_vec
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .unwrap(),
                    ),

                    dtype => {
                        return Err(DataFusionError::Execution(format!(
                            "Unsupported data type for cosine_similarity target vector: {:?}",
                            dtype
                        )))
                    }
                },
                DataType::List(fld) => match fld.data_type() {
                    DataType::Float64 | DataType::Float16 | DataType::Float32 => {
                        let tv = target_vec.as_any().downcast_ref::<ListArray>().unwrap();
                        // arrow_cast for some reason doesnt support list -> fixed size list
                        let values = tv.values().clone();
                        let nulls = tv.nulls().map(|x| x.to_owned());
                        let len = tv.len() as i32;
                        let fld = fld.clone();
                        let fsl = FixedSizeListArray::new(fld, len, values, nulls);
                        Cow::Owned(fsl)
                    }

                    dtype => {
                        return Err(DataFusionError::Execution(format!(
                            "Unsupported data type for cosine_similarity target vector: {:?}",
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
            };

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
