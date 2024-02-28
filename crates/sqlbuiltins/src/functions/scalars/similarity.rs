use std::borrow::Cow;
use std::sync::Arc;

use arrow_cast::{cast_with_options, CastOptions};
use datafusion::arrow::array::{
    make_array,
    Array,
    BooleanBufferBuilder,
    FixedSizeListArray,
    GenericListArray,
    ListArray,
    MutableArrayData,
    OffsetSizeTrait,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
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
        // TODO: see https://github.com/apache/arrow-datafusion/issues/9139.
        // This should be ( FixedSizeList | List) [DataType::Float64 | DataType::Float16 | DataType::Float32]
        let sig = Signature::any(2, Volatility::Immutable);
        Some(sig)
    }
}

fn arr_to_query_vec(
    arr: &dyn Array,
    to_type: &DataType,
) -> datafusion::error::Result<Arc<dyn Array>> {
    Ok(match arr.data_type() {
        dtype @ DataType::List(fld) => match fld.data_type() {
            DataType::Float64 | DataType::Float16 | DataType::Float32 => {
                let arr = arr.as_any().downcast_ref::<ListArray>().unwrap().value(0);
                arrow_cast::cast(&arr, to_type)?
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported data type for cosine_similarity query vector: {:?}",
                    dtype
                )))
            }
        },
        dtype @ DataType::FixedSizeList(fld, _) => match fld.data_type() {
            DataType::Float64 | DataType::Float16 | DataType::Float32 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(0);
                arrow_cast::cast(&arr, to_type)?
            }
            _ => {
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
        DataType::FixedSizeList(fld, size) => match fld.data_type() {
            DataType::Float64 => {
                Cow::Borrowed(arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap())
            }
            DataType::Float16 | DataType::Float32 => {
                let to_type = Arc::new(Field::new("item", DataType::Float64, false));
                let arr = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

                let target_vec = cast_fsl_inner(arr, &to_type, *size, &Default::default())
                    .map_err(|e| DataFusionError::Execution(e.to_string()));


                Cow::Owned(target_vec?)
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
                let to_cast = arr.as_any().downcast_ref::<ListArray>().unwrap();

                let fsl_len = to_cast.value(0).len();

                let to_type = Arc::new(Field::new("item", DataType::Float64, false));

                let target_vec = cast_list_to_fixed_size_list(
                    to_cast,
                    &to_type,
                    fsl_len as i32,
                    &Default::default(),
                )
                .map_err(|e| DataFusionError::Execution(e.to_string()));

                Cow::Owned(target_vec?)
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

            let v0 = target_vec.value(0);
            let to_type = v0.data_type();

            let query_vec = match &args[0] {
                ColumnarValue::Array(arr) => arr_to_query_vec(arr, to_type),
                ColumnarValue::Scalar(ScalarValue::List(arr)) => {
                    arr_to_query_vec(arr.as_ref(), to_type)
                }
                ColumnarValue::Scalar(ScalarValue::FixedSizeList(arr)) => {
                    arr_to_query_vec(arr.as_ref(), to_type)
                }
                _ => {
                    return Err(DataFusionError::Execution(
                        "Invalid argument type".to_string(),
                    ))
                }
            }?;


            let dimension = target_vec.value_length() as usize;
            if query_vec.len() != dimension {
                return Err(DataFusionError::Execution(
                    "Query vector and target vector must have the same length".to_string(),
                ));
            }

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

/// modified/copied from arrow_cast
/// https://github.com/apache/arrow-rs/blob/865a9d3fe81587ad85e9b4c9577948701f7cb3d9/arrow-cast/src/cast.rs#L3229
/// modified to return FixedSizeListArray instead of ArrayRef
fn cast_list_to_fixed_size_list<OffsetSize>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
    size: i32,
    cast_options: &CastOptions,
) -> Result<FixedSizeListArray, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let cap = array.len() * size as usize;

    let mut nulls = (cast_options.safe || array.null_count() != 0).then(|| {
        let mut buffer = BooleanBufferBuilder::new(array.len());
        match array.nulls() {
            Some(n) => buffer.append_buffer(n.inner()),
            None => buffer.append_n(array.len(), true),
        }
        buffer
    });

    // Nulls in FixedSizeListArray take up space and so we must pad the values
    let values = array.values().to_data();
    let mut mutable = MutableArrayData::new(vec![&values], cast_options.safe, cap);
    // The end position in values of the last incorrectly-sized list slice
    let mut last_pos = 0;
    for (idx, w) in array.offsets().windows(2).enumerate() {
        let start_pos = w[0].as_usize();
        let end_pos = w[1].as_usize();
        let len = end_pos - start_pos;

        if len != size as usize {
            if cast_options.safe || array.is_null(idx) {
                if last_pos != start_pos {
                    // Extend with valid slices
                    mutable.extend(0, last_pos, start_pos);
                }
                // Pad this slice with nulls
                mutable.extend_nulls(size as _);
                nulls.as_mut().unwrap().set_bit(idx, false);
                // Set last_pos to the end of this slice's values
                last_pos = end_pos
            } else {
                return Err(ArrowError::CastError(format!(
                    "Cannot cast to FixedSizeList({size}): value at index {idx} has length {len}",
                )));
            }
        }
    }

    let values = match last_pos {
        0 => array.values().slice(0, cap), // All slices were the correct length
        _ => {
            if mutable.len() != cap {
                // Remaining slices were all correct length
                let remaining = cap - mutable.len();
                mutable.extend(0, last_pos, last_pos + remaining)
            }
            make_array(mutable.freeze())
        }
    };

    // Cast the inner values if necessary
    let values = cast_with_options(values.as_ref(), field.data_type(), cast_options)?;

    // Construct the FixedSizeListArray
    let nulls = nulls.map(|mut x| x.finish().into());
    let array = FixedSizeListArray::new(field.clone(), size, values, nulls);
    Ok(array)
}


// modified copy from arrow_cast

/// modified/copied from arrow_cast
/// https://github.com/apache/arrow-rs/blob/865a9d3fe81587ad85e9b4c9577948701f7cb3d9/arrow-cast/src/cast.rs#L3229
/// modified to take in FixedSizeListArray instead of GenericListArray
/// and return FixedSizeListArray instead of ArrayRef
fn cast_fsl_inner(
    array: &FixedSizeListArray,
    field: &FieldRef,
    size: i32,
    cast_options: &CastOptions,
) -> Result<FixedSizeListArray, ArrowError> {
    let nulls = (cast_options.safe || array.null_count() != 0).then(|| {
        let mut buffer = BooleanBufferBuilder::new(array.len());
        match array.nulls() {
            Some(n) => buffer.append_buffer(n.inner()),
            None => buffer.append_n(array.len(), true),
        }
        buffer
    });

    // Nulls in FixedSizeListArray take up space and so we must pad the values
    let values = array.values();
    let values = cast_with_options(values.as_ref(), field.data_type(), cast_options)?;


    // Construct the FixedSizeListArray
    let nulls = nulls.map(|mut x| x.finish().into());
    let array = FixedSizeListArray::new(field.clone(), size, values, nulls);
    Ok(array)
}
