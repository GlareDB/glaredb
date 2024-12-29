use std::borrow::Borrow;

use half::f16;
use rayexec_error::{not_implemented, RayexecError, Result};
use serde::{Deserialize, Serialize};

use crate::arrays::array::{Array2, ArrayData2};
use crate::arrays::bitmap::Bitmap;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{
    ArrayBuilder,
    ArrayDataBuffer,
    BooleanBuffer,
    GermanVarlenBuffer,
    PrimitiveBuffer,
};
use crate::arrays::executor::physical_type::{
    PhysicalBinary_2,
    PhysicalBool_2,
    PhysicalF16_2,
    PhysicalF32_2,
    PhysicalF64_2,
    PhysicalI128_2,
    PhysicalI16_2,
    PhysicalI32_2,
    PhysicalI64_2,
    PhysicalI8_2,
    PhysicalList_2,
    PhysicalStorage2,
    PhysicalType2,
    PhysicalU128_2,
    PhysicalU16_2,
    PhysicalU32_2,
    PhysicalU64_2,
    PhysicalU8_2,
    PhysicalUtf8_2,
};
use crate::arrays::executor::scalar::UnaryExecutor2;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListExtract;

impl FunctionInfo for ListExtract {
    fn name(&self) -> &'static str {
        "list_extract"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::List, DataTypeId::Int64],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: Some(&Documentation {
                category: Category::List,
                description: "Extract an item from the list. Used 1-based indexing.",
                arguments: &["list", "index"],
                example: Some(Example {
                    example: "list_extract([4,5,6], 2)",
                    output: "5",
                }),
            }),
        }]
    }
}

impl ScalarFunction for ListExtract {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        plan_check_num_args(self, &datatypes, 2)?;

        let index = ConstFold::rewrite(table_list, inputs[1].clone())?
            .try_into_scalar()?
            .try_as_i64()?;

        if index <= 0 {
            return Err(RayexecError::new("Index cannot be less than 1"));
        }
        let index = (index - 1) as usize;

        let inner_datatype = match &datatypes[0] {
            DataType::List(meta) => meta.datatype.as_ref().clone(),
            other => {
                return Err(RayexecError::new(format!(
                    "Cannot index into non-list type, got {other}",
                )))
            }
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: inner_datatype.clone(),
            inputs,
            function_impl: Box::new(ListExtractImpl {
                index,
                inner_datatype,
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListExtractImpl {
    inner_datatype: DataType,
    index: usize,
}

impl ScalarFunctionImpl for ListExtractImpl {
    fn execute2(&self, inputs: &[&Array2]) -> Result<Array2> {
        let input = inputs[0];
        extract(input, self.index)
    }
}

fn extract(array: &Array2, idx: usize) -> Result<Array2> {
    let data = match array.array_data() {
        ArrayData2::List(list) => list.as_ref(),
        _other => return Err(RayexecError::new("Unexpected storage type")),
    };

    match data.inner_array().physical_type() {
        PhysicalType2::UntypedNull => not_implemented!("NULL list extract"),
        PhysicalType2::Boolean => {
            let builder = ArrayBuilder {
                datatype: DataType::Boolean,
                buffer: BooleanBuffer::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalBool_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Int8 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int8,
                buffer: PrimitiveBuffer::<i8>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI8_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Int16 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int16,
                buffer: PrimitiveBuffer::<i16>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI16_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Int32 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int32,
                buffer: PrimitiveBuffer::<i32>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI32_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Int64 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int64,
                buffer: PrimitiveBuffer::<i64>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI64_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Int128 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int128,
                buffer: PrimitiveBuffer::<i128>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI128_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::UInt8 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt8,
                buffer: PrimitiveBuffer::<u8>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU8_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::UInt16 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt16,
                buffer: PrimitiveBuffer::<u16>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU16_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::UInt32 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt32,
                buffer: PrimitiveBuffer::<u32>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU32_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::UInt64 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt64,
                buffer: PrimitiveBuffer::<u64>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU64_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::UInt128 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt128,
                buffer: PrimitiveBuffer::<u128>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU128_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Float16 => {
            let builder = ArrayBuilder {
                datatype: DataType::Float16,
                buffer: PrimitiveBuffer::<f16>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalF16_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Float32 => {
            let builder = ArrayBuilder {
                datatype: DataType::Float32,
                buffer: PrimitiveBuffer::<f32>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalF32_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Float64 => {
            let builder = ArrayBuilder {
                datatype: DataType::Float64,
                buffer: PrimitiveBuffer::<f64>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalF64_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Utf8 => {
            let builder = ArrayBuilder {
                datatype: DataType::Utf8,
                buffer: GermanVarlenBuffer::<str>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalUtf8_2, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType2::Binary => {
            let builder = ArrayBuilder {
                datatype: DataType::Binary,
                buffer: GermanVarlenBuffer::<[u8]>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalBinary_2, _>(builder, array, data.inner_array(), idx)
        }
        other => not_implemented!("List extract for physical type {other:?}"),
    }
}

fn extract_inner<'a, S, B>(
    mut builder: ArrayBuilder<B>,
    outer: &Array2,
    inner: &'a Array2,
    el_idx: usize,
) -> Result<Array2>
where
    S: PhysicalStorage2,
    B: ArrayDataBuffer,
    S::Type<'a>: Borrow<<B as ArrayDataBuffer>::Type>,
{
    let el_idx = el_idx as i32;

    let mut validity = Bitmap::new_with_all_true(builder.buffer.len());

    UnaryExecutor2::for_each::<PhysicalList_2, _>(outer, |idx, metadata| {
        if let Some(metadata) = metadata {
            if el_idx >= metadata.len {
                // Indexing outside of the list. Mark null
                validity.set_unchecked(idx, false);
                return;
            }

            // Otherwise put the element into the builder.
            let inner_el_idx = metadata.offset + el_idx;
            match UnaryExecutor2::value_at::<S>(inner, inner_el_idx as usize) {
                Ok(Some(el)) => {
                    builder.buffer.put(idx, el.borrow());
                    return;
                }
                _ => {
                    // TODO: Do something if Err, just fall through right now.
                }
            }
        }

        // Metadata null, tried to extract from null array, mark null.
        validity.set_unchecked(idx, false);
    })?;

    Ok(Array2::new_with_validity_and_array_data(
        builder.datatype,
        validity,
        builder.buffer.into_data(),
    ))
}
