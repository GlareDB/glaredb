use std::borrow::Borrow;

use half::f16;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{
    ArrayBuilder,
    ArrayDataBuffer,
    BooleanBuffer,
    GermanVarlenBuffer,
    PrimitiveBuffer,
};
use rayexec_bullet::executor::physical_type::{
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalList,
    PhysicalStorage,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::storage::AddressableStorage;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
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
            input: &[DataTypeId::List, DataTypeId::Int64],
            variadic: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl ScalarFunction for ListExtract {
    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let mut packed = PackedDecoder::new(state);
        let datatype = DataType::from_proto(packed.decode_next()?)?;
        let index: u64 = packed.decode_next()?;
        Ok(Box::new(ListExtractImpl {
            inner_datatype: datatype,
            index: index as usize,
        }))
    }

    fn plan_from_expressions(
        &self,
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        plan_check_num_args(self, &datatypes, 2)?;

        let index = ConstFold::rewrite(bind_context, inputs[1].clone())?
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

        Ok(Box::new(ListExtractImpl {
            inner_datatype,
            index,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListExtractImpl {
    inner_datatype: DataType,
    index: usize,
}

impl PlannedScalarFunction for ListExtractImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &ListExtract
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(state);
        packed.encode_next(&self.inner_datatype.to_proto()?)?;
        packed.encode_next(&(self.index as u64))?;
        Ok(())
    }

    fn return_type(&self) -> DataType {
        self.inner_datatype.clone()
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        extract(input, self.index)
    }
}

fn extract(array: &Array, idx: usize) -> Result<Array> {
    let data = match array.array_data() {
        ArrayData::List(list) => list.as_ref(),
        _other => return Err(RayexecError::new("Unexpected storage type")),
    };

    match data.inner_array().physical_type() {
        PhysicalType::UntypedNull => not_implemented!("NULL list extract"),
        PhysicalType::Boolean => {
            let builder = ArrayBuilder {
                datatype: DataType::Boolean,
                buffer: BooleanBuffer::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalBool, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Int8 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int8,
                buffer: PrimitiveBuffer::<i8>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI8, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Int16 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int16,
                buffer: PrimitiveBuffer::<i16>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI16, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Int32 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int32,
                buffer: PrimitiveBuffer::<i32>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI32, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Int64 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int64,
                buffer: PrimitiveBuffer::<i64>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI64, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Int128 => {
            let builder = ArrayBuilder {
                datatype: DataType::Int128,
                buffer: PrimitiveBuffer::<i128>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalI128, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::UInt8 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt8,
                buffer: PrimitiveBuffer::<u8>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU8, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::UInt16 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt16,
                buffer: PrimitiveBuffer::<u16>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU16, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::UInt32 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt32,
                buffer: PrimitiveBuffer::<u32>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU32, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::UInt64 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt64,
                buffer: PrimitiveBuffer::<u64>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU64, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::UInt128 => {
            let builder = ArrayBuilder {
                datatype: DataType::UInt128,
                buffer: PrimitiveBuffer::<u128>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalU128, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Float16 => {
            let builder = ArrayBuilder {
                datatype: DataType::Float16,
                buffer: PrimitiveBuffer::<f16>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalF16, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Float32 => {
            let builder = ArrayBuilder {
                datatype: DataType::Float32,
                buffer: PrimitiveBuffer::<f32>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalF32, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Float64 => {
            let builder = ArrayBuilder {
                datatype: DataType::Float64,
                buffer: PrimitiveBuffer::<f64>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalF64, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Utf8 => {
            let builder = ArrayBuilder {
                datatype: DataType::Utf8,
                buffer: GermanVarlenBuffer::<str>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalUtf8, _>(builder, array, data.inner_array(), idx)
        }
        PhysicalType::Binary => {
            let builder = ArrayBuilder {
                datatype: DataType::Binary,
                buffer: GermanVarlenBuffer::<[u8]>::with_len(array.logical_len()),
            };
            extract_inner::<PhysicalBinary, _>(builder, array, data.inner_array(), idx)
        }
        other => not_implemented!("List extract for physical type {other:?}"),
    }
}

fn extract_inner<'a, S, B>(
    mut builder: ArrayBuilder<B>,
    outer: &Array,
    inner: &'a Array,
    el_idx: usize,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    B: ArrayDataBuffer,
    S::Type: Borrow<<B as ArrayDataBuffer>::Type>,
{
    let el_idx = el_idx as i32;

    let mut validity = Bitmap::new_with_all_true(builder.buffer.len());

    UnaryExecutor::for_each::<PhysicalList, _>(outer, |idx, metadata| {
        if let Some(metadata) = metadata {
            if el_idx >= metadata.len {
                // Indexing outside of the list. Mark null
                validity.set_unchecked(idx, false);
                return;
            }

            // Otherwise put the element into the builder.
            let inner_el_idx = metadata.offset + el_idx;
            match UnaryExecutor::value_at::<S>(inner, inner_el_idx as usize) {
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

    Ok(Array::new_with_validity_and_array_data(
        builder.datatype,
        validity,
        builder.buffer.into_data(),
    ))
}
