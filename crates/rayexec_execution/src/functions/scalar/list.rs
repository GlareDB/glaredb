use std::{ops::Deref, sync::Arc};

use rayexec_bullet::{
    array::{
        Array, ListArray, OffsetIndex, PrimitiveArray, ValuesBuffer, VarlenArray, VarlenType,
        VarlenValuesBuffer,
    },
    bitmap::Bitmap,
    datatype::{DataType, DataTypeId, ListTypeMeta},
};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::{
    expr::Expression,
    functions::{plan_check_num_args, FunctionInfo, Signature},
    logical::{binder::bind_context::BindContext, consteval::ConstEval},
};

use super::{PlannedScalarFunction, ScalarFunction};

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
            datatype,
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

        let index = ConstEval::default()
            .fold(inputs[1].clone())?
            .try_unwrap_constant()?
            .try_as_i64()?;

        if index <= 0 {
            return Err(RayexecError::new("Index cannot be less than 1"));
        }
        let index = (index - 1) as usize;

        let inner_datatype = match &datatypes[0] {
            DataType::List(meta) => meta.datatype.deref().clone(),
            other => {
                return Err(RayexecError::new(format!(
                    "Cannot index into non-list type, got {other}",
                )))
            }
        };

        Ok(Box::new(ListExtractImpl {
            datatype: inner_datatype,
            index,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListExtractImpl {
    datatype: DataType,
    index: usize,
}

impl PlannedScalarFunction for ListExtractImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &ListExtract
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(state);
        packed.encode_next(&self.datatype.to_proto()?)?;
        packed.encode_next(&(self.index as u64))?;
        Ok(())
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        let list = match &inputs[0].as_ref() {
            Array::List(list) => list,
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected array type: {}",
                    other.datatype()
                )))
            }
        };

        let offsets = list.offsets();
        let validity = list.validity();

        Ok(match list.child_array().as_ref() {
            Array::Int8(arr) => {
                Array::Int8(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::Int16(arr) => {
                Array::Int16(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::Int32(arr) => {
                Array::Int32(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::Int64(arr) => {
                Array::Int64(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::Int128(arr) => {
                Array::Int128(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::UInt8(arr) => {
                Array::UInt8(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::UInt16(arr) => {
                Array::UInt16(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::UInt32(arr) => {
                Array::UInt32(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::UInt64(arr) => {
                Array::UInt64(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::UInt128(arr) => {
                Array::UInt128(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::Float32(arr) => {
                Array::Float32(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::Float64(arr) => {
                Array::Float64(list_extract_primitive(arr, offsets, validity, self.index)?)
            }
            Array::Utf8(arr) => {
                Array::Utf8(list_extract_varlen(arr, offsets, validity, self.index)?)
            }
            Array::LargeUtf8(arr) => {
                Array::LargeUtf8(list_extract_varlen(arr, offsets, validity, self.index)?)
            }
            other => not_implemented!("list extract {}", other.datatype()),
        })
    }
}

fn list_extract_primitive<T, O>(
    array: &PrimitiveArray<T>,
    offsets: &[O],
    validity: Option<&Bitmap>,
    idx: usize,
) -> Result<PrimitiveArray<T>>
where
    T: Copy + Default,
    O: OffsetIndex,
{
    let mut result_validity = Bitmap::with_capacity(offsets.len() - 1);
    let mut values = Vec::with_capacity(offsets.len() - 1);

    for row_idx in 0..(offsets.len() - 1) {
        let offset = offsets[row_idx].as_usize();
        let value_offset = offset + idx;
        if value_offset >= offsets[row_idx + 1].as_usize() {
            result_validity.push(false);
            values.push(T::default());
        } else {
            result_validity.push(validity.map(|v| v.value(value_offset)).unwrap_or(true));
            values.push(*array.value(value_offset).expect("value to exist"));
        }
    }

    Ok(PrimitiveArray::new(values, Some(result_validity)))
}

fn list_extract_varlen<T, O1, O2>(
    array: &VarlenArray<T, O1>,
    offsets: &[O2],
    validity: Option<&Bitmap>,
    idx: usize,
) -> Result<VarlenArray<T, O1>>
where
    T: VarlenType + ?Sized,
    O1: OffsetIndex,
    O2: OffsetIndex,
{
    let mut result_validity = Bitmap::with_capacity(offsets.len() - 1);
    let mut values = VarlenValuesBuffer::default();

    for row_idx in 0..(offsets.len() - 1) {
        let offset = offsets[row_idx].as_usize();
        let value_offset = offset + idx;
        if value_offset >= offsets[row_idx + 1].as_usize() {
            result_validity.push(false);
            values.push_value(T::NULL);
        } else {
            result_validity.push(validity.map(|v| v.value(value_offset)).unwrap_or(true));
            values.push_value(array.value(value_offset).expect("value to exist"))
        }
    }

    Ok(VarlenArray::new(values, Some(result_validity)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListValues;

impl FunctionInfo for ListValues {
    fn name(&self) -> &'static str {
        "list_values"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
        }]
    }
}

impl ScalarFunction for ListValues {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(ListValuesImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        let first = match inputs.first() {
            Some(dt) => dt,
            None => {
                return Ok(Box::new(ListValuesImpl {
                    datatype: DataType::Null,
                }))
            }
        };

        for dt in inputs {
            if dt != first {
                return Err(RayexecError::new(format!(
                    "Not all inputs are the same type, got {dt}, expected {first}"
                )));
            }
        }

        Ok(Box::new(ListValuesImpl {
            datatype: DataType::List(ListTypeMeta {
                datatype: Box::new(first.clone()),
            }),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListValuesImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for ListValuesImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &ListValues
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        let refs: Vec<_> = inputs.iter().map(|a| a.as_ref()).collect();
        let array = if refs.is_empty() {
            ListArray::new_empty_with_n_rows(1)
        } else {
            ListArray::try_from_children(&refs)?
        };

        Ok(Array::List(array))
    }
}
