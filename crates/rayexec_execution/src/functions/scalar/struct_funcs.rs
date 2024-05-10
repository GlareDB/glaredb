use super::{GenericScalarFunction, ScalarFn, SpecializedScalarFunction};
use crate::functions::{InputTypes, ReturnType, Signature};
use rayexec_bullet::array::StructArray;
use rayexec_bullet::scalar::ScalarValue;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructPack;

impl GenericScalarFunction for StructPack {
    fn name(&self) -> &str {
        "struct_pack"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: InputTypes::Dynamic,
            return_type: ReturnType::Dynamic,
        }]
    }

    fn return_type_for_inputs(&self, inputs: &[DataType]) -> Option<DataType> {
        // TODO: Check "key" types.

        let value_types = inputs.iter().skip(1).step_by(2).cloned().collect();
        Some(DataType::Struct {
            fields: value_types,
        })
    }

    fn specialize(&self, _inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        Ok(Box::new(StructPackDynamic))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructPackDynamic;

impl SpecializedScalarFunction for StructPackDynamic {
    fn function_impl(&self) -> ScalarFn {
        struct_pack
    }
}

/// Creates a struct array from some input arrays.
///
/// Key and values arrays are alternating.
///
/// It's assumed key arrays are string arrays containing all of the same value.
fn struct_pack(arrays: &[&Arc<Array>]) -> Result<Array> {
    let keys = arrays
        .iter()
        .step_by(2)
        .map(|arr| match arr.scalar(0).expect("scalar to exist") {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => Ok(v.to_string()),
            other => Err(RayexecError::new(format!(
                "Invalid value for struct key: {other}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;

    let values: Vec<_> = arrays
        .iter()
        .skip(1)
        .step_by(2)
        .map(|&arr| arr.clone())
        .collect();

    Ok(Array::Struct(StructArray::try_new(keys, values)?))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructExtract;

impl GenericScalarFunction for StructExtract {
    fn name(&self) -> &str {
        "struct_extract"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: InputTypes::Dynamic,
            return_type: ReturnType::Dynamic,
        }]
    }

    fn return_type_for_inputs(&self, inputs: &[DataType]) -> Option<DataType> {
        unimplemented!()
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        unimplemented!()
    }
}
