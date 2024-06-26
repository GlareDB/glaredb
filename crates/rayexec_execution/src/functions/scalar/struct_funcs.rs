use super::{GenericScalarFunction, SpecializedScalarFunction};
use crate::functions::{FunctionInfo, Signature};
use rayexec_bullet::array::Array;
use rayexec_bullet::array::StructArray;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::scalar::ScalarValue;
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructPack;

impl FunctionInfo for StructPack {
    fn name(&self) -> &'static str {
        "struct_pack"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Struct],
            return_type: DataTypeId::Struct,
        }]
    }

    fn return_type_for_inputs(&self, _inputs: &[DataType]) -> Option<DataType> {
        // TODO: Check "key" types.

        unimplemented!()
    }
}

impl GenericScalarFunction for StructPack {
    fn specialize(&self, _inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        Ok(Box::new(StructPackDynamic))
    }
}

/// Creates a struct array from some input arrays.
///
/// Key and values arrays are alternating.
///
/// It's assumed key arrays are string arrays containing all of the same value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructPackDynamic;

impl SpecializedScalarFunction for StructPackDynamic {
    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructExtract;

impl FunctionInfo for StructExtract {
    fn name(&self) -> &'static str {
        "struct_extract"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Struct],
            return_type: DataTypeId::Any,
        }]
    }

    fn return_type_for_inputs(&self, _inputs: &[DataType]) -> Option<DataType> {
        unimplemented!()
    }
}

impl GenericScalarFunction for StructExtract {
    fn specialize(&self, _inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        unimplemented!()
    }
}
