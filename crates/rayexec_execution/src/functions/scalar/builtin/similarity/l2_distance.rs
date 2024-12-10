use std::ops::AddAssign;

use num_traits::{AsPrimitive, Float};
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalType,
};
use rayexec_bullet::executor::scalar::{BinaryListReducer, ListExecutor};
use rayexec_error::{RayexecError, Result};

use crate::functions::scalar::{PlannedScalarFunction2, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

/// Euclidean distance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Distance;

impl FunctionInfo for L2Distance {
    fn name(&self) -> &'static str {
        "l2_distance"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["array_distance"]
    }

    fn signatures(&self) -> &[Signature] {
        // TODO: Ideally return type would depend on the primitive type in the
        // list.
        &[Signature {
            input: &[DataTypeId::List, DataTypeId::List],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl ScalarFunction for L2Distance {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(L2DistanceImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::List(a), DataType::List(b)) => {
                match (a.datatype.as_ref(), b.datatype.as_ref()) {
                    (DataType::Float16, DataType::Float16)
                    | (DataType::Float32, DataType::Float32)
                    | (DataType::Float64, DataType::Float64) => Ok(Box::new(L2DistanceImpl)),
                    (a, b) => Err(invalid_input_types_error(self, &[a, b])),
                }
            }
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2DistanceImpl;

impl PlannedScalarFunction2 for L2DistanceImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &L2Distance
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let a = inputs[0];
        let b = inputs[1];

        let physical_type = match a.array_data() {
            ArrayData::List(l) => l.inner_array().physical_type(),
            _other => return Err(RayexecError::new("Unexpected storage type")),
        };

        let builder = ArrayBuilder {
            datatype: DataType::Float64,
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        match physical_type {
            PhysicalType::Float16 => {
                ListExecutor::execute_binary_reduce::<PhysicalF16, _, L2DistanceReducer<_>>(
                    a, b, builder,
                )
            }
            PhysicalType::Float32 => {
                ListExecutor::execute_binary_reduce::<PhysicalF32, _, L2DistanceReducer<_>>(
                    a, b, builder,
                )
            }
            PhysicalType::Float64 => {
                ListExecutor::execute_binary_reduce::<PhysicalF64, _, L2DistanceReducer<_>>(
                    a, b, builder,
                )
            }
            other => panic!("unexpected physical type: {other:?}"),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct L2DistanceReducer<F> {
    pub distance: F,
}

impl<F> BinaryListReducer<F, f64> for L2DistanceReducer<F>
where
    F: Float + AddAssign + AsPrimitive<f64> + Default,
{
    fn put_values(&mut self, v1: F, v2: F) {
        let diff = v1 - v2;
        self.distance += diff * diff;
    }

    fn finish(self) -> f64 {
        self.distance.as_().sqrt()
    }
}
