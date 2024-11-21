use num_traits::Float;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalStorage,
    PhysicalType,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;

use super::{check_is_unary_numeric_input, UNARY_NUMERIC_INPUT_OUTPUT_SIGS};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{unhandled_physical_types_err, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Floor;

impl FunctionInfo for Floor {
    fn name(&self) -> &'static str {
        "floor"
    }

    fn signatures(&self) -> &[Signature] {
        UNARY_NUMERIC_INPUT_OUTPUT_SIGS
    }
}

impl ScalarFunction for Floor {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let ret = DataType::from_proto(PackedDecoder::new(state).decode_next()?)?;
        Ok(Box::new(FloorImpl { ret }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        check_is_unary_numeric_input(self, inputs)?;
        Ok(Box::new(FloorImpl {
            ret: inputs[0].clone(),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct FloorImpl {
    ret: DataType,
}

impl PlannedScalarFunction for FloorImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Floor
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.ret.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.ret.clone()
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        match input.physical_type() {
            PhysicalType::Float16 => floor_float_execute::<PhysicalF16>(input, self.ret.clone()),
            PhysicalType::Float32 => floor_float_execute::<PhysicalF32>(input, self.ret.clone()),
            PhysicalType::Float64 => floor_float_execute::<PhysicalF64>(input, self.ret.clone()),
            other => Err(unhandled_physical_types_err(self, [other])),
        }
    }
}

fn floor_float_execute<'a, S>(input: &'a Array, ret: DataType) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    S::Type: Float + Default,
    ArrayData: From<PrimitiveStorage<S::Type>>,
{
    let builder = ArrayBuilder {
        datatype: ret,
        buffer: PrimitiveBuffer::with_len(input.logical_len()),
    };
    UnaryExecutor::execute::<S, _, _>(input, builder, |v, buf| buf.put(&v.floor()))
}
