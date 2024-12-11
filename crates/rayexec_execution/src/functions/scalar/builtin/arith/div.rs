use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::compute::cast::array::cast_decimal_to_float;
use rayexec_bullet::compute::cast::behavior::CastFailBehavior;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalStorage,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFuntion, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Div;

impl FunctionInfo for Div {
    fn name(&self) -> &'static str {
        "/"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["div"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float16, DataTypeId::Float16],
                variadic: None,
                return_type: DataTypeId::Float16,
            },
            Signature {
                input: &[DataTypeId::Float32, DataTypeId::Float32],
                variadic: None,
                return_type: DataTypeId::Float32,
            },
            Signature {
                input: &[DataTypeId::Float64, DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Int8, DataTypeId::Int8],
                variadic: None,
                return_type: DataTypeId::Int8,
            },
            Signature {
                input: &[DataTypeId::Int16, DataTypeId::Int16],
                variadic: None,
                return_type: DataTypeId::Int16,
            },
            Signature {
                input: &[DataTypeId::Int32, DataTypeId::Int32],
                variadic: None,
                return_type: DataTypeId::Int32,
            },
            Signature {
                input: &[DataTypeId::Int64, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
            Signature {
                input: &[DataTypeId::UInt8, DataTypeId::UInt8],
                variadic: None,
                return_type: DataTypeId::UInt8,
            },
            Signature {
                input: &[DataTypeId::UInt16, DataTypeId::UInt16],
                variadic: None,
                return_type: DataTypeId::UInt16,
            },
            Signature {
                input: &[DataTypeId::UInt32, DataTypeId::UInt32],
                variadic: None,
                return_type: DataTypeId::UInt32,
            },
            Signature {
                input: &[DataTypeId::UInt64, DataTypeId::UInt64],
                variadic: None,
                return_type: DataTypeId::UInt64,
            },
            Signature {
                input: &[DataTypeId::Date32, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Date32,
            },
            Signature {
                input: &[DataTypeId::Interval, DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Interval,
            },
            Signature {
                input: &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Decimal128, DataTypeId::Decimal128],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
        ]
    }
}

impl ScalarFunction for Div {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        plan_check_num_args(self, &inputs, 2)?;

        let (function_impl, return_type): (Box<dyn ScalarFunctionImpl>, _) = match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Float16, DataType::Float16) => (
                Box::new(DivImpl::<PhysicalF16>::new(DataType::Float16)),
                DataType::Float16,
            ),
            (DataType::Float32, DataType::Float32) => (
                Box::new(DivImpl::<PhysicalF32>::new(DataType::Float32)),
                DataType::Float32,
            ),
            (DataType::Float64, DataType::Float64) => (
                Box::new(DivImpl::<PhysicalF64>::new(DataType::Float64)),
                DataType::Float64,
            ),
            (DataType::Int8, DataType::Int8) => (
                Box::new(DivImpl::<PhysicalI8>::new(DataType::Int8)),
                DataType::Int8,
            ),
            (DataType::Int16, DataType::Int16) => (
                Box::new(DivImpl::<PhysicalI16>::new(DataType::Int16)),
                DataType::Int16,
            ),
            (DataType::Int32, DataType::Int32) => (
                Box::new(DivImpl::<PhysicalI32>::new(DataType::Int32)),
                DataType::Int32,
            ),
            (DataType::Int64, DataType::Int64) => (
                Box::new(DivImpl::<PhysicalI64>::new(DataType::Int64)),
                DataType::Int64,
            ),
            (DataType::Int128, DataType::Int128) => (
                Box::new(DivImpl::<PhysicalI128>::new(DataType::Int128)),
                DataType::Int128,
            ),
            (DataType::UInt8, DataType::UInt8) => (
                Box::new(DivImpl::<PhysicalU8>::new(DataType::UInt8)),
                DataType::UInt8,
            ),
            (DataType::UInt16, DataType::UInt16) => (
                Box::new(DivImpl::<PhysicalU16>::new(DataType::UInt16)),
                DataType::UInt16,
            ),
            (DataType::UInt32, DataType::UInt32) => (
                Box::new(DivImpl::<PhysicalU32>::new(DataType::UInt32)),
                DataType::UInt32,
            ),
            (DataType::UInt64, DataType::UInt64) => (
                Box::new(DivImpl::<PhysicalU64>::new(DataType::UInt64)),
                DataType::UInt64,
            ),
            (DataType::UInt128, DataType::UInt128) => (
                Box::new(DivImpl::<PhysicalU128>::new(DataType::UInt128)),
                DataType::UInt128,
            ),

            // Decimals
            (DataType::Decimal64(_), DataType::Decimal64(_)) => (
                Box::new(DecimalDivImpl::<Decimal64Type>::new()),
                DataType::Float64,
            ),
            (DataType::Decimal128(_), DataType::Decimal128(_)) => (
                Box::new(DecimalDivImpl::<Decimal128Type>::new()),
                DataType::Float64,
            ),

            // TODO: Interval, dates
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        };

        Ok(PlannedScalarFuntion {
            function: Box::new(*self),
            return_type,
            inputs,
            function_impl,
        })
    }
}

// TODO: We could possibly wrap inputs in a cast and avoid the special casing
// here.
#[derive(Debug, Clone)]
pub struct DecimalDivImpl<D> {
    _d: PhantomData<D>,
}

impl<D> DecimalDivImpl<D> {
    fn new() -> Self {
        DecimalDivImpl { _d: PhantomData }
    }
}

impl<D> ScalarFunctionImpl for DecimalDivImpl<D>
where
    D: DecimalType,
{
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let a = inputs[0];
        let b = inputs[1];

        let a = cast_decimal_to_float::<D::Storage, f64>(
            a,
            DataType::Float64,
            CastFailBehavior::Error,
        )?;
        let b = cast_decimal_to_float::<D::Storage, f64>(
            b,
            DataType::Float64,
            CastFailBehavior::Error,
        )?;

        let builder = ArrayBuilder {
            datatype: DataType::Float64,
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        BinaryExecutor::execute::<PhysicalF64, PhysicalF64, _, _>(&a, &b, builder, |a, b, buf| {
            buf.put(&(a / b))
        })
    }
}

#[derive(Debug, Clone)]
pub struct DivImpl<S> {
    datatype: DataType,
    _s: PhantomData<S>,
}

impl<S> DivImpl<S> {
    fn new(datatype: DataType) -> Self {
        DivImpl {
            datatype,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunctionImpl for DivImpl<S>
where
    S: PhysicalStorage,
    for<'a> S::Type<'a>: std::ops::Div<Output = S::Type<'static>> + Default + Copy,
    ArrayData: From<PrimitiveStorage<S::Type<'static>>>,
{
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let a = inputs[0];
        let b = inputs[1];

        let builder = ArrayBuilder {
            datatype: self.datatype.clone(),
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        BinaryExecutor::execute::<S, S, _, _>(a, b, builder, |a, b, buf| buf.put(&(a / b)))
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::DataType;

    use super::*;
    use crate::functions::scalar::ScalarFunction;

    #[test]
    fn div_i32() {
        let a = Array::from_iter([4, 5, 6]);
        let b = Array::from_iter([1, 2, 3]);

        let specialized = Div
            .plan_from_datatypes(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::from_iter([4, 2, 2]);

        assert_eq!(expected, out);
    }
}
