use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::{Array2, ArrayData2};
use crate::arrays::compute::cast::array::cast_decimal_to_float;
use crate::arrays::compute::cast::behavior::CastFailBehavior;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::executor::physical_type::{
    PhysicalF16_2,
    PhysicalF32_2,
    PhysicalF64_2,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalStorage2,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use crate::arrays::executor::scalar::BinaryExecutor2;
use crate::arrays::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use crate::arrays::storage::PrimitiveStorage;
use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
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
        const SIGS: &[Signature] = &[
            Signature::new_positional(
                &[DataTypeId::Float16, DataTypeId::Float16],
                DataTypeId::Float16,
            ),
            Signature::new_positional(
                &[DataTypeId::Float32, DataTypeId::Float32],
                DataTypeId::Float32,
            ),
            Signature::new_positional(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            Signature::new_positional(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            Signature::new_positional(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            Signature::new_positional(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            Signature::new_positional(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            Signature::new_positional(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            Signature::new_positional(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            Signature::new_positional(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            Signature::new_positional(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            Signature::new_positional(&[DataTypeId::Date32, DataTypeId::Int64], DataTypeId::Date32),
            Signature::new_positional(
                &[DataTypeId::Interval, DataTypeId::Int64],
                DataTypeId::Interval,
            ),
            Signature::new_positional(
                &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                DataTypeId::Float64,
            ),
            Signature::new_positional(
                &[DataTypeId::Decimal128, DataTypeId::Decimal128],
                DataTypeId::Float64,
            ),
        ];
        SIGS
    }
}

impl ScalarFunction for Div {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        let (function_impl, return_type): (Box<dyn ScalarFunctionImpl>, _) = match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Float16, DataType::Float16) => (
                Box::new(DivImpl::<PhysicalF16_2>::new(DataType::Float16)),
                DataType::Float16,
            ),
            (DataType::Float32, DataType::Float32) => (
                Box::new(DivImpl::<PhysicalF32_2>::new(DataType::Float32)),
                DataType::Float32,
            ),
            (DataType::Float64, DataType::Float64) => (
                Box::new(DivImpl::<PhysicalF64_2>::new(DataType::Float64)),
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

        Ok(PlannedScalarFunction {
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
    fn execute2(&self, inputs: &[&Array2]) -> Result<Array2> {
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

        BinaryExecutor2::execute::<PhysicalF64_2, PhysicalF64_2, _, _>(
            &a,
            &b,
            builder,
            |a, b, buf| buf.put(&(a / b)),
        )
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
    S: PhysicalStorage2,
    for<'a> S::Type<'a>: std::ops::Div<Output = S::Type<'static>> + Default + Copy,
    ArrayData2: From<PrimitiveStorage<S::Type<'static>>>,
{
    fn execute2(&self, inputs: &[&Array2]) -> Result<Array2> {
        let a = inputs[0];
        let b = inputs[1];

        let builder = ArrayBuilder {
            datatype: self.datatype.clone(),
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        BinaryExecutor2::execute::<S, S, _, _>(a, b, builder, |a, b, buf| buf.put(&(a / b)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::expr;
    use crate::functions::scalar::ScalarFunction;

    #[test]
    fn div_i32() {
        let a = Array2::from_iter([4, 5, 6]);
        let b = Array2::from_iter([1, 2, 3]);

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Div
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let out = planned.function_impl.execute2(&[&a, &b]).unwrap();
        let expected = Array2::from_iter([4, 2, 2]);

        assert_eq!(expected, out);
    }
}
