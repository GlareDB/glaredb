use std::fmt::Debug;
use std::marker::PhantomData;

use num_traits::{NumCast, PrimInt};
use rayexec_bullet::array::{ArrayData, ArrayOld};
use rayexec_bullet::datatype::{DataType, DataTypeId, DecimalTypeMeta};
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
    PhysicalInterval,
    PhysicalStorage,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_bullet::scalar::interval::Interval;
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mul;

impl FunctionInfo for Mul {
    fn name(&self) -> &'static str {
        "*"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["mul"]
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
            // Interval * Int (commutative)
            Signature::new_positional(
                &[DataTypeId::Interval, DataTypeId::Int32],
                DataTypeId::Interval,
            ),
            Signature::new_positional(
                &[DataTypeId::Int32, DataTypeId::Interval],
                DataTypeId::Interval,
            ),
            Signature::new_positional(
                &[DataTypeId::Interval, DataTypeId::Int64],
                DataTypeId::Interval,
            ),
            Signature::new_positional(
                &[DataTypeId::Int64, DataTypeId::Interval],
                DataTypeId::Interval,
            ),
            // Decimal
            Signature::new_positional(
                &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                DataTypeId::Decimal64,
            ),
        ];
        SIGS
    }
}

impl ScalarFunction for Mul {
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
                Box::new(MulImpl::<PhysicalF16>::new(DataType::Float16)),
                DataType::Float16,
            ),
            (DataType::Float32, DataType::Float32) => (
                Box::new(MulImpl::<PhysicalF32>::new(DataType::Float32)),
                DataType::Float32,
            ),
            (DataType::Float64, DataType::Float64) => (
                Box::new(MulImpl::<PhysicalF64>::new(DataType::Float64)),
                DataType::Float64,
            ),
            (DataType::Int8, DataType::Int8) => (
                Box::new(MulImpl::<PhysicalI8>::new(DataType::Int8)),
                DataType::Int8,
            ),
            (DataType::Int16, DataType::Int16) => (
                Box::new(MulImpl::<PhysicalI16>::new(DataType::Int16)),
                DataType::Int16,
            ),
            (DataType::Int32, DataType::Int32) => (
                Box::new(MulImpl::<PhysicalI32>::new(DataType::Int32)),
                DataType::Int32,
            ),
            (DataType::Int64, DataType::Int64) => (
                Box::new(MulImpl::<PhysicalI64>::new(DataType::Int64)),
                DataType::Int64,
            ),
            (DataType::Int128, DataType::Int128) => (
                Box::new(MulImpl::<PhysicalI128>::new(DataType::Int128)),
                DataType::Int128,
            ),
            (DataType::UInt8, DataType::UInt8) => (
                Box::new(MulImpl::<PhysicalU8>::new(DataType::UInt8)),
                DataType::UInt8,
            ),
            (DataType::UInt16, DataType::UInt16) => (
                Box::new(MulImpl::<PhysicalU16>::new(DataType::UInt16)),
                DataType::UInt16,
            ),
            (DataType::UInt32, DataType::UInt32) => (
                Box::new(MulImpl::<PhysicalU32>::new(DataType::UInt32)),
                DataType::UInt32,
            ),
            (DataType::UInt64, DataType::UInt64) => (
                Box::new(MulImpl::<PhysicalU64>::new(DataType::UInt64)),
                DataType::UInt64,
            ),
            (DataType::UInt128, DataType::UInt128) => (
                Box::new(MulImpl::<PhysicalU128>::new(DataType::UInt128)),
                DataType::UInt128,
            ),

            // Decimal
            (DataType::Decimal64(a), DataType::Decimal64(b)) => {
                // Since we're multiplying, might as well go wide as possible.
                // Eventually we'll want to bumpt up to 128 if the precision is
                // over some threshold to be more resilient to overflows.
                let precision = Decimal64Type::MAX_PRECISION;
                let scale = a.scale + b.scale;
                let return_type = DataType::Decimal64(DecimalTypeMeta { precision, scale });
                (
                    Box::new(DecimalMulImpl::<Decimal64Type>::new(return_type.clone())),
                    return_type,
                )
            }
            (DataType::Decimal128(a), DataType::Decimal128(b)) => {
                let precision = Decimal128Type::MAX_PRECISION;
                let scale = a.scale + b.scale;
                let return_type = DataType::Decimal128(DecimalTypeMeta { precision, scale });
                (
                    Box::new(DecimalMulImpl::<Decimal128Type>::new(return_type.clone())),
                    return_type,
                )
            }

            // Interval
            (DataType::Interval, DataType::Int32) => (
                Box::new(IntervalMulImpl::<PhysicalI32, false>::new()),
                DataType::Interval,
            ),
            (DataType::Interval, DataType::Int64) => (
                Box::new(IntervalMulImpl::<PhysicalI64, false>::new()),
                DataType::Interval,
            ),
            (DataType::Int32, DataType::Interval) => (
                Box::new(IntervalMulImpl::<PhysicalI32, true>::new()),
                DataType::Interval,
            ),
            (DataType::Int64, DataType::Interval) => (
                Box::new(IntervalMulImpl::<PhysicalI64, true>::new()),
                DataType::Interval,
            ),

            // TODO: Date
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

#[derive(Debug, Clone)]
pub struct IntervalMulImpl<Rhs, const LHS_RHS_FLIPPED: bool> {
    _rhs: PhantomData<Rhs>,
}

impl<Rhs, const LHS_RHS_FLIPPED: bool> IntervalMulImpl<Rhs, LHS_RHS_FLIPPED> {
    fn new() -> Self {
        IntervalMulImpl { _rhs: PhantomData }
    }
}

impl<Rhs, const LHS_RHS_FLIPPED: bool> ScalarFunctionImpl for IntervalMulImpl<Rhs, LHS_RHS_FLIPPED>
where
    Rhs: PhysicalStorage,
    for<'a> Rhs::Type<'a>: PrimInt,
{
    fn execute(&self, inputs: &[&ArrayOld]) -> Result<ArrayOld> {
        let (lhs, rhs) = if LHS_RHS_FLIPPED {
            (inputs[1], inputs[0])
        } else {
            (inputs[0], inputs[1])
        };

        let builder = ArrayBuilder {
            datatype: DataType::Interval,
            buffer: PrimitiveBuffer::<Interval>::with_len(lhs.logical_len()),
        };

        BinaryExecutor::execute::<PhysicalInterval, Rhs, _, _>(lhs, rhs, builder, |a, b, buf| {
            // TODO: Overflow check
            buf.put(&Interval {
                months: a.months * (<i32 as NumCast>::from(b).unwrap_or_default()),
                days: a.days * (<i32 as NumCast>::from(b).unwrap_or_default()),
                nanos: a.nanos * (<i64 as NumCast>::from(b).unwrap_or_default()),
            })
        })
    }
}

#[derive(Debug, Clone)]
pub struct DecimalMulImpl<D> {
    datatype: DataType,
    _d: PhantomData<D>,
}

impl<D> DecimalMulImpl<D> {
    fn new(datatype: DataType) -> Self {
        DecimalMulImpl {
            datatype,
            _d: PhantomData,
        }
    }
}

impl<D> ScalarFunctionImpl for DecimalMulImpl<D>
where
    D: DecimalType,
    ArrayData: From<PrimitiveStorage<D::Primitive>>,
{
    fn execute(&self, inputs: &[&ArrayOld]) -> Result<ArrayOld> {
        let a = inputs[0];
        let b = inputs[1];

        let builder = ArrayBuilder {
            datatype: self.datatype.clone(),
            buffer: PrimitiveBuffer::<D::Primitive>::with_len(a.logical_len()),
        };

        BinaryExecutor::execute::<D::Storage, D::Storage, _, _>(a, b, builder, |a, b, buf| {
            buf.put(&(a * b))
        })
    }
}

#[derive(Debug, Clone)]
pub struct MulImpl<S> {
    datatype: DataType,
    _s: PhantomData<S>,
}

impl<S> MulImpl<S> {
    fn new(datatype: DataType) -> Self {
        MulImpl {
            datatype,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunctionImpl for MulImpl<S>
where
    S: PhysicalStorage,
    for<'a> S::Type<'a>: std::ops::Mul<Output = S::Type<'static>> + Default + Copy,
    ArrayData: From<PrimitiveStorage<S::Type<'static>>>,
{
    fn execute(&self, inputs: &[&ArrayOld]) -> Result<ArrayOld> {
        let a = inputs[0];
        let b = inputs[1];

        let builder = ArrayBuilder {
            datatype: self.datatype.clone(),
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        BinaryExecutor::execute::<S, S, _, _>(a, b, builder, |a, b, buf| buf.put(&(a * b)))
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::DataType;

    use super::*;
    use crate::expr;
    use crate::functions::scalar::ScalarFunction;

    #[test]
    fn mul_i32() {
        let a = ArrayOld::from_iter([4, 5, 6]);
        let b = ArrayOld::from_iter([1, 2, 3]);

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Mul
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();
        let expected = ArrayOld::from_iter([4, 10, 18]);

        assert_eq!(expected, out);
    }
}
