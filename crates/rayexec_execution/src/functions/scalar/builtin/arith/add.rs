use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_bullet::array::{ArrayData, ArrayOld};
use rayexec_bullet::datatype::{DataTypeId, DataTypeOld};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16Old,
    PhysicalF32Old,
    PhysicalF64Old,
    PhysicalI128Old,
    PhysicalI16Old,
    PhysicalI32Old,
    PhysicalI64Old,
    PhysicalI8Old,
    PhysicalStorageOld,
    PhysicalU128Old,
    PhysicalU16Old,
    PhysicalU32Old,
    PhysicalU64Old,
    PhysicalU8Old,
};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Add;

impl FunctionInfo for Add {
    fn name(&self) -> &'static str {
        "+"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["add"]
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
            Signature::new_positional(&[DataTypeId::Date32, DataTypeId::Int32], DataTypeId::Date32),
            Signature::new_positional(&[DataTypeId::Int32, DataTypeId::Date32], DataTypeId::Date32),
            Signature::new_positional(
                &[DataTypeId::Interval, DataTypeId::Int64],
                DataTypeId::Interval,
            ),
            Signature::new_positional(
                &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                DataTypeId::Decimal64,
            ),
        ];
        SIGS
    }
}

impl ScalarFunction for Add {
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
            (DataTypeOld::Float16, DataTypeOld::Float16) => (
                Box::new(AddImpl::<PhysicalF16Old>::new(DataTypeOld::Float16)),
                DataTypeOld::Float16,
            ),
            (DataTypeOld::Float32, DataTypeOld::Float32) => (
                Box::new(AddImpl::<PhysicalF32Old>::new(DataTypeOld::Float32)),
                DataTypeOld::Float32,
            ),
            (DataTypeOld::Float64, DataTypeOld::Float64) => (
                Box::new(AddImpl::<PhysicalF64Old>::new(DataTypeOld::Float64)),
                DataTypeOld::Float64,
            ),
            (DataTypeOld::Int8, DataTypeOld::Int8) => (
                Box::new(AddImpl::<PhysicalI8Old>::new(DataTypeOld::Int8)),
                DataTypeOld::Int8,
            ),
            (DataTypeOld::Int16, DataTypeOld::Int16) => (
                Box::new(AddImpl::<PhysicalI16Old>::new(DataTypeOld::Int16)),
                DataTypeOld::Int16,
            ),
            (DataTypeOld::Int32, DataTypeOld::Int32) => (
                Box::new(AddImpl::<PhysicalI32Old>::new(DataTypeOld::Int32)),
                DataTypeOld::Int32,
            ),
            (DataTypeOld::Int64, DataTypeOld::Int64) => (
                Box::new(AddImpl::<PhysicalI64Old>::new(DataTypeOld::Int64)),
                DataTypeOld::Int64,
            ),
            (DataTypeOld::Int128, DataTypeOld::Int128) => (
                Box::new(AddImpl::<PhysicalI128Old>::new(DataTypeOld::Int128)),
                DataTypeOld::Int128,
            ),
            (DataTypeOld::UInt8, DataTypeOld::UInt8) => (
                Box::new(AddImpl::<PhysicalU8Old>::new(DataTypeOld::UInt8)),
                DataTypeOld::UInt8,
            ),
            (DataTypeOld::UInt16, DataTypeOld::UInt16) => (
                Box::new(AddImpl::<PhysicalU16Old>::new(DataTypeOld::UInt16)),
                DataTypeOld::UInt16,
            ),
            (DataTypeOld::UInt32, DataTypeOld::UInt32) => (
                Box::new(AddImpl::<PhysicalU32Old>::new(DataTypeOld::UInt32)),
                DataTypeOld::UInt32,
            ),
            (DataTypeOld::UInt64, DataTypeOld::UInt64) => (
                Box::new(AddImpl::<PhysicalU64Old>::new(DataTypeOld::UInt64)),
                DataTypeOld::UInt64,
            ),
            (DataTypeOld::UInt128, DataTypeOld::UInt128) => (
                Box::new(AddImpl::<PhysicalU128Old>::new(DataTypeOld::UInt128)),
                DataTypeOld::UInt128,
            ),

            // TODO: Split out decimal (for scaling)
            datatypes @ (DataTypeOld::Decimal64(_), DataTypeOld::Decimal64(_)) => (
                Box::new(AddImpl::<PhysicalI64Old>::new(datatypes.0.clone())),
                datatypes.0,
            ),
            datatypes @ (DataTypeOld::Decimal128(_), DataTypeOld::Decimal128(_)) => (
                Box::new(AddImpl::<PhysicalI128Old>::new(datatypes.0.clone())),
                datatypes.0,
            ),

            // Date + days
            (DataTypeOld::Date32, DataTypeOld::Int32) => (
                Box::new(AddImpl::<PhysicalI32Old>::new(DataTypeOld::Date32)),
                DataTypeOld::Date32,
            ),
            // Days + date
            // Note both are represented as i32 physical type, we don't need to worry about flipping the sides.
            (DataTypeOld::Int32, DataTypeOld::Date32) => (
                Box::new(AddImpl::<PhysicalI32Old>::new(DataTypeOld::Date32)),
                DataTypeOld::Date32,
            ),

            // TODO: Interval
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
pub struct AddImpl<S> {
    datatype: DataTypeOld,
    _s: PhantomData<S>,
}

impl<S> AddImpl<S> {
    fn new(datatype: DataTypeOld) -> Self {
        AddImpl {
            datatype,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunctionImpl for AddImpl<S>
where
    S: PhysicalStorageOld,
    for<'a> S::Type<'a>: std::ops::Add<Output = S::Type<'static>> + Default + Copy,
    ArrayData: From<PrimitiveStorage<S::Type<'static>>>,
{
    fn execute_old(&self, inputs: &[&ArrayOld]) -> Result<ArrayOld> {
        let a = inputs[0];
        let b = inputs[1];

        let builder = ArrayBuilder {
            datatype: self.datatype.clone(),
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        BinaryExecutor::execute::<S, S, _, _>(a, b, builder, |a, b, buf| buf.put(&(a + b)))
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::DataTypeOld;

    use super::*;
    use crate::expr;
    use crate::functions::scalar::ScalarFunction;

    #[test]
    fn add_i32() {
        let a = ArrayOld::from_iter([1, 2, 3]);
        let b = ArrayOld::from_iter([4, 5, 6]);

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataTypeOld::Int32, DataTypeOld::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Add
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let out = planned.function_impl.execute_old(&[&a, &b]).unwrap();
        let expected = ArrayOld::from_iter([5, 7, 9]);

        assert_eq!(expected, out);
    }
}
