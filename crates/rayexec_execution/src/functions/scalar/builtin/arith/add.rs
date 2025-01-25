use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::executor::OutBuffer;
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
            (DataType::Float16, DataType::Float16) => {
                (Box::new(AddImpl::<PhysicalF16>::new()), DataType::Float16)
            }
            (DataType::Float32, DataType::Float32) => {
                (Box::new(AddImpl::<PhysicalF32>::new()), DataType::Float32)
            }
            (DataType::Float64, DataType::Float64) => {
                (Box::new(AddImpl::<PhysicalF64>::new()), DataType::Float64)
            }
            (DataType::Int8, DataType::Int8) => {
                (Box::new(AddImpl::<PhysicalI8>::new()), DataType::Int8)
            }
            (DataType::Int16, DataType::Int16) => {
                (Box::new(AddImpl::<PhysicalI16>::new()), DataType::Int16)
            }
            (DataType::Int32, DataType::Int32) => {
                (Box::new(AddImpl::<PhysicalI32>::new()), DataType::Int32)
            }
            (DataType::Int64, DataType::Int64) => {
                (Box::new(AddImpl::<PhysicalI64>::new()), DataType::Int64)
            }
            (DataType::Int128, DataType::Int128) => {
                (Box::new(AddImpl::<PhysicalI128>::new()), DataType::Int128)
            }
            (DataType::UInt8, DataType::UInt8) => {
                (Box::new(AddImpl::<PhysicalU8>::new()), DataType::UInt8)
            }
            (DataType::UInt16, DataType::UInt16) => {
                (Box::new(AddImpl::<PhysicalU16>::new()), DataType::UInt16)
            }
            (DataType::UInt32, DataType::UInt32) => {
                (Box::new(AddImpl::<PhysicalU32>::new()), DataType::UInt32)
            }
            (DataType::UInt64, DataType::UInt64) => {
                (Box::new(AddImpl::<PhysicalU64>::new()), DataType::UInt64)
            }
            (DataType::UInt128, DataType::UInt128) => {
                (Box::new(AddImpl::<PhysicalU128>::new()), DataType::UInt128)
            }

            // TODO: Split out decimal (for scaling)
            datatypes @ (DataType::Decimal64(_), DataType::Decimal64(_)) => {
                (Box::new(AddImpl::<PhysicalI64>::new()), datatypes.0)
            }
            datatypes @ (DataType::Decimal128(_), DataType::Decimal128(_)) => {
                (Box::new(AddImpl::<PhysicalI128>::new()), datatypes.0)
            }

            // Date + days
            (DataType::Date32, DataType::Int32) => {
                (Box::new(AddImpl::<PhysicalI32>::new()), DataType::Date32)
            }
            // Days + date
            // Note both are represented as i32 physical type, we don't need to worry about flipping the sides.
            (DataType::Int32, DataType::Date32) => {
                (Box::new(AddImpl::<PhysicalI32>::new()), DataType::Date32)
            }

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
    _s: PhantomData<S>,
}

impl<S> AddImpl<S> {
    const fn new() -> Self {
        AddImpl { _s: PhantomData }
    }
}

impl<S> ScalarFunctionImpl for AddImpl<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Add<Output = S::StorageType> + Sized + Copy,
{
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        BinaryExecutor::execute::<S, S, S, _>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| buf.put(&(a + b)),
        )
    }
}

#[cfg(test)]
mod tests {
    

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;
    use crate::expr;
    use crate::functions::scalar::ScalarFunction;

    #[test]
    fn add_i32() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();
        let batch = Batch::try_from_arrays([a, b]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Add
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::try_new(&NopBufferManager, DataType::Int32, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([5, 7, 9]).unwrap();

        assert_arrays_eq(&expected, &out);
    }
}
