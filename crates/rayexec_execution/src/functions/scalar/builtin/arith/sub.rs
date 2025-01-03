use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::exp::Array;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::physical_type::{
    MutablePhysicalStorage,
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
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor_exp::scalar::binary::BinaryExecutor;
use crate::arrays::executor_exp::OutBuffer;
use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sub;

impl FunctionInfo for Sub {
    fn name(&self) -> &'static str {
        "-"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["sub"]
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
            Signature::new_positional(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
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
            Signature::new_positional(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            Signature::new_positional(&[DataTypeId::Date32, DataTypeId::Int32], DataTypeId::Date32),
            // TODO
            // Signature {
            //     input: &[DataTypeId::Interval, DataTypeId::Int64],
            //     variadic: None,
            //     return_type: DataTypeId::Interval,
            // },
            Signature::new_positional(
                &[DataTypeId::Decimal64, DataTypeId::Decimal64],
                DataTypeId::Decimal64,
            ),
            Signature::new_positional(
                &[DataTypeId::Decimal128, DataTypeId::Decimal128],
                DataTypeId::Decimal128,
            ),
        ];
        SIGS
    }
}

impl ScalarFunction for Sub {
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
                (Box::new(SubImpl::<PhysicalF16>::new()), DataType::Float16)
            }
            (DataType::Float32, DataType::Float32) => {
                (Box::new(SubImpl::<PhysicalF32>::new()), DataType::Float32)
            }
            (DataType::Float64, DataType::Float64) => {
                (Box::new(SubImpl::<PhysicalF64>::new()), DataType::Float64)
            }
            (DataType::Int8, DataType::Int8) => {
                (Box::new(SubImpl::<PhysicalI8>::new()), DataType::Int8)
            }
            (DataType::Int16, DataType::Int16) => {
                (Box::new(SubImpl::<PhysicalI16>::new()), DataType::Int16)
            }
            (DataType::Int32, DataType::Int32) => {
                (Box::new(SubImpl::<PhysicalI32>::new()), DataType::Int32)
            }
            (DataType::Int64, DataType::Int64) => {
                (Box::new(SubImpl::<PhysicalI64>::new()), DataType::Int64)
            }
            (DataType::Int128, DataType::Int128) => {
                (Box::new(SubImpl::<PhysicalI128>::new()), DataType::Int128)
            }
            (DataType::UInt8, DataType::UInt8) => {
                (Box::new(SubImpl::<PhysicalU8>::new()), DataType::UInt8)
            }
            (DataType::UInt16, DataType::UInt16) => {
                (Box::new(SubImpl::<PhysicalU16>::new()), DataType::UInt16)
            }
            (DataType::UInt32, DataType::UInt32) => {
                (Box::new(SubImpl::<PhysicalU32>::new()), DataType::UInt32)
            }
            (DataType::UInt64, DataType::UInt64) => {
                (Box::new(SubImpl::<PhysicalU64>::new()), DataType::UInt64)
            }
            (DataType::UInt128, DataType::UInt128) => {
                (Box::new(SubImpl::<PhysicalU128>::new()), DataType::UInt128)
            }

            // TODO: Split out decimal (for scaling)
            datatypes @ (DataType::Decimal64(_), DataType::Decimal64(_)) => {
                (Box::new(SubImpl::<PhysicalI64>::new()), datatypes.0)
            }
            datatypes @ (DataType::Decimal128(_), DataType::Decimal128(_)) => {
                (Box::new(SubImpl::<PhysicalI128>::new()), datatypes.0)
            }

            // Date - days
            (DataType::Date32, DataType::Int32) => {
                (Box::new(SubImpl::<PhysicalI32>::new()), DataType::Date32)
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
pub struct SubImpl<S> {
    _s: PhantomData<S>,
}

impl<S> SubImpl<S> {
    const fn new() -> Self {
        SubImpl { _s: PhantomData }
    }
}

impl<S> ScalarFunctionImpl for SubImpl<S>
where
    S: MutablePhysicalStorage,
    S::StorageType: std::ops::Sub<Output = S::StorageType> + Sized + Copy,
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
            |&a, &b, buf| buf.put(&(a - b)),
        )
    }
}

#[cfg(test)]
mod tests {
    use iterutil::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::buffer::buffer_manager::NopBufferManager;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;
    use crate::expr;
    use crate::functions::scalar::ScalarFunction;

    #[test]
    fn sub_i32() {
        let a = Array::try_from_iter([4, 5, 6]).unwrap();
        let b = Array::try_from_iter([1, 2, 3]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Sub
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([3, 3, 3]).unwrap();

        assert_arrays_eq(&expected, &out);
    }
}
