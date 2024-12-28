use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::{Array2, ArrayData2};
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
use crate::arrays::storage::PrimitiveStorage;
use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rem;

impl FunctionInfo for Rem {
    fn name(&self) -> &'static str {
        "%"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["rem", "mod"]
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
            // TODO
            // Signature {
            //     input: &[DataTypeId::Date32, DataTypeId::Int64],
            //     variadic: None,
            //     return_type: DataTypeId::Date32,
            // },
            // Signature {
            //     input: &[DataTypeId::Interval, DataTypeId::Int64],
            //     variadic: None,
            //     return_type: DataTypeId::Interval,
            // },
            // Signature {
            //     input: &[DataTypeId::Decimal64, DataTypeId::Decimal64],
            //     variadic: None,
            //     return_type: DataTypeId::Decimal64,
            // },
        ];
        SIGS
    }
}

impl ScalarFunction for Rem {
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
                Box::new(RemImpl::<PhysicalF16_2>::new(DataType::Float16)),
                DataType::Float16,
            ),
            (DataType::Float32, DataType::Float32) => (
                Box::new(RemImpl::<PhysicalF32_2>::new(DataType::Float32)),
                DataType::Float32,
            ),
            (DataType::Float64, DataType::Float64) => (
                Box::new(RemImpl::<PhysicalF64_2>::new(DataType::Float64)),
                DataType::Float64,
            ),
            (DataType::Int8, DataType::Int8) => (
                Box::new(RemImpl::<PhysicalI8>::new(DataType::Int8)),
                DataType::Int8,
            ),
            (DataType::Int16, DataType::Int16) => (
                Box::new(RemImpl::<PhysicalI16>::new(DataType::Int16)),
                DataType::Int16,
            ),
            (DataType::Int32, DataType::Int32) => (
                Box::new(RemImpl::<PhysicalI32>::new(DataType::Int32)),
                DataType::Int32,
            ),
            (DataType::Int64, DataType::Int64) => (
                Box::new(RemImpl::<PhysicalI64>::new(DataType::Int64)),
                DataType::Int64,
            ),
            (DataType::Int128, DataType::Int128) => (
                Box::new(RemImpl::<PhysicalI128>::new(DataType::Int128)),
                DataType::Int128,
            ),
            (DataType::UInt8, DataType::UInt8) => (
                Box::new(RemImpl::<PhysicalU8>::new(DataType::UInt8)),
                DataType::UInt8,
            ),
            (DataType::UInt16, DataType::UInt16) => (
                Box::new(RemImpl::<PhysicalU16>::new(DataType::UInt16)),
                DataType::UInt16,
            ),
            (DataType::UInt32, DataType::UInt32) => (
                Box::new(RemImpl::<PhysicalU32>::new(DataType::UInt32)),
                DataType::UInt32,
            ),
            (DataType::UInt64, DataType::UInt64) => (
                Box::new(RemImpl::<PhysicalU64>::new(DataType::UInt64)),
                DataType::UInt64,
            ),
            (DataType::UInt128, DataType::UInt128) => (
                Box::new(RemImpl::<PhysicalU128>::new(DataType::UInt128)),
                DataType::UInt128,
            ),

            // TODO: Interval, date, decimal
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
pub struct RemImpl<S> {
    datatype: DataType,
    _s: PhantomData<S>,
}

impl<S> RemImpl<S> {
    fn new(datatype: DataType) -> Self {
        RemImpl {
            datatype,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunctionImpl for RemImpl<S>
where
    S: PhysicalStorage2,
    for<'a> S::Type<'a>: std::ops::Rem<Output = S::Type<'static>> + Default + Copy,
    ArrayData2: From<PrimitiveStorage<S::Type<'static>>>,
{
    fn execute2(&self, inputs: &[&Array2]) -> Result<Array2> {
        let a = inputs[0];
        let b = inputs[1];

        let builder = ArrayBuilder {
            datatype: self.datatype.clone(),
            buffer: PrimitiveBuffer::with_len(a.logical_len()),
        };

        BinaryExecutor2::execute::<S, S, _, _>(a, b, builder, |a, b, buf| buf.put(&(a % b)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::expr;
    use crate::functions::scalar::ScalarFunction;

    #[test]
    fn rem_i32() {
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

        let planned = Rem
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let out = planned.function_impl.execute2(&[&a, &b]).unwrap();
        let expected = Array2::from_iter([0, 1, 0]);

        assert_eq!(expected, out);
    }
}
