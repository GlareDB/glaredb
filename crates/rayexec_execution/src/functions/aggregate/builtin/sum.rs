use std::fmt::Debug;
use std::ops::AddAssign;

use num_traits::CheckedAdd;
use rayexec_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64, PhysicalI128, PhysicalI64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::aggregate::states::{AggregateFunctionImpl, UnaryStateLogic};
use crate::functions::aggregate::{AggregateFunction2, PlannedAggregateFunction2};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

pub const FUNCTION_SET_SUM: AggregateFunctionSet = AggregateFunctionSet {
    name: "sum",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Compute the sum of all non-NULL inputs.",
        arguments: &["inputs"],
        example: None,
    }),
    functions: &[],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sum;

impl FunctionInfo for Sum {
    fn name(&self) -> &'static str {
        "sum"
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: &Documentation = &Documentation {
            category: Category::Aggregate,
            description: "Compute the sum of all non-NULL inputs.",
            arguments: &["inputs"],
            example: None,
        };

        &[
            Signature {
                positional_args: &[DataTypeId::Float64],
                variadic_arg: None,
                return_type: DataTypeId::Float64,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Int64, // TODO: Should be big num
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Decimal64],
                variadic_arg: None,
                return_type: DataTypeId::Decimal64,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Decimal128],
                variadic_arg: None,
                return_type: DataTypeId::Decimal128,
                doc: Some(DOC),
            },
        ]
    }
}

impl AggregateFunction2 for Sum {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction2> {
        plan_check_num_args(self, &inputs, 1)?;

        let (function_impl, return_type) = match inputs[0].datatype()? {
            DataType::Int64 => {
                let function_impl = AggregateFunctionImpl::new::<
                    UnaryStateLogic<SumStateCheckedAdd<i64>, PhysicalI64, PhysicalI64>,
                >(None);
                (function_impl, DataType::Int64)
            }
            DataType::Float64 => {
                let function_impl = AggregateFunctionImpl::new::<
                    UnaryStateLogic<SumStateAdd<f64>, PhysicalF64, PhysicalF64>,
                >(None);
                (function_impl, DataType::Int64)
            }
            DataType::Decimal64(m) => {
                let datatype = DataType::Decimal64(m);
                let function_impl = AggregateFunctionImpl::new::<
                    UnaryStateLogic<SumStateCheckedAdd<i64>, PhysicalI64, PhysicalI64>,
                >(None);
                (function_impl, datatype)
            }
            DataType::Decimal128(m) => {
                let datatype = DataType::Decimal128(m);
                let function_impl = AggregateFunctionImpl::new::<
                    UnaryStateLogic<SumStateCheckedAdd<i128>, PhysicalI128, PhysicalI128>,
                >(None);
                (function_impl, datatype)
            }
            other => return Err(invalid_input_types_error(self, &[other])),
        };

        Ok(PlannedAggregateFunction2 {
            function: Box::new(*self),
            return_type,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Default)]
pub struct SumStateCheckedAdd<T> {
    sum: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for SumStateCheckedAdd<T>
where
    T: CheckedAdd + Default + Debug + Copy + Sync + Send,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum = self.sum.checked_add(&other.sum).unwrap_or_default(); // TODO
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, input: &T) -> Result<()> {
        self.sum = self.sum.checked_add(input).unwrap_or_default(); // TODO
        self.valid = true;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.sum);
        } else {
            output.put_null();
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SumStateAdd<T> {
    sum: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for SumStateAdd<T>
where
    T: AddAssign + Default + Debug + Copy + Sync + Send,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, &input: &T) -> Result<()> {
        self.sum += input;
        self.valid = true;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.sum);
        } else {
            output.put_null();
        }
        Ok(())
    }
}
