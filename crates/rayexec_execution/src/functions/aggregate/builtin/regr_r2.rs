use std::fmt::Debug;

use rayexec_error::Result;

use super::corr::CorrelationState;
use crate::buffer::buffer_manager::BufferManager;
use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::expr::Expression;
use crate::functions::aggregate::states::{AggregateFunctionImpl, BinaryStateLogic};
use crate::functions::aggregate::{AggregateFunction, PlannedAggregateFunction};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrR2;

impl FunctionInfo for RegrR2 {
    fn name(&self) -> &'static str {
        "regr_r2"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Compute the square of the correlation coefficient.",
                arguments: &["y", "x"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for RegrR2 {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Float64, DataType::Float64) => Ok(PlannedAggregateFunction {
                function: Box::new(*self),
                return_type: DataType::Float64,
                inputs,
                function_impl: AggregateFunctionImpl::new::<
                    BinaryStateLogic<RegrR2State, PhysicalF64, PhysicalF64, PhysicalF64>,
                >(None),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Default)]
pub struct RegrR2State {
    corr: CorrelationState,
}

impl AggregateState<(&f64, &f64), f64> for RegrR2State {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.corr.merge(&mut other.corr)?;
        Ok(())
    }

    fn update(&mut self, input: (&f64, &f64)) -> Result<()> {
        self.corr.update(input)?;
        Ok(())
    }

    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = f64>,
        B: BufferManager,
    {
        match self.corr.finalize_value() {
            Some(val) => {
                let val = val.powi(2);
                output.put(&val);
            }
            None => output.put_null(),
        }
        Ok(())
    }
}
