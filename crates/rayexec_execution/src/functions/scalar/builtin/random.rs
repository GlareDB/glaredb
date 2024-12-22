use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::datatype::{DataTypeId, DataTypeOld};
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::scalar::{
    FunctionVolatility,
    PlannedScalarFunction,
    ScalarFunction,
    ScalarFunctionImpl,
};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Random;

impl FunctionInfo for Random {
    fn name(&self) -> &'static str {
        "random"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation {
                category: Category::Numeric,
                description: "Return a random float.",
                arguments: &[],
                example: None,
            }),
        }]
    }
}

impl ScalarFunction for Random {
    fn volatility(&self) -> FunctionVolatility {
        FunctionVolatility::Volatile
    }

    fn plan(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 0)?;
        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataTypeOld::Float64,
            inputs,
            function_impl: Box::new(RandomImpl),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RandomImpl;

impl ScalarFunctionImpl for RandomImpl {
    fn execute_old(&self, _inputs: &[&ArrayOld]) -> Result<ArrayOld> {
        // TODO: Need to pass in dummy input to produce all unique values.
        let val = rand::random::<f64>();
        Ok(ArrayOld::new_with_array_data(
            DataTypeOld::Float64,
            PrimitiveStorage::from(vec![val]),
        ))
    }
}
