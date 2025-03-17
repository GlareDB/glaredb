use std::fmt::Debug;

use glaredb_error::Result;
use serde::{Deserialize, Serialize};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalBool};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor, UniformExecutor};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_AND: ScalarFunctionSet = ScalarFunctionSet {
    name: "and",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Boolean and all inputs.",
        arguments: &["var_args"],
        example: Some(Example {
            example: "and(true, false, true)",
            output: "false",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature {
            positional_args: &[DataTypeId::Boolean],
            variadic_arg: Some(DataTypeId::Boolean),
            return_type: DataTypeId::Boolean,
            doc: None,
        },
        &And,
    )],
};

pub const FUNCTION_SET_OR: ScalarFunctionSet = ScalarFunctionSet {
    name: "or",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Boolean or all inputs.",
        arguments: &["var_args"],
        example: Some(Example {
            example: "or(true, false, true)",
            output: "true",
        }),
    }),
    functions: &[RawScalarFunction::new(
        &Signature {
            positional_args: &[DataTypeId::Boolean],
            variadic_arg: Some(DataTypeId::Boolean),
            return_type: DataTypeId::Boolean,
            doc: None,
        },
        &Or,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct And;

impl ScalarFunction for And {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match input.arrays().len() {
            0 => {
                // TODO: Default to false?
                let vals = PhysicalBool::get_addressable_mut(&mut output.data)?;
                for v in vals.slice {
                    *v = false;
                }
            }
            1 => {
                let input = &input.arrays()[0];
                UnaryExecutor::execute::<PhysicalBool, PhysicalBool, _>(
                    input,
                    sel,
                    OutBuffer::from_array(output)?,
                    |v, buf| buf.put(v),
                )?;
            }
            2 => {
                let a = &input.arrays()[0];
                let b = &input.arrays()[1];

                BinaryExecutor::execute::<PhysicalBool, PhysicalBool, PhysicalBool, _>(
                    a,
                    sel,
                    b,
                    sel,
                    OutBuffer::from_array(output)?,
                    |&a, &b, buf| buf.put(&(a && b)),
                )?;
            }
            _ => UniformExecutor::execute::<PhysicalBool, PhysicalBool, _>(
                input.arrays(),
                sel,
                OutBuffer::from_array(output)?,
                |bools, buf| buf.put(&(bools.iter().all(|b| **b))),
            )?,
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Or;

impl ScalarFunction for Or {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match input.arrays().len() {
            0 => {
                // TODO: Default to false?
                let vals = PhysicalBool::get_addressable_mut(&mut output.data)?;
                for v in vals.slice {
                    *v = false;
                }
            }
            1 => {
                let input = &input.arrays()[0];
                UnaryExecutor::execute::<PhysicalBool, PhysicalBool, _>(
                    input,
                    sel,
                    OutBuffer::from_array(output)?,
                    |v, buf| buf.put(v),
                )?;
            }
            2 => {
                let a = &input.arrays()[0];
                let b = &input.arrays()[1];

                BinaryExecutor::execute::<PhysicalBool, PhysicalBool, PhysicalBool, _>(
                    a,
                    sel,
                    b,
                    sel,
                    OutBuffer::from_array(output)?,
                    |&a, &b, buf| buf.put(&(a || b)),
                )?;
            }
            _ => UniformExecutor::execute::<PhysicalBool, PhysicalBool, _>(
                input.arrays(),
                sel,
                OutBuffer::from_array(output)?,
                |bools, buf| buf.put(&(bools.iter().any(|b| **b))),
            )?,
        }

        Ok(())
    }
}
