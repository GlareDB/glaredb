use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UniformExecutor};
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

// TODO: Currently '||' aliases to this, however there should be two separate
// concat functions. One that should return null on any null arguments (||), and
// one that should omit null arguments when concatenating (the normal concat).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Concat;

impl FunctionInfo for Concat {
    fn name(&self) -> &'static str {
        "concat"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: Some(DataTypeId::Utf8),
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for Concat {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let datatypes = inputs
            .iter()
            .map(|input| input.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        if !datatypes.iter().all(|dt| dt == &DataType::Utf8) {
            return Err(invalid_input_types_error(self, &datatypes));
        }

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Utf8,
            inputs,
            function_impl: Box::new(StringConcatImpl),
        })
    }
}

#[derive(Debug, Clone)]
pub struct StringConcatImpl;

impl ScalarFunctionImpl for StringConcatImpl {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        match inputs.len() {
            0 => {
                let mut array = Array::from_iter([""]);
                array.set_physical_validity(0, false);
                Ok(array)
            }
            1 => Ok(inputs[0].clone()),
            2 => {
                let a = inputs[0];
                let b = inputs[1];

                let mut string_buf = String::new();

                // TODO: Compute data capacity.

                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype: DataType::Utf8,
                        buffer: GermanVarlenBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| {
                        string_buf.clear();
                        string_buf.push_str(a);
                        string_buf.push_str(b);
                        buf.put(string_buf.as_str());
                    },
                )
            }
            _ => {
                let mut string_buf = String::new();

                UniformExecutor::execute::<PhysicalUtf8, _, _>(
                    inputs,
                    ArrayBuilder {
                        datatype: DataType::Utf8,
                        buffer: GermanVarlenBuffer::with_len(inputs[0].logical_len()),
                    },
                    |strings, buf| {
                        string_buf.clear();
                        for s in strings {
                            string_buf.push_str(s);
                        }
                        buf.put(string_buf.as_str());
                    },
                )
            }
        }
    }
}
