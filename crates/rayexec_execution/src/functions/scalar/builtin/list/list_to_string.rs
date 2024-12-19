use std::fmt::{self, Write as _};
use std::marker::PhantomData;

use rayexec_bullet::array::Array;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, ArrayDataBuffer, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalStorage,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
};
use rayexec_bullet::executor::scalar::UnaryListExecutor;
use rayexec_error::{RayexecError, Result};

use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListToString;

impl FunctionInfo for ListToString {
    fn name(&self) -> &'static str {
        "list_to_string"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["array_to_string"]
    }

    fn signatures(&self) -> &[Signature] {
        // TODO: Variant with null string.
        &[Signature {
            positional_args: &[DataTypeId::List, DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Utf8,
            doc: Some(&Documentation {
                category: Category::List,
                description: "Convert each element to a string and concatenate with a delimiter.",
                arguments: &["list", "delimiter"],
                example: Some(Example {
                    example: "list_to_string([4, 5, 6], ',')",
                    output: "4,5,6",
                }),
            }),
        }]
    }
}

impl ScalarFunction for ListToString {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        let list_meta = match inputs[0].datatype(table_list)? {
            DataType::List(m) => m,
            other => return Err(invalid_input_types_error(self, &[other])),
        };

        // Requires constant sep for now.
        let sep = ConstFold::rewrite(table_list, inputs[1].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        let function_impl: Box<dyn ScalarFunctionImpl> = match list_meta.datatype.as_ref() {
            DataType::Int8 => Box::new(ListToStringImpl::<PhysicalI8>::new(sep)),
            DataType::Int16 => Box::new(ListToStringImpl::<PhysicalI16>::new(sep)),
            DataType::Int32 => Box::new(ListToStringImpl::<PhysicalI32>::new(sep)),
            DataType::Int64 => Box::new(ListToStringImpl::<PhysicalI64>::new(sep)),
            DataType::Int128 => Box::new(ListToStringImpl::<PhysicalI128>::new(sep)),
            DataType::UInt8 => Box::new(ListToStringImpl::<PhysicalU8>::new(sep)),
            DataType::UInt16 => Box::new(ListToStringImpl::<PhysicalU16>::new(sep)),
            DataType::UInt32 => Box::new(ListToStringImpl::<PhysicalU32>::new(sep)),
            DataType::UInt64 => Box::new(ListToStringImpl::<PhysicalU64>::new(sep)),
            DataType::UInt128 => Box::new(ListToStringImpl::<PhysicalI128>::new(sep)),
            DataType::Float16 => Box::new(ListToStringImpl::<PhysicalF16>::new(sep)),
            DataType::Float32 => Box::new(ListToStringImpl::<PhysicalF32>::new(sep)),
            DataType::Float64 => Box::new(ListToStringImpl::<PhysicalF64>::new(sep)),
            DataType::Utf8 => Box::new(ListToStringImpl::<PhysicalUtf8>::new(sep)),

            // TODO: Need special impls for decimals, dates, etc. that wrap the
            // format logic.
            other => {
                return Err(RayexecError::new(format!(
                    "Cannot yet format lists containing {other} datatypes",
                )))
            }
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Utf8,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ListToStringImpl<S> {
    sep: String,
    _s: PhantomData<S>,
}

impl<S> ListToStringImpl<S> {
    fn new(sep: String) -> Self {
        ListToStringImpl {
            sep,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunctionImpl for ListToStringImpl<S>
where
    S: PhysicalStorage,
    for<'a> S::Type<'a>: fmt::Display + fmt::Debug,
{
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let lists = inputs[0];
        list_to_string_inner::<S>(lists, &self.sep)
    }
}

fn list_to_string_inner<'a, S>(lists: &'a Array, sep: &str) -> Result<Array>
where
    S: PhysicalStorage,
    S::Type<'a>: fmt::Display + fmt::Debug,
{
    let mut builder = ArrayBuilder {
        datatype: DataType::Utf8,
        buffer: GermanVarlenBuffer::with_len(lists.logical_len()),
    };
    let mut validity = Bitmap::new_with_all_true(lists.logical_len());

    let mut skip_sep = true;
    let mut buf = String::new();

    let mut curr_row = 0;

    UnaryListExecutor::for_each::<S, _>(lists, |row, ent| {
        if row != curr_row {
            builder.buffer.put(curr_row, buf.as_str());

            skip_sep = true;
            buf.clear();
        }

        curr_row = row;

        // TODO: Handle errors.

        match ent {
            Some(ent) => {
                if let Some(val) = ent.entry {
                    if !skip_sep {
                        let _ = write!(buf, "{sep}");
                    }
                    let _ = write!(buf, "{}", val);
                    skip_sep = false;
                }
            }
            None => {
                validity.set_unchecked(row, false);
            }
        }
    })?;

    // Write last row.
    builder.buffer.put(curr_row, buf.as_str());

    Ok(Array::new_with_array_data(
        builder.datatype,
        builder.buffer.into_data(),
    ))
}
