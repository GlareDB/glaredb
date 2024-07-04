pub mod arith;
pub mod boolean;
pub mod comparison;
pub mod concat;
pub mod like;
pub mod list;
pub mod negate;
pub mod numeric;
pub mod random;
pub mod string;
pub mod struct_funcs;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::field::TypeSchema;
use rayexec_bullet::{array::Array, datatype::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

use crate::logical::expr::LogicalExpression;

use super::FunctionInfo;

// List of all scalar functions.
pub static BUILTIN_SCALAR_FUNCTIONS: Lazy<Vec<Box<dyn ScalarFunction>>> = Lazy::new(|| {
    vec![
        // Arith
        Box::new(arith::Add),
        Box::new(arith::Sub),
        Box::new(arith::Mul),
        Box::new(arith::Div),
        Box::new(arith::Rem),
        // Boolean
        Box::new(boolean::And),
        Box::new(boolean::Or),
        // Comparison
        Box::new(comparison::Eq),
        Box::new(comparison::Neq),
        Box::new(comparison::Lt),
        Box::new(comparison::LtEq),
        Box::new(comparison::Gt),
        Box::new(comparison::GtEq),
        // Numeric
        Box::new(numeric::Ceil),
        Box::new(numeric::Floor),
        Box::new(numeric::IsNan),
        // String
        Box::new(string::Repeat),
        Box::new(concat::Concat),
        Box::new(like::StartsWith),
        Box::new(like::EndsWith),
        Box::new(like::Contains),
        // Like
        Box::new(like::Like),
        // Struct
        Box::new(struct_funcs::StructPack),
        // Unary
        Box::new(negate::Negate),
        // Random
        Box::new(random::Random),
        // List
        Box::new(list::ListExtract),
        Box::new(list::ListValues),
    ]
});

/// A generic scalar function that can specialize into a more specific function
/// depending on input types.
///
/// Generic scalar functions must be cheaply cloneable.
pub trait ScalarFunction: FunctionInfo + Debug + Sync + Send + DynClone {
    /// Plan a scalar function based on datatype inputs.
    ///
    /// The datatypes passed in correspond directly to the arguments to the
    /// function. This is expected to error if the number of arguments or the
    /// data types are incorrect.
    ///
    /// Most functions will only need to implement this as data types are often
    /// times sufficient for function planning.
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>>;

    /// Plan a scalar function based on expression inputs.
    ///
    /// This allows functions to check for constant expressions and generate a
    /// function state for use throughout the entire query.
    ///
    /// Most functions won't need to implement this, and the default
    /// implementation will forward to `plan_from_datatypes` by extracting the
    /// data types from the function.
    fn plan_from_expressions(
        &self,
        inputs: &[&LogicalExpression],
        operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        // TODO: Are we going to need to pass in the outer schemas here?
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(operator_schema, &[]))
            .collect::<Result<Vec<_>>>()?;

        self.plan_from_datatypes(&datatypes)
    }
}

impl Clone for Box<dyn ScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn ScalarFunction> for Box<dyn ScalarFunction + '_> {
    fn eq(&self, other: &dyn ScalarFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn ScalarFunction + '_ {
    fn eq(&self, other: &dyn ScalarFunction) -> bool {
        self.name() == other.name() && self.signatures() == other.signatures()
    }
}

/// A specialized scalar function.
///
/// We're using a trait instead of returning the function pointer directly from
/// `GenericScalarFunction` because this will be what's serialized when
/// serializing pipelines for distributed execution.
pub trait PlannedScalarFunction: Debug + Sync + Send + DynClone {
    fn name(&self) -> &'static str;

    /// Return type of the function.
    fn return_type(&self) -> DataType;

    /// Execution the function array inputs.
    ///
    /// For functions that accept no input (e.g. random), an array of length one
    /// should be returned. During evaluation, this one element array will be
    /// extended to be of the appropriate size.
    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array>;
}

impl PartialEq<dyn PlannedScalarFunction> for Box<dyn PlannedScalarFunction + '_> {
    fn eq(&self, other: &dyn PlannedScalarFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn PlannedScalarFunction + '_ {
    fn eq(&self, other: &dyn PlannedScalarFunction) -> bool {
        self.name() == other.name() && self.return_type() == other.return_type()
    }
}

impl Clone for Box<dyn PlannedScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

mod macros {
    macro_rules! primitive_unary_execute {
        ($input:expr, $output_variant:ident, $operation:expr) => {{
            use rayexec_bullet::array::{Array, PrimitiveArray};
            use rayexec_bullet::executor::scalar::UnaryExecutor;

            let mut buffer = Vec::with_capacity($input.len());
            UnaryExecutor::execute($input, $operation, &mut buffer)?;
            Array::$output_variant(PrimitiveArray::new(buffer, $input.validity().cloned()))
        }};
    }
    pub(crate) use primitive_unary_execute;

    macro_rules! primitive_unary_execute_bool {
        ($input:expr, $operation:expr) => {{
            use rayexec_bullet::array::{Array, BooleanArray, BooleanValuesBuffer};
            use rayexec_bullet::executor::scalar::UnaryExecutor;

            let mut buffer = BooleanValuesBuffer::with_capacity($input.len());
            UnaryExecutor::execute($input, $operation, &mut buffer)?;
            Array::Boolean(BooleanArray::new(buffer, $input.validity().cloned()))
        }};
    }
    pub(crate) use primitive_unary_execute_bool;

    macro_rules! primitive_binary_execute {
        ($first:expr, $second:expr, $output_variant:ident, $operation:expr) => {{
            use rayexec_bullet::array::{Array, PrimitiveArray};
            use rayexec_bullet::executor::scalar::BinaryExecutor;

            let mut buffer = Vec::with_capacity($first.len());
            let validity = BinaryExecutor::execute($first, $second, $operation, &mut buffer)?;
            Array::$output_variant(PrimitiveArray::new(buffer, validity))
        }};
    }
    pub(crate) use primitive_binary_execute;

    macro_rules! primitive_binary_execute_no_wrap {
        ($first:expr, $second:expr, $operation:expr) => {{
            use rayexec_bullet::array::PrimitiveArray;
            use rayexec_bullet::executor::scalar::BinaryExecutor;

            let mut buffer = Vec::with_capacity($first.len());
            let validity = BinaryExecutor::execute($first, $second, $operation, &mut buffer)?;
            PrimitiveArray::new(buffer, validity)
        }};
    }
    pub(crate) use primitive_binary_execute_no_wrap;

    macro_rules! primitive_binary_execute_bool {
        ($first:expr, $second:expr, $operation:expr) => {{
            use rayexec_bullet::array::{Array, BooleanArray, BooleanValuesBuffer};
            use rayexec_bullet::executor::scalar::BinaryExecutor;

            let mut buffer = BooleanValuesBuffer::with_capacity($first.len());
            let validity = BinaryExecutor::execute($first, $second, $operation, &mut buffer)?;
            Array::Boolean(BooleanArray::new(buffer, validity))
        }};
    }
    pub(crate) use primitive_binary_execute_bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_eq_check() {
        let fn1 = Box::new(arith::Add) as Box<dyn ScalarFunction>;
        let fn2 = Box::new(arith::Sub) as Box<dyn ScalarFunction>;
        let fn3 = Box::new(arith::Sub) as Box<dyn ScalarFunction>;

        assert_ne!(fn1, fn2);
        assert_eq!(fn2, fn3);
    }
}
