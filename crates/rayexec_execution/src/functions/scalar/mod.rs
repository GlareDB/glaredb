pub mod arith;
pub mod boolean;
pub mod comparison;
pub mod negate;
pub mod numeric;
pub mod string;
pub mod struct_funcs;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::{array::Array, datatype::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

use super::FunctionInfo;

// List of all scalar functions.
pub static BUILTIN_SCALAR_FUNCTIONS: Lazy<Vec<Box<dyn GenericScalarFunction>>> = Lazy::new(|| {
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
        // Struct
        Box::new(struct_funcs::StructPack),
        // Unary
        Box::new(negate::Negate),
    ]
});

/// A generic scalar function that can specialize into a more specific function
/// depending on input types.
///
/// Generic scalar functions must be cheaply cloneable.
pub trait GenericScalarFunction: FunctionInfo + Debug + Sync + Send + DynClone {
    /// Specialize the function using the given input types.
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>>;
}

impl Clone for Box<dyn GenericScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn GenericScalarFunction> for Box<dyn GenericScalarFunction + '_> {
    fn eq(&self, other: &dyn GenericScalarFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn GenericScalarFunction + '_ {
    fn eq(&self, other: &dyn GenericScalarFunction) -> bool {
        self.name() == other.name() && self.signatures() == other.signatures()
    }
}

/// A specialized scalar function.
///
/// We're using a trait instead of returning the function pointer directly from
/// `GenericScalarFunction` because this will be what's serialized when
/// serializing pipelines for distributed execution.
pub trait SpecializedScalarFunction: Debug + Sync + Send + DynClone {
    /// Execution the function array inputs.
    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array>;
}

impl Clone for Box<dyn SpecializedScalarFunction> {
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

    macro_rules! cmp_binary_execute {
        ($first:expr, $second:expr, $operation:expr) => {{
            use rayexec_bullet::array::{Array, BooleanArray, BooleanValuesBuffer};
            use rayexec_bullet::executor::scalar::BinaryExecutor;

            let mut buffer = BooleanValuesBuffer::with_capacity($first.len());
            let validity = BinaryExecutor::execute($first, $second, $operation, &mut buffer)?;
            Array::Boolean(BooleanArray::new(buffer, validity))
        }};
    }
    pub(crate) use cmp_binary_execute;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_eq_check() {
        let fn1 = Box::new(arith::Add) as Box<dyn GenericScalarFunction>;
        let fn2 = Box::new(arith::Sub) as Box<dyn GenericScalarFunction>;
        let fn3 = Box::new(arith::Sub) as Box<dyn GenericScalarFunction>;

        assert_ne!(fn1, fn2);
        assert_eq!(fn2, fn3);
    }
}
