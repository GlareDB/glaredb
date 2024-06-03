pub mod arith;
pub mod boolean;
pub mod comparison;
pub mod numeric;
pub mod string;
pub mod struct_funcs;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::sync::Arc;

use super::{ReturnType, Signature};

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
    ]
});

/// A function pointer with the concrete implementation of a scalar function.
pub type ScalarFn = fn(&[&Arc<Array>]) -> Result<Array>;

/// A generic scalar function that can specialize into a more specific function
/// depending on input types.
///
/// Generic scalar functions must be cheaply cloneable.
pub trait GenericScalarFunction: Debug + Sync + Send + DynClone {
    /// Name of the function.
    fn name(&self) -> &str;

    /// Optional aliases for this function.
    fn aliases(&self) -> &[&str] {
        &[]
    }

    /// Signatures of the function.
    fn signatures(&self) -> &[Signature];

    /// Get the return type for this function.
    ///
    /// This is expected to be overridden by functions that return a dynamic
    /// type based on input. The default implementation can only determine the
    /// output if it can be statically determined.
    fn return_type_for_inputs(&self, inputs: &[DataType]) -> Option<DataType> {
        let sig = self
            .signatures()
            .iter()
            .find(|sig| sig.inputs_satisfy_signature(inputs))?;

        match &sig.return_type {
            ReturnType::Static(datatype) => Some(datatype.clone()),
            ReturnType::Dynamic => None,
        }
    }

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
    /// Return the function pointer that implements this scalar function.
    fn function_impl(&self) -> ScalarFn;
}

impl Clone for Box<dyn SpecializedScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

pub(crate) fn specialize_check_num_args(
    scalar: &impl GenericScalarFunction,
    inputs: &[DataType],
    expected: usize,
) -> Result<()> {
    if inputs.len() != expected {
        return Err(RayexecError::new(format!(
            "Expected {} input for '{}', received {}",
            expected,
            scalar.name(),
            inputs.len(),
        )));
    }
    Ok(())
}

pub(crate) fn specialize_invalid_input_type(
    scalar: &impl GenericScalarFunction,
    got: &[&DataType],
) -> RayexecError {
    let got_types = got
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join(",");
    RayexecError::new(format!(
        "Got invalid type(s) '{}' for '{}'",
        got_types,
        scalar.name()
    ))
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
