pub mod builtin;

use std::fmt::Debug;
use std::hash::Hash;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use super::FunctionInfo;
use crate::expr::Expression;
use crate::logical::binder::bind_context::BindContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionVolatility {
    /// Every call to this function with the same arguemnts is not guaranteed to
    /// return the same value.
    Volatile,
    /// This function is consistent within the query.
    Consistent,
}

/// A generic scalar function that can specialize into a more specific function
/// depending on input types.
///
/// Generic scalar functions must be cheaply cloneable.
pub trait ScalarFunction: FunctionInfo + Debug + Sync + Send + DynClone {
    fn volatility(&self) -> FunctionVolatility {
        FunctionVolatility::Consistent
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>>;

    /// Plan a scalar function based on datatype inputs.
    ///
    /// The datatypes passed in correspond directly to the arguments to the
    /// function. This is expected to error if the number of arguments or the
    /// data types are incorrect.
    ///
    /// Most functions will only need to implement this as data types are often
    /// times sufficient for function planning.
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>>;

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
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedScalarFunction2>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        self.plan_from_datatypes(&datatypes)
    }

    fn plan(
        &self,
        bind_context: &BindContext,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        unimplemented!()
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

impl Eq for dyn ScalarFunction {}

#[derive(Debug, Clone)]
pub struct PlannedScalarFuntion {
    pub function: Box<dyn ScalarFunction>,
    /// Return type of the functions.
    pub return_type: DataType,
    /// Inputs to the functions.
    pub inputs: Vec<Expression>,
    /// The function implmentation.
    pub function_impl: Box<dyn ScalarFunctionImpl>,
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedScalarFuntion {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.return_type == other.return_type
            && self.inputs == other.inputs
    }
}

impl Eq for PlannedScalarFuntion {}

pub trait ScalarFunctionImpl: Debug + Sync + Send + DynClone {
    fn execute(&self, inputs: &[&Array]) -> Result<Array>;
}

impl Clone for Box<dyn ScalarFunctionImpl> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

/// A scalar function with potentially some state associated with it.
pub trait PlannedScalarFunction2: Debug + Sync + Send + DynClone {
    /// The scalar function that's able to produce an instance of this planned
    /// function.
    fn scalar_function(&self) -> &dyn ScalarFunction;

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()>;

    /// Return type of the function.
    fn return_type(&self) -> DataType;

    /// Execution the function array inputs.
    ///
    /// For functions that accept no input (e.g. random), an array of length one
    /// should be returned. During evaluation, this one element array will be
    /// extended to be of the appropriate size.
    fn execute(&self, inputs: &[&Array]) -> Result<Array>;
}

impl PartialEq<dyn PlannedScalarFunction2> for Box<dyn PlannedScalarFunction2 + '_> {
    fn eq(&self, other: &dyn PlannedScalarFunction2) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn PlannedScalarFunction2 + '_ {
    fn eq(&self, other: &dyn PlannedScalarFunction2) -> bool {
        self.scalar_function() == other.scalar_function()
            && self.return_type() == other.return_type()
    }
}

impl Eq for dyn PlannedScalarFunction2 {}

impl Clone for Box<dyn PlannedScalarFunction2> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl Hash for dyn PlannedScalarFunction2 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.scalar_function().name().hash(state);
        self.return_type().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_eq_check() {
        let fn1 = Box::new(builtin::arith::Add) as Box<dyn ScalarFunction>;
        let fn2 = Box::new(builtin::arith::Sub) as Box<dyn ScalarFunction>;
        let fn3 = Box::new(builtin::arith::Sub) as Box<dyn ScalarFunction>;

        assert_ne!(fn1, fn2);
        assert_eq!(fn2, fn3);
    }
}
