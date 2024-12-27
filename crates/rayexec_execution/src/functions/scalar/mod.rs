pub mod builtin;

use std::fmt::Debug;
use std::hash::Hash;

use dyn_clone::DynClone;
use rayexec_error::Result;

use super::FunctionInfo;
use crate::arrays::array::Array2;
use crate::arrays::datatype::DataType;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableList;

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

    /// Plan a scalar function based on expression inputs.
    ///
    /// This allows functions to check for constant expressions and generate a
    /// function state for use throughout the entire query.
    ///
    /// The returned planned function will hold onto its logical inputs. These
    /// inputs can be modified during optimization, but the datatype is
    /// guaranteed to remain constant.
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction>;
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

/// Represents a function that knows its inputs and the return type of its
/// output.
#[derive(Debug, Clone)]
pub struct PlannedScalarFunction {
    /// The function that produced this state.
    ///
    /// This is kept around for display user-readable names, as well as for
    /// serialized/deserializing planned functions.
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
impl PartialEq for PlannedScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.return_type == other.return_type
            && self.inputs == other.inputs
    }
}

impl Eq for PlannedScalarFunction {}

impl Hash for PlannedScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.function.name().hash(state);
        self.return_type.hash(state);
        self.inputs.hash(state);
    }
}

pub trait ScalarFunctionImpl: Debug + Sync + Send + DynClone {
    fn execute(&self, inputs: &[&Array2]) -> Result<Array2>;
}

impl Clone for Box<dyn ScalarFunctionImpl> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
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
