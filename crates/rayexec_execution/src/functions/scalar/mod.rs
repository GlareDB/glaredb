pub mod arith;
pub mod boolean;
pub mod comparison;
pub mod concat;
pub mod datetime;
pub mod is;
pub mod like;
pub mod list;
pub mod negate;
pub mod numeric;
pub mod random;
pub mod similarity;
pub mod string;
pub mod struct_funcs;

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
        Box::new(numeric::Ceil::new()),
        Box::new(numeric::Floor::new()),
        Box::new(numeric::Abs::new()),
        Box::new(numeric::Acos::new()),
        Box::new(numeric::Asin::new()),
        Box::new(numeric::Atan::new()),
        Box::new(numeric::Cbrt::new()),
        Box::new(numeric::Cos::new()),
        Box::new(numeric::Exp::new()),
        Box::new(numeric::Ln::new()),
        Box::new(numeric::Log::new()),
        Box::new(numeric::Log2::new()),
        Box::new(numeric::Sin::new()),
        Box::new(numeric::Sqrt::new()),
        Box::new(numeric::Tan::new()),
        Box::new(numeric::Degrees::new()),
        Box::new(numeric::Radians::new()),
        Box::new(numeric::IsNan),
        // String
        Box::new(string::Lower),
        Box::new(string::Upper),
        Box::new(string::Repeat),
        Box::new(string::Substring),
        Box::new(string::StartsWith),
        Box::new(string::EndsWith),
        Box::new(string::Contains),
        Box::new(string::Length),
        Box::new(concat::Concat),
        Box::new(string::RegexpReplace),
        Box::new(string::Ascii),
        // Like
        Box::new(like::Like),
        // Struct
        Box::new(struct_funcs::StructPack),
        // Unary
        Box::new(negate::Negate),
        Box::new(negate::Not),
        // Random
        Box::new(random::Random),
        // List
        Box::new(list::ListExtract),
        Box::new(list::ListValues),
        // Datetime
        Box::new(datetime::DatePart),
        Box::new(datetime::DateTrunc),
        Box::new(datetime::EpochMs),
        Box::new(datetime::Epoch),
        // Is
        Box::new(is::IsNull),
        Box::new(is::IsNotNull),
        Box::new(is::IsTrue),
        Box::new(is::IsNotTrue),
        Box::new(is::IsFalse),
        Box::new(is::IsNotFalse),
        // Distance
        Box::new(similarity::L2Distance),
    ]
});

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

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>>;

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
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
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

impl Eq for dyn ScalarFunction {}

/// A scalar function with potentially some state associated with it.
pub trait PlannedScalarFunction: Debug + Sync + Send + DynClone {
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

impl PartialEq<dyn PlannedScalarFunction> for Box<dyn PlannedScalarFunction + '_> {
    fn eq(&self, other: &dyn PlannedScalarFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn PlannedScalarFunction + '_ {
    fn eq(&self, other: &dyn PlannedScalarFunction) -> bool {
        self.scalar_function() == other.scalar_function()
            && self.return_type() == other.return_type()
    }
}

impl Eq for dyn PlannedScalarFunction {}

impl Clone for Box<dyn PlannedScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl Hash for dyn PlannedScalarFunction {
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
        let fn1 = Box::new(arith::Add) as Box<dyn ScalarFunction>;
        let fn2 = Box::new(arith::Sub) as Box<dyn ScalarFunction>;
        let fn3 = Box::new(arith::Sub) as Box<dyn ScalarFunction>;

        assert_ne!(fn1, fn2);
        assert_eq!(fn2, fn3);
    }
}
