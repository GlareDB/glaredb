pub mod builtin;

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use dyn_clone::DynClone;
use rayexec_error::{RayexecError, Result};

use super::{FunctionInfo, Signature};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
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

#[derive(Debug, Clone)]
pub struct PlannedScalarFunction {
    /// Name of this function.
    pub name: &'static str,
    /// VTable of the scalar function.
    pub vtable: &'static RawScalarFunctionVTable,
    /// State for the function (inputs, return type).
    pub state: RawBindState,
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.state.return_type == other.state.return_type
            && self.state.inputs == other.state.inputs
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RawScalarFunctionVTable {
    /// Create the function state and compute the return type.
    bind_fn: unsafe fn(
        function: *const (),
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<RawBindState>,

    /// Execute the function. First argument is a pointer to the function state.
    execute_fn: unsafe fn(
        function: *const (),
        state: *const (),
        input: &Batch,
        output: &mut Array,
    ) -> Result<()>,
}

#[derive(Debug, Clone, Copy)]
pub struct RawScalarFunction {
    function: *const (),
    signature: Signature,
    volatility: FunctionVolatility,
    vtable: &'static RawScalarFunctionVTable,
}

impl RawScalarFunction {
    pub const fn new<F>(sig: Signature, function: &'static F) -> Self
    where
        F: ScalarFunction,
    {
        let function = (function as *const F).cast();
        RawScalarFunction {
            function,
            signature: sig,
            volatility: F::VOLATILITY,
            vtable: F::VTABLE,
        }
    }

    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    pub fn volatility(&self) -> FunctionVolatility {
        self.volatility
    }
}

#[derive(Debug, Clone)]
pub struct RawBindState {
    pub state: RawScalarFunctionState,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}

/// Bind state for a function. Paramterized on the function state.
#[derive(Debug)]
pub struct BindState<S> {
    pub state: S,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}

/// State passed to functions during execute.
///
/// Inner state is wrapped in an arc since we allow cloning functions (for now).
#[derive(Debug, Clone)]
pub struct RawScalarFunctionState(Arc<StateInner>);

#[derive(Debug)]
struct StateInner {
    ptr: *const (),
    drop_fn: unsafe fn(ptr: *const ()),
}

impl Drop for RawScalarFunctionState {
    fn drop(&mut self) {
        unsafe { (self.0.drop_fn)(self.0.ptr) }
    }
}

pub trait ScalarFunction: Debug + Sync + Send + Sized {
    const VOLATILITY: FunctionVolatility = FunctionVolatility::Consistent;

    /// State that gets passed to the function during execute.
    type State;

    /// Compute the return type from the expression inputs and return a function
    /// state.
    ///
    /// This will only be called with expressions that match the signature this
    /// function was registered with.
    fn bind(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<BindState<Self::State>>;

    /// Execute the function the input batch, writing the output for each row
    /// into `output` at the same index.
    ///
    /// `output` has the following guarantees:
    /// - Has at least the primary buffer capacity needed to write the results.
    /// - All validities are initalized to 'valid'.
    /// - Array data can be written to.
    ///
    /// The batch's `selection` method should be called to determine which rows
    /// should be looked at during function eval.
    fn execute(&self, state: &Self::State, input: &Batch, output: &mut Array) -> Result<()>;
}

trait ScalarFunctionVTable: ScalarFunction {
    const VTABLE: &'static RawScalarFunctionVTable = &RawScalarFunctionVTable {
        bind_fn: |function: *const (),
                  table_list: &TableList,
                  inputs: Vec<Expression>|
         -> Result<RawBindState> {
            let function = unsafe { function.cast::<Self>().as_ref().unwrap() };
            let state = function.bind(table_list, inputs)?;
            let ptr = (&state.state as *const Self::State).cast();
            std::mem::forget(state.state); // We'll handle dropping manually.

            let raw = RawScalarFunctionState(Arc::new(StateInner {
                ptr,
                drop_fn: |ptr: *const ()| {
                    let ptr = ptr.cast::<Self::State>().cast_mut();
                    unsafe { ptr.drop_in_place() };
                },
            }));

            Ok(RawBindState {
                state: raw,
                return_type: state.return_type,
                inputs: state.inputs,
            })
        },
        execute_fn: |function: *const (),
                     state: *const (),
                     input: &Batch,
                     output: &mut Array|
         -> Result<()> {
            let function = unsafe { function.cast::<Self>().as_ref().unwrap() };
            let state = unsafe { state.cast::<Self::State>().as_ref().unwrap() };
            function.execute(state, input, output)
        },
    };
}

impl<F> ScalarFunctionVTable for F where F: ScalarFunction {}

/// Try to create a return type for a data type id.
///
/// For data type that require additional metadata (e.g. precision and scale for
/// decimals), this will error. Functions that return such types need to handle
/// determining the exact type to return.
fn try_return_type_from_type_id(id: DataTypeId) -> Result<DataType> {
    fn fmt_err(id: DataTypeId) -> RayexecError {
        RayexecError::new("Cannot create a default return type for type id")
            .with_field("type_id", id)
    }

    Ok(match id {
        DataTypeId::Any => return Err(fmt_err(id)),
        DataTypeId::Null => DataType::Null,
        DataTypeId::Boolean => DataType::Boolean,
        DataTypeId::Int8 => DataType::Int8,
        DataTypeId::Int16 => DataType::Int16,
        DataTypeId::Int32 => DataType::Int32,
        DataTypeId::Int64 => DataType::Int64,
        DataTypeId::Int128 => DataType::Int128,
        DataTypeId::UInt8 => DataType::UInt8,
        DataTypeId::UInt16 => DataType::UInt16,
        DataTypeId::UInt32 => DataType::UInt32,
        DataTypeId::UInt64 => DataType::UInt64,
        DataTypeId::UInt128 => DataType::UInt128,
        DataTypeId::Float16 => DataType::Float16,
        DataTypeId::Float32 => DataType::Float32,
        DataTypeId::Float64 => DataType::Float64,
        DataTypeId::Decimal64 => return Err(fmt_err(id)),
        DataTypeId::Decimal128 => return Err(fmt_err(id)),
        DataTypeId::Timestamp => return Err(fmt_err(id)),
        DataTypeId::Date32 => DataType::Date32,
        DataTypeId::Date64 => DataType::Date64,
        DataTypeId::Interval => DataType::Interval,
        DataTypeId::Utf8 => DataType::Utf8,
        DataTypeId::Binary => DataType::Binary,
        DataTypeId::Struct => return Err(fmt_err(id)),
        DataTypeId::List => return Err(fmt_err(id)),
    })
}

/// A generic scalar function that can specialize into a more specific function
/// depending on input types.
///
/// Generic scalar functions must be cheaply cloneable.
pub trait ScalarFunction2: FunctionInfo + Debug + Sync + Send + DynClone {
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
    ) -> Result<PlannedScalarFunction2>;
}

impl Clone for Box<dyn ScalarFunction2> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn ScalarFunction2> for Box<dyn ScalarFunction2 + '_> {
    fn eq(&self, other: &dyn ScalarFunction2) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn ScalarFunction2 + '_ {
    fn eq(&self, other: &dyn ScalarFunction2) -> bool {
        self.name() == other.name() && self.signatures() == other.signatures()
    }
}

impl Eq for dyn ScalarFunction2 {}

/// Represents a function that knows its inputs and the return type of its
/// output.
#[derive(Debug, Clone)]
pub struct PlannedScalarFunction2 {
    /// The function that produced this state.
    ///
    /// This is kept around for display user-readable names, as well as for
    /// serialized/deserializing planned functions.
    pub function: Box<dyn ScalarFunction2>,
    /// Return type of the functions.
    pub return_type: DataType,
    /// Inputs to the functions.
    pub inputs: Vec<Expression>,
    /// The function implmentation.
    pub function_impl: Box<dyn ScalarFunctionImpl>,
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedScalarFunction2 {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.return_type == other.return_type
            && self.inputs == other.inputs
    }
}

impl Eq for PlannedScalarFunction2 {}

impl Hash for PlannedScalarFunction2 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.function.name().hash(state);
        self.return_type.hash(state);
        self.inputs.hash(state);
    }
}

// TODO: Function pointer?
pub trait ScalarFunctionImpl: Debug + Sync + Send + DynClone {
    /// Execute the function the input batch, writing the output for each row
    /// into `output` at the same index.
    ///
    /// `output` has the following guarantees:
    /// - Has at least the primary buffer capacity needed to write the results.
    /// - All validities are initalized to 'valid'.
    /// - Array data can be made mutable via `try_as_mut()`.
    ///
    /// The batch's `selection` method should be called to determine which rows
    /// should be looked at during function eval.
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()>;
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
        let fn1 = Box::new(builtin::arith::Add) as Box<dyn ScalarFunction2>;
        let fn2 = Box::new(builtin::arith::Sub) as Box<dyn ScalarFunction2>;
        let fn3 = Box::new(builtin::arith::Sub) as Box<dyn ScalarFunction2>;

        assert_ne!(fn1, fn2);
        assert_eq!(fn2, fn3);
    }
}
