pub mod builtin;
pub mod states;

use std::fmt::Debug;
use std::hash::Hash;

use dyn_clone::DynClone;
use rayexec_error::Result;
use states::AggregateFunctionImpl;

use super::bind_state::{BindState, RawBindState};
use super::{FunctionInfo, Signature};
use crate::arrays::datatype::DataType;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableList;

pub struct PlannedAggregateFunction {
    pub(crate) name: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub struct RawAggregateFunction {
    function: *const (),
    signature: Signature,
    vtable: &'static RawAggregateFunctionVTable,
}

#[derive(Debug, Clone, Copy)]
pub struct RawAggregateFunctionVTable {
    bind_fn: unsafe fn(function: *const (), inputs: Vec<Expression>) -> Result<RawBindState>,
}

// TODO: State naming.
pub trait AggregateFunction {
    /// Bind state passed to update, combine, and finalize functions.
    type State: Sync + Send;

    /// The type of the aggregate values that get updated.
    type AggregateState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>>;
}

/// A generic aggregate function that can be specialized into a more specific
/// function depending on type.
pub trait AggregateFunction2: FunctionInfo + Debug + Sync + Send + DynClone {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction2>;
}

impl Clone for Box<dyn AggregateFunction2> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn AggregateFunction2> for Box<dyn AggregateFunction2 + '_> {
    fn eq(&self, other: &dyn AggregateFunction2) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn AggregateFunction2 + '_ {
    fn eq(&self, other: &dyn AggregateFunction2) -> bool {
        self.name() == other.name() && self.signatures() == other.signatures()
    }
}

impl Eq for dyn AggregateFunction2 {}

#[derive(Debug, Clone)]
pub struct PlannedAggregateFunction2 {
    pub function: Box<dyn AggregateFunction2>,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
    pub function_impl: AggregateFunctionImpl,
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedAggregateFunction2 {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.return_type == other.return_type
            && self.inputs == other.inputs
    }
}

impl Eq for PlannedAggregateFunction2 {}

impl Hash for PlannedAggregateFunction2 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.function.name().hash(state);
        self.return_type.hash(state);
        self.inputs.hash(state);
    }
}
