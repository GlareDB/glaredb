pub mod builtin;
pub mod states;

use std::fmt::Debug;
use std::hash::Hash;

use dyn_clone::DynClone;
use rayexec_error::Result;
use states::AggregateFunctionImpl;

use super::FunctionInfo;
use crate::arrays::datatype::DataType;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableList;

/// A generic aggregate function that can be specialized into a more specific
/// function depending on type.
pub trait AggregateFunction: FunctionInfo + Debug + Sync + Send + DynClone {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction>;
}

impl Clone for Box<dyn AggregateFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn AggregateFunction> for Box<dyn AggregateFunction + '_> {
    fn eq(&self, other: &dyn AggregateFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn AggregateFunction + '_ {
    fn eq(&self, other: &dyn AggregateFunction) -> bool {
        self.name() == other.name() && self.signatures() == other.signatures()
    }
}

impl Eq for dyn AggregateFunction {}

#[derive(Debug, Clone)]
pub struct PlannedAggregateFunction {
    pub function: Box<dyn AggregateFunction>,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
    pub function_impl: AggregateFunctionImpl,
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedAggregateFunction {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.return_type == other.return_type
            && self.inputs == other.inputs
    }
}

impl Eq for PlannedAggregateFunction {}

impl Hash for PlannedAggregateFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.function.name().hash(state);
        self.return_type.hash(state);
        self.inputs.hash(state);
    }
}
