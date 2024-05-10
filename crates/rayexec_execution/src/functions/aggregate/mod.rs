pub mod numeric;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::{array::Array, executor::aggregate::AggregateState, field::DataType};
use rayexec_error::Result;
use std::{fmt::Debug, marker::PhantomData};

use super::{ReturnType, Signature};

pub static ALL_AGGREGATE_FUNCTIONS: Lazy<Vec<Box<dyn GenericAggregateFunction>>> =
    Lazy::new(|| vec![Box::new(numeric::Sum)]);

/// A generic aggregate function that can be specialized into a more specific
/// function depending on type.
pub trait GenericAggregateFunction: Debug + Sync + Send + DynClone {
    /// Name of the function.
    fn name(&self) -> &str;

    /// Optional aliases for this function.
    fn aliases(&self) -> &[&str] {
        &[]
    }

    fn signatures(&self) -> &[Signature];

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

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>>;
}

impl Clone for Box<dyn GenericAggregateFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

pub trait SpecializedAggregateFunction: Debug + Sync + Send + DynClone {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates>;
}

// TODO: Combine
pub trait GroupedStates {
    fn new_group(&mut self) -> usize;
    fn update_from_arrays(&mut self, inputs: &[Array], mapping: &[usize]) -> Result<()>;
    fn finalize(&mut self) -> Result<Array>;
}

pub struct DefaultGroupedStates<S, T, O, UF, FF> {
    states: Vec<S>,

    update_fn: UF,
    finalize_fn: FF,

    _t: PhantomData<T>,
    _o: PhantomData<O>,
}

impl<S, T, O, UF, FF> DefaultGroupedStates<S, T, O, UF, FF>
where
    S: AggregateState<T, O>,
    UF: Fn(&[Array], &[usize], &mut [S]) -> Result<()>,
    FF: Fn(Vec<S>) -> Result<Array>,
{
    fn new(update_fn: UF, finalize_fn: FF) -> Self {
        DefaultGroupedStates {
            states: Vec::new(),
            update_fn,
            finalize_fn,
            _t: PhantomData,
            _o: PhantomData,
        }
    }
}

impl<S, T, O, UF, FF> GroupedStates for DefaultGroupedStates<S, T, O, UF, FF>
where
    S: AggregateState<T, O>,
    UF: Fn(&[Array], &[usize], &mut [S]) -> Result<()>,
    FF: Fn(Vec<S>) -> Result<Array>,
{
    fn new_group(&mut self) -> usize {
        let idx = self.states.len();
        self.states.push(S::default());
        idx
    }

    fn update_from_arrays(&mut self, inputs: &[Array], mapping: &[usize]) -> Result<()> {
        (self.update_fn)(inputs, mapping, &mut self.states)
    }

    fn finalize(&mut self) -> Result<Array> {
        (self.finalize_fn)(std::mem::take(&mut self.states))
    }
}
