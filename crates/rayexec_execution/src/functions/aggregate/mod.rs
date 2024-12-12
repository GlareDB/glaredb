pub mod builtin;
pub mod states;

use std::any::Any;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::PhantomData;

use dyn_clone::DynClone;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::aggregate::{
    AggregateState,
    RowToStateMapping,
    StateCombiner,
    StateFinalizer,
    UnaryNonNullUpdater,
};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::PhysicalStorage;
use rayexec_bullet::storage::{AddressableStorage, PrimitiveStorage};
use rayexec_error::{RayexecError, Result};

use super::FunctionInfo;
use crate::execution::operators::hash_aggregate::hash_table::GroupAddress;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableList;

/// A generic aggregate function that can be specialized into a more specific
/// function depending on type.
pub trait AggregateFunction: FunctionInfo + Debug + Sync + Send + DynClone {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction2>>;

    /// Plans an aggregate function from input data types.
    ///
    /// The data types passed in correspond directly to the arguments to the
    /// aggregate.
    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction2>>;

    /// Plan an aggregate based on expressions.
    ///
    /// Similar to scalar functions, the default implementation for this will
    /// call `plan_from_datatype`.
    fn plan_from_expressions(
        &self,
        table_list: &TableList,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedAggregateFunction2>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        self.plan_from_datatypes(&datatypes)
    }
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

pub struct PlannedAggregateFunction {
    pub function: Box<dyn AggregateFunction>,
    pub return_type: DataType,
}

pub trait AggregateFunctionImpl {}

pub trait AggregateGroupStates {
    fn new_groups(&mut self, count: usize);
    fn update_states(&mut self, inputs: &[&Array], mapping: ChunkGroupAddressIter) -> Result<()>;
    fn combine(
        &mut self,
        consume: &mut Box<dyn GroupedStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()>;
}

pub trait PlannedAggregateFunction2: Debug + Sync + Send + DynClone {
    /// The aggregate function that produce this instance.
    fn aggregate_function(&self) -> &dyn AggregateFunction;

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()>;

    /// Return type of the aggregate.
    fn return_type(&self) -> DataType;

    /// Create a new `GroupedStates` that's able to hold the aggregate state for
    /// multiple groups.
    fn new_grouped_state(&self) -> Result<Box<dyn GroupedStates>>;
}

impl Clone for Box<dyn PlannedAggregateFunction2> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn PlannedAggregateFunction2> for Box<dyn PlannedAggregateFunction2 + '_> {
    fn eq(&self, other: &dyn PlannedAggregateFunction2) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn PlannedAggregateFunction2 + '_ {
    fn eq(&self, other: &dyn PlannedAggregateFunction2) -> bool {
        self.aggregate_function() == other.aggregate_function()
            && self.return_type() == other.return_type()
    }
}

impl Eq for dyn PlannedAggregateFunction2 {}

impl Hash for dyn PlannedAggregateFunction2 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.aggregate_function().name().hash(state);
        self.return_type().hash(state);
    }
}

pub trait GroupedStates: Debug + Send {
    /// Needed to allow downcasting to the concrete type when combining multiple
    /// states that were computed in parallel.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Generate new states for never before seen groups in an aggregate.
    ///
    /// Returns the index of the newly initialized state that can be used to
    /// reference the state.
    fn new_groups(&mut self, count: usize);

    /// Get the number of group states we're tracking.
    fn num_groups(&self) -> usize;

    /// Updates states for groups using the provided inputs.
    ///
    /// Each row selected in `inputs` corresponds to values that should be used
    /// to update states.
    ///
    /// `mapping` provides a mapping from the selected input row to the state
    /// that should be updated.
    fn update_states(&mut self, inputs: &[&Array], mapping: ChunkGroupAddressIter) -> Result<()>;

    /// Try to combine two sets of grouped states into a single set of states.
    ///
    /// `mapping` is used to map groups in `consume` to the target groups in
    /// self that should be merged.
    ///
    /// Errors if the concrete types do not match. Essentially this prevents
    /// trying to combine state between different aggregates (SumI32 and AvgF32)
    /// _and_ type (SumI32 and SumI64).
    fn combine(
        &mut self,
        consume: &mut Box<dyn GroupedStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()>;

    /// Drains the internal results producing a single output array.
    fn drain(&mut self) -> Result<Array>;
}

pub trait AggregateGroupedStatesTrait {
    fn update_states(&mut self, inputs: &[&Array], mapping: ChunkGroupAddressIter) -> Result<()>;
}

#[derive(Debug)]
pub struct BoxedStates<S> {
    states: Vec<S>,
}

#[derive(Debug)]
pub struct AggregateGroupedStates<S> {
    states: Box<dyn Any>,

    _s: PhantomData<S>,
}

impl<S> AggregateGroupedStates<S> {}

/// Provides a default implementation of `GroupedStates`.
///
/// Since we're working with multiple aggregates at a time, we need to be able
/// to box `GroupedStates`, and this type just enables doing that easily.
///
/// This essetially provides a wrapping around functions provided by the
/// aggregate executors, and some number of aggregate states.
pub struct DefaultGroupedStates<State, InputType, OutputType, CreateFn, UpdateFn, FinalizeFn> {
    /// All states we're tracking.
    ///
    /// Each state corresponds to a single group.
    states: Vec<State>,

    /// How new states shoudl be created.
    create_fn: CreateFn,

    /// How we should update states given inputs and a mapping array.
    update_fn: UpdateFn,

    /// How we should finalize the states once we're done updating states.
    finalize_fn: FinalizeFn,

    _t: PhantomData<InputType>,
    _o: PhantomData<OutputType>,
}

impl<State, InputType, OutputType, CreateFn, UpdateFn, FinalizeFn>
    DefaultGroupedStates<State, InputType, OutputType, CreateFn, UpdateFn, FinalizeFn>
where
    State: AggregateState<InputType, OutputType>,
    CreateFn: Fn() -> State,
    UpdateFn: Fn(&[&Array], ChunkGroupAddressIter, &mut [State]) -> Result<()>,
    FinalizeFn: Fn(&mut [State]) -> Result<Array>,
{
    fn new(create_fn: CreateFn, update_fn: UpdateFn, finalize_fn: FinalizeFn) -> Self {
        DefaultGroupedStates {
            states: Vec::new(),
            create_fn,
            update_fn,
            finalize_fn,
            _t: PhantomData,
            _o: PhantomData,
        }
    }
}

impl<State, InputType, OutputType, CreateFn, UpdateFn, FinalizeFn> GroupedStates
    for DefaultGroupedStates<State, InputType, OutputType, CreateFn, UpdateFn, FinalizeFn>
where
    State: AggregateState<InputType, OutputType> + Send + 'static,
    InputType: Send + 'static,
    OutputType: Send + 'static,
    CreateFn: Fn() -> State + Send + 'static,
    UpdateFn: Fn(&[&Array], ChunkGroupAddressIter, &mut [State]) -> Result<()> + Send + 'static,
    FinalizeFn: Fn(&mut [State]) -> Result<Array> + Send + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn new_groups(&mut self, count: usize) {
        self.states.extend((0..count).map(|_| (self.create_fn)()))
    }

    fn num_groups(&self) -> usize {
        self.states.len()
    }

    fn update_states(&mut self, inputs: &[&Array], mapping: ChunkGroupAddressIter) -> Result<()> {
        (self.update_fn)(inputs, mapping, &mut self.states)
    }

    fn combine(
        &mut self,
        consume: &mut Box<dyn GroupedStates>,
        mapping: ChunkGroupAddressIter,
    ) -> Result<()> {
        let other = match consume.as_any_mut().downcast_mut::<Self>() {
            Some(other) => other,
            None => {
                return Err(RayexecError::new(
                    "Attempted to combine aggregate states of different types",
                ))
            }
        };

        StateCombiner::combine(&mut other.states, mapping, &mut self.states)
    }

    fn drain(&mut self) -> Result<Array> {
        let arr = (self.finalize_fn)(&mut self.states)?;
        Ok(arr)
    }
}

impl<S, T, O, CF, UF, FF> Debug for DefaultGroupedStates<S, T, O, CF, UF, FF>
where
    S: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultGroupedStates")
            .field("states", &self.states)
            .finish_non_exhaustive()
    }
}

/// Iterator that internally filters an iterator of group addresses to to just
/// row mappings that correspond to a single chunk.
#[derive(Debug)]
pub struct ChunkGroupAddressIter<'a> {
    pub row_idx: usize,
    pub chunk_idx: u16,
    pub addresses: std::slice::Iter<'a, GroupAddress>,
}

impl<'a> ChunkGroupAddressIter<'a> {
    pub fn new(chunk_idx: u16, addrs: &'a [GroupAddress]) -> Self {
        ChunkGroupAddressIter {
            row_idx: 0,
            chunk_idx,
            addresses: addrs.iter(),
        }
    }
}

impl Iterator for ChunkGroupAddressIter<'_> {
    type Item = RowToStateMapping;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        for addr in self.addresses.by_ref() {
            if addr.chunk_idx == self.chunk_idx {
                let row = self.row_idx;
                self.row_idx += 1;
                return Some(RowToStateMapping {
                    from_row: row,
                    to_state: addr.row_idx as usize,
                });
            }
            self.row_idx += 1;
        }
        None
    }
}

/// Helper function for using with `DefaultGroupedStates`.
pub fn unary_update<State, Storage, Output>(
    arrays: &[&Array],
    mapping: ChunkGroupAddressIter,
    states: &mut [State],
) -> Result<()>
where
    Storage: PhysicalStorage,
    State: for<'a> AggregateState<
        <<Storage as PhysicalStorage>::Storage<'a> as AddressableStorage>::T,
        Output,
    >,
{
    UnaryNonNullUpdater::update::<Storage, _, _, _>(arrays[0], mapping, states)
}

pub fn untyped_null_finalize<State>(states: &mut [State]) -> Result<Array> {
    Ok(Array::new_untyped_null_array(states.len()))
}

pub fn boolean_finalize<State, Input>(datatype: DataType, states: &mut [State]) -> Result<Array>
where
    State: AggregateState<Input, bool>,
{
    let builder = ArrayBuilder {
        datatype,
        buffer: BooleanBuffer::with_len(states.len()),
    };
    StateFinalizer::finalize(states, builder)
}

pub fn primitive_finalize<State, Input, Output>(
    datatype: DataType,
    states: &mut [State],
) -> Result<Array>
where
    State: AggregateState<Input, Output>,
    Output: Copy + Default,
    ArrayData: From<PrimitiveStorage<Output>>,
{
    let builder = ArrayBuilder {
        datatype,
        buffer: PrimitiveBuffer::with_len(states.len()),
    };
    StateFinalizer::finalize(states, builder)
}
