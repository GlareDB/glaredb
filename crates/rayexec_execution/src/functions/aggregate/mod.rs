pub mod avg;
pub mod count;
pub mod covar;
pub mod first;
pub mod minmax;
pub mod sum;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::array::Array;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::aggregate::{AggregateState, StateCombiner};
use rayexec_error::{RayexecError, Result};
use std::any::Any;
use std::{
    fmt::{self, Debug},
    marker::PhantomData,
    vec,
};

use crate::expr::Expression;
use crate::logical::binder::bind_context::BindContext;

use super::FunctionInfo;

pub static BUILTIN_AGGREGATE_FUNCTIONS: Lazy<Vec<Box<dyn AggregateFunction>>> = Lazy::new(|| {
    vec![
        Box::new(sum::Sum),
        Box::new(avg::Avg),
        Box::new(count::Count),
        Box::new(minmax::Min),
        Box::new(minmax::Max),
        Box::new(first::First),
    ]
});

/// A generic aggregate function that can be specialized into a more specific
/// function depending on type.
pub trait AggregateFunction: FunctionInfo + Debug + Sync + Send + DynClone {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>>;

    /// Plans an aggregate function from input data types.
    ///
    /// The data types passed in correspond directly to the arguments to the
    /// aggregate.
    fn plan_from_datatypes(&self, inputs: &[DataType])
        -> Result<Box<dyn PlannedAggregateFunction>>;

    /// Plan an aggregate based on expressions.
    ///
    /// Similar to scalar functions, the default implementation for this will
    /// call `plan_from_datatype`.
    fn plan_from_expressions(
        &self,
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
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

pub trait PlannedAggregateFunction: Debug + Sync + Send + DynClone {
    /// The aggregate function that produce this instance.
    fn aggregate_function(&self) -> &dyn AggregateFunction;

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()>;

    /// Return type of the aggregate.
    fn return_type(&self) -> DataType;

    /// Create a new `GroupedStates` that's able to hold the aggregate state for
    /// multiple groups.
    fn new_grouped_state(&self) -> Box<dyn GroupedStates>;
}

impl Clone for Box<dyn PlannedAggregateFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn PlannedAggregateFunction> for Box<dyn PlannedAggregateFunction + '_> {
    fn eq(&self, other: &dyn PlannedAggregateFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn PlannedAggregateFunction + '_ {
    fn eq(&self, other: &dyn PlannedAggregateFunction) -> bool {
        self.aggregate_function() == other.aggregate_function()
            && self.return_type() == other.return_type()
    }
}

pub trait GroupedStates: Debug + Send {
    /// Needed to allow downcasting to the concrete type when combining multiple
    /// states that were computed in parallel.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Generate a new state for a never before seen group in an aggregate.
    ///
    /// Returns the index of the newly initialized state that can be used to
    /// reference the state.
    fn new_group(&mut self) -> usize;

    /// Get the number of group states we're tracking.
    fn num_groups(&self) -> usize;

    /// Updates states for groups using the provided inputs.
    ///
    /// Each row selected in `inputs` corresponds to values that should be used
    /// to update states.
    ///
    /// `mapping` provides a mapping from the selected input row to the state
    /// that should be updated. The 'n'th selected row in the input corresponds
    /// to the 'n'th value in `mapping` which corresponds to the state to be
    /// updated with the 'n'th selected row.
    fn update_states(
        &mut self,
        row_selection: &Bitmap,
        inputs: &[&Array],
        mapping: &[usize],
    ) -> Result<()>;

    /// Try to combine two sets of grouped states into a single set of states.
    ///
    /// `mapping` is used to map groups in `consume` to the target groups in
    /// self that should be merged.
    ///
    /// Errors if the concrete types do not match. Essentially this prevents
    /// trying to combine state between different aggregates (SumI32 and AvgF32)
    /// _and_ type (SumI32 and SumI64).
    fn try_combine(&mut self, consume: Box<dyn GroupedStates>, mapping: &[usize]) -> Result<()>;

    /// Drains some number of internal states, finalizing them and producing an
    /// array of the results.
    ///
    /// May produce an array with length less than n
    ///
    /// Returns None when all internal states have been drained and finalized.
    fn drain_finalize_n(&mut self, n: usize) -> Result<Option<Array>>;
}

/// Provides a default implementation of `GroupedStates`.
///
/// Since we're working with multiple aggregates at a time, we need to be able
/// to box `GroupedStates`, and this type just enables doing that easily.
///
/// This essetially provides a wrapping around functions provided by the
/// aggregate executors, and some number of aggregate states.
pub struct DefaultGroupedStates<State, InputType, OutputType, UpdateFn, FinalizeFn> {
    /// All states we're tracking.
    ///
    /// Each state corresponds to a single group.
    states: Vec<State>,

    /// How we should update states given inputs and a mapping array.
    update_fn: UpdateFn,

    /// How we should finalize the states once we're done updating states.
    finalize_fn: FinalizeFn,

    _t: PhantomData<InputType>,
    _o: PhantomData<OutputType>,
}

impl<S, T, O, UF, FF> DefaultGroupedStates<S, T, O, UF, FF>
where
    S: AggregateState<T, O>,
    UF: Fn(&Bitmap, &[&Array], &[usize], &mut [S]) -> Result<()>,
    FF: Fn(vec::Drain<'_, S>) -> Result<Array>,
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

impl<State, InputType, OutputType, UpdateFn, FinalizeFn> GroupedStates
    for DefaultGroupedStates<State, InputType, OutputType, UpdateFn, FinalizeFn>
where
    State: AggregateState<InputType, OutputType> + Send + 'static,
    InputType: Send + 'static,
    OutputType: Send + 'static,
    UpdateFn: Fn(&Bitmap, &[&Array], &[usize], &mut [State]) -> Result<()> + Send + 'static,
    FinalizeFn: Fn(vec::Drain<'_, State>) -> Result<Array> + Send + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn new_group(&mut self) -> usize {
        let idx = self.states.len();
        self.states.push(State::default());
        idx
    }

    fn num_groups(&self) -> usize {
        self.states.len()
    }

    fn update_states(
        &mut self,
        row_selection: &Bitmap,
        inputs: &[&Array],
        mapping: &[usize],
    ) -> Result<()> {
        (self.update_fn)(row_selection, inputs, mapping, &mut self.states)
    }

    fn try_combine(
        &mut self,
        mut consume: Box<dyn GroupedStates>,
        mapping: &[usize],
    ) -> Result<()> {
        let other = match consume.as_any_mut().downcast_mut::<Self>() {
            Some(other) => other,
            None => {
                return Err(RayexecError::new(
                    "Attempted to combine aggregate states of different types",
                ))
            }
        };

        let consume = std::mem::take(&mut other.states);
        StateCombiner::combine(consume, mapping, &mut self.states)
    }

    fn drain_finalize_n(&mut self, n: usize) -> Result<Option<Array>> {
        assert_ne!(0, n);

        let n = usize::min(n, self.states.len());
        if n == 0 {
            return Ok(None);
        }

        let drain = self.states.drain(0..n);
        let arr = (self.finalize_fn)(drain)?;
        Ok(Some(arr))
    }
}

impl<S, T, O, UF, FF> Debug for DefaultGroupedStates<S, T, O, UF, FF>
where
    S: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultGroupedStates")
            .field("states", &self.states)
            .finish_non_exhaustive()
    }
}

/// Helper to drain from multiple states at a time.
///
/// Errors if all states do not produce arrays of the same length.
///
/// Returns None if there's nothing left to drain.
pub fn multi_array_drain(
    states: &mut [Box<dyn GroupedStates>],
    n: usize,
) -> Result<Option<Vec<Array>>> {
    let mut iter = states.iter_mut();
    let first = match iter.next() {
        Some(state) => state.drain_finalize_n(n)?,
        None => return Err(RayexecError::new("No states to drain from")),
    };

    let first = match first {
        Some(array) => array,
        None => {
            // Check to make sure all other states produce none.
            //
            // If they don't, that means we're working with different numbers of
            // groups.
            for state in iter {
                if state.drain_finalize_n(n)?.is_some() {
                    return Err(RayexecError::new("Not all states completed"));
                }
            }

            return Ok(None);
        }
    };

    let len = first.len();
    let mut arrays = Vec::new();
    arrays.push(first);

    for state in iter {
        match state.drain_finalize_n(n)? {
            Some(arr) => {
                if arr.len() != len {
                    return Err(RayexecError::new("Drained arrays differ in length"));
                }
                arrays.push(arr);
            }
            None => return Err(RayexecError::new("Draining completed early for state")),
        }
    }

    Ok(Some(arrays))
}

mod helpers {
    use std::vec;

    use rayexec_bullet::{
        array::{Array, PrimitiveArray, TimestampArray},
        bitmap::Bitmap,
        datatype::TimeUnit,
        executor::aggregate::{AggregateState, StateFinalizer, UnaryNonNullUpdater},
    };

    macro_rules! create_single_boolean_input_grouped_state {
        ($state:ty) => {{
            use crate::functions::aggregate::DefaultGroupedStates;
            use rayexec_bullet::array::{Array, BooleanArray, BooleanValuesBuffer};
            use rayexec_bullet::bitmap::Bitmap;
            use rayexec_bullet::executor::aggregate::{StateFinalizer, UnaryNonNullUpdater};
            use std::vec;

            Box::new(DefaultGroupedStates::new(
                |row_selection: &Bitmap,
                 arrays: &[&Array],
                 mapping: &[usize],
                 states: &mut [$state]| {
                    match &arrays[0] {
                        Array::Boolean(arr) => {
                            UnaryNonNullUpdater::update(row_selection, arr, mapping, states)
                        }
                        other => panic!("unexpected array type: {other:?}"),
                    }
                },
                |states: vec::Drain<$state>| {
                    let mut values = BooleanValuesBuffer::with_capacity(states.len());
                    let mut bitmap = Bitmap::with_capacity(states.len());
                    StateFinalizer::finalize(states, &mut values, &mut bitmap)?;
                    Ok(Array::Boolean(BooleanArray::new(values, Some(bitmap))))
                },
            ))
        }};
    }
    pub(crate) use create_single_boolean_input_grouped_state;

    macro_rules! create_single_primitive_input_grouped_state {
        ($variant:ident, $state:ty) => {{
            use crate::functions::aggregate::DefaultGroupedStates;
            use rayexec_bullet::array::{Array, PrimitiveArray};
            use rayexec_bullet::bitmap::Bitmap;
            use rayexec_bullet::executor::aggregate::{StateFinalizer, UnaryNonNullUpdater};
            use std::vec;

            Box::new(DefaultGroupedStates::new(
                |row_selection: &Bitmap,
                 arrays: &[&Array],
                 mapping: &[usize],
                 states: &mut [$state]| {
                    match &arrays[0] {
                        Array::$variant(arr) => {
                            UnaryNonNullUpdater::update(row_selection, arr, mapping, states)
                        }
                        other => panic!("unexpected array type: {other:?}"),
                    }
                },
                |states: vec::Drain<$state>| {
                    let mut buffer = Vec::with_capacity(states.len());
                    let mut bitmap = Bitmap::with_capacity(states.len());
                    StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
                    Ok(Array::$variant(PrimitiveArray::new(buffer, Some(bitmap))))
                },
            ))
        }};
    }
    pub(crate) use create_single_primitive_input_grouped_state;

    macro_rules! create_single_decimal_input_grouped_state {
        ($variant:ident, $state:ty, $precision:expr, $scale:expr) => {{
            use crate::functions::aggregate::DefaultGroupedStates;
            use rayexec_bullet::array::{Array, DecimalArray, PrimitiveArray};
            use rayexec_bullet::bitmap::Bitmap;
            use rayexec_bullet::executor::aggregate::{StateFinalizer, UnaryNonNullUpdater};
            use std::vec;

            let precision = $precision.clone();
            let scale = $scale.clone();
            Box::new(DefaultGroupedStates::new(
                |row_selection: &Bitmap,
                 arrays: &[&Array],
                 mapping: &[usize],
                 states: &mut [$state]| {
                    match &arrays[0] {
                        Array::$variant(arr) => UnaryNonNullUpdater::update(
                            row_selection,
                            arr.get_primitive(),
                            mapping,
                            states,
                        ),
                        other => panic!("unexpected array type: {other:?}"),
                    }
                },
                move |states: vec::Drain<$state>| {
                    let mut buffer = Vec::with_capacity(states.len());
                    let mut bitmap = Bitmap::with_capacity(states.len());
                    StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
                    let arr = PrimitiveArray::new(buffer, Some(bitmap));
                    Ok(Array::$variant(DecimalArray::new(precision, scale, arr)))
                },
            ))
        }};
    }
    pub(crate) use create_single_decimal_input_grouped_state;

    use super::{DefaultGroupedStates, GroupedStates};

    pub(crate) fn create_single_timestamp_input_grouped_state<
        S: AggregateState<i64, i64> + Send + 'static,
    >(
        unit: TimeUnit,
    ) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(
            |row_selection: &Bitmap, arrays: &[&Array], mapping: &[usize], states: &mut [S]| {
                match &arrays[0] {
                    Array::Timestamp(arr) => UnaryNonNullUpdater::update(
                        row_selection,
                        arr.get_primitive(),
                        mapping,
                        states,
                    ),
                    other => panic!("unexpected array type: {other:?}"),
                }
            },
            move |states: vec::Drain<S>| {
                let mut buffer = Vec::with_capacity(states.len());
                let mut bitmap = Bitmap::with_capacity(states.len());
                StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
                let arr = PrimitiveArray::new(buffer, Some(bitmap));
                Ok(Array::Timestamp(TimestampArray::new(unit, arr)))
            },
        ))
    }
}
