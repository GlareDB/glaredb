use glaredb_error::{DbError, Result};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI64,
    PhysicalU64,
};
use crate::arrays::compute::hash::hash_array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::PutBuffer;
use crate::arrays::executor::aggregate::AggregateState;
use crate::buffer::buffer_manager::DefaultBufferManager;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::{AggregateFunction, RawAggregateFunction};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::statistics::hll::HyperLogLog;

pub const FUNCTION_SET_APPROX_COUNT_DISTINCT: AggregateFunctionSet = AggregateFunctionSet {
    name: "approx_count_distinct",
    aliases: &["approx_unique"],
    doc: &[&Documentation {
        category: Category::APPROXIMATE_AGGREGATE,
        description: r#"
Return an estimated number of distinct, non-NULL values in the input. This is an
approximate version of `COUNT(DISTINCT ...)`.

Internally, this uses a HyperLogLog sketch and yields roughly a 1.6% relative
error.
"#,
        arguments: &["input"],
        example: None,
    }],
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Any], DataTypeId::Int64),
        &ApproxCountDistinct,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct ApproxCountDistinct;

impl AggregateFunction for ApproxCountDistinct {
    type BindState = ();
    type GroupState = ApproxDistinctState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int64(),
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        ApproxDistinctState {
            hll: HyperLogLog::new(HyperLogLog::DEFAULT_P),
        }
    }

    fn update(
        _state: &Self::BindState,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::GroupState],
    ) -> Result<()> {
        let input = &inputs[0];
        if num_rows != states.len() {
            return Err(DbError::new(
                "Invalid number of states for selection in count agggregate executor",
            )
            .with_field("num_rows", num_rows)
            .with_field("states_len", states.len()));
        }

        let validity = &input.validity;

        let mut hashes_arr = Array::new(&DefaultBufferManager, DataType::uint64(), num_rows)?;
        let hashes = PhysicalU64::buffer_downcast_mut(&mut hashes_arr.data)?;
        let hashes = &mut hashes.buffer.as_slice_mut()[0..num_rows];
        hash_array(input, 0..num_rows, hashes)?;

        // Only update update states for non-null input values.
        for (idx, (&mut state_ptr, hash)) in states.iter_mut().zip(hashes).enumerate() {
            if !validity.is_valid(idx) {
                continue;
            }
            let state = unsafe { &mut *state_ptr };
            state.hll.insert(*hash);
        }

        Ok(())
    }

    fn combine(
        _state: &Self::BindState,
        src: &mut [&mut Self::GroupState],
        dest: &mut [&mut Self::GroupState],
    ) -> Result<()> {
        if src.len() != dest.len() {
            return Err(
                DbError::new("Source and destination have different number of states")
                    .with_field("source", src.len())
                    .with_field("dest", dest.len()),
            );
        }

        for (src, dest) in src.iter_mut().zip(dest) {
            dest.merge(&(), src)?;
        }

        Ok(())
    }

    fn finalize(
        _state: &Self::BindState,
        states: &mut [&mut Self::GroupState],
        output: &mut Array,
    ) -> Result<()> {
        let buffer = &mut PhysicalI64::get_addressable_mut(&mut output.data)?;
        let validity = &mut output.validity;

        for (idx, state) in states.iter_mut().enumerate() {
            state.finalize(&(), PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ApproxDistinctState {
    hll: HyperLogLog,
}

impl AggregateState<&u64, i64> for ApproxDistinctState {
    type BindState = ();

    fn merge(&mut self, _state: &Self::BindState, other: &mut Self) -> Result<()> {
        self.hll.merge(&other.hll);
        Ok(())
    }

    fn update(&mut self, _state: &Self::BindState, &input: &u64) -> Result<()> {
        self.hll.insert(input);
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &Self::BindState, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = i64>,
    {
        let est = self.hll.count();
        output.put(&(est as i64));
        Ok(())
    }
}
