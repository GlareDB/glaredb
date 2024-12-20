use rayexec_error::{not_implemented, RayexecError, Result};

use super::array::Array;
use super::buffer::physical_type::{PhysicalI32, PhysicalI8, PhysicalType, PhysicalUtf8};
use super::buffer::reservation::{NopReservationTracker, ReservationTracker};
use crate::compute::util::IntoExactSizedIterator;
use crate::datatype::DataType;
use crate::exp::buffer::ArrayBuffer;

/// Collection of same length arrays.
pub struct Batch<R: ReservationTracker = NopReservationTracker> {
    pub(crate) arrays: Vec<Array<R>>,
    pub(crate) capacity: usize,
    pub(crate) len: usize,
}

impl<R> Batch<R>
where
    R: ReservationTracker,
{
    pub fn new(
        tracker: &R,
        datatypes: impl IntoExactSizedIterator<Item = DataType>,
        capacity: usize,
    ) -> Result<Self> {
        let datatypes = datatypes.into_iter();
        let mut arrays = Vec::with_capacity(datatypes.len());

        for datatype in datatypes {
            let buffer = init_array_buffer(tracker, &datatype, capacity)?;
            let array = Array::new(datatype, buffer);
            arrays.push(array)
        }

        Ok(Batch {
            arrays,
            capacity,
            len: 0,
        })
    }

    pub fn get_array(&self, idx: usize) -> Result<&Array<R>> {
        self.get_array_opt(idx)
            .ok_or_else(|| RayexecError::new("Missing array").with_field("idx", idx))
    }

    pub fn get_array_opt(&self, idx: usize) -> Option<&Array<R>> {
        self.arrays.get(idx)
    }

    pub fn get_array_mut(&mut self, idx: usize) -> Result<&mut Array<R>> {
        self.get_array_mut_opt(idx)
            .ok_or_else(|| RayexecError::new("Missing array").with_field("idx", idx))
    }

    pub fn get_array_mut_opt(&mut self, idx: usize) -> Option<&mut Array<R>> {
        self.arrays.get_mut(idx)
    }

    pub fn num_rows(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

fn init_array_buffer<R>(tracker: &R, datatype: &DataType, cap: usize) -> Result<ArrayBuffer<R>>
where
    R: ReservationTracker,
{
    // TODO: Child buffers.
    match datatype.exp_physical_type() {
        PhysicalType::Int8 => ArrayBuffer::with_len::<PhysicalI8>(tracker, cap),
        PhysicalType::Int32 => ArrayBuffer::with_len::<PhysicalI32>(tracker, cap),
        PhysicalType::Utf8 => ArrayBuffer::with_len::<PhysicalUtf8>(tracker, cap),
        other => not_implemented!("init array buffer: {other}"),
    }
}
