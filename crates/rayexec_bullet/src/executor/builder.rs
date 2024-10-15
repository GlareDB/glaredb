use std::marker::PhantomData;
use std::sync::Arc;

use super::physical_type::{AsBytes, VarlenType};
use crate::array::{ArrayData, BinaryData};
use crate::bitmap::Bitmap;
use crate::datatype::DataType;
use crate::storage::{
    BooleanStorage,
    GermanVarlenStorage,
    PrimitiveStorage,
    UnionedGermanMetadata,
    INLINE_THRESHOLD,
};

#[derive(Debug)]
pub struct ArrayBuilder<B> {
    pub datatype: DataType,
    pub buffer: B,
}

/// Small wrapper containing the array buffer we're building up and an index for
/// where to write a value to.
#[derive(Debug)]
pub struct OutputBuffer<B> {
    /// Index in the buffer we're writing to.
    ///
    /// This index corresponds to the logical index in the input arrays, and the
    /// physical (and logical) index in the output array.
    pub(crate) idx: usize,
    /// The buffer itself.
    pub(crate) buffer: B,
}

impl<B> OutputBuffer<B>
where
    B: ArrayDataBuffer,
{
    pub fn put(&mut self, val: &B::Type) {
        self.buffer.put(self.idx, val)
    }
}

/// Pre-allocated buffer for arbitrarily putting values into.
pub trait ArrayDataBuffer {
    type Type: ?Sized;

    /// Length of the buffer.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Put a value at `idx`. Guaranteed to be in bounds according to `len`.
    fn put(&mut self, idx: usize, val: &Self::Type);

    /// Convert the buffer into array data.
    fn into_data(self) -> ArrayData;
}

#[derive(Debug)]
pub struct BooleanBuffer {
    pub(crate) values: Bitmap,
}

impl BooleanBuffer {
    pub fn with_len(len: usize) -> Self {
        BooleanBuffer {
            values: Bitmap::new_with_all_false(len),
        }
    }
}

impl ArrayDataBuffer for BooleanBuffer {
    type Type = bool;

    fn len(&self) -> usize {
        self.values.len()
    }

    fn put(&mut self, idx: usize, val: &Self::Type) {
        self.values.set_unchecked(idx, *val)
    }

    fn into_data(self) -> ArrayData {
        ArrayData::Boolean(Arc::new(BooleanStorage(self.values)))
    }
}

#[derive(Debug)]
pub struct PrimitiveBuffer<T> {
    pub(crate) values: Vec<T>,
}

impl<T> PrimitiveBuffer<T>
where
    T: Default + Copy,
    Vec<T>: Into<PrimitiveStorage<T>>,
{
    pub fn with_len(len: usize) -> Self {
        PrimitiveBuffer {
            values: vec![T::default(); len],
        }
    }
}

impl<T> ArrayDataBuffer for PrimitiveBuffer<T>
where
    T: Copy,
    Vec<T>: Into<PrimitiveStorage<T>>,
    ArrayData: From<PrimitiveStorage<T>>,
{
    type Type = T;

    fn len(&self) -> usize {
        self.values.len()
    }

    fn put(&mut self, idx: usize, val: &Self::Type) {
        self.values[idx] = *val
    }

    fn into_data(self) -> ArrayData {
        PrimitiveStorage::from(self.values).into()
    }
}

#[derive(Debug)]
pub struct GermanVarlenBuffer<T: ?Sized> {
    pub(crate) metadata: Vec<UnionedGermanMetadata>,
    pub(crate) data: Vec<u8>,
    pub(crate) _type: PhantomData<T>,
}

impl<T> GermanVarlenBuffer<T>
where
    T: VarlenType + ?Sized,
{
    pub fn with_len(len: usize) -> Self {
        Self::with_len_and_data_capacity(len, 0)
    }

    pub fn with_len_and_data_capacity(len: usize, data_cap: usize) -> Self {
        GermanVarlenBuffer {
            metadata: vec![UnionedGermanMetadata::zero(); len],
            data: Vec::with_capacity(data_cap),
            _type: PhantomData,
        }
    }
}

impl<T> ArrayDataBuffer for GermanVarlenBuffer<T>
where
    T: AsBytes + ?Sized,
{
    type Type = T;

    fn len(&self) -> usize {
        self.metadata.len()
    }

    fn put(&mut self, idx: usize, val: &Self::Type) {
        let val = val.as_bytes();

        if val.len() as i32 <= INLINE_THRESHOLD {
            // Store completely inline.
            let meta = self.metadata[idx].as_small_mut();
            meta.len = val.len() as i32;
            meta.inline[0..val.len()].copy_from_slice(val);
        } else {
            // Store prefix, buf index, and offset in line. Store complete copy
            // in buffer.
            let meta = self.metadata[idx].as_large_mut();
            meta.len = val.len() as i32;

            // Prefix
            meta.prefix.copy_from_slice(&val[0..4]);

            // Buffer index, currently always zero.
            meta.buffer_idx = 0;

            // Offset, 4 bytes
            let offset = self.data.len();
            meta.offset = offset as i32;

            self.data.extend_from_slice(val);
        }
    }

    fn into_data(self) -> ArrayData {
        let storage = GermanVarlenStorage {
            metadata: self.metadata.into(),
            data: self.data.into(),
        };

        ArrayData::Binary(BinaryData::German(Arc::new(storage)))
    }
}
