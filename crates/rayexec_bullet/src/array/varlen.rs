use rayexec_error::{RayexecError, Result};

use crate::bitmap::Bitmap;
use crate::storage::PrimitiveStorage;
use std::marker::PhantomData;
use std::{borrow::Cow, fmt::Debug};

use super::{is_valid, ArrayAccessor, ValuesBuffer};

/// Trait for determining how to interpret binary data stored in a variable
/// length array.
pub trait VarlenType: PartialEq + PartialOrd {
    /// Interpret some binary input into an output type.
    fn interpret(input: &[u8]) -> &Self;

    /// Convert self into binary.
    fn as_binary(&self) -> &[u8];
}

impl VarlenType for [u8] {
    fn interpret(input: &[u8]) -> &Self {
        input
    }

    fn as_binary(&self) -> &[u8] {
        self
    }
}

impl VarlenType for str {
    fn interpret(input: &[u8]) -> &Self {
        std::str::from_utf8(input).expect("input should be valid utf8")
    }

    fn as_binary(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Helper trait to convert types into references we can use when building a
/// varlen array.
pub trait AsVarlenType {
    type AsType: VarlenType + ?Sized;
    fn as_varlen_type(&self) -> &Self::AsType;
}

impl AsVarlenType for String {
    type AsType = str;
    fn as_varlen_type(&self) -> &Self::AsType {
        self.as_str()
    }
}

impl AsVarlenType for &String {
    type AsType = str;
    fn as_varlen_type(&self) -> &Self::AsType {
        self.as_str()
    }
}

impl<'a> AsVarlenType for Cow<'a, str> {
    type AsType = str;
    fn as_varlen_type(&self) -> &Self::AsType {
        self.as_ref()
    }
}

impl AsVarlenType for Vec<u8> {
    type AsType = [u8];
    fn as_varlen_type(&self) -> &Self::AsType {
        self.as_slice()
    }
}

impl<'a> AsVarlenType for Cow<'a, [u8]> {
    type AsType = [u8];
    fn as_varlen_type(&self) -> &Self::AsType {
        self.as_ref()
    }
}

impl<'a, T: VarlenType + ?Sized> AsVarlenType for &'a T {
    type AsType = T;
    fn as_varlen_type(&self) -> &Self::AsType {
        self
    }
}

pub trait OffsetIndex {
    fn as_usize(&self) -> usize;
    fn from_usize(u: usize) -> Self;
}

impl OffsetIndex for i32 {
    fn as_usize(&self) -> usize {
        (*self).try_into().expect("index to be positive")
    }

    fn from_usize(u: usize) -> Self {
        u as Self
    }
}

impl OffsetIndex for i64 {
    fn as_usize(&self) -> usize {
        (*self).try_into().expect("index to be positive")
    }

    fn from_usize(u: usize) -> Self {
        u as Self
    }
}

#[derive(Debug)]
pub struct VarlenValuesBuffer<O: OffsetIndex> {
    offsets: Vec<O>,
    data: Vec<u8>,
}

impl<O: OffsetIndex> VarlenValuesBuffer<O> {
    pub fn try_new(data: Vec<u8>, offsets: Vec<O>) -> Result<Self> {
        if data.len() != offsets.len() + 1 {
            return Err(RayexecError::new("Invalid buffer lengths"));
        }
        Ok(VarlenValuesBuffer { offsets, data })
    }

    pub fn into_data_and_offsets(self) -> (Vec<u8>, Vec<O>) {
        (self.data, self.offsets)
    }
}

impl<A: AsVarlenType, O: OffsetIndex> ValuesBuffer<A> for VarlenValuesBuffer<O> {
    fn push_value(&mut self, value: A) {
        self.data.extend(value.as_varlen_type().as_binary());
        let offset = self.data.len();
        self.offsets.push(O::from_usize(offset));
    }

    fn push_null(&mut self) {
        let offset = self.data.len();
        self.offsets.push(O::from_usize(offset));
    }
}

impl<O: OffsetIndex> Default for VarlenValuesBuffer<O> {
    fn default() -> Self {
        let offsets: Vec<O> = vec![O::from_usize(0)];
        let data: Vec<u8> = Vec::new();
        VarlenValuesBuffer { offsets, data }
    }
}

impl<A: AsVarlenType, O: OffsetIndex> FromIterator<A> for VarlenValuesBuffer<O> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut buf = Self::default();

        for v in iter {
            buf.push_value(v);
        }

        buf
    }
}

#[derive(Debug)]
pub struct VarlenArray<T: VarlenType + ?Sized, O: OffsetIndex> {
    /// Value validities.
    validity: Option<Bitmap>,

    /// Offsets into the data buffer.
    ///
    /// Length should be one more than the number of values being held in this
    /// array.
    offsets: PrimitiveStorage<O>,

    /// The raw data.
    data: PrimitiveStorage<u8>,

    /// How to interpret the binary data.
    varlen_type: PhantomData<T>,
}

pub type Utf8Array = VarlenArray<str, i32>;
pub type LargeUtf8Array = VarlenArray<str, i64>;
pub type BinaryArray = VarlenArray<[u8], i32>;
pub type LargeBinaryArray = VarlenArray<[u8], i64>;

impl<T, O> VarlenArray<T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    pub fn new_nulls(len: usize) -> Self {
        let values = VarlenValuesBuffer::default();
        let validity = Bitmap::all_false(len);
        Self::new(values, Some(validity))
    }

    pub fn new(values: VarlenValuesBuffer<O>, validity: Option<Bitmap>) -> Self {
        VarlenArray {
            validity,
            offsets: values.offsets.into(),
            data: values.data.into(),
            varlen_type: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.as_ref().len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, idx: usize) -> Option<&T> {
        if idx >= self.len() {
            return None;
        }

        let offset = self
            .offsets
            .as_ref()
            .get(idx)
            .expect("offset for idx to exist")
            .as_usize();
        let len: usize = self
            .offsets
            .as_ref()
            .get(idx + 1)
            .expect("offset for idx+1 to exist")
            .as_usize();

        let val = self
            .data
            .as_ref()
            .get(offset..len)
            .expect("value to exist in data array");
        let val = T::interpret(val);

        Some(val)
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(is_valid(self.validity.as_ref(), idx))
    }

    /// Get a reference to the underlying validity bitmap.
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    pub fn values_iter(&self) -> VarlenArrayIter<'_, T, O> {
        VarlenArrayIter { idx: 0, arr: self }
    }

    /// Get a reference to the raw data buffer.
    pub(crate) fn data(&self) -> &PrimitiveStorage<u8> {
        &self.data
    }
}

impl<O: OffsetIndex> From<Vec<String>> for VarlenArray<str, O> {
    fn from(value: Vec<String>) -> Self {
        Self::from_iter(value.iter().map(|s| s.as_str()))
    }
}

impl<O: OffsetIndex> From<Vec<Option<String>>> for VarlenArray<str, O> {
    fn from(value: Vec<Option<String>>) -> Self {
        Self::from_iter(value)
    }
}

impl<'a, A: VarlenType + ?Sized, O: OffsetIndex> FromIterator<&'a A> for VarlenArray<A, O> {
    fn from_iter<T: IntoIterator<Item = &'a A>>(iter: T) -> Self {
        let buffer = VarlenValuesBuffer::from_iter(iter);
        VarlenArray::new(buffer, None)
    }
}

impl<'a, A: VarlenType + ?Sized, O: OffsetIndex> FromIterator<Option<&'a A>> for VarlenArray<A, O> {
    fn from_iter<T: IntoIterator<Item = Option<&'a A>>>(iter: T) -> Self {
        let mut validity = Bitmap::default();
        let mut values = VarlenValuesBuffer::default();

        for item in iter {
            match item {
                Some(value) => {
                    validity.push(true);
                    values.push_value(value);
                }
                None => {
                    validity.push(false);
                    values.push_value("");
                }
            }
        }

        VarlenArray::new(values, Some(validity))
    }
}

impl<O: OffsetIndex> FromIterator<String> for VarlenArray<str, O> {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        let buffer = VarlenValuesBuffer::from_iter(iter);
        VarlenArray::new(buffer, None)
    }
}

impl<O: OffsetIndex> FromIterator<Option<String>> for VarlenArray<str, O> {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        let mut validity = Bitmap::default();
        let mut values = VarlenValuesBuffer::default();

        for item in iter {
            match item {
                Some(value) => {
                    validity.push(true);
                    values.push_value(value);
                }
                None => {
                    validity.push(false);
                    values.push_value("");
                }
            }
        }

        VarlenArray::new(values, Some(validity))
    }
}

#[derive(Debug)]
pub struct VarlenArrayIter<'a, T: VarlenType + ?Sized, O: OffsetIndex> {
    idx: usize,
    arr: &'a VarlenArray<T, O>,
}

impl<'a, T, O> Iterator for VarlenArrayIter<'a, T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let val = self.arr.value(self.idx);
        self.idx += 1;
        val
    }
}

impl<T, O> PartialEq for VarlenArray<T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        // TODO: Validity check

        let left = self.values_iter();
        let right = other.values_iter();

        left.zip(right).all(|(left, right)| left == right)
    }
}

impl<'a, T, O> ArrayAccessor<&'a T> for &'a VarlenArray<T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    type ValueIter = VarlenArrayIter<'a, T, O>;

    fn len(&self) -> usize {
        self.offsets.as_ref().len() - 1
    }

    fn values_iter(&self) -> Self::ValueIter {
        VarlenArrayIter { idx: 0, arr: self }
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }
}
