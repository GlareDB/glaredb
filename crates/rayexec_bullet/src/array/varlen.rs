use crate::bitmap::Bitmap;
use crate::storage::PrimitiveStorage;
use std::marker::PhantomData;
use std::{borrow::Cow, fmt::Debug};

use super::{is_valid, ArrayAccessor, ArrayBuilder};

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

impl AsVarlenType for &str {
    type AsType = str;
    fn as_varlen_type(&self) -> &Self::AsType {
        self
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

impl AsVarlenType for &[u8] {
    type AsType = [u8];
    fn as_varlen_type(&self) -> &Self::AsType {
        self
    }
}

impl<'a> AsVarlenType for Cow<'a, [u8]> {
    type AsType = [u8];
    fn as_varlen_type(&self) -> &Self::AsType {
        self.as_ref()
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
    pub fn len(&self) -> usize {
        self.offsets.as_ref().len() - 1
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

    pub(crate) fn put_validity(&mut self, validity: Bitmap) {
        assert_eq!(validity.len(), self.len());
        self.validity = Some(validity);
    }
}

impl<'a, A: VarlenType + ?Sized, O: OffsetIndex> FromIterator<&'a A> for VarlenArray<A, O> {
    fn from_iter<T: IntoIterator<Item = &'a A>>(iter: T) -> Self {
        let mut offsets: Vec<O> = vec![O::from_usize(0)];
        let mut data: Vec<u8> = Vec::new();

        for item in iter.into_iter() {
            data.extend(item.as_binary());
            let offset = data.len();
            offsets.push(O::from_usize(offset));
        }

        let offsets = PrimitiveStorage::from(offsets);
        let data = PrimitiveStorage::from(data);

        VarlenArray {
            validity: None,
            offsets,
            data,
            varlen_type: PhantomData,
        }
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

#[derive(Debug)]
pub struct VarlenArrayBuilder<A, O>
where
    A: AsVarlenType,
    O: OffsetIndex,
{
    validity: Option<Bitmap>,
    offsets: Vec<O>,
    data: Vec<u8>,
    varlen_type: PhantomData<A>,
}

impl<'a, A: AsVarlenType, O: OffsetIndex> VarlenArrayBuilder<A, O> {
    // TODO: With capacity
    pub fn new() -> Self {
        VarlenArrayBuilder {
            validity: None,
            offsets: vec![O::from_usize(0)],
            data: Vec::new(),
            varlen_type: PhantomData,
        }
    }

    pub fn into_typed_array(self) -> VarlenArray<A::AsType, O> {
        VarlenArray {
            validity: self.validity,
            offsets: self.offsets.into(),
            data: self.data.into(),
            varlen_type: PhantomData,
        }
    }
}

impl<'a, A: AsVarlenType, O: OffsetIndex> ArrayBuilder<A> for VarlenArrayBuilder<A, O> {
    fn push_value(&mut self, value: A) {
        self.data.extend(value.as_varlen_type().as_binary());
        let offset = self.data.len();
        self.offsets.push(O::from_usize(offset));
    }

    fn put_validity(&mut self, validity: Bitmap) {
        self.validity = Some(validity)
    }
}
