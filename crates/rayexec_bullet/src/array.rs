use crate::bitmap::Bitmap;
use crate::datatype::DataType;
use crate::scalar::ScalarValue;
use crate::storage::PrimitiveStorage;
use std::fmt::Debug;
use std::marker::PhantomData;

#[derive(Debug, PartialEq)]
pub enum Array {
    Null(NullArray),
    Boolean(BooleanArray),
    Float32(Float32Array),
    Float64(Float64Array),
    Int8(Int8Array),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    UInt8(UInt8Array),
    UInt16(UInt16Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
    Utf8(Utf8Array),
    LargeUtf8(LargeUtf8Array),
    Binary(BinaryArray),
    LargeBinary(LargeBinaryArray),
}

impl Array {
    pub fn datatype(&self) -> DataType {
        match self {
            Array::Null(_) => DataType::Null,
            Array::Boolean(_) => DataType::Boolean,
            Array::Float32(_) => DataType::Float32,
            Array::Float64(_) => DataType::Float64,
            Array::Int8(_) => DataType::Int8,
            Array::Int16(_) => DataType::Int16,
            Array::Int32(_) => DataType::Int32,
            Array::Int64(_) => DataType::Int64,
            Array::UInt8(_) => DataType::UInt8,
            Array::UInt16(_) => DataType::UInt16,
            Array::UInt32(_) => DataType::UInt32,
            Array::UInt64(_) => DataType::UInt64,
            Array::Utf8(_) => DataType::Utf8,
            Array::LargeUtf8(_) => DataType::LargeUtf8,
            Array::Binary(_) => DataType::Binary,
            Array::LargeBinary(_) => DataType::LargeBinary,
        }
    }

    /// Get a scalar value at the given index.
    pub fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        if !self.is_valid(idx)? {
            return Some(ScalarValue::Null);
        }

        Some(match self {
            Self::Null(_) => panic!("nulls should be handled by validity check"),
            Self::Boolean(arr) => ScalarValue::Boolean(arr.value(idx)?),
            Self::Float32(arr) => ScalarValue::Float32(*arr.value(idx)?),
            Self::Float64(arr) => ScalarValue::Float64(*arr.value(idx)?),
            Self::Int8(arr) => ScalarValue::Int8(*arr.value(idx)?),
            Self::Int16(arr) => ScalarValue::Int16(*arr.value(idx)?),
            Self::Int32(arr) => ScalarValue::Int32(*arr.value(idx)?),
            Self::Int64(arr) => ScalarValue::Int64(*arr.value(idx)?),
            Self::UInt8(arr) => ScalarValue::UInt8(*arr.value(idx)?),
            Self::UInt16(arr) => ScalarValue::UInt16(*arr.value(idx)?),
            Self::UInt32(arr) => ScalarValue::UInt32(*arr.value(idx)?),
            Self::UInt64(arr) => ScalarValue::UInt64(*arr.value(idx)?),
            Self::Utf8(arr) => ScalarValue::Utf8(arr.value(idx)?.into()),
            Self::LargeUtf8(arr) => ScalarValue::Utf8(arr.value(idx)?.into()),
            Self::Binary(arr) => ScalarValue::Binary(arr.value(idx)?.into()),
            Self::LargeBinary(arr) => ScalarValue::LargeBinary(arr.value(idx)?.into()),
        })
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        match self {
            Self::Null(arr) => arr.is_valid(idx),
            Self::Boolean(arr) => arr.is_valid(idx),
            Self::Float32(arr) => arr.is_valid(idx),
            Self::Float64(arr) => arr.is_valid(idx),
            Self::Int8(arr) => arr.is_valid(idx),
            Self::Int16(arr) => arr.is_valid(idx),
            Self::Int32(arr) => arr.is_valid(idx),
            Self::Int64(arr) => arr.is_valid(idx),
            Self::UInt8(arr) => arr.is_valid(idx),
            Self::UInt16(arr) => arr.is_valid(idx),
            Self::UInt32(arr) => arr.is_valid(idx),
            Self::UInt64(arr) => arr.is_valid(idx),
            Self::Utf8(arr) => arr.is_valid(idx),
            Self::LargeUtf8(arr) => arr.is_valid(idx),
            Self::Binary(arr) => arr.is_valid(idx),
            Self::LargeBinary(arr) => arr.is_valid(idx),
        }
    }
}

/// A logical array for representing some number of Nulls.
#[derive(Debug, PartialEq)]
pub struct NullArray {
    len: usize,
}

impl NullArray {
    pub fn new(len: usize) -> Self {
        NullArray { len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len {
            return None;
        }
        Some(false)
    }
}

/// A logical array for representing bools.
#[derive(Debug, PartialEq)]
pub struct BooleanArray {
    validity: Option<Bitmap>,
    values: Bitmap,
}

impl BooleanArray {
    pub fn new_with_values(values: Bitmap) -> Self {
        BooleanArray {
            validity: None,
            values,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        let valid = self
            .validity
            .as_ref()
            .map(|bm| bm.value(idx))
            .unwrap_or(true);

        Some(valid)
    }

    pub fn value(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.values.value(idx))
    }

    pub(crate) fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    pub(crate) fn values(&self) -> &Bitmap {
        &self.values
    }
}

impl FromIterator<bool> for BooleanArray {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        Self::new_with_values(Bitmap::from_iter(iter))
    }
}

impl FromIterator<Option<bool>> for BooleanArray {
    fn from_iter<T: IntoIterator<Item = Option<bool>>>(iter: T) -> Self {
        let mut validity = Bitmap::default();
        let mut bools = Bitmap::default();

        for item in iter {
            match item {
                Some(value) => {
                    validity.push(true);
                    bools.push(value);
                }
                None => {
                    validity.push(false);
                    bools.push(false);
                }
            }
        }

        BooleanArray {
            validity: Some(validity),
            values: bools,
        }
    }
}

pub trait PrimitiveNumeric: Sized {
    const MIN_VALUE: Self;
    const MAX_VALUE: Self;
    const ZERO_VALUE: Self;

    fn from_str(s: &str) -> Option<Self>;
}

impl PrimitiveNumeric for i8 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for i16 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for i32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for i64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for u8 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for u16 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for u32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for u64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for f32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0.0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for f64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0.0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

/// Array for storing primitive values.
#[derive(Debug, PartialEq)]
pub struct PrimitiveArray<T> {
    /// Validity bitmap.
    ///
    /// "True" values indicate the value at index is valid, "false" indicates
    /// null.
    validity: Option<Bitmap>,

    /// Underlying primitive values.
    values: PrimitiveStorage<T>,
}

pub type Int8Array = PrimitiveArray<i8>;
pub type Int16Array = PrimitiveArray<i16>;
pub type Int32Array = PrimitiveArray<i32>;
pub type Int64Array = PrimitiveArray<i64>;
pub type UInt8Array = PrimitiveArray<u8>;
pub type UInt16Array = PrimitiveArray<u16>;
pub type UInt32Array = PrimitiveArray<u32>;
pub type UInt64Array = PrimitiveArray<u64>;
pub type Float32Array = PrimitiveArray<f32>;
pub type Float64Array = PrimitiveArray<f64>;

impl<T> PrimitiveArray<T> {
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Get the value at the given index.
    ///
    /// This does not take validity into account.
    pub fn value(&self, idx: usize) -> Option<&T> {
        if idx >= self.len() {
            return None;
        }

        self.values.get(idx)
    }

    /// Get the validity at the given index.
    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(is_valid(self.validity.as_ref(), idx))
    }

    /// Get a reference to the underlying validity bitmap.
    pub(crate) fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Get a reference to the underlying primitive values.
    pub(crate) fn values(&self) -> &PrimitiveStorage<T> {
        &self.values
    }

    /// Get a mutable reference to the underlying primitive values.
    pub(crate) fn values_mut(&mut self) -> &mut PrimitiveStorage<T> {
        &mut self.values
    }
}

impl<A> FromIterator<A> for PrimitiveArray<A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let values = PrimitiveStorage::from(iter.into_iter().collect::<Vec<_>>());
        PrimitiveArray {
            validity: None,
            values,
        }
    }
}

impl<A: Default> FromIterator<Option<A>> for PrimitiveArray<A> {
    fn from_iter<T: IntoIterator<Item = Option<A>>>(iter: T) -> Self {
        let mut validity = Bitmap::default();
        let mut values = Vec::new();

        for item in iter {
            match item {
                Some(value) => {
                    validity.push(true);
                    values.push(value);
                }
                None => {
                    validity.push(false);
                    values.push(A::default());
                }
            }
        }

        PrimitiveArray {
            validity: Some(validity),
            values: values.into(),
        }
    }
}

impl<T> From<Vec<T>> for PrimitiveArray<T> {
    fn from(value: Vec<T>) -> Self {
        PrimitiveArray {
            values: PrimitiveStorage::Vec(value),
            validity: None,
        }
    }
}

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
        self.offsets.len() - 1
    }

    pub fn value(&self, idx: usize) -> Option<&T> {
        if idx >= self.len() {
            return None;
        }

        let offset = self
            .offsets
            .get(idx)
            .expect("offset for idx to exist")
            .as_usize();
        let len: usize = self
            .offsets
            .get(idx + 1)
            .expect("offset for idx+1 to exist")
            .as_usize();

        let val = self
            .data
            .get_slice(offset, len)
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
    pub(crate) fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    pub fn values_iter(&self) -> VarlenArrayIter<'_, T, O> {
        VarlenArrayIter { idx: 0, arr: self }
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

/// Helper for determining if a value at a given index should be considered
/// valid.
///
/// If the bitmap is None, it's assumed that all values, regardless of the
/// index, are valid.
///
/// Panics if index is out of bounds.
fn is_valid(validity: Option<&Bitmap>, idx: usize) -> bool {
    validity.map(|bm| bm.value(idx)).unwrap_or(true)
}
