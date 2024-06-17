use crate::bitmap::Bitmap;
use crate::storage::PrimitiveStorage;
use std::fmt::Debug;

use super::{is_valid, ArrayAccessor, ArrayBuilder};

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
pub type TimestampArray = PrimitiveArray<i64>;

impl<T> PrimitiveArray<T> {
    pub fn new_from_values_and_validity(values: Vec<T>, validity: Bitmap) -> Self {
        assert_eq!(values.len(), validity.len());
        PrimitiveArray {
            values: values.into(),
            validity: Some(validity),
        }
    }

    pub fn len(&self) -> usize {
        self.values.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the value at the given index.
    ///
    /// This does not take validity into account.
    pub fn value(&self, idx: usize) -> Option<&T> {
        if idx >= self.len() {
            return None;
        }

        self.values.as_ref().get(idx)
    }

    /// Get the validity at the given index.
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

    /// Get a reference to the underlying primitive values.
    pub fn values(&self) -> &PrimitiveStorage<T> {
        &self.values
    }

    /// Get a mutable reference to the underlying primitive values.
    pub(crate) fn values_mut(&mut self) -> &mut PrimitiveStorage<T> {
        &mut self.values
    }

    pub fn iter(&self) -> PrimitiveArrayIter<T> {
        PrimitiveArrayIter {
            idx: 0,
            values: self.values.as_ref(),
        }
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

#[derive(Debug)]
pub struct PrimitiveArrayIter<'a, T> {
    idx: usize,
    values: &'a [T],
}

impl<T: Copy> Iterator for PrimitiveArrayIter<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.values.len() {
            None
        } else {
            let val = self.values[self.idx];
            self.idx += 1;
            Some(val)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.values.len() - self.idx,
            Some(self.values.len() - self.idx),
        )
    }
}

impl<'a, T: Copy> ArrayAccessor<T> for &'a PrimitiveArray<T> {
    type ValueIter = PrimitiveArrayIter<'a, T>;

    fn len(&self) -> usize {
        self.values.as_ref().len()
    }

    fn values_iter(&self) -> Self::ValueIter {
        PrimitiveArrayIter {
            idx: 0,
            values: self.values.as_ref(),
        }
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }
}

#[derive(Debug)]
pub struct PrimitiveArrayBuilder<T> {
    values: Vec<T>,
    validity: Option<Bitmap>,
}

impl<T> PrimitiveArrayBuilder<T> {
    pub fn with_capacity(cap: usize) -> Self {
        PrimitiveArrayBuilder {
            values: Vec::with_capacity(cap),
            validity: None,
        }
    }

    pub fn into_typed_array(self) -> PrimitiveArray<T> {
        PrimitiveArray {
            validity: self.validity,
            values: self.values.into(),
        }
    }
}

impl<T> ArrayBuilder<T> for PrimitiveArrayBuilder<T> {
    fn push_value(&mut self, value: T) {
        self.values.push(value);
    }

    fn put_validity(&mut self, validity: Bitmap) {
        self.validity = Some(validity);
    }
}
