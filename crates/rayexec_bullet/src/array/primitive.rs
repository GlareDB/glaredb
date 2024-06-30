use crate::bitmap::Bitmap;
use crate::scalar::interval::Interval;
use crate::storage::PrimitiveStorage;

use super::{is_valid, ArrayAccessor};

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
pub type Int128Array = PrimitiveArray<i128>;
pub type UInt8Array = PrimitiveArray<u8>;
pub type UInt16Array = PrimitiveArray<u16>;
pub type UInt32Array = PrimitiveArray<u32>;
pub type UInt64Array = PrimitiveArray<u64>;
pub type UInt128Array = PrimitiveArray<u128>;
pub type Float32Array = PrimitiveArray<f32>;
pub type Float64Array = PrimitiveArray<f64>;
pub type TimestampSecondsArray = PrimitiveArray<i64>;
pub type TimestampMillsecondsArray = PrimitiveArray<i64>;
pub type TimestampMicrosecondsArray = PrimitiveArray<i64>;
pub type TimestampNanosecondsArray = PrimitiveArray<i64>;
pub type Date32Array = PrimitiveArray<i32>;
pub type Date64Array = PrimitiveArray<i64>;
pub type IntervalArray = PrimitiveArray<Interval>;

impl<T: Default + Clone> PrimitiveArray<T> {
    pub fn new_nulls(len: usize) -> Self {
        let values = vec![T::default(); len];
        let validity = Bitmap::all_false(len);
        Self::new(values, Some(validity))
    }
}

impl<T> PrimitiveArray<T> {
    pub fn new(values: Vec<T>, validity: Option<Bitmap>) -> Self {
        PrimitiveArray {
            values: values.into(),
            validity,
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

/// Wrapper around a primitive array for storing the precision+scale for a
/// decimal type.
#[derive(Debug, PartialEq)]
pub struct DecimalArray<T> {
    precision: u8,
    scale: i8,
    array: PrimitiveArray<T>,
}

pub type Decimal64Array = DecimalArray<i64>;
pub type Decimal128Array = DecimalArray<i128>;

impl<T> DecimalArray<T> {
    pub fn new(precision: u8, scale: i8, array: PrimitiveArray<T>) -> Self {
        DecimalArray {
            precision,
            scale,
            array,
        }
    }

    pub fn get_primitive(&self) -> &PrimitiveArray<T> {
        &self.array
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }
}
