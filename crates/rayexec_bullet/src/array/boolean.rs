use crate::bitmap::{Bitmap, BitmapIter};
use std::fmt::Debug;

use super::{ArrayAccessor, ValuesBuffer};

#[derive(Debug, PartialEq, Default)]
pub struct BooleanValuesBuffer {
    pub bitmap: Bitmap,
}

impl BooleanValuesBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        BooleanValuesBuffer {
            bitmap: Bitmap::with_capacity(cap),
        }
    }
}

impl ValuesBuffer<bool> for BooleanValuesBuffer {
    fn push_value(&mut self, value: bool) {
        self.bitmap.push(value);
    }

    fn push_null(&mut self) {
        self.bitmap.push(false);
    }
}

impl FromIterator<bool> for BooleanValuesBuffer {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (cap, _) = iter.size_hint();
        let mut buf = Self::with_capacity(cap);

        for v in iter {
            buf.push_value(v);
        }

        buf
    }
}

impl From<BooleanValuesBuffer> for Bitmap {
    fn from(value: BooleanValuesBuffer) -> Self {
        value.bitmap
    }
}

/// A logical array for representing bools.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BooleanArray {
    validity: Option<Bitmap>,
    values: Bitmap,
}

impl BooleanArray {
    pub fn new_nulls(len: usize) -> Self {
        let values = Bitmap::all_false(len);
        let validity = Bitmap::all_false(len);
        Self::new(values, Some(validity))
    }

    pub fn new(values: impl Into<Bitmap>, validity: Option<Bitmap>) -> Self {
        let values = values.into();
        if let Some(validity) = &validity {
            assert_eq!(values.len(), validity.len());
        }

        BooleanArray { values, validity }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    /// Get the number of non-null true values in the array.
    pub fn true_count(&self) -> usize {
        match &self.validity {
            Some(validity) => {
                assert_eq!(validity.len(), self.values.len());
                // TODO: Could probably go byte by byte instead bit by bit.
                self.values
                    .iter()
                    .zip(validity.iter())
                    .fold(
                        0,
                        |acc, (valid, is_true)| if valid && is_true { acc + 1 } else { acc },
                    )
            }
            None => self.values.count_trues(),
        }
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    pub fn values(&self) -> &Bitmap {
        &self.values
    }

    /// Turns this boolean array into a selection bitmap for filtering.
    ///
    /// This will treat invalid (NULL) values as false.
    pub fn into_selection_bitmap(self) -> Bitmap {
        let mut bitmap = self.values;
        if let Some(validity) = self.validity {
            bitmap
                .bit_and_mut(&validity)
                .expect("bool array to bitmap to not fail");
        }
        bitmap
    }
}

impl FromIterator<bool> for BooleanArray {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        Self::new(Bitmap::from_iter(iter), None)
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

impl<'a> ArrayAccessor<bool> for &'a BooleanArray {
    type ValueIter = BitmapIter<'a>;

    fn len(&self) -> usize {
        self.values.len()
    }

    fn values_iter(&self) -> Self::ValueIter {
        self.values.iter()
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }
}

#[derive(Debug)]
pub struct BooleanArrayBuilder {
    values: Bitmap,
    validity: Option<Bitmap>,
}

impl Default for BooleanArrayBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BooleanArrayBuilder {
    pub fn new() -> Self {
        BooleanArrayBuilder {
            values: Bitmap::default(),
            validity: None,
        }
    }

    pub fn into_typed_array(self) -> BooleanArray {
        BooleanArray {
            validity: self.validity,
            values: self.values,
        }
    }
}
