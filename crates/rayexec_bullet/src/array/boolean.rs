use crate::bitmap::{Bitmap, BitmapIter};
use std::fmt::Debug;

use super::{ArrayAccessor, ArrayBuilder};

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

    pub fn new_with_values_and_validity(values: Bitmap, validity: Bitmap) -> Self {
        assert_eq!(values.len(), validity.len());
        BooleanArray {
            values,
            validity: Some(validity),
        }
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
            None => self.values.popcnt(),
        }
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    pub fn values(&self) -> &Bitmap {
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

impl ArrayBuilder<bool> for BooleanArrayBuilder {
    fn push_value(&mut self, value: bool) {
        self.values.push(value)
    }

    fn put_validity(&mut self, validity: Bitmap) {
        self.validity = Some(validity)
    }
}
