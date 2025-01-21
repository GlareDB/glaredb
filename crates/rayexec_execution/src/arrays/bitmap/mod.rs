pub mod zip;

use std::borrow::BorrowMut;
use std::fmt;

use rayexec_error::{RayexecError, Result};

use crate::arrays::compute::util::IntoExtactSizeIterator;

/// An LSB ordered bitmap.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Bitmap {
    len: usize,
    data: Vec<u8>,
}

impl Bitmap {
    pub fn try_new(data: Vec<u8>, len: usize) -> Result<Self> {
        // TODO: Validite
        Ok(Bitmap { len, data })
    }

    pub fn with_capacity(cap: usize) -> Self {
        Bitmap {
            len: 0,
            data: Vec::with_capacity(cap + 1),
        }
    }

    /// Create a new bitmap of a given length with all values initialized to the
    /// given value.
    pub fn new_with_val(val: bool, len: usize) -> Self {
        if val {
            Self::new_with_all_true(len)
        } else {
            Self::new_with_all_false(len)
        }
    }

    pub fn new_with_all_true(len: usize) -> Self {
        let cap = (len + 7) / 8;
        Bitmap {
            len,
            data: vec![u8::MAX; cap],
        }
    }

    pub fn new_with_all_false(len: usize) -> Self {
        let cap = (len + 7) / 8;
        Bitmap {
            len,
            data: vec![0; cap],
        }
    }

    /// Get the number of bits being tracked by this bitmap.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Check if this bitmap is empty.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Resets all bits in the bitmap to some initial value.
    pub fn reset(&mut self, value: bool) {
        if value {
            self.data.iter_mut().for_each(|v| *v = u8::MAX);
        } else {
            self.data.iter_mut().for_each(|v| *v = 0);
        }
    }

    /// Get the total number of bytes of the underlying data.
    pub fn num_bytes(&self) -> usize {
        self.data.len()
    }

    pub fn count_trues(&self) -> usize {
        let mut count = self
            .data
            .iter()
            .map(|&b| b.count_ones())
            .fold(0, |acc, v| acc + (v as usize));

        // Make sure we're only counting the bits that make up the "logical"
        // portion of the bitmap.
        let rem = self.len % 8;
        if rem != 0 {
            let last = self.data.last().unwrap();
            count -= last.count_ones() as usize;
            let mask = (255 << (8 - rem)) >> (8 - rem);
            count += (mask & last).count_ones() as usize;
        }

        count
    }

    pub fn is_all_true(&self) -> bool {
        self.count_trues() == self.len()
    }

    /// Push a value onto the end of the bitmap.
    pub fn push(&mut self, val: bool) {
        if self.len == self.data.len() * 8 {
            self.data.push(0);
        }
        let idx = self.len;
        self.len += 1;
        self.set_unchecked(idx, val);
    }

    /// Get the value at index.
    ///
    /// Panics if index is out of bounds.
    #[inline]
    pub fn value(&self, idx: usize) -> bool {
        let byte = self.data[idx >> 3]; // Equivalent to idx / 8
        (byte >> (idx & 7)) & 1 != 0 // `idx & 7` equivalent to `idx % 8`
    }

    /// Set a bit at index.
    ///
    /// Panics if index is out of bounds.
    #[inline]
    pub fn set_unchecked(&mut self, idx: usize, val: bool) {
        let byte = idx / 8;
        let bit = idx & 7; // Same as idx % 8
        if val {
            // Set bit.
            self.data[byte] |= 1 << bit;
        } else {
            // Unset bit.
            self.data[byte] &= !(1 << bit);
        }
    }

    /// Get an iterator over the bitmap.
    pub const fn iter(&self) -> BitmapIter {
        BitmapIter::new(self)
    }

    /// Get an iterator over the bitmap returning indexes of the bitmap where
    /// the bit is set to '1'.
    pub const fn index_iter(&self) -> BitmapIndexIter {
        BitmapIndexIter {
            front: 0,
            back: self.len(),
            bitmap: self,
        }
    }

    /// Bit OR this bitmap with some other bitmap.
    pub fn bit_or_mut(&mut self, other: &Bitmap) -> Result<()> {
        if self.len() != other.len() {
            return Err(RayexecError::new(format!(
                "Bitmap lengths do not match (or), got {} and {}",
                self.len(),
                other.len()
            )));
        }

        for (byte, other) in self.data.iter_mut().zip(other.data.iter()) {
            *byte |= *other;
        }

        Ok(())
    }

    /// Bit AND this bitmap with some other bitmap.
    pub fn bit_and_mut(&mut self, other: &Bitmap) -> Result<()> {
        if self.len() != other.len() {
            return Err(RayexecError::new(format!(
                "Bitmap lengths do not match (and), got {} and {}",
                self.len(),
                other.len()
            )));
        }

        for (byte, other) in self.data.iter_mut().zip(other.data.iter()) {
            *byte &= *other;
        }

        Ok(())
    }

    /// Bit AND NOT this bitmap with some other bitmap.
    pub fn bit_and_not_mut(&mut self, other: &Bitmap) -> Result<()> {
        if self.len() != other.len() {
            return Err(RayexecError::new(format!(
                "Bitmap lengths do not match (and not), got {} and {}",
                self.len(),
                other.len()
            )));
        }

        for (byte, other) in self.data.iter_mut().zip(other.data.iter()) {
            *byte &= !*other;
        }

        Ok(())
    }

    pub fn bit_negate(&mut self) {
        for b in self.data.iter_mut() {
            *b = !*b;
        }
    }

    pub fn try_as_u64(&self) -> Result<u64> {
        if self.len() > 64 {
            return Err(RayexecError::new("Bitmap too large, cannot turn into u64"));
        }

        let mut val = [0; 8];
        for (idx, byte) in self.data.iter().enumerate() {
            val[idx] = *byte;
        }

        let rem = self.len % 8;
        if rem != 0 {
            let mask = (1u8 << rem) - 1;
            val[(self.len + 7) / 8 - 1] &= mask;
        }

        Ok(u64::from_le_bytes(val))
    }
}

impl fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values: Vec<_> = self.iter().collect();
        f.debug_struct("Bitmap").field("values", &values).finish()
    }
}

impl FromIterator<bool> for Bitmap {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let mut iter = iter.into_iter();

        let mut data = Vec::new();
        let mut len = 0;

        loop {
            let mut byte = 0;
            let mut bit_len = 0;

            for (idx, bit) in iter.borrow_mut().take(8).enumerate() {
                bit_len += 1;
                if bit {
                    byte |= 1 << idx;
                }
            }

            // No more bits, exit loop.
            if bit_len == 0 {
                break;
            }

            // Push byte, continue loop to get next 8 values.
            data.push(byte);
            len += bit_len;
        }

        Bitmap { len, data }
    }
}

impl<I> From<I> for Bitmap
where
    I: ExactSizeIterator<Item = bool>,
{
    #[rustfmt::skip]
    fn from(mut iter: I) -> Self {
        let cap = (iter.len() + 7) / 8;

        let mut data = vec![0; cap];
        let mut len = 0;

        // Process each group of 8 bits
        let mut idx = 0;
        while iter.len() > 0 {
            let mut byte = 0;

            if let Some(bit) = iter.next() { if bit { byte |= 1 << 0; } len += 1; }
            if let Some(bit) = iter.next() { if bit { byte |= 1 << 1; } len += 1; }
            if let Some(bit) = iter.next() { if bit { byte |= 1 << 2; } len += 1; }
            if let Some(bit) = iter.next() { if bit { byte |= 1 << 3; } len += 1; }
            if let Some(bit) = iter.next() { if bit { byte |= 1 << 4; } len += 1; }
            if let Some(bit) = iter.next() { if bit { byte |= 1 << 5; } len += 1; }
            if let Some(bit) = iter.next() { if bit { byte |= 1 << 6; } len += 1; }
            if let Some(bit) = iter.next() { if bit { byte |= 1 << 7; } len += 1; }

            data[idx] = byte;
            idx += 1;
        }

        Bitmap { len, data }
    }
}

impl Extend<bool> for Bitmap {
    fn extend<T: IntoIterator<Item = bool>>(&mut self, iter: T) {
        for v in iter {
            self.push(v)
        }
    }
}

impl<'a> IntoExtactSizeIterator for &'a Bitmap {
    type Item = bool;
    type IntoIter = BitmapIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over individual bits (bools) in the bitmap.
#[derive(Debug)]
pub struct BitmapIter<'a> {
    idx: usize,
    bitmap: &'a Bitmap,
    current_byte: u8, // Cached byte
    bits_left: u8,    // Number of bits left in the cached byte
}

impl<'a> BitmapIter<'a> {
    pub const fn new(bitmap: &'a Bitmap) -> Self {
        BitmapIter {
            idx: 0,
            bitmap,
            current_byte: 0,
            bits_left: 0,
        }
    }
}

impl Iterator for BitmapIter<'_> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // If we've iterated over all bits, return None
        if self.idx >= self.bitmap.len() {
            return None;
        }

        // If there are no bits left in the cached byte, load a new byte
        if self.bits_left == 0 {
            let byte_idx = self.idx / 8;
            // SAFETY: Bounds checked above.
            self.current_byte = unsafe { *self.bitmap.data.get_unchecked(byte_idx) };
            self.bits_left = 8;
        }

        // Get the value of the current bit
        let bit_idx = self.idx % 8;
        let bit = (self.current_byte >> bit_idx) & 1 != 0;

        // Update the index and decrement the bits left in the cache
        self.idx += 1;
        self.bits_left -= 1;

        Some(bit)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.bitmap.len() - self.idx;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for BitmapIter<'_> {}

/// Iterator over all "valid" indexes in the bitmap.
#[derive(Debug)]
pub struct BitmapIndexIter<'a> {
    front: usize,
    back: usize,
    bitmap: &'a Bitmap,
}

impl Iterator for BitmapIndexIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.front >= self.back {
                return None;
            }

            if self.bitmap.value(self.front) {
                let idx = self.front;
                self.front += 1;
                return Some(idx);
            }

            self.front += 1;
            // Continue to next iteration.
        }
    }
}

impl DoubleEndedIterator for BitmapIndexIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if self.front >= self.back {
                return None;
            }

            if self.bitmap.value(self.back - 1) {
                let idx = self.back;
                self.back -= 1;
                return Some(idx - 1);
            }

            self.back -= 1;
            // Continue to next iteration.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let bits = [true, false, true, false, true, true, true, true];
        let bm = Bitmap::from_iter(bits);

        assert_eq!(8, bm.len());

        let got: Vec<_> = bm.iter().collect();
        assert_eq!(bits.as_slice(), got);
    }

    #[test]
    fn simple_multiple_bytes() {
        let bits = [
            true, false, true, false, true, true, true, true, //
            true, false, true, false, false, true, true, true, //
            true, false, true, false, true, false, true, true,
        ];
        let bm = Bitmap::from_iter(bits);

        assert_eq!(24, bm.len());

        let got: Vec<_> = bm.iter().collect();
        assert_eq!(bits.as_slice(), got);
    }

    #[test]
    fn not_multiple_of_eight() {
        let bits = [
            true, false, true, false, true, true, true, true, //
            true, false, true, false,
        ];
        let bm = Bitmap::from_iter(bits);

        assert_eq!(12, bm.len());

        let got: Vec<_> = bm.iter().collect();
        assert_eq!(bits.as_slice(), got);
    }

    #[test]
    fn set_simple() {
        let bits = [true, false, true, false, true, true, true, true];
        let mut bm = Bitmap::from_iter(bits);

        bm.set_unchecked(0, false);
        assert!(!bm.value(0));

        bm.set_unchecked(1, true);
        assert!(bm.value(1));
    }

    #[test]
    fn push() {
        let mut bm = Bitmap::default();

        bm.push(true);
        assert_eq!([true].as_slice(), bm.iter().collect::<Vec<_>>());

        bm.push(false);
        assert_eq!([true, false].as_slice(), bm.iter().collect::<Vec<_>>());

        bm.push(false);
        assert_eq!(
            [true, false, false].as_slice(),
            bm.iter().collect::<Vec<_>>()
        );

        // Make sure we're not pushing additional bytes if it's not needed.
        assert_eq!(1, bm.num_bytes());

        // Continue to push to fill up the first byte.
        for _ in 0..5 {
            bm.push(true);
        }

        // Push one more, this should allocate an additional byte.
        bm.push(true);
        assert_eq!(
            [true, false, false, true, true, true, true, true, true].as_slice(),
            bm.iter().collect::<Vec<_>>()
        );

        assert_eq!(9, bm.len());
        assert_eq!(2, bm.num_bytes());
    }

    #[test]
    fn bit_or() {
        let left = [false, true, true, true, true, false, false, false];
        let right = [true, true, true, true, true, true, false, false];
        let mut left_bm = Bitmap::from_iter(left);
        let right_bm = Bitmap::from_iter(right);

        left_bm.bit_or_mut(&right_bm).unwrap();

        let expected = [true, true, true, true, true, true, false, false];
        let got: Vec<_> = left_bm.iter().collect();
        assert_eq!(expected.as_slice(), got);
    }

    #[test]
    fn bit_and() {
        let left = [false, true, true, true, true, false, false, false];
        let right = [true, true, true, true, true, true, false, false];
        let mut left_bm = Bitmap::from_iter(left);
        let right_bm = Bitmap::from_iter(right);

        left_bm.bit_and_mut(&right_bm).unwrap();

        let expected = [false, true, true, true, true, false, false, false];
        let got: Vec<_> = left_bm.iter().collect();
        assert_eq!(expected.as_slice(), got);
    }

    #[test]
    fn bit_negate() {
        let mut bm = Bitmap::from_iter([false, true, true, true, true, false, false, false]);
        bm.bit_negate();

        let expected = [true, false, false, false, false, true, true, true];
        let got: Vec<_> = bm.iter().collect();
        assert_eq!(expected.as_slice(), got);
    }

    #[test]
    fn bit_or_length_mismatch() {
        let left = [true, false];
        let right = [false];
        let mut left_bm = Bitmap::from_iter(left);
        let right_bm = Bitmap::from_iter(right);

        left_bm.bit_or_mut(&right_bm).unwrap_err();
    }

    #[test]
    fn popcnt_simple() {
        let mut bm = Bitmap::from_iter([true, false, false, true, false]);
        assert_eq!(2, bm.count_trues());

        bm.bit_negate();
        assert_eq!(3, bm.count_trues());

        let bm = Bitmap::from_iter([true, false, false, true, false, true, false, false]);
        assert_eq!(3, bm.count_trues());
    }

    #[test]
    fn index_iter_simple() {
        let bm = Bitmap::from_iter([true, false, false, true, false]);
        let indexes: Vec<_> = bm.index_iter().collect();
        assert_eq!(vec![0, 3], indexes);
    }

    #[test]
    fn index_iter_no_valid_bits() {
        let bm = Bitmap::from_iter([false, false, false, false, false]);
        let indexes: Vec<_> = bm.index_iter().collect();
        assert!(indexes.is_empty());
    }

    #[test]
    fn index_iter_rev() {
        let bm = Bitmap::from_iter([true, false, false, true, false, true]);
        let indexes: Vec<_> = bm.index_iter().rev().collect();
        assert_eq!(vec![5, 3, 0], indexes);
    }

    #[test]
    fn try_as_u64_cases() {
        struct TestCase {
            bitmap: Bitmap,
            expected: u64,
        }

        let cases = [
            TestCase {
                bitmap: Bitmap::from_iter([false, false, false, false, false]),
                expected: 0,
            },
            TestCase {
                bitmap: Bitmap::from_iter([true, false, false, false, false]),
                expected: 1,
            },
            TestCase {
                bitmap: Bitmap::from_iter([true, false, true, false, false]),
                expected: 5,
            },
            TestCase {
                bitmap: Bitmap::from_iter([
                    true, false, true, false, false, false, false, false, //
                    false, true, true, false, false, false, false, false, //
                ]),
                expected: 1541,
            },
            TestCase {
                bitmap: Bitmap::from_iter([
                    true, false, true, false, false, false, false, false, //
                    false, true, true, false, false, false, false, false, //
                    true,
                ]),
                expected: 67077,
            },
        ];

        for case in cases {
            let got = case.bitmap.try_as_u64().unwrap();
            assert_eq!(case.expected, got);
        }
    }

    #[test]
    fn reset_all_false() {
        let mut bm = Bitmap::new_with_all_false(10);
        bm.set_unchecked(7, true);
        bm.set_unchecked(9, true);

        bm.reset(false);

        let expected = vec![false; 10];
        let got: Vec<_> = bm.iter().collect();

        assert_eq!(expected, got);
    }

    #[test]
    fn reset_all_true() {
        let mut bm = Bitmap::new_with_all_false(10);
        bm.set_unchecked(7, true);
        bm.set_unchecked(9, true);

        bm.reset(true);

        let expected = vec![true; 10];
        let got: Vec<_> = bm.iter().collect();

        assert_eq!(expected, got);
    }
}
