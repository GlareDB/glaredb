use std::borrow::BorrowMut;
use rayexec_error::{ Result, RayexecError };

/// An LSB ordered bitmap.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bitmap {
    len: usize,
    data: Vec<u8>,
}

impl Bitmap {
    /// Get the number of bits being tracked by this bitmap.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Get the total number of bytes of the underlying data.
    pub fn num_bytes(&self) -> usize {
        self.data.len()
    }

    pub fn popcnt(&self) -> usize {
        self.data
            .iter()
            .map(|&b| b.count_ones())
            .fold(0, |acc, v| acc + (v as usize))
    }

    /// Push a value onto the end of the bitmap.
    pub fn push(&mut self, val: bool) {
        if self.len == self.data.len() * 8 {
            self.data.push(0);
        }
        let idx = self.len;
        self.len += 1;
        self.set(idx, val);
    }

    /// Get the value at index.
    ///
    /// Panics if index is out of bounds.
    pub fn value(&self, idx: usize) -> bool {
        assert!(idx < self.len);
        self.data[idx / 8] & (1 << (idx % 8)) != 0
    }

    /// Set a bit at index.
    pub fn set(&mut self, idx: usize, val: bool) {
        assert!(idx < self.len);
        if val {
            // Set bit.
            self.data[idx / 8] = self.data[idx / 8] | (1 << (idx % 8))
        } else {
            // Unset bit
            self.data[idx / 8] = self.data[idx / 8] & !(1 << (idx % 8))
        }
    }

    /// Get an iterator over the bitmap.
    pub const fn iter(&self) -> BitmapIter {
        BitmapIter {
            idx: 0,
            bitmap: self,
        }
    }

    /// Get an iterator over the bitmap returning indexes of the bitmap where
    /// the bit is set to '1'.
    pub const fn index_iter(&self) -> BitmapIndexIter {
        BitmapIndexIter {
            idx: 0,
            bitmap: self,
        }
    }

    /// Bit OR this bitmap with some other bitmap.
    pub fn bit_or_mut(&mut self, other: &Bitmap) -> Result<()> {
        if self.len() != other.len() {
            return Err(RayexecError::new("Bitmap lengths do not match"));
        }

        for (byte, other) in self.data.iter_mut().zip(other.data.iter()) {
            *byte |= *other;
        }

        Ok(())
    }

    /// Bit AND this bitmap with some other bitmap.
    pub fn bit_and_mut(&mut self, other: &Bitmap) -> Result<()> {
        if self.len() != other.len() {
            return Err(RayexecError::new("Bitmap lengths do not match"));
        }

        for (byte, other) in self.data.iter_mut().zip(other.data.iter()) {
            *byte &= *other;
        }

        Ok(())
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
                    byte = byte | (1 << idx);
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

/// Iterator over individual bits (bools) in the bitmap.
#[derive(Debug)]
pub struct BitmapIter<'a> {
    idx: usize,
    bitmap: &'a Bitmap,
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.bitmap.len() {
            return None;
        }

        let v = self.bitmap.value(self.idx);
        self.idx += 1;
        Some(v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.bitmap.len() - self.idx,
            Some(self.bitmap.len() - self.idx),
        )
    }
}

/// Iterator over all "valid" indexes in the bitmap.
#[derive(Debug)]
pub struct BitmapIndexIter<'a> {
    idx: usize,
    bitmap: &'a Bitmap,
}

impl<'a> Iterator for BitmapIndexIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.idx >= self.bitmap.len() {
                return None;
            }

            if self.bitmap.value(self.idx) {
                let idx = self.idx;
                self.idx += 1;
                return Some(idx);
            }

            self.idx += 1;
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

        bm.set(0, false);
        assert_eq!(false, bm.value(0));

        bm.set(1, true);
        assert_eq!(true, bm.value(1));
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
    fn bit_or_length_mismatch() {
        let left = [true, false];
        let right = [false];
        let mut left_bm = Bitmap::from_iter(left);
        let right_bm = Bitmap::from_iter(right);

        left_bm.bit_or_mut(&right_bm).unwrap_err();
    }

    #[test]
    fn popcnt_simple() {
        let bm = Bitmap::from_iter([true, false, false, true, false]);
        assert_eq!(2, bm.popcnt());
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
}
