use rayexec_error::{RayexecError, Result};

use super::Bitmap;

/// Zip multiple bitmaps into a single iterator. Specializes zipping multiple
/// bitmaps to reduce work during iteration.
///
/// Logical bit ANDs the bitmaps.
///
/// Bitmaps must all have the same lengths.
#[derive(Debug)]
pub struct ZipBitmapsIter<'a, const N: usize> {
    len: usize,
    bitmaps: [&'a Bitmap; N],
    idx: usize,
    current_byte: u8, // Cached byte
    bits_left: u8,    // Number of bits left in the cached byte
}

impl<'a, const N: usize> ZipBitmapsIter<'a, N> {
    pub fn try_new(bitmaps: [&'a Bitmap; N]) -> Result<Self> {
        let len = match bitmaps.first() {
            Some(b) => b.len(),
            None => return Err(RayexecError::new("Cannot zip zero bitmaps")),
        };

        for bm in bitmaps {
            if bm.len() != len {
                return Err(RayexecError::new(format!(
                    "Bitmap lengths differ, have {} and {}",
                    len,
                    bm.len()
                )));
            }
        }

        Ok(ZipBitmapsIter {
            len,
            bitmaps,
            idx: 0,
            current_byte: 0,
            bits_left: 0,
        })
    }
}

impl<const N: usize> Iterator for ZipBitmapsIter<'_, N> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // If we've iterated over all bits, return None
        if self.idx >= self.len {
            return None;
        }

        // If there are no bits left in the cached byte, load a new byte
        if self.bits_left == 0 {
            let byte_idx = self.idx / 8;
            self.current_byte = 255;
            for bitmap in self.bitmaps {
                // SAFETY: Len checked at start, all bitmaps should have same
                // length (checked during iter create).
                self.current_byte &= unsafe { *bitmap.data.get_unchecked(byte_idx) };
            }
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
        let remaining = self.len - self.idx;
        (remaining, Some(remaining))
    }
}

impl<const N: usize> ExactSizeIterator for ZipBitmapsIter<'_, N> {}
