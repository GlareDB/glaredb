/// Compute number of bytes needed for a bitmap to hold some number of entries.
pub const fn num_bytes_for_bitmap(entries: usize) -> usize {
    (entries + 7) / 8
}

/// View on top of a byte slice for LSB ordering within each byte.
#[derive(Debug)]
pub struct BitmapView<'a> {
    len: usize,
    data: &'a [u8],
}

impl<'a> BitmapView<'a> {
    /// Create a new view on a slice.
    ///
    /// `data` must have the capacity needed to hold `len` entries.
    #[inline]
    pub const fn new(data: &'a [u8], len: usize) -> Self {
        debug_assert!(data.len() >= num_bytes_for_bitmap(len));
        BitmapView { len, data }
    }

    /// Get the bit value at a given index.
    ///
    /// Index must be in bounds.
    #[inline]
    pub fn value(&self, idx: usize) -> bool {
        debug_assert!(idx <= self.len, "idx: {}, len: {}", idx, self.len);
        let byte = self.data[idx >> 3]; // Equivalent to idx / 8
        (byte >> (idx & 7)) & 1 != 0 // `idx & 7` equivalent to `idx % 8`
    }

    /// Counts the number of trues in the bitmap
    ///
    /// Note this will ensure to not look at bits past the logical length of the
    /// bitmap.
    #[inline]
    pub fn count_trues(&self) -> usize {
        let subset = &self.data[0..num_bytes_for_bitmap(self.len)];

        let mut count = subset
            .iter()
            .map(|&b| b.count_ones())
            .fold(0, |acc, v| acc + (v as usize));

        // Make sure we're only counting the bits that make up the "logical"
        // portion of the bitmap.
        let rem = self.len % 8;
        if rem != 0 {
            let last = subset.last().unwrap();
            count -= last.count_ones() as usize;
            let mask = (255 << (8 - rem)) >> (8 - rem);
            count += (mask & last).count_ones() as usize;
        }

        count
    }

    /// Returns if all bits are true in the bitmap.
    #[inline]
    pub fn all_true(&self) -> bool {
        self.count_trues() == self.len
    }
}

/// Mutable view on top of a byte slice for manipulating bits (LSB).
#[derive(Debug)]
pub struct BitmapViewMut<'a> {
    len: usize,
    data: &'a mut [u8],
}

impl<'a> BitmapViewMut<'a> {
    /// Create a new mutable view on a slice.
    ///
    /// `data` must have the capacity needed to hold `len` entries.
    #[inline]
    pub const fn new(data: &'a mut [u8], len: usize) -> Self {
        debug_assert!(data.len() >= num_bytes_for_bitmap(len));
        BitmapViewMut { len, data }
    }

    /// Sets the bit at the given index to true.
    ///
    /// Index must be in bounds.
    #[inline]
    pub const fn set(&mut self, idx: usize) {
        debug_assert!(idx <= self.len);
        let byte = idx / 8;
        let bit = idx & 7; // Same as idx % 8
        self.data[byte] |= 1 << bit;
    }

    /// Sets the bit at the given index to false.
    ///
    /// Index must be in bounds.
    #[inline]
    pub const fn unset(&mut self, idx: usize) {
        debug_assert!(idx <= self.len);
        let byte = idx / 8;
        let bit = idx & 7; // Same as idx % 8
        self.data[byte] &= !(1 << bit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_all_false() {
        let data = &[0, 0, 0, 0];
        let view = BitmapView::new(data, 32);

        assert!(!view.value(0));
        assert!(!view.value(13));
        assert_eq!(0, view.count_trues());
    }

    #[test]
    fn view_all_true() {
        let data = &[u8::MAX, u8::MAX, u8::MAX, u8::MAX];
        let view = BitmapView::new(data, 32);

        assert!(view.value(0));
        assert!(view.value(13));
        assert_eq!(32, view.count_trues());
    }

    #[test]
    fn view_all_true_logical_subset() {
        let data = &[u8::MAX, u8::MAX, u8::MAX, u8::MAX];
        let view = BitmapView::new(data, 8);

        assert!(view.value(0));
        assert!(view.value(8));
        assert_eq!(8, view.count_trues());
    }

    #[test]
    fn view_all_true_logical_subset_partial_byte() {
        let data = &[u8::MAX, u8::MAX, u8::MAX, u8::MAX];
        let view = BitmapView::new(data, 3);

        assert!(view.value(0));
        assert_eq!(3, view.count_trues());
    }

    #[test]
    fn set_bit() {
        let mut data = [0, 0];
        let len = 10;

        BitmapViewMut::new(&mut data, len).set(3);
        BitmapViewMut::new(&mut data, len).set(8);
        BitmapViewMut::new(&mut data, len).set(9);

        let view = BitmapView::new(&data, len);

        assert!(!view.value(0));
        assert!(view.value(3));
        assert!(view.value(8));
        assert!(view.value(9));
        assert_eq!(3, view.count_trues());
    }

    #[test]
    fn unset_bit() {
        let mut data = [u8::MAX, u8::MAX];
        let len = 10;

        BitmapViewMut::new(&mut data, len).unset(3);
        BitmapViewMut::new(&mut data, len).unset(8);
        BitmapViewMut::new(&mut data, len).unset(9);

        let view = BitmapView::new(&data, len);

        assert!(view.value(0));
        assert!(!view.value(3));
        assert!(!view.value(8));
        assert!(!view.value(9));
        assert_eq!(7, view.count_trues());
    }
}
