use std::sync::Arc;

use rayexec_error::Result;

use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::validity::Validity;
use crate::arrays::batch::Batch;
use crate::arrays::bitmap::Bitmap;

/// Tracker visited rows for outer joins.
#[derive(Debug)]
pub struct OuterJoinTracker {
    /// Bitmap for tracking matched rows.
    matches: Bitmap,
}

impl OuterJoinTracker {
    pub fn new(num_rows: usize) -> Self {
        let matches = Bitmap::new_with_all_false(num_rows);
        OuterJoinTracker { matches }
    }

    pub fn set_match(&mut self, idx: usize) {
        self.matches.set_unchecked(idx, true);
    }

    pub fn set_matches(&mut self, rows: impl IntoIterator<Item = usize>) {
        rows.into_iter().for_each(|row| self.set_match(row))
    }

    pub fn reset(&mut self) {
        self.matches.reset(false);
    }

    /// Set the output for a right join.
    ///
    /// Rows from the right that haven't been matched will be in the output,
    /// with all rows from the left columns being set to null.
    pub fn set_right_join_output(&self, right: &mut Batch, output: &mut Batch) -> Result<()> {
        let manager = Arc::new(NopBufferManager);

        let col_offset = output.arrays.len() - right.arrays.len();

        // Move all rows from right into the output.
        for (idx, array) in right.arrays.iter_mut().enumerate() {
            output.arrays[idx + col_offset].try_clone_from(&manager, array)?;
        }

        // TODO: Avoid this.
        let mut selection = Vec::with_capacity(right.num_rows());
        for idx in 0..right.num_rows() {
            if !self.matches.value(idx) {
                selection.push(idx);
            }
        }

        if selection.is_empty() {
            output.set_num_rows(0)?;
            return Ok(());
        }

        // Set all column on the left side to null.
        for idx in 0..col_offset {
            let out_array = &mut output.arrays[idx];
            let validity = Validity::new_all_invalid(out_array.capacity());
            out_array.put_validity(validity)?;
        }

        // Select output arrays.
        output.select(Selection::slice(&selection))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_batches_eq;

    #[test]
    fn right_join_none_match() {
        let tracker = OuterJoinTracker::new(4);

        let mut right =
            Batch::try_from_arrays([Array::try_from_iter([1, 2, 3, 4]).unwrap()]).unwrap();
        let mut out = Batch::try_new([DataType::Utf8, DataType::Int32], 4).unwrap();

        tracker.set_right_join_output(&mut right, &mut out).unwrap();

        let expected = Batch::try_from_arrays([
            Array::try_from_iter::<[Option<&str>; 4]>([None, None, None, None]).unwrap(),
            Array::try_from_iter([1, 2, 3, 4]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &out);
    }

    #[test]
    fn right_join_all_match() {
        let mut tracker = OuterJoinTracker::new(4);
        tracker.set_matches([0, 1, 2, 3]);

        let mut right =
            Batch::try_from_arrays([Array::try_from_iter([1, 2, 3, 4]).unwrap()]).unwrap();
        let mut out = Batch::try_new([DataType::Utf8, DataType::Int32], 4).unwrap();

        tracker.set_right_join_output(&mut right, &mut out).unwrap();
        assert_eq!(0, out.num_rows());
    }

    #[test]
    fn right_join_some_match() {
        let mut tracker = OuterJoinTracker::new(4);
        tracker.set_matches([0, 3]);

        let mut right =
            Batch::try_from_arrays([Array::try_from_iter([1, 2, 3, 4]).unwrap()]).unwrap();
        let mut out = Batch::try_new([DataType::Utf8, DataType::Int32], 4).unwrap();

        tracker.set_right_join_output(&mut right, &mut out).unwrap();

        let expected = Batch::try_from_arrays([
            Array::try_from_iter::<[Option<&str>; 2]>([None, None]).unwrap(),
            Array::try_from_iter([2, 3]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &out);
    }
}
