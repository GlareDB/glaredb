use glaredb_error::Result;

use crate::arrays::array::validity::Validity;
use crate::arrays::batch::Batch;
use crate::arrays::cache::NopCache;
use crate::buffer::buffer_manager::NopBufferManager;

/// Tracks row matches.
#[derive(Debug)]
pub struct MatchTracker {
    matches: Vec<bool>,
}

impl MatchTracker {
    pub fn empty() -> Self {
        MatchTracker {
            matches: Vec::new(),
        }
    }

    /// Ensures that the matches are initialized to fit exactly `num_rows`.
    ///
    /// Calling this more than once for the same right batch will not clear any
    /// existing match values.
    pub fn ensure_initialized(&mut self, num_rows: usize) {
        self.matches.resize(num_rows, false);
    }

    /// Clears the matches.
    pub fn reset(&mut self) {
        self.matches.clear()
    }

    /// Sets a match for the given row.
    pub fn set_match(&mut self, row_idx: usize) {
        self.matches[row_idx] = true;
    }

    pub fn set_matches(&mut self, matches: impl IntoIterator<Item = usize>) {
        matches.into_iter().for_each(|idx| self.set_match(idx));
    }

    /// Writes the result of a right outer join to `output`.
    ///
    /// `output` is expected to contain the expected output columns for the
    /// join. `right` will be written to the right-most columns in `output`,
    /// with the left columns being resized and made to be all nulls.
    pub fn right_outer_result(&self, right: &mut Batch, output: &mut Batch) -> Result<()> {
        debug_assert_eq!(self.matches.len(), right.num_rows());
        debug_assert!(right.arrays.len() <= output.arrays.len());

        let not_match_iter = NotMatchIter::new(&self.matches);
        let output_rows = not_match_iter.rem_no_matches;

        if output_rows == 0 {
            // Don't need to do anything, just set output to have no rows.
            output.set_num_rows(0)?;
            return Ok(());
        }

        // Otherwise do some slicing/cloning of arrays.

        let arr_offset = output.arrays.len() - right.arrays.len();

        // Slice the right arrays.
        for arr_idx in 0..right.arrays.len() {
            let right = &mut right.arrays[arr_idx];
            let right_out = &mut output.arrays[arr_idx + arr_offset];

            let selection = not_match_iter.clone();
            right_out.select_from_other(&NopBufferManager, right, selection, &mut NopCache)?;
        }

        // Set the validities for the left arrays to all null.
        for arr_idx in 0..arr_offset {
            let left = &mut output.arrays[arr_idx];
            left.put_validity(Validity::new_all_invalid(left.logical_len()))?;
        }

        output.set_num_rows(output_rows)?;

        Ok(())
    }

    /// Writes the results of a left outer join to `output`.
    ///
    /// `left_offset` indicates the row offset that the left batch starts at.
    pub fn left_outer_result(
        &self,
        left_offset: usize,
        left: &mut Batch,
        output: &mut Batch,
    ) -> Result<()> {
        debug_assert!(left.num_rows() + left_offset <= self.matches.len());

        // Slice matches to only the ones for this batch.
        let matches = &self.matches[left_offset..(left_offset + left.num_rows())];

        let not_match_iter = NotMatchIter::new(matches);
        let output_rows = not_match_iter.rem_no_matches;

        if output_rows == 0 {
            // Don't need to do anything, just set output to have no rows.
            output.set_num_rows(0)?;
            return Ok(());
        }

        // Slice the left arrays.
        for arr_idx in 0..left.arrays.len() {
            let left = &mut left.arrays[arr_idx];
            let left_out = &mut output.arrays[arr_idx];

            let selection = not_match_iter.clone();
            left_out.select_from_other(&NopBufferManager, left, selection, &mut NopCache)?;
        }

        // Set the validities for the rights arrays to all null.
        for right_arr in &mut output.arrays[left.arrays.len()..] {
            right_arr.put_validity(Validity::new_all_invalid(right_arr.logical_len()))?;
        }

        output.set_num_rows(output_rows)?;

        Ok(())
    }
}

/// Returns indices for rows that didn't match.
#[derive(Debug, Clone)]
struct NotMatchIter<'a> {
    matches: &'a [bool],
    idx: usize,
    rem_no_matches: usize,
}

impl<'a> NotMatchIter<'a> {
    fn new(matches: &'a [bool]) -> Self {
        let no_match_count: usize = matches
            .iter()
            .map(|&did_match| if did_match { 0 } else { 1 })
            .sum();

        NotMatchIter {
            matches,
            idx: 0,
            rem_no_matches: no_match_count,
        }
    }
}

impl Iterator for NotMatchIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.rem_no_matches == 0 {
                return None;
            }

            let idx = self.idx;
            let v = self.matches[idx];
            self.idx += 1;

            if !v {
                self.rem_no_matches -= 1;
                return Some(idx);
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rem_no_matches, Some(self.rem_no_matches))
    }
}

impl ExactSizeIterator for NotMatchIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn right_outer_all_rows_match() {
        // No output produced.

        let mut tracker = MatchTracker::empty();
        tracker.ensure_initialized(4);

        for idx in 0..4 {
            tracker.set_match(idx);
        }

        let mut output = Batch::new([DataType::Int32, DataType::Utf8], 4).unwrap();
        let mut right = generate_batch!(["a", "b", "c", "d"]);

        tracker.right_outer_result(&mut right, &mut output).unwrap();
        assert_eq!(0, output.num_rows());
    }

    #[test]
    fn right_outer_no_rows_match() {
        // Should produce all of right.

        let mut tracker = MatchTracker::empty();
        tracker.ensure_initialized(4);

        let mut output = Batch::new([DataType::Int32, DataType::Utf8], 4).unwrap();
        let mut right = generate_batch!(["a", "b", "c", "d"]);

        tracker.right_outer_result(&mut right, &mut output).unwrap();

        let expected = generate_batch!(
            [None as Option<i32>, None, None, None],
            ["a", "b", "c", "d"]
        );
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn right_outer_some_rows_match() {
        let mut tracker = MatchTracker::empty();
        tracker.ensure_initialized(4);

        // Should produce indices 0 and 2 for the right.
        tracker.set_match(1);
        tracker.set_match(3);

        let mut output = Batch::new([DataType::Int32, DataType::Utf8], 4).unwrap();
        let mut right = generate_batch!(["a", "b", "c", "d"]);

        tracker.right_outer_result(&mut right, &mut output).unwrap();

        let expected = generate_batch!([None as Option<i32>, None], ["a", "c"]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn left_outer_all_rows_match() {
        // COLLECTION SIZE: 4
        // OUTPUT SIZE: 2
        //
        // No output produced

        let mut tracker = MatchTracker::empty();
        tracker.ensure_initialized(4);

        for idx in 0..4 {
            tracker.set_match(idx);
        }

        let mut output = Batch::new([DataType::Int32, DataType::Utf8], 2).unwrap();

        let mut left = generate_batch!([1, 2]);
        tracker
            .left_outer_result(0, &mut left, &mut output)
            .unwrap();
        assert_eq!(0, output.num_rows());

        let mut left = generate_batch!([3, 4]);
        tracker
            .left_outer_result(0, &mut left, &mut output)
            .unwrap();
        assert_eq!(0, output.num_rows());
    }

    #[test]
    fn left_outer_no_rows_match() {
        // COLLECTION SIZE: 4
        // OUTPUT SIZE: 2
        //
        // All output produced

        let mut tracker = MatchTracker::empty();
        tracker.ensure_initialized(4);

        let mut output = Batch::new([DataType::Int32, DataType::Utf8], 2).unwrap();

        let mut left = generate_batch!([1, 2]);
        tracker
            .left_outer_result(0, &mut left, &mut output)
            .unwrap();
        let expected = generate_batch!([1, 2], [None as Option<&str>, None]);
        assert_batches_eq(&expected, &output);

        let mut left = generate_batch!([3, 4]);
        tracker
            .left_outer_result(0, &mut left, &mut output)
            .unwrap();
        let expected = generate_batch!([3, 4], [None as Option<&str>, None]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn left_outer_some_rows_match() {
        // COLLECTION SIZE: 4
        // OUTPUT SIZE: 2

        let mut tracker = MatchTracker::empty();
        tracker.ensure_initialized(4);

        // Should produce rows 1 and 3
        tracker.set_matches([0, 2]);

        let mut output = Batch::new([DataType::Int32, DataType::Utf8], 2).unwrap();

        let mut left = generate_batch!([1, 2]);
        tracker
            .left_outer_result(0, &mut left, &mut output)
            .unwrap();
        let expected = generate_batch!([2], [None as Option<&str>]);
        assert_batches_eq(&expected, &output);

        let mut left = generate_batch!([3, 4]);
        tracker
            .left_outer_result(0, &mut left, &mut output)
            .unwrap();
        let expected = generate_batch!([4], [None as Option<&str>]);
        assert_batches_eq(&expected, &output);
    }
}
