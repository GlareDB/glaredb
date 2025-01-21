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
}
