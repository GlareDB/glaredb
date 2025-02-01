use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use super::row::{RowAddress, RowCollection};
use crate::arrays::array::Array;

/// Matches rows by comparing encoded values with non-encoded values.
#[derive(Debug)]
pub struct PredicateRowMatcher {}

impl PredicateRowMatcher {
    pub fn find_matches(
        &self,
        collection: &RowCollection,
        lhs_rows: impl IntoExactSizeIterator<Item = *const u8>,
        lhs_columns: &[usize],
        rhs_columns: &[Array],
        rhs_selection: impl IntoExactSizeIterator<Item = usize>,
        rhs_matches: &mut Vec<usize>,
    ) -> Result<()> {
        unimplemented!()
    }
}
