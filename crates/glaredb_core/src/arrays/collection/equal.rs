use glaredb_error::{DbError, Result};

use super::concurrent::ConcurrentColumnCollection;
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalBool, ScalarStorage};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::buffer::buffer_manager::NopBufferManager;
use crate::expr;
use crate::functions::scalar::builtin::comparison::FUNCTION_SET_EQ;
use crate::storage::projections::Projections;
use crate::util::fmt::displayable::IntoDisplayableSlice;

/// Verifies that two collections are equal.
///
/// `batch_size` determines the size of the intermediates batches to use when
/// scanning both collections.
// TODO: Use this when verifying the results of an optimized query with the
// unoptimized query.
#[allow(unused)]
pub fn verify_collections_eq(
    left: &ConcurrentColumnCollection,
    right: &ConcurrentColumnCollection,
    batch_size: usize,
) -> Result<()> {
    if left.datatypes() != right.datatypes() {
        return Err(
            DbError::new("Left and right collections don't have same datatypes")
                .with_field("left", left.datatypes().display_as_list().to_string())
                .with_field("right", right.datatypes().display_as_list().to_string()),
        );
    }

    if left.flushed_rows() != right.flushed_rows() {
        return Err(
            DbError::new("Left and right collections have different number of rows")
                .with_field("left", left.flushed_rows())
                .with_field("right", right.flushed_rows()),
        );
    }

    let expected_row_count = left.flushed_rows();

    // Compare a column at a time. This will repeatedly scan both collections.
    for (col_idx, datatype) in left.datatypes().iter().enumerate() {
        let mut left_scan = left.init_scan_state();
        let mut right_scan = right.init_scan_state();

        let projections = Projections::new([col_idx]);

        let mut left_batch = Batch::new([datatype.clone()], batch_size).unwrap();
        let mut right_batch = Batch::new([datatype.clone()], batch_size).unwrap();

        // TODO: This should be using IS NOT DISTINCT FROM. Currently fails on nulls...
        let eq_func = expr::scalar_function(
            &FUNCTION_SET_EQ,
            vec![
                expr::column((0, 0), datatype.clone()),
                expr::column((1, 0), datatype.clone()),
            ],
        )?;

        let mut row_count = 0;
        let mut out = Array::new(&NopBufferManager, DataType::Boolean, batch_size)?;

        loop {
            if left_batch.num_rows() == 0 {
                left.scan(&projections, &mut left_scan, &mut left_batch)?;
            }
            if right_batch.num_rows() == 0 {
                right.scan(&projections, &mut right_scan, &mut right_batch)?;
            }

            if left_batch.num_rows() == 0 && right_batch.num_rows() == 0 {
                // Move to next column to compare.
                break;
            }

            if left_batch.num_rows() == 0 {
                return Err(DbError::new(
                    "Left batch returned zero rows while right batch still has rows remaining",
                )
                .with_field("col_idx", col_idx)
                .with_field("remaining", right_batch.num_rows()));
            }
            if right_batch.num_rows() == 0 {
                return Err(DbError::new(
                    "Right batch returned zero rows while left batch still has rows remaining",
                )
                .with_field("col_idx", col_idx)
                .with_field("remaining", left_batch.num_rows()));
            }

            // Collections aren't guaranteed to fill batches completely during
            // scans, slice both batches to just the rows we can compare.

            let compare_count = usize::min(left_batch.num_rows, right_batch.num_rows);

            let mut left_cmp = Batch::new_from_other(&mut left_batch)?;
            if left_cmp.num_rows() > compare_count {
                left_cmp.select(0..compare_count)?;
            }
            debug_assert_eq!(compare_count, left_cmp.num_rows);
            let mut right_cmp = Batch::new_from_other(&mut right_batch)?;
            if right_cmp.num_rows() > compare_count {
                right_cmp.select(0..compare_count)?;
            }
            debug_assert_eq!(compare_count, right_cmp.num_rows);

            let mut cmp_batch = Batch::from_arrays([
                left_cmp.arrays.pop().unwrap(),
                right_cmp.arrays.pop().unwrap(),
            ])?;
            cmp_batch.set_num_rows(compare_count)?;
            debug_assert_eq!(compare_count, cmp_batch.num_rows);

            // Do the compare.
            eq_func.function.call_execute(&cmp_batch, &mut out)?;

            let out = PhysicalBool::get_addressable(&out.data)?;
            let pos = out.slice[0..compare_count].iter().position(|&eq| !eq);

            if let Some(pos) = pos {
                return Err(DbError::new(format!(
                    "Row {} for column {} is not equal",
                    pos + row_count,
                    col_idx
                ))
                // Use position relative to the comparison batch, not overall.
                .with_field("left", cmp_batch.arrays[0].get_value(pos)?.to_string())
                .with_field("right", cmp_batch.arrays[1].get_value(pos)?.to_string()));
            }

            row_count += compare_count;

            // Update batches to slice the remaining rows we need to compare.
            // Note these batches weren't modified (no arrays popped).

            if left_batch.num_rows == compare_count {
                left_batch.set_num_rows(0)?;
            } else {
                let num_rows = left_batch.num_rows();
                left_batch.select(compare_count..num_rows)?;
            }

            if right_batch.num_rows == compare_count {
                right_batch.set_num_rows(0)?;
            } else {
                let num_rows = right_batch.num_rows();
                right_batch.select(compare_count..num_rows)?;
            }
        }

        // This would indicate a bug with the scan logic.
        if row_count != expected_row_count {
            return Err(DbError::new(format!(
                "Expected {expected_row_count} rows, but scanned {row_count} rows for column {col_idx}"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate_batch;

    #[test]
    fn verify_equal() {
        let c1 = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 2, 2);
        let c2 = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 2, 2);

        let mut s1 = c1.init_append_state();
        let mut s2 = c2.init_append_state();

        let b1 = generate_batch!([4, 5, 6], ["a", "b", "c"]);
        c1.append_batch(&mut s1, &b1).unwrap();
        c2.append_batch(&mut s2, &b1).unwrap();

        let b2 = generate_batch!([7, 8, 9], ["d", "e", "f"]);
        c1.append_batch(&mut s1, &b2).unwrap();
        c2.append_batch(&mut s2, &b2).unwrap();

        c1.flush(&mut s1).unwrap();
        c2.flush(&mut s2).unwrap();

        verify_collections_eq(&c1, &c2, 2048).unwrap();
    }

    #[test]
    fn verify_equal_different_segment_chunk_size() {
        // Ensure our batch slicing logic works.

        let c1 = ConcurrentColumnCollection::new([DataType::Int32], 2, 16);
        let c2 = ConcurrentColumnCollection::new([DataType::Int32], 3, 27);

        let mut s1 = c1.init_append_state();
        let mut s2 = c2.init_append_state();

        for idx in 0..4 {
            let b1 = generate_batch!(std::iter::repeat(idx).take(4096));
            c1.append_batch(&mut s1, &b1).unwrap();
            c2.append_batch(&mut s2, &b1).unwrap();

            c1.flush(&mut s1).unwrap();
            c2.flush(&mut s2).unwrap();
        }

        verify_collections_eq(&c1, &c2, 2048).unwrap();
    }

    #[test]
    fn verify_not_equal_different_number_of_rows() {
        let c1 = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 2, 2);
        let c2 = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 2, 2);

        let mut s1 = c1.init_append_state();
        let mut s2 = c2.init_append_state();

        let b1 = generate_batch!([4, 5, 6], ["a", "b", "c"]);
        c1.append_batch(&mut s1, &b1).unwrap();
        // No append to c2 here.

        let b2 = generate_batch!([7, 8, 9], ["d", "e", "f"]);
        c1.append_batch(&mut s1, &b2).unwrap();
        c2.append_batch(&mut s2, &b2).unwrap();

        c1.flush(&mut s1).unwrap();
        c2.flush(&mut s2).unwrap();

        verify_collections_eq(&c1, &c2, 2048).unwrap_err();
    }

    #[test]
    fn verify_not_equal_different_values() {
        let c1 = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 2, 2);
        let c2 = ConcurrentColumnCollection::new([DataType::Int32, DataType::Utf8], 2, 2);

        let mut s1 = c1.init_append_state();
        let mut s2 = c2.init_append_state();

        let b1 = generate_batch!([4, 5, 6], ["a", "b", "c"]);
        c1.append_batch(&mut s1, &b1).unwrap();
        c2.append_batch(&mut s2, &b1).unwrap();

        let b2 = generate_batch!([7, 8, 9], ["d", "e", "f"]);
        c1.append_batch(&mut s1, &b2).unwrap();
        let b2 = generate_batch!([7, 8, 9], ["d", "differentvaluehere", "f"]);
        c2.append_batch(&mut s2, &b2).unwrap();

        c1.flush(&mut s1).unwrap();
        c2.flush(&mut s2).unwrap();

        verify_collections_eq(&c1, &c2, 2048).unwrap_err();
    }

    #[test]
    fn verify_not_equal_different_segment_chunk_size_different_row_count() {
        let c1 = ConcurrentColumnCollection::new([DataType::Int32], 2, 16);
        let c2 = ConcurrentColumnCollection::new([DataType::Int32], 3, 27);

        let mut s1 = c1.init_append_state();
        let mut s2 = c2.init_append_state();

        for idx in 0..4 {
            let b1 = generate_batch!(std::iter::repeat(idx).take(1024));
            c1.append_batch(&mut s1, &b1).unwrap();
            c2.append_batch(&mut s2, &b1).unwrap();

            c1.flush(&mut s1).unwrap();
            c2.flush(&mut s2).unwrap();
        }

        // Extra batch for c1.
        let b = generate_batch!([4]);
        c1.append_batch(&mut s1, &b).unwrap();
        c1.flush(&mut s1).unwrap();

        verify_collections_eq(&c1, &c2, 2048).unwrap_err();
    }

    #[test]
    fn verify_not_equal_different_segment_chunk_size_different_values() {
        let c1 = ConcurrentColumnCollection::new([DataType::Int32], 2, 16);
        let c2 = ConcurrentColumnCollection::new([DataType::Int32], 3, 27);

        let mut s1 = c1.init_append_state();
        let mut s2 = c2.init_append_state();

        for idx in 0..4 {
            let b = generate_batch!(std::iter::repeat(idx).take(1024));
            c1.append_batch(&mut s1, &b).unwrap();
            c2.append_batch(&mut s2, &b).unwrap();

            // Add in very small batch with different value.
            c1.append_batch(&mut s1, &generate_batch!([4])).unwrap();
            c2.append_batch(&mut s2, &generate_batch!([5])).unwrap();

            c1.flush(&mut s1).unwrap();
            c2.flush(&mut s2).unwrap();
        }

        verify_collections_eq(&c1, &c2, 2048).unwrap_err();
    }
}
