use rayexec_error::Result;

use crate::arrays::array::Array;

/// Interleave multiple arrays into one.
///
/// Indices contains (array_idx, row_idx) pairs where 'row_idx' is the row
/// within the array. The length of indices indicates the length of the output
/// array.
///
/// Indices may be specified more than once.
pub(crate) fn interleave(
    inputs: &[&Array],
    indices: &[(usize, usize)],
    output: &mut Array,
) -> Result<()> {
    for (idx, arr) in inputs.iter().enumerate() {
        // Generate mapping for indices for just this array.
        let mapping = indices
            .iter()
            .enumerate()
            .filter_map(|(out_idx, &(arr_idx, row_idx))| {
                if arr_idx == idx {
                    Some((row_idx, out_idx))
                } else {
                    None
                }
            });

        arr.copy_rows(mapping, output)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;

    #[test]
    fn interleave_two_i32() {
        let arr1 = Array::try_from_iter([4, 5, 6]).unwrap();
        let arr2 = Array::try_from_iter([7, 8, 9]).unwrap();

        let indices = [(0, 1), (0, 2), (1, 0), (1, 1), (0, 0), (1, 2)];

        let mut out = Array::try_new(&NopBufferManager, DataType::Int32, 6).unwrap();
        interleave(&[&arr1, &arr2], &indices, &mut out).unwrap();

        let expected = Array::try_from_iter([5, 6, 7, 8, 4, 9]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn interleave_two_i32_repeated() {
        let arr1 = Array::try_from_iter([4, 5]).unwrap();
        let arr2 = Array::try_from_iter([7, 8]).unwrap();

        let indices = [(0, 1), (1, 1), (0, 1), (1, 1)];

        let mut out = Array::try_new(&NopBufferManager, DataType::Int32, 4).unwrap();
        interleave(&[&arr1, &arr2], &indices, &mut out).unwrap();

        let expected = Array::try_from_iter([5, 8, 5, 8]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}
