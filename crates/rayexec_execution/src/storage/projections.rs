use rayexec_error::{RayexecError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;

/// Scan projections.
// TODO: Probably move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projections {
    /// Column indices to project out of the scan.
    indices: Vec<usize>,
}

impl Projections {
    /// Create a new projections object from the given column indices.
    // TODO: Should we check to make sure the indices are sorted/unique? Does
    // that matter?
    pub fn new(indices: impl IntoIterator<Item = usize>) -> Self {
        let indices = indices.into_iter().collect();
        Projections { indices }
    }

    pub fn indices(&self) -> &[usize] {
        &self.indices
    }

    /// Execute a function for each array in the output.
    ///
    /// The provide function accepts a projection index and the associated
    /// output array for that projected column.
    pub fn for_each_column<F>(&self, output: &mut Batch, column_fn: &mut F) -> Result<()>
    where
        F: FnMut(usize, &mut Array) -> Result<()>,
    {
        if output.arrays.len() != self.indices.len() {
            return Err(RayexecError::new(
                "Output batch must have the same number of arrays as the projection list",
            )
            .with_field("num_arrays", output.arrays.len())
            .with_field("num_projections", self.indices.len()));
        }

        for (&idx, array) in self.indices.iter().zip(&mut output.arrays) {
            column_fn(idx, array)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalI32};
    use crate::arrays::datatype::DataType;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;

    #[test]
    fn all_columns() {
        let projections = Projections::new([0, 1, 2]);
        let mut output =
            Batch::new([DataType::Int32, DataType::Int32, DataType::Int32], 1).unwrap();

        projections
            .for_each_column(&mut output, &mut |proj_idx, array| match proj_idx {
                0 => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &0);
                    Ok(())
                }
                1 => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &1);
                    Ok(())
                }
                2 => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &2);
                    Ok(())
                }
                other => panic!("unexpected value: {other}"),
            })
            .unwrap();
        output.set_num_rows(1).unwrap();

        let expected = generate_batch!([0], [1], [2]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn column_subset() {
        let projections = Projections::new([0, 2]);
        let mut output = Batch::new([DataType::Int32, DataType::Int32], 1).unwrap();

        projections
            .for_each_column(&mut output, &mut |proj_idx, array| match proj_idx {
                0 => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &0);
                    Ok(())
                }
                2 => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &2);
                    Ok(())
                }
                other => panic!("unexpected value: {other}"),
            })
            .unwrap();
        output.set_num_rows(1).unwrap();

        let expected = generate_batch!([0], [2]);
        assert_batches_eq(&expected, &output);
    }
}
