use glaredb_error::{DbError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectedColumn {
    /// Normal data column.
    Data(usize),
    /// A virtual metadata column (e.g. filename, row number, etc)
    Virtual(usize),
}

/// Projections to use when scanning base table or materializations.
///
/// Internally this tracks two set of indices, the "data" indices and the
/// "virtual" indices. "Data" indices are the normal columns we scan from
/// tables, while "virtual" indices represent columns with generated or virtual
/// data (e.g. filenames).
///
/// The physical layout for batches used during scan should have "data" columns
/// first followed by the "virtual" columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projections {
    /// Column indices for data columns being projected.
    data_indices: Vec<usize>,
    /// Column indices for virtual columns being projected.
    virtual_indices: Vec<usize>,
}

impl Projections {
    /// Create a new projections object from the given data column indices.
    // TODO: Should we check to make sure the indices are sorted/unique? Does
    // that matter?
    pub fn new(data_indices: impl IntoIterator<Item = usize>) -> Self {
        let data_indices = data_indices.into_iter().collect();
        Projections {
            data_indices,
            virtual_indices: Vec::new(),
        }
    }

    /// Create a new projections object with both data projections and virtual
    /// column projections.
    pub fn new_with_virtual(
        data_indices: impl IntoIterator<Item = usize>,
        virtual_indices: impl IntoIterator<Item = usize>,
    ) -> Self {
        let data_indices = data_indices.into_iter().collect();
        let virtual_indices = virtual_indices.into_iter().collect();
        Projections {
            data_indices,
            virtual_indices,
        }
    }

    /// Returns a reference to the data indices.
    pub fn data_indices(&self) -> &[usize] {
        &self.data_indices
    }

    /// Returns a reference to the virtual indices.
    ///
    /// Note that these are zero-index relative to the "virtual" table. The
    /// length of data indices needs to be added to these indices to the get the
    /// physical index to use in a batch.
    pub fn virtual_indices(&self) -> &[usize] {
        &self.virtual_indices
    }

    /// Returns an iterator for physical indices to use for virtual columns.
    pub fn physical_virtual_indices(&self) -> impl Iterator<Item = usize> {
        let offset = self.data_indices.len();
        self.virtual_indices.iter().map(move |idx| idx + offset)
    }

    /// Execute a function for each array in the output.
    ///
    /// The provide function accepts a projection index and the associated
    /// output array for that projected column.
    pub fn for_each_column<F>(&self, output: &mut Batch, column_fn: &mut F) -> Result<()>
    where
        F: FnMut(ProjectedColumn, &mut Array) -> Result<()>,
    {
        let total_indices = self.data_indices.len() + self.virtual_indices.len();
        if output.arrays.len() != total_indices {
            return Err(DbError::new(
                "Output batch must have the same number of arrays as the projection list",
            )
            .with_field("num_arrays", output.arrays.len())
            .with_field("num_projections", total_indices));
        }

        // Data columns.
        for (&idx, array) in self.data_indices.iter().zip(&mut output.arrays) {
            column_fn(ProjectedColumn::Data(idx), array)?;
        }

        // Virtual columns.
        //
        // Virtual columns are written to the end of the batch.
        if !self.virtual_indices.is_empty() {
            let virtual_arrays = &mut output.arrays[self.data_indices.len()..];

            for (&idx, array) in self.virtual_indices.iter().zip(virtual_arrays) {
                column_fn(ProjectedColumn::Virtual(idx), array)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::array::physical_type::{
        AddressableMut,
        MutableScalarStorage,
        PhysicalI32,
        PhysicalUtf8,
    };
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
                ProjectedColumn::Data(0) => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &0);
                    Ok(())
                }
                ProjectedColumn::Data(1) => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &1);
                    Ok(())
                }
                ProjectedColumn::Data(2) => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &2);
                    Ok(())
                }
                other => panic!("unexpected value: {other:?}"),
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
                ProjectedColumn::Data(0) => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &0);
                    Ok(())
                }
                ProjectedColumn::Data(2) => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &2);
                    Ok(())
                }
                other => panic!("unexpected value: {other:?}"),
            })
            .unwrap();
        output.set_num_rows(1).unwrap();

        let expected = generate_batch!([0], [2]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn virtual_columns() {
        let projections = Projections::new_with_virtual([0, 1], [0]);
        let mut output = Batch::new([DataType::Int32, DataType::Int32, DataType::Utf8], 1).unwrap();

        projections
            .for_each_column(&mut output, &mut |proj_idx, array| match proj_idx {
                ProjectedColumn::Data(0) => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &0);
                    Ok(())
                }
                ProjectedColumn::Data(1) => {
                    let mut s = PhysicalI32::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, &1);
                    Ok(())
                }
                ProjectedColumn::Virtual(0) => {
                    let mut s = PhysicalUtf8::get_addressable_mut(&mut array.data).unwrap();
                    s.put(0, "my_file.parquet");
                    Ok(())
                }
                other => panic!("unexpected value: {other:?}"),
            })
            .unwrap();
        output.set_num_rows(1).unwrap();

        let expected = generate_batch!([0], [1], ["my_file.parquet"]);
        assert_batches_eq(&expected, &output);
    }
}
