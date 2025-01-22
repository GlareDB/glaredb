use rayexec_error::{RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::array_buffer::SecondaryBuffer;
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutablePhysicalStorage,
    PhysicalList,
    PhysicalStorage,
};
use crate::arrays::array::Array;
use crate::arrays::executor::OutBuffer;

pub trait BinaryReducer<T1, T2, O>: Default {
    /// Put two values from each list into the reducer.
    fn put_values(&mut self, v1: T1, v2: T2);
    /// Produce the final value from the reducer.
    fn finish(self) -> O;
}

#[derive(Debug, Clone)]
pub struct BinaryListReducer;

impl BinaryListReducer {
    /// Iterate two list arrays, reducing lists from each array.
    ///
    /// List reduction requires that if both lists for a given row are non-null,
    /// then both lists must be the same length and not contain nulls.
    ///
    /// If either list is null, the output row will be set to null (same as
    /// other executor logic).
    ///
    /// `R` is used to create a new reducer for each pair of lists.
    ///
    /// `S1` and `S2` should be for the inner type within the list.
    pub fn reduce<S1, S2, R, O>(
        array1: &Array,
        sel1: impl IntoExactSizeIterator<Item = usize>,
        array2: &Array,
        sel2: impl IntoExactSizeIterator<Item = usize>,
        out: OutBuffer,
    ) -> Result<()>
    where
        S1: PhysicalStorage,
        S2: PhysicalStorage,
        O: MutablePhysicalStorage,
        O::StorageType: Sized,
        for<'a> R: BinaryReducer<&'a S1::StorageType, &'a S2::StorageType, O::StorageType>,
    {
        if array1.is_dictionary() || array2.is_dictionary() {
            // TODO
        }

        let inner1 = match array1.data.get_secondary() {
            SecondaryBuffer::List(list) => &list.child,
            _ => return Err(RayexecError::new("Array 1 not a list array")),
        };

        let inner2 = match array2.data.get_secondary() {
            SecondaryBuffer::List(list) => &list.child,
            _ => return Err(RayexecError::new("Array 2 not a list array")),
        };

        if !inner1.validity.all_valid() || !inner2.validity.all_valid() {
            // TODO: This can be more selective. Rows that don't conform
            // could be skipped with the selections.
            return Err(RayexecError::new(
                "List reduction requires all values be non-null",
            ));
        }

        let metadata1 = PhysicalList::get_addressable(&array1.data)?;
        let metadata2 = PhysicalList::get_addressable(&array2.data)?;

        let validity1 = &array1.validity;
        let validity2 = &array2.validity;

        let mut output = O::get_addressable_mut(out.buffer)?;

        let input1 = S1::get_addressable(&inner1.data)?;
        let input2 = S2::get_addressable(&inner2.data)?;

        if validity1.all_valid() && validity2.all_valid() {
            for (output_idx, (input1_idx, input2_idx)) in
                sel1.into_iter().zip(sel2.into_iter()).enumerate()
            {
                let meta1 = metadata1.get(input1_idx).unwrap();
                let meta2 = metadata2.get(input2_idx).unwrap();

                if meta1.len != meta2.len {
                    return Err(RayexecError::new(
                        "List reduction requires lists be the same length",
                    )
                    .with_field("len1", meta1.len)
                    .with_field("len2", meta2.len));
                }

                let mut reducer = R::default();

                for offset in 0..meta1.len {
                    let idx1 = meta1.offset + offset;
                    let idx2 = meta2.offset + offset;

                    let v1 = input1.get(idx1 as usize).unwrap();
                    let v2 = input2.get(idx2 as usize).unwrap();

                    reducer.put_values(v1, v2);
                }

                output.put(output_idx, &reducer.finish());
            }
        } else {
            for (output_idx, (input1_idx, input2_idx)) in
                sel1.into_iter().zip(sel2.into_iter()).enumerate()
            {
                if !validity1.is_valid(input1_idx) || !validity2.is_valid(input2_idx) {
                    out.validity.set_invalid(output_idx);
                    continue;
                }

                let meta1 = metadata1.get(input1_idx).unwrap();
                let meta2 = metadata2.get(input2_idx).unwrap();

                if meta1.len != meta2.len {
                    return Err(RayexecError::new(
                        "List reduction requires lists be the same length",
                    )
                    .with_field("len1", meta1.len)
                    .with_field("len2", meta2.len));
                }

                let mut reducer = R::default();

                for offset in 0..meta1.len {
                    let idx1 = meta1.offset + offset;
                    let idx2 = meta2.offset + offset;

                    let v1 = input1.get(idx1 as usize).unwrap();
                    let v2 = input2.get(idx2 as usize).unwrap();

                    reducer.put_values(v1, v2);
                }

                output.put(output_idx, &reducer.finish());
            }
        }

        Ok(())
    }
}
