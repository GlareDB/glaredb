use rayexec_error::{not_implemented, RayexecError, Result};

use crate::array::{Array, ArrayData};
use crate::executor::builder::{ArrayBuilder, ArrayDataBuffer};
use crate::executor::physical_type::{PhysicalList, PhysicalStorage};
use crate::executor::scalar::{can_skip_validity_check, validate_logical_len};
use crate::selection::{self, SelectionVector};
use crate::storage::AddressableStorage;

pub trait BinaryListReducer<T, O>: Default {
    fn put_values(&mut self, v1: T, v2: T);
    fn finish(self) -> O;
}

#[derive(Debug, Clone, Copy)]
pub struct ListExecutor;

impl ListExecutor {
    pub fn execute_binary_reduce<'a, S, B, R>(
        array1: &'a Array,
        array2: &'a Array,
        mut builder: ArrayBuilder<B>,
    ) -> Result<Array>
    where
        R: BinaryListReducer<S::Type, B::Type>,
        S: PhysicalStorage<'a>,
        B: ArrayDataBuffer,
        <B as ArrayDataBuffer>::Type: Sized,
    {
        let len = validate_logical_len(&builder.buffer, array1)?;
        let _ = validate_logical_len(&builder.buffer, array2)?;

        let selection1 = array1.selection_vector();
        let selection2 = array2.selection_vector();

        let validity1 = array1.validity();
        let validity2 = array2.validity();

        if can_skip_validity_check([validity1, validity2]) {
            let metadata1 = PhysicalList::get_storage(array1.array_data())?;
            let metadata2 = PhysicalList::get_storage(array2.array_data())?;

            let values1 = get_inner_array_storage::<S>(array1)?;
            let values2 = get_inner_array_storage::<S>(array2)?;

            let inner_sel1 = get_inner_array_selection(array1)?;
            let inner_sel2 = get_inner_array_selection(array2)?;

            for idx in 0..len {
                let sel1 = unsafe { selection::get_unchecked(selection1, idx) };
                let sel2 = unsafe { selection::get_unchecked(selection2, idx) };

                let m1 = unsafe { metadata1.get_unchecked(sel1) };
                let m2 = unsafe { metadata2.get_unchecked(sel2) };

                if m1.len != m2.len {
                    return Err(RayexecError::new(format!(
                        "Cannot reduce arrays with differing lengths, got {} and {}",
                        m1.len, m2.len
                    )));
                }

                let mut reducer = R::default();

                for inner_idx in 0..m1.len {
                    let idx1 = m1.offset + inner_idx;
                    let idx2 = m2.offset + inner_idx;

                    let sel1 = unsafe { selection::get_unchecked(inner_sel1, idx1 as usize) };
                    let sel2 = unsafe { selection::get_unchecked(inner_sel2, idx2 as usize) };

                    let v1 = unsafe { values1.get_unchecked(sel1) };
                    let v2 = unsafe { values2.get_unchecked(sel2) };

                    reducer.put_values(v1, v2);
                }

                let out = reducer.finish();

                builder.buffer.put(idx, &out);
            }

            Ok(Array {
                datatype: builder.datatype,
                selection: None,
                validity: None,
                data: builder.buffer.into_data(),
            })
        } else {
            // let mut out_validity = None;
            not_implemented!("list validity execute")
        }
    }
}

/// Gets the inner array storage. Checks to ensure the inner array does not
/// contain NULLs.
fn get_inner_array_storage<'a, S>(array: &'a Array) -> Result<S::Storage>
where
    S: PhysicalStorage<'a>,
{
    match array.array_data() {
        ArrayData::List(d) => {
            if !can_skip_validity_check([d.array.validity()]) {
                return Err(RayexecError::new("Cannot reduce list containing NULLs"));
            }

            S::get_storage(d.array.array_data())
        }
        _ => Err(RayexecError::new("Expected list array data")),
    }
}

fn get_inner_array_selection(array: &Array) -> Result<Option<&SelectionVector>> {
    match array.array_data() {
        ArrayData::List(d) => Ok(d.array.selection_vector()),
        _ => Err(RayexecError::new("Expected list array data")),
    }
}
