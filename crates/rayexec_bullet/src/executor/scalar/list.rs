use rayexec_error::{not_implemented, RayexecError, Result};

use crate::array::{Array, ArrayData};
use crate::bitmap::Bitmap;
use crate::executor::builder::{ArrayBuilder, ArrayDataBuffer};
use crate::executor::physical_type::{PhysicalList, PhysicalStorage};
use crate::executor::scalar::{can_skip_validity_check, check_validity, validate_logical_len};
use crate::selection::{self, SelectionVector};
use crate::storage::{AddressableStorage, ListItemMetadata};

pub trait BinaryListReducer<T, O> {
    fn new(left_len: i32, right_len: i32) -> Self;
    fn put_values(&mut self, v1: T, v2: T);
    fn finish(self) -> O;
}

/// List executor that allows for different list lengths, and nulls inside of
/// lists.
pub type FlexibleListExecutor = ListExecutor<true, true>;

/// Execute reductions on lists.
///
/// `ALLOW_DIFFERENT_LENS` controls whether or not this allows for reducing
/// lists of different lengths.
///
/// `ALLOW_NULLS` controls if this allows nulls in lists.
#[derive(Debug, Clone, Copy)]
pub struct ListExecutor<const ALLOW_DIFFERENT_LENS: bool, const ALLOW_NULLS: bool>;

impl<const ALLOW_DIFFERENT_LENS: bool, const ALLOW_NULLS: bool>
    ListExecutor<ALLOW_DIFFERENT_LENS, ALLOW_NULLS>
{
    /// Execute a reducer on two list arrays.
    pub fn binary_reduce<'a, S, B, R>(
        array1: &'a Array,
        array2: &'a Array,
        mut builder: ArrayBuilder<B>,
    ) -> Result<Array>
    where
        R: BinaryListReducer<S::Type<'a>, B::Type>,
        S: PhysicalStorage,
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

            let (values1, inner_validity1) = get_inner_array_storage::<S>(array1)?;
            let (values2, inner_validity2) = get_inner_array_storage::<S>(array2)?;

            let inner_sel1 = get_inner_array_selection(array1)?;
            let inner_sel2 = get_inner_array_selection(array2)?;

            if can_skip_validity_check([inner_validity1, inner_validity2]) {
                for idx in 0..len {
                    let sel1 = unsafe { selection::get_unchecked(selection1, idx) };
                    let sel2 = unsafe { selection::get_unchecked(selection2, idx) };

                    let m1 = unsafe { metadata1.get_unchecked(sel1) };
                    let m2 = unsafe { metadata2.get_unchecked(sel2) };

                    let len = Self::item_iter_len(m1, m2)?;

                    let mut reducer = R::new(m1.len, m2.len);

                    for inner_idx in 0..len {
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
                if !ALLOW_NULLS {
                    return Err(RayexecError::new("Cannot reduce list containing NULLs"));
                }

                for idx in 0..len {
                    let sel1 = unsafe { selection::get_unchecked(selection1, idx) };
                    let sel2 = unsafe { selection::get_unchecked(selection2, idx) };

                    let m1 = unsafe { metadata1.get_unchecked(sel1) };
                    let m2 = unsafe { metadata2.get_unchecked(sel2) };

                    let len = Self::item_iter_len(m1, m2)?;

                    let mut reducer = R::new(m1.len, m2.len);

                    for inner_idx in 0..len {
                        let idx1 = m1.offset + inner_idx;
                        let idx2 = m2.offset + inner_idx;

                        let sel1 = unsafe { selection::get_unchecked(inner_sel1, idx1 as usize) };
                        let sel2 = unsafe { selection::get_unchecked(inner_sel2, idx2 as usize) };

                        if check_validity(sel1, inner_validity1)
                            && check_validity(sel2, inner_validity2)
                        {
                            let v1 = unsafe { values1.get_unchecked(sel1) };
                            let v2 = unsafe { values2.get_unchecked(sel2) };

                            reducer.put_values(v1, v2);
                        }
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
            }
        } else {
            // let mut out_validity = None;
            not_implemented!("list validity execute")
        }
    }

    fn item_iter_len(m1: ListItemMetadata, m2: ListItemMetadata) -> Result<i32> {
        if m1.len == m2.len {
            Ok(m1.len)
        } else if ALLOW_DIFFERENT_LENS {
            Ok(std::cmp::min(m1.len, m2.len))
        } else {
            Err(RayexecError::new(format!(
                "Cannot reduce arrays with differing lengths, got {} and {}",
                m1.len, m2.len
            )))
        }
    }
}

/// Gets the inner array storage. Checks to ensure the inner array does not
/// contain NULLs.
fn get_inner_array_storage<S>(array: &Array) -> Result<(S::Storage<'_>, Option<&Bitmap>)>
where
    S: PhysicalStorage,
{
    match array.array_data() {
        ArrayData::List(d) => {
            let storage = S::get_storage(d.array.array_data())?;
            let validity = d.array.validity();
            Ok((storage, validity))
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
