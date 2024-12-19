use rayexec_error::Result;

use crate::array::Array;
use crate::executor::physical_type::{PhysicalList, PhysicalStorage};
use crate::executor::scalar::list::{get_inner_array_selection, get_inner_array_storage};
use crate::selection;
use crate::storage::AddressableStorage;

#[derive(Debug)]
pub struct ListEntry<T> {
    /// Index of the entry within the list.
    pub inner_idx: i32,
    /// Then entry, None if not valid
    pub entry: Option<T>,
}

#[derive(Debug, Clone, Copy)]
pub struct UnaryListExecutor;

impl UnaryListExecutor {
    pub fn for_each<'a, S, Op>(array: &'a Array, mut op: Op) -> Result<()>
    where
        Op: FnMut(usize, Option<ListEntry<S::Type<'a>>>),
        S: PhysicalStorage,
    {
        let selection = array.selection_vector();
        let metadata = PhysicalList::get_storage(array.array_data())?;

        let (values, inner_validity) = get_inner_array_storage::<S>(array)?;
        let inner_sel = get_inner_array_selection(array)?;

        match (array.validity(), inner_validity) {
            (None, None) => {
                // No validities for parent array or child array.

                println!("LEN: {}", array.logical_len());

                for row_idx in 0..array.logical_len() {
                    let sel = unsafe { selection::get_unchecked(selection, row_idx) };
                    let m = unsafe { metadata.get_unchecked(sel) };

                    for inner_idx in 0..m.len {
                        let sel =
                            unsafe { selection::get_unchecked(inner_sel, inner_idx as usize) };
                        let v = unsafe { values.get_unchecked(sel) };

                        op(
                            row_idx,
                            Some(ListEntry {
                                inner_idx,
                                entry: Some(v),
                            }),
                        );
                    }
                }
            }
            (Some(outer_validity), None) => {
                // Outer validity, no child validity.
                for row_idx in 0..array.logical_len() {
                    let sel = unsafe { selection::get_unchecked(selection, row_idx) };

                    if !outer_validity.value(row_idx) {
                        op(row_idx, None);
                        continue;
                    }

                    let m = unsafe { metadata.get_unchecked(sel) };

                    for inner_idx in 0..m.len {
                        let sel =
                            unsafe { selection::get_unchecked(inner_sel, inner_idx as usize) };
                        let v = unsafe { values.get_unchecked(sel) };

                        op(
                            row_idx,
                            Some(ListEntry {
                                inner_idx,
                                entry: Some(v),
                            }),
                        );
                    }
                }
            }
            (None, Some(inner_validity)) => {
                // Outer all valid, inner contains validities.
                for row_idx in 0..array.logical_len() {
                    let sel = unsafe { selection::get_unchecked(selection, row_idx) };
                    let m = unsafe { metadata.get_unchecked(sel) };

                    for inner_idx in 0..m.len {
                        let sel =
                            unsafe { selection::get_unchecked(inner_sel, inner_idx as usize) };

                        if !inner_validity.value(sel) {
                            op(
                                row_idx,
                                Some(ListEntry {
                                    inner_idx,
                                    entry: None,
                                }),
                            );
                            continue;
                        }

                        let v = unsafe { values.get_unchecked(sel) };

                        op(
                            row_idx,
                            Some(ListEntry {
                                inner_idx,
                                entry: Some(v),
                            }),
                        );
                    }
                }
            }
            (Some(outer_validity), Some(inner_validity)) => {
                // Need to check everything.
                for row_idx in 0..array.logical_len() {
                    let sel = unsafe { selection::get_unchecked(selection, row_idx) };

                    if !outer_validity.value(row_idx) {
                        op(row_idx, None);
                        continue;
                    }

                    let m = unsafe { metadata.get_unchecked(sel) };

                    for inner_idx in 0..m.len {
                        let sel =
                            unsafe { selection::get_unchecked(inner_sel, inner_idx as usize) };

                        if !inner_validity.value(sel) {
                            op(
                                row_idx,
                                Some(ListEntry {
                                    inner_idx,
                                    entry: None,
                                }),
                            );
                            continue;
                        }

                        let v = unsafe { values.get_unchecked(sel) };

                        op(
                            row_idx,
                            Some(ListEntry {
                                inner_idx,
                                entry: Some(v),
                            }),
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
