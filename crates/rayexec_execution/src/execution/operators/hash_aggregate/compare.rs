use std::collections::BTreeSet;

use rayexec_error::{not_implemented, Result};

use super::chunk::GroupChunk;
use super::hash_table::GroupAddress;
use crate::arrays::array::Array2;
use crate::arrays::executor::physical_type::{
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16_2,
    PhysicalF32_2,
    PhysicalF64_2,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalStorage2,
    PhysicalType2,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::executor::scalar::{can_skip_validity_check, check_validity};
use crate::arrays::selection::{self, SelectionVector};
use crate::arrays::storage::AddressableStorage;

pub fn group_values_eq(
    inputs: &[Array2],
    input_sel: &SelectionVector,
    chunks: &[GroupChunk],
    addresses: &[GroupAddress],
    chunk_indices: &BTreeSet<u16>,
    not_eq_rows: &mut BTreeSet<usize>,
) -> Result<()> {
    for &chunk_idx in chunk_indices {
        // Get only input rows that have its compare partner row in this chunk.
        let rows1 = input_sel.iter_locations().filter(|&loc| {
            let addr = &addresses[loc];
            addr.chunk_idx == chunk_idx
        });

        // Get only the locations from addresses that point to this chunk.
        let rows2 = input_sel.iter_locations().filter_map(|loc| {
            let addr = &addresses[loc];
            if addr.chunk_idx == chunk_idx {
                Some(addr.row_idx as usize)
            } else {
                None
            }
        });

        compare_group_rows_eq(
            inputs,
            &chunks[chunk_idx as usize].arrays,
            rows1,
            rows2,
            not_eq_rows,
        )?;
    }

    Ok(())
}

fn compare_group_rows_eq<I1, I2>(
    arrays1: &[Array2],
    arrays2: &[Array2],
    rows1: I1,
    rows2: I2,
    not_eq_rows: &mut BTreeSet<usize>,
) -> Result<()>
where
    I1: Iterator<Item = usize> + Clone,
    I2: Iterator<Item = usize> + Clone,
{
    for col_idx in 0..arrays1.len() {
        let rows1 = rows1.clone();
        let rows2 = rows2.clone();

        let array1 = &arrays1[col_idx];
        let array2 = &arrays2[col_idx];

        // We need to handle trying to compare against untyped nulls in case
        // there's a hash collision with groups from different grouping sets
        // (e.g. group may have no masked columns but we're comparing against a
        // group with masked columns).
        if array1.physical_type() != array2.physical_type() {
            not_eq_rows.extend(rows1);
            return Ok(());
        }

        match array1.physical_type() {
            PhysicalType2::UntypedNull => compare_rows_eq::<PhysicalUntypedNull, _, _>(
                array1,
                array2,
                rows1,
                rows2,
                not_eq_rows,
            )?,
            PhysicalType2::Boolean => {
                compare_rows_eq::<PhysicalBool, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Int8 => {
                compare_rows_eq::<PhysicalI8, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Int16 => {
                compare_rows_eq::<PhysicalI16, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Int32 => {
                compare_rows_eq::<PhysicalI32, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Int64 => {
                compare_rows_eq::<PhysicalI64, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Int128 => {
                compare_rows_eq::<PhysicalI128, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::UInt8 => {
                compare_rows_eq::<PhysicalU8, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::UInt16 => {
                compare_rows_eq::<PhysicalU16, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::UInt32 => {
                compare_rows_eq::<PhysicalU32, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::UInt64 => {
                compare_rows_eq::<PhysicalU64, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::UInt128 => {
                compare_rows_eq::<PhysicalU128, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Float16 => {
                compare_rows_eq::<PhysicalF16_2, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Float32 => {
                compare_rows_eq::<PhysicalF32_2, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Float64 => {
                compare_rows_eq::<PhysicalF64_2, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Interval => compare_rows_eq::<PhysicalInterval, _, _>(
                array1,
                array2,
                rows1,
                rows2,
                not_eq_rows,
            )?,
            PhysicalType2::Binary => {
                compare_rows_eq::<PhysicalBinary, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::Utf8 => {
                compare_rows_eq::<PhysicalUtf8, _, _>(array1, array2, rows1, rows2, not_eq_rows)?
            }
            PhysicalType2::List => {
                not_implemented!("Row compare list")
            }
        }
    }

    Ok(())
}

/// Compares rows from two arrays, iterating each array using independent row
/// iters.
///
/// When a row is not equal, the row from the `rows1` iter will be inserted into
/// `not_eq_rows`.
fn compare_rows_eq<'a, S, I1, I2>(
    array1: &'a Array2,
    array2: &'a Array2,
    rows1: I1,
    rows2: I2,
    not_eq_rows: &mut BTreeSet<usize>,
) -> Result<()>
where
    S: PhysicalStorage2,
    <S::Storage<'a> as AddressableStorage>::T: PartialEq,
    I1: Iterator<Item = usize>,
    I2: Iterator<Item = usize>,
{
    let selection1 = array1.selection_vector();
    let selection2 = array2.selection_vector();

    let validity1 = array1.validity();
    let validity2 = array2.validity();

    let values1 = S::get_storage(array1.array_data())?;
    let values2 = S::get_storage(array2.array_data())?;

    if can_skip_validity_check([validity1, validity2]) {
        for (row1, row2) in rows1.zip(rows2) {
            let sel1 = unsafe { selection::get_unchecked(selection1, row1) };
            let sel2 = unsafe { selection::get_unchecked(selection2, row2) };

            let val1 = unsafe { values1.get_unchecked(sel1) };
            let val2 = unsafe { values2.get_unchecked(sel2) };

            if val1 != val2 {
                not_eq_rows.insert(row1);
            }
        }
    } else {
        for (row1, row2) in rows1.zip(rows2) {
            let sel1 = unsafe { selection::get_unchecked(selection1, row1) };
            let sel2 = unsafe { selection::get_unchecked(selection2, row2) };

            match (
                check_validity(sel1, validity1),
                check_validity(sel2, validity2),
            ) {
                (true, true) => {
                    // Rows both valid, check value equality.
                    let val1 = unsafe { values1.get_unchecked(sel1) };
                    let val2 = unsafe { values2.get_unchecked(sel2) };

                    if val1 != val2 {
                        not_eq_rows.insert(row1);
                    }
                }
                (false, false) => {
                    // Both rows "equal" in this case. When comparing GROUP BY
                    // values, we consider NULLs to be equal.
                }
                _ => {
                    // Not equal.
                    not_eq_rows.insert(row1);
                }
            }
        }
    }

    Ok(())
}
