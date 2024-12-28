use std::borrow::Borrow;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::{Array2, ArrayData2};
use crate::arrays::bitmap::Bitmap;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{
    ArrayBuilder,
    ArrayDataBuffer,
    BooleanBuffer,
    GermanVarlenBuffer,
    PrimitiveBuffer,
};
use crate::arrays::executor::physical_type::{
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalList,
    PhysicalStorage,
    PhysicalType2,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::selection;
use crate::arrays::storage::{
    AddressableStorage,
    ListItemMetadata,
    ListStorage,
    PrimitiveStorage,
    UntypedNullStorage,
};

/// Singular mapping of a `from` index to a `to` index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillMapping {
    pub from: usize,
    pub to: usize,
}

impl From<(usize, usize)> for FillMapping {
    fn from(value: (usize, usize)) -> Self {
        FillMapping {
            from: value.0,
            to: value.1,
        }
    }
}

/// Incrementally put values into a new array buffer from existing arrays using
/// a fill map.
#[derive(Debug)]
pub struct FillState<B: ArrayDataBuffer> {
    validity: Bitmap,
    builder: ArrayBuilder<B>,
}

impl<B> FillState<B>
where
    B: ArrayDataBuffer,
{
    pub fn new(builder: ArrayBuilder<B>) -> Self {
        let validity = Bitmap::new_with_all_true(builder.buffer.len());
        FillState { validity, builder }
    }

    /// Fill a new array buffer using values from some other array.
    ///
    /// `fill_map` is an iterator of mappings that map indices from `array` to
    /// where they should be placed in the buffer.
    pub fn fill<'a, S, I>(&mut self, array: &'a Array2, fill_map: I) -> Result<()>
    where
        S: PhysicalStorage,
        I: IntoIterator<Item = FillMapping>,
        S::Type<'a>: Borrow<<B as ArrayDataBuffer>::Type>,
    {
        let selection = array.selection_vector();

        match array.validity() {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                for mapping in fill_map.into_iter() {
                    let sel = unsafe { selection::get_unchecked(selection, mapping.from) };

                    if validity.value(sel) {
                        let val = unsafe { values.get_unchecked(sel) };
                        self.builder.buffer.put(mapping.to, val.borrow());
                    } else {
                        self.validity.set_unchecked(mapping.to, false)
                    }
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;

                for mapping in fill_map.into_iter() {
                    let sel = selection::get(selection, mapping.from);
                    let val = unsafe { values.get_unchecked(sel) };
                    self.builder.buffer.put(mapping.to, val.borrow());
                }
            }
        }

        Ok(())
    }

    pub fn finish(self) -> Array2 {
        let validity = if self.validity.is_all_true() {
            None
        } else {
            Some(self.validity.into())
        };

        Array2 {
            datatype: self.builder.datatype,
            selection: None,
            validity,
            data: self.builder.buffer.into_data(),
        }
    }
}

/// Concatenate multiple arrays into a single array.
pub fn concat(arrays: &[&Array2]) -> Result<Array2> {
    let total_len: usize = arrays.iter().map(|a| a.logical_len()).sum();
    concat_with_exact_total_len(arrays, total_len)
}

/// Concatenate multiple arrays into a single array.
///
/// `total_len` should be the exact length of the output.
///
/// This function exists so that we can compute the total length once for a set
/// of batches that we're concatenating instead of once per array.
pub(crate) fn concat_with_exact_total_len(arrays: &[&Array2], total_len: usize) -> Result<Array2> {
    let datatype = match arrays.first() {
        Some(arr) => arr.datatype(),
        None => return Err(RayexecError::new("Cannot concat zero arrays")),
    };

    match datatype.physical_type2()? {
        PhysicalType2::UntypedNull => Ok(Array2 {
            datatype: datatype.clone(),
            selection: None,
            validity: None,
            data: UntypedNullStorage(total_len).into(),
        }),
        PhysicalType2::Boolean => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: BooleanBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalBool, _>(arrays, state)
        }
        PhysicalType2::Int8 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalI8, _>(arrays, state)
        }
        PhysicalType2::Int16 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalI16, _>(arrays, state)
        }
        PhysicalType2::Int32 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalI32, _>(arrays, state)
        }
        PhysicalType2::Int64 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalI64, _>(arrays, state)
        }
        PhysicalType2::Int128 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalI128, _>(arrays, state)
        }
        PhysicalType2::UInt8 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalU8, _>(arrays, state)
        }
        PhysicalType2::UInt16 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalU16, _>(arrays, state)
        }
        PhysicalType2::UInt32 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalU32, _>(arrays, state)
        }
        PhysicalType2::UInt64 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalU64, _>(arrays, state)
        }
        PhysicalType2::UInt128 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalU128, _>(arrays, state)
        }
        PhysicalType2::Float16 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalF16, _>(arrays, state)
        }
        PhysicalType2::Float32 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalF32, _>(arrays, state)
        }
        PhysicalType2::Float64 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalF64, _>(arrays, state)
        }
        PhysicalType2::Interval => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalInterval, _>(arrays, state)
        }
        PhysicalType2::Utf8 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: GermanVarlenBuffer::<str>::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalUtf8, _>(arrays, state)
        }
        PhysicalType2::Binary => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: GermanVarlenBuffer::<[u8]>::with_len(total_len),
            });
            concat_with_fill_state::<PhysicalBinary, _>(arrays, state)
        }
        PhysicalType2::List => concat_lists(datatype.clone(), arrays, total_len),
    }
}

fn concat_lists(datatype: DataType, arrays: &[&Array2], total_len: usize) -> Result<Array2> {
    let inner_arrays = arrays
        .iter()
        .map(|arr| match arr.array_data() {
            ArrayData2::List(list) => {
                if list.array.has_selection() {
                    return Err(RayexecError::new("List child array has selection"));
                }
                Ok(&list.array)
            }
            other => Err(RayexecError::new(format!(
                "Invalid inner array data for concatenating lists, got {:?}",
                other.physical_type()
            ))),
        })
        .collect::<Result<Vec<_>>>()?;

    let concatenated = concat(&inner_arrays)?;

    // Update metadata objects.
    let mut metadatas = Vec::with_capacity(total_len);
    let mut validity = Bitmap::new_with_all_true(total_len);

    let mut acc_rows = 0;

    for (array, child_array) in arrays.iter().zip(inner_arrays) {
        UnaryExecutor::for_each::<PhysicalList, _>(array, |_row_num, metadata| match metadata {
            Some(metadata) => {
                metadatas.push(ListItemMetadata {
                    offset: metadata.offset + acc_rows,
                    len: metadata.len,
                });
            }
            None => {
                metadatas.push(ListItemMetadata::default());
                validity.set_unchecked(metadatas.len() - 1, false);
            }
        })?;

        acc_rows += child_array.logical_len() as i32;
    }

    let data = ListStorage {
        metadata: PrimitiveStorage::from(metadatas),
        array: concatenated,
    };

    Ok(Array2 {
        datatype,
        selection: None,
        validity: Some(validity.into()),
        data: data.into(),
    })
}

fn concat_with_fill_state<'a, S, B>(
    arrays: &'a [&Array2],
    mut fill_state: FillState<B>,
) -> Result<Array2>
where
    S: PhysicalStorage,
    B: ArrayDataBuffer,
    S::Type<'a>: Borrow<<B as ArrayDataBuffer>::Type>,
{
    let mut offset = 0;

    for array in arrays {
        let len = array.logical_len();
        let iter = (0..len).map(|idx| FillMapping {
            from: idx,
            to: idx + offset,
        });

        fill_state.fill::<S, _>(array, iter)?;

        offset += len;
    }

    Ok(fill_state.finish())
}

/// Interleave multiple arrays into one.
///
/// Indices contains (array_idx, row_idx) pairs where 'row_idx' is the row
/// within the array. The length of indices indicates the length of the output
/// array.
///
/// Indices may be specified more than once.
pub fn interleave(arrays: &[&Array2], indices: &[(usize, usize)]) -> Result<Array2> {
    let datatype = match arrays.first() {
        Some(arr) => arr.datatype(),
        None => return Err(RayexecError::new("Cannot interleave zero arrays")),
    };

    match datatype.physical_type2()? {
        PhysicalType2::UntypedNull => Ok(Array2 {
            datatype: datatype.clone(),
            selection: None,
            validity: None,
            data: UntypedNullStorage(indices.len()).into(),
        }),
        PhysicalType2::Boolean => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: BooleanBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalBool, _>(arrays, indices, state)
        }
        PhysicalType2::Int8 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalI8, _>(arrays, indices, state)
        }
        PhysicalType2::Int16 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalI16, _>(arrays, indices, state)
        }
        PhysicalType2::Int32 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalI32, _>(arrays, indices, state)
        }
        PhysicalType2::Int64 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalI64, _>(arrays, indices, state)
        }
        PhysicalType2::Int128 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalI128, _>(arrays, indices, state)
        }
        PhysicalType2::UInt8 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalU8, _>(arrays, indices, state)
        }
        PhysicalType2::UInt16 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalU16, _>(arrays, indices, state)
        }
        PhysicalType2::UInt32 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalU32, _>(arrays, indices, state)
        }
        PhysicalType2::UInt64 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalU64, _>(arrays, indices, state)
        }
        PhysicalType2::UInt128 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalU128, _>(arrays, indices, state)
        }
        PhysicalType2::Float16 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalF16, _>(arrays, indices, state)
        }
        PhysicalType2::Float32 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalF32, _>(arrays, indices, state)
        }
        PhysicalType2::Float64 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalF64, _>(arrays, indices, state)
        }
        PhysicalType2::Interval => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalInterval, _>(arrays, indices, state)
        }
        PhysicalType2::Utf8 => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: GermanVarlenBuffer::<str>::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalUtf8, _>(arrays, indices, state)
        }
        PhysicalType2::Binary => {
            let state = FillState::new(ArrayBuilder {
                datatype: datatype.clone(),
                buffer: GermanVarlenBuffer::<[u8]>::with_len(indices.len()),
            });
            interleave_with_fill_state::<PhysicalBinary, _>(arrays, indices, state)
        }
        PhysicalType2::List => {
            // TODO: Also doable
            Err(RayexecError::new(
                "interleaving list arrays not yet supported",
            ))
        }
    }
}

fn interleave_with_fill_state<'a, S, B>(
    arrays: &'a [&Array2],
    indices: &[(usize, usize)],
    mut fill_state: FillState<B>,
) -> Result<Array2>
where
    S: PhysicalStorage,
    B: ArrayDataBuffer,
    S::Type<'a>: Borrow<<B as ArrayDataBuffer>::Type>,
{
    for (idx, array) in arrays.iter().enumerate() {
        // Generates an iter that maps rows from the array we're currently on to
        // the rows in the output.
        let iter =
            indices
                .iter()
                .enumerate()
                .filter_map(|(row_idx, (array_idx, array_row_idx))| {
                    if *array_idx == idx {
                        Some(FillMapping {
                            from: *array_row_idx,
                            to: row_idx,
                        })
                    } else {
                        None
                    }
                });

        fill_state.fill::<S, _>(array, iter)?;
    }

    Ok(fill_state.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::executor::builder::PrimitiveBuffer;
    use crate::arrays::executor::physical_type::PhysicalI32;
    use crate::arrays::scalar::ScalarValue;

    #[test]
    fn fill_simple_linear() {
        let mut state = FillState::new(ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        });

        let arr = Array2::from_iter([4, 5, 6]);
        let mapping = [
            FillMapping { from: 0, to: 0 },
            FillMapping { from: 1, to: 1 },
            FillMapping { from: 2, to: 2 },
        ];

        state.fill::<PhysicalI32, _>(&arr, mapping).unwrap();

        let got = state.finish();

        assert_eq!(ScalarValue::from(4), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(6), got.logical_value(2).unwrap());
    }

    #[test]
    fn fill_repeated() {
        let mut state = FillState::new(ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        });

        let arr = Array2::from_iter([4, 5, 6]);
        let mapping = [
            FillMapping { from: 1, to: 0 },
            FillMapping { from: 1, to: 1 },
            FillMapping { from: 1, to: 2 },
        ];

        state.fill::<PhysicalI32, _>(&arr, mapping).unwrap();

        let got = state.finish();

        assert_eq!(ScalarValue::from(5), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(2).unwrap());
    }

    #[test]
    fn fill_out_of_order() {
        let mut state = FillState::new(ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(3),
        });

        let arr = Array2::from_iter([4, 5, 6]);
        let mapping = [
            FillMapping { from: 0, to: 1 },
            FillMapping { from: 1, to: 2 },
            FillMapping { from: 2, to: 0 },
        ];

        state.fill::<PhysicalI32, _>(&arr, mapping).unwrap();

        let got = state.finish();

        assert_eq!(ScalarValue::from(6), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(4), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(2).unwrap());
    }

    #[test]
    fn fill_from_different_arrays() {
        let mut state = FillState::new(ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::<i32>::with_len(6),
        });

        let arr1 = Array2::from_iter([4, 5, 6]);
        let mapping1 = [
            FillMapping { from: 0, to: 2 },
            FillMapping { from: 1, to: 4 },
            FillMapping { from: 2, to: 0 },
        ];
        state.fill::<PhysicalI32, _>(&arr1, mapping1).unwrap();

        let arr2 = Array2::from_iter([7, 8, 9]);
        let mapping2 = [
            FillMapping { from: 0, to: 1 },
            FillMapping { from: 1, to: 3 },
            FillMapping { from: 2, to: 5 },
        ];
        state.fill::<PhysicalI32, _>(&arr2, mapping2).unwrap();

        let got = state.finish();

        assert_eq!(ScalarValue::from(6), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(7), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(4), got.logical_value(2).unwrap());
        assert_eq!(ScalarValue::from(8), got.logical_value(3).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(4).unwrap());
        assert_eq!(ScalarValue::from(9), got.logical_value(5).unwrap());
    }

    #[test]
    fn interleave_2() {
        let arr1 = Array2::from_iter([4, 5, 6]);
        let arr2 = Array2::from_iter([7, 8, 9]);

        let indices = [(0, 1), (0, 2), (1, 0), (1, 1), (0, 0), (1, 2)];

        let got = interleave(&[&arr1, &arr2], &indices).unwrap();

        assert_eq!(ScalarValue::from(5), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(6), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(7), got.logical_value(2).unwrap());
        assert_eq!(ScalarValue::from(8), got.logical_value(3).unwrap());
        assert_eq!(ScalarValue::from(4), got.logical_value(4).unwrap());
        assert_eq!(ScalarValue::from(9), got.logical_value(5).unwrap());
    }

    #[test]
    fn interleave_2_repeated() {
        let arr1 = Array2::from_iter([4, 5]);
        let arr2 = Array2::from_iter([7, 8]);

        let indices = [(0, 1), (1, 1), (0, 1), (1, 1)];

        let got = interleave(&[&arr1, &arr2], &indices).unwrap();

        assert_eq!(ScalarValue::from(5), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(8), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(2).unwrap());
        assert_eq!(ScalarValue::from(8), got.logical_value(3).unwrap());
    }

    #[test]
    fn concat_2() {
        let arr1 = Array2::from_iter([4, 5, 6]);
        let arr2 = Array2::from_iter([7, 8]);

        let got = concat(&[&arr1, &arr2]).unwrap();

        assert_eq!(ScalarValue::from(4), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(5), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(6), got.logical_value(2).unwrap());
        assert_eq!(ScalarValue::from(7), got.logical_value(3).unwrap());
        assert_eq!(ScalarValue::from(8), got.logical_value(4).unwrap());
    }

    #[test]
    fn concat_lists() {
        let arr1 = ScalarValue::List(vec![1.into(), 2.into()])
            .as_array(1)
            .unwrap();
        let arr2 = ScalarValue::List(vec![3.into(), 4.into(), 5.into()])
            .as_array(1)
            .unwrap();

        let got = concat(&[&arr1, &arr2]).unwrap();

        assert_eq!(
            ScalarValue::List(vec![1.into(), 2.into()]),
            got.logical_value(0).unwrap()
        );
        assert_eq!(
            ScalarValue::List(vec![3.into(), 4.into(), 5.into()]),
            got.logical_value(1).unwrap()
        );
    }
}
