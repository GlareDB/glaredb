use crate::array::{Array, OffsetIndex, PrimitiveArray, VarlenArray, VarlenType};
use rayexec_error::{RayexecError, Result};

/// Binary-encoded rows suitable for comparisons.
#[derive(Debug)]
pub struct ComparableRows {
    /// Underlying row data.
    data: Vec<u8>,

    /// Offsets into the data buffer.
    offsets: Vec<usize>,
}

impl ComparableRows {
    pub fn num_rows(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn first(&self) -> Option<ComparableRow<'_>> {
        self.row(0)
    }

    pub fn last(&self) -> Option<ComparableRow<'_>> {
        if self.num_rows() == 0 {
            return None;
        }
        self.row(self.num_rows() - 1)
    }

    pub fn row(&self, idx: usize) -> Option<ComparableRow<'_>> {
        if idx > self.num_rows() {
            return None;
        }

        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];

        Some(ComparableRow {
            data: &self.data[start..end],
        })
    }

    pub fn iter(&self) -> ComparableRowIter {
        ComparableRowIter { rows: self, idx: 0 }
    }
}

/// A row that can be compared to another row
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ComparableRow<'a> {
    data: &'a [u8],
}

#[derive(Debug)]
pub struct ComparableRowIter<'a> {
    rows: &'a ComparableRows,
    idx: usize,
}

impl<'a> Iterator for ComparableRowIter<'a> {
    type Item = ComparableRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.rows.num_rows() {
            return None;
        }
        let row = self.rows.row(self.idx).expect("row to exist");
        self.idx += 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.num_rows() - self.idx;
        (remaining, Some(remaining))
    }
}

impl<'a> ComparableRow<'a> {
    pub fn data(&self) -> &'a [u8] {
        &self.data
    }
}

#[derive(Debug, Clone)]
pub struct ComparableColumn {
    pub desc: bool,
    pub nulls_first: bool,
}

impl ComparableColumn {
    const fn null_bit(&self) -> u8 {
        if self.nulls_first {
            1
        } else {
            0
        }
    }

    const fn valid_bit(&self) -> u8 {
        1 - self.null_bit()
    }

    /// Invert all bits in buf if this column should be ordered descending.
    ///
    /// Does nothing if the column is ascending.
    ///
    /// This is done to encode ordering in the encoding which lets us skip
    /// having extra logic to handle ordering in the operators. While that would
    /// be easy with something like 'ORDER BY a DESC', it would get tricky with
    /// 'ORDER BY a DESC, b ASC, c DESC'.
    fn invert_if_desc(&self, buf: &mut [u8]) {
        if self.desc {
            for b in buf {
                *b = !*b;
            }
        }
    }
}

/// Encoder for encoding arrays into rows.
#[derive(Debug, Clone)]
pub struct ComparableRowEncoder {
    /// Columns we'll be encoding.
    pub columns: Vec<ComparableColumn>,
}

impl ComparableRowEncoder {
    pub fn encode(&self, columns: &[&Array]) -> Result<ComparableRows> {
        if columns.len() != self.columns.len() {
            return Err(RayexecError::new("Column mismatch"));
        }

        let num_rows = columns
            .first()
            .ok_or_else(|| RayexecError::new("Cannot encode zero columns"))?
            .len();
        for arr in columns {
            if arr.len() != num_rows {
                return Err(RayexecError::new(format!(
                    "Expected array to be length {num_rows}, got {}",
                    arr.len()
                )));
            }
        }

        let size = self.compute_data_size(columns);
        let mut data = vec![0; size];

        let mut offsets: Vec<usize> = vec![0];

        for row_idx in 0..num_rows {
            let mut row_offset = *offsets.last().unwrap();
            for (arr, cmp_col) in columns.iter().zip(self.columns.iter()) {
                row_offset = match arr {
                    Array::Null(_) => unimplemented!(),
                    Array::Boolean(_) => unimplemented!(),
                    Array::Int8(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::Int16(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::Int32(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::Int64(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::UInt8(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::UInt16(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::UInt32(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::UInt64(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::Float32(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::Float64(arr) => Self::encode_primitive(
                        cmp_col,
                        arr,
                        row_idx,
                        data.as_mut_slice(),
                        row_offset,
                    ),
                    Array::Utf8(arr) => {
                        Self::encode_varlen(cmp_col, arr, row_idx, data.as_mut_slice(), row_offset)
                    }
                    Array::LargeUtf8(arr) => {
                        Self::encode_varlen(cmp_col, arr, row_idx, data.as_mut_slice(), row_offset)
                    }
                    Array::Binary(arr) => {
                        Self::encode_varlen(cmp_col, arr, row_idx, data.as_mut_slice(), row_offset)
                    }
                    Array::LargeBinary(arr) => {
                        Self::encode_varlen(cmp_col, arr, row_idx, data.as_mut_slice(), row_offset)
                    }
                    _ => unimplemented!(),
                };
            }

            offsets.push(row_offset);
        }

        Ok(ComparableRows { data, offsets })
    }

    /// Compute the size of the data buffer we'll need for storing all encoded
    /// rows.
    fn compute_data_size(&self, columns: &[&Array]) -> usize {
        let mut size = 0;
        for arr in columns {
            let mut arr_size = match arr {
                Array::Null(_) => 0, // Nulls will be encoded in the "validity" portion of the row.
                Array::Boolean(arr) => arr.len() * std::mem::size_of::<bool>(), // Note this will expand the 1 bit bools to bytes.
                Array::Int8(arr) => arr.len() * std::mem::size_of::<i8>(),
                Array::Int16(arr) => arr.len() * std::mem::size_of::<i16>(),
                Array::Int32(arr) => arr.len() * std::mem::size_of::<i32>(),
                Array::Int64(arr) => arr.len() * std::mem::size_of::<i64>(),
                Array::UInt8(arr) => arr.len() * std::mem::size_of::<u8>(),
                Array::UInt16(arr) => arr.len() * std::mem::size_of::<u16>(),
                Array::UInt32(arr) => arr.len() * std::mem::size_of::<u32>(),
                Array::UInt64(arr) => arr.len() * std::mem::size_of::<u64>(),
                Array::Float32(arr) => arr.len() * std::mem::size_of::<f32>(),
                Array::Float64(arr) => arr.len() * std::mem::size_of::<f64>(),
                Array::Utf8(arr) => arr.data().as_ref().len(),
                Array::LargeUtf8(arr) => arr.data().as_ref().len(),
                Array::Binary(arr) => arr.data().as_ref().len(),
                Array::LargeBinary(arr) => arr.data().as_ref().len(),
                _ => unimplemented!(),
            };

            // Account for validities.
            //
            // Currently all rows will have validities written for every column
            // even if there's no validity bitmap for the column. This just
            // makes implementation easier.
            arr_size += std::mem::size_of::<u8>() * arr.len();

            size += arr_size;
        }

        size
    }

    /// Encodes a variable length array into `buf` starting at `start`.
    ///
    /// This should return the new offset to write to for the next value.
    fn encode_varlen<T: ComparableEncode + VarlenType + ?Sized, O: OffsetIndex>(
        col: &ComparableColumn,
        arr: &VarlenArray<T, O>,
        row: usize,
        buf: &mut [u8],
        start: usize,
    ) -> usize {
        let null_bit = col.null_bit();
        let valid_bit = col.valid_bit();

        if arr.is_valid(row).expect("row to be in bounds") {
            buf[start] = valid_bit;
            let value = arr.value(row).expect("row to be in bounds");
            let end = start + 1 + value.as_binary().len();
            let write_buf = &mut buf[start + 1..end];
            value.encode(write_buf);
            col.invert_if_desc(write_buf);

            let offset = start + 1 + write_buf.len();
            offset
        } else {
            buf[start] = null_bit;

            let offset = start + 1;
            offset
        }
    }

    /// Encodes a primitive length array into `buf` starting at `start`.
    ///
    /// This should return the new offset to write to for the next value.
    fn encode_primitive<T: ComparableEncode>(
        col: &ComparableColumn,
        arr: &PrimitiveArray<T>,
        row: usize,
        buf: &mut [u8],
        start: usize,
    ) -> usize {
        let null_bit = col.null_bit();
        let valid_bit = col.valid_bit();

        if arr.is_valid(row).expect("row to be in bounds") {
            buf[start] = valid_bit;
            let value = arr.value(row).expect("row to be in bounds");
            let end = start + 1 + std::mem::size_of::<T>();
            let write_buf = &mut buf[start + 1..end];
            value.encode(write_buf);
            col.invert_if_desc(write_buf);

            let offset = start + 1 + write_buf.len();
            offset
        } else {
            buf[start] = null_bit;

            let offset = start + 1;
            offset
        }
    }
}

/// Trait for types that can encode themselves into a comparable binary
/// representation.
trait ComparableEncode {
    fn encode(&self, buf: &mut [u8]);
}

/// Implements `ComparableEncode` for unsigned ints.
macro_rules! comparable_encode_unsigned {
    ($type:ty) => {
        impl ComparableEncode for $type {
            fn encode(&self, buf: &mut [u8]) {
                let b = self.to_be_bytes();
                buf.copy_from_slice(&b);
            }
        }
    };
}

comparable_encode_unsigned!(u8);
comparable_encode_unsigned!(u16);
comparable_encode_unsigned!(u32);
comparable_encode_unsigned!(u64);

/// Implements `ComparableEncode` for signed ints.
macro_rules! comparable_encode_signed {
    ($type:ty) => {
        impl ComparableEncode for $type {
            fn encode(&self, buf: &mut [u8]) {
                let mut b = self.to_be_bytes();
                b[0] ^= 128; // Flip sign bit.
                buf.copy_from_slice(&b);
            }
        }
    };
}

comparable_encode_signed!(i8);
comparable_encode_signed!(i16);
comparable_encode_signed!(i32);
comparable_encode_signed!(i64);

impl ComparableEncode for f32 {
    fn encode(&self, buf: &mut [u8]) {
        // Adapted from <https://github.com/rust-lang/rust/blob/791adf759cc065316f054961875052d5bc03e16c/library/core/src/num/f32.rs#L1456-L1485>
        let bits = self.to_bits() as i32;
        let v = bits ^ (((bits >> 31) as u32) >> 1) as i32;
        v.encode(buf)
    }
}

impl ComparableEncode for f64 {
    fn encode(&self, buf: &mut [u8]) {
        // Adapted from <https://github.com/rust-lang/rust/blob/791adf759cc065316f054961875052d5bc03e16c/library/core/src/num/f32.rs#L1456-L1485>
        let bits = self.to_bits() as i64;
        let v = bits ^ (((bits >> 31) as u64) >> 1) as i64;
        v.encode(buf)
    }
}

impl ComparableEncode for str {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(self.as_bytes())
    }
}

impl ComparableEncode for [u8] {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(self)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn simple_primitive_cmp_between_cols_asc() {
        let col1 = Array::Int32(Int32Array::from_iter([-1, 0, 1]));
        let col2 = Array::Int32(Int32Array::from_iter([1, 0, -1]));

        let encoder = ComparableRowEncoder {
            columns: vec![ComparableColumn {
                desc: false,
                nulls_first: false,
            }],
        };

        let rows1 = encoder.encode(&[&col1]).unwrap();
        let rows2 = encoder.encode(&[&col2]).unwrap();

        assert_eq!(3, rows1.num_rows());
        assert_eq!(3, rows2.num_rows());

        let cmps: Vec<_> = (rows1.iter().zip(rows2.iter()))
            .map(|(left, right)| left.cmp(&right))
            .collect();

        let expected = vec![Ordering::Less, Ordering::Equal, Ordering::Greater];
        assert_eq!(expected, cmps);
    }

    #[test]
    fn simple_primitive_cmp_between_cols_desc() {
        let col1 = Array::Int32(Int32Array::from_iter([-1, 0, 1]));
        let col2 = Array::Int32(Int32Array::from_iter([1, 0, -1]));

        let encoder = ComparableRowEncoder {
            columns: vec![ComparableColumn {
                desc: true,
                nulls_first: false,
            }],
        };

        let rows1 = encoder.encode(&[&col1]).unwrap();
        let rows2 = encoder.encode(&[&col2]).unwrap();

        assert_eq!(3, rows1.num_rows());
        assert_eq!(3, rows2.num_rows());

        let cmps: Vec<_> = (rows1.iter().zip(rows2.iter()))
            .map(|(left, right)| left.cmp(&right))
            .collect();

        // Flipped from above, since if we're ordering in descending order.
        let expected = vec![Ordering::Greater, Ordering::Equal, Ordering::Less];
        assert_eq!(expected, cmps);
    }

    #[test]
    fn simple_varlen_cmp_between_cols_asc() {
        let col1 = Array::Utf8(Utf8Array::from_iter(["a", "aa", "bb"]));
        let col2 = Array::Utf8(Utf8Array::from_iter(["aa", "a", "bb"]));

        let encoder = ComparableRowEncoder {
            columns: vec![ComparableColumn {
                desc: false,
                nulls_first: false,
            }],
        };

        let rows1 = encoder.encode(&[&col1]).unwrap();
        let rows2 = encoder.encode(&[&col2]).unwrap();

        assert_eq!(3, rows1.num_rows());
        assert_eq!(3, rows2.num_rows());

        let cmps: Vec<_> = (rows1.iter().zip(rows2.iter()))
            .map(|(left, right)| left.cmp(&right))
            .collect();

        let expected = vec![Ordering::Less, Ordering::Greater, Ordering::Equal];
        assert_eq!(expected, cmps);
    }
}
