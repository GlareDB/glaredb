use half::f16;
use rayexec_error::{not_implemented, RayexecError, Result};

use crate::arrays::array::{Array2, ArrayData, BinaryData};
use crate::arrays::executor::physical_type::{
    AsBytes,
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
    PhysicalStorage,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::scalar::interval::Interval;

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
        self.data
    }
}

/// Configuration for how to encode a column.
#[derive(Debug, Clone)]
pub struct ComparableColumn {
    /// If we should encode columns to reverse the natural sort order for
    /// values.
    ///
    /// If this is false, this should correspond to sorting in ascending order
    /// (e.g. '1 < 2' evaluates to true).
    ///
    /// If true, this is reverse the sort order (e.g. '1 < 2' evaluates to
    /// false, causing '2' to come before '1').
    pub desc: bool,
    /// If we should encode nulls such that they should be ordered before any
    /// valid values.
    pub nulls_first: bool,
}

impl ComparableColumn {
    const fn null_byte(&self) -> u8 {
        if self.nulls_first {
            0
        } else {
            0xFF
        }
    }

    const fn valid_byte(&self) -> u8 {
        !self.null_byte()
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
    pub fn encode(&self, columns: &[&Array2]) -> Result<ComparableRows> {
        if columns.len() != self.columns.len() {
            return Err(RayexecError::new("Column mismatch"));
        }

        let num_rows = columns
            .first()
            .ok_or_else(|| RayexecError::new("Cannot encode zero columns"))?
            .logical_len();
        for arr in columns {
            if arr.logical_len() != num_rows {
                return Err(RayexecError::new(format!(
                    "Expected array to be length {num_rows}, got {}",
                    arr.logical_len()
                )));
            }
        }

        let size = self.compute_data_size(columns)?;
        let mut data = vec![0; size];

        // Track start offset per row.
        //
        // First offset is always 0.
        let mut offsets: Vec<usize> = Vec::with_capacity(num_rows + 1);
        offsets.push(0);

        for row_idx in 0..num_rows {
            let data = data.as_mut_slice();

            let mut row_offset = *offsets.last().unwrap();
            for (arr, cmp_col) in columns.iter().zip(self.columns.iter()) {
                row_offset = match arr.array_data() {
                    ArrayData::UntypedNull(_) => {
                        Self::encode_untyped_null(cmp_col, data, row_offset)?
                    }
                    ArrayData::Boolean(_) => Self::encode_primitive::<PhysicalBool>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Int8(_) => Self::encode_primitive::<PhysicalI8>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Int16(_) => Self::encode_primitive::<PhysicalI16>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Int32(_) => Self::encode_primitive::<PhysicalI32>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Int64(_) => Self::encode_primitive::<PhysicalI64>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Int128(_) => Self::encode_primitive::<PhysicalI128>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::UInt8(_) => Self::encode_primitive::<PhysicalU8>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::UInt16(_) => Self::encode_primitive::<PhysicalU16>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::UInt32(_) => Self::encode_primitive::<PhysicalU32>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::UInt64(_) => Self::encode_primitive::<PhysicalU64>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::UInt128(_) => Self::encode_primitive::<PhysicalU128>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Float16(_) => Self::encode_primitive::<PhysicalF16>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Float32(_) => Self::encode_primitive::<PhysicalF32>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Float64(_) => Self::encode_primitive::<PhysicalF64>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Interval(_) => Self::encode_primitive::<PhysicalInterval>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::Binary(_) => Self::encode_varlen::<PhysicalBinary>(
                        cmp_col, arr, row_idx, data, row_offset,
                    )?,
                    ArrayData::List(_) => not_implemented!("Row encode list"),
                };
            }

            offsets.push(row_offset);
        }

        Ok(ComparableRows { data, offsets })
    }

    /// Compute the size of the data buffer we'll need for storing all encoded
    /// rows.
    fn compute_data_size(&self, columns: &[&Array2]) -> Result<usize> {
        let mut size = 0;
        for arr in columns {
            let mut arr_size = match arr.array_data() {
                ArrayData::UntypedNull(_) => 0, // Nulls will be encoded in the "validity" portion of the row.
                ArrayData::Boolean(d) => d.len() * std::mem::size_of::<bool>(), // Note this will expand the 1 bit bools to bytes.
                ArrayData::Int8(d) => d.data_size_bytes(),
                ArrayData::Int16(d) => d.data_size_bytes(),
                ArrayData::Int32(d) => d.data_size_bytes(),
                ArrayData::Int64(d) => d.data_size_bytes(),
                ArrayData::Int128(d) => d.data_size_bytes(),
                ArrayData::UInt8(d) => d.data_size_bytes(),
                ArrayData::UInt16(d) => d.data_size_bytes(),
                ArrayData::UInt32(d) => d.data_size_bytes(),
                ArrayData::UInt64(d) => d.data_size_bytes(),
                ArrayData::UInt128(d) => d.data_size_bytes(),
                ArrayData::Float16(d) => d.data_size_bytes(),
                ArrayData::Float32(d) => d.data_size_bytes(),
                ArrayData::Float64(d) => d.data_size_bytes(),
                ArrayData::Interval(d) => d.data_size_bytes(),
                ArrayData::Binary(d) => match d {
                    BinaryData::Binary(d) => d.data_size_bytes(),
                    BinaryData::LargeBinary(d) => d.data_size_bytes(),
                    BinaryData::German(d) => d.data_size_bytes(),
                },
                ArrayData::List(_) => not_implemented!("Row encode list"),
            };

            // Account for validities.
            //
            // Currently all rows will have validities written for every column
            // even if there's no validity bitmap for the column. This just
            // makes implementation easier.
            arr_size += std::mem::size_of::<u8>() * arr.logical_len();

            size += arr_size;
        }

        Ok(size)
    }

    /// Encodes a variable length array into `buf` starting at `start`.
    ///
    /// This should return the new offset to write to for the next value.
    fn encode_varlen<'a, S>(
        col: &ComparableColumn,
        arr: &'a Array2,
        row: usize,
        buf: &mut [u8],
        start: usize,
    ) -> Result<usize>
    where
        S: PhysicalStorage,
        S::Type<'a>: ComparableEncode + AsBytes,
    {
        let null_byte = col.null_byte();
        let valid_byte = col.valid_byte();

        match UnaryExecutor::value_at::<S>(arr, row)? {
            Some(val) => {
                buf[start] = valid_byte;
                let end = start + 1 + val.as_bytes().len();
                let write_buf = &mut buf[start + 1..end];
                val.encode(write_buf);
                col.invert_if_desc(write_buf);

                Ok(start + 1 + write_buf.len())
            }
            None => {
                buf[start] = null_byte;

                Ok(start + 1)
            }
        }
    }

    fn encode_untyped_null(col: &ComparableColumn, buf: &mut [u8], start: usize) -> Result<usize> {
        let null_byte = col.null_byte();
        buf[start] = null_byte;
        Ok(start + 1)
    }

    /// Encodes a primitive length array into `buf` starting at `start`.
    ///
    /// This should return the new offset to write to for the next value.
    fn encode_primitive<'a, S>(
        col: &ComparableColumn,
        arr: &'a Array2,
        row: usize,
        buf: &mut [u8],
        start: usize,
    ) -> Result<usize>
    where
        S: PhysicalStorage,
        S::Type<'a>: ComparableEncode,
    {
        let null_byte = col.null_byte();
        let valid_byte = col.valid_byte();

        match UnaryExecutor::value_at::<S>(arr, row)? {
            Some(val) => {
                buf[start] = valid_byte;
                let end = start + 1 + std::mem::size_of::<S::Type<'a>>();
                let write_buf = &mut buf[start + 1..end];
                val.encode(write_buf);
                col.invert_if_desc(write_buf);

                Ok(start + 1 + write_buf.len())
            }
            None => {
                buf[start] = null_byte;

                Ok(start + 1)
            }
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
comparable_encode_unsigned!(u128);

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
comparable_encode_signed!(i128);

impl ComparableEncode for f16 {
    fn encode(&self, buf: &mut [u8]) {
        let bits = self.to_bits() as i16;
        let v = bits ^ (((bits >> 15) as u16) >> 1) as i16;
        v.encode(buf)
    }
}

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

impl ComparableEncode for Interval {
    fn encode(&self, buf: &mut [u8]) {
        // TODO: We'll probably need to ensure intervals are normalized.
        self.months.encode(buf);
        self.days.encode(buf);
        self.nanos.encode(buf);
    }
}

// FALSE < TRUE
impl ComparableEncode for bool {
    fn encode(&self, buf: &mut [u8]) {
        if *self {
            buf[0] = 0;
        } else {
            buf[0] = 255;
        }
    }
}

impl ComparableEncode for &str {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(self.as_bytes())
    }
}

impl ComparableEncode for &[u8] {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(self)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn simple_primitive_cmp_between_cols_asc() {
        let col1 = Array2::from_iter([-1, 0, 1]);
        let col2 = Array2::from_iter([1, 0, -1]);

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
        let col1 = Array2::from_iter([-1, 0, 1]);
        let col2 = Array2::from_iter([1, 0, -1]);

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
        let col1 = Array2::from_iter(["a", "aa", "bb"]);
        let col2 = Array2::from_iter(["aa", "a", "bb"]);

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

    #[test]
    fn primitive_nulls_last_asc() {
        let col1 = Array2::from_iter([Some(-1), None, Some(1), Some(2)]);
        let col2 = Array2::from_iter([Some(1), Some(0), Some(-1), None]);

        let encoder = ComparableRowEncoder {
            columns: vec![ComparableColumn {
                desc: false,
                nulls_first: false,
            }],
        };

        let rows1 = encoder.encode(&[&col1]).unwrap();
        let rows2 = encoder.encode(&[&col2]).unwrap();

        assert_eq!(4, rows1.num_rows());
        assert_eq!(4, rows2.num_rows());

        assert!(rows1.row(0).unwrap() < rows2.row(0).unwrap());
        assert!(rows1.row(1).unwrap() > rows2.row(1).unwrap());
        assert!(rows1.row(2).unwrap() > rows2.row(2).unwrap());
        assert!(rows1.row(3).unwrap() < rows2.row(3).unwrap());
    }

    #[test]
    fn primitive_nulls_last_desc() {
        let col1 = Array2::from_iter([Some(-1), None, Some(1), Some(2)]);
        let col2 = Array2::from_iter([Some(1), Some(0), Some(-1), None]);

        let encoder = ComparableRowEncoder {
            columns: vec![ComparableColumn {
                desc: true,
                nulls_first: false,
            }],
        };

        let rows1 = encoder.encode(&[&col1]).unwrap();
        let rows2 = encoder.encode(&[&col2]).unwrap();

        assert_eq!(4, rows1.num_rows());
        assert_eq!(4, rows2.num_rows());

        assert!(rows1.row(0).unwrap() > rows2.row(0).unwrap());
        assert!(rows1.row(1).unwrap() > rows2.row(1).unwrap());
        assert!(rows1.row(2).unwrap() < rows2.row(2).unwrap());
        assert!(rows1.row(3).unwrap() < rows2.row(3).unwrap());
    }

    #[test]
    fn primitive_nulls_first_asc() {
        let col1 = Array2::from_iter([Some(-1), None, Some(1), Some(2)]);
        let col2 = Array2::from_iter([Some(1), Some(0), Some(-1), None]);

        let encoder = ComparableRowEncoder {
            columns: vec![ComparableColumn {
                desc: false,
                nulls_first: true,
            }],
        };

        let rows1 = encoder.encode(&[&col1]).unwrap();
        let rows2 = encoder.encode(&[&col2]).unwrap();

        assert_eq!(4, rows1.num_rows());
        assert_eq!(4, rows2.num_rows());

        assert!(rows1.row(0).unwrap() < rows2.row(0).unwrap());
        assert!(rows1.row(1).unwrap() < rows2.row(1).unwrap());
        assert!(rows1.row(2).unwrap() > rows2.row(2).unwrap());
        assert!(rows1.row(3).unwrap() > rows2.row(3).unwrap());
    }

    #[test]
    fn primitive_nulls_first_desc() {
        let col1 = Array2::from_iter([Some(-1), None, Some(1), Some(2)]);
        let col2 = Array2::from_iter([Some(1), Some(0), Some(-1), None]);

        let encoder = ComparableRowEncoder {
            columns: vec![ComparableColumn {
                desc: true,
                nulls_first: true,
            }],
        };

        let rows1 = encoder.encode(&[&col1]).unwrap();
        let rows2 = encoder.encode(&[&col2]).unwrap();

        assert_eq!(4, rows1.num_rows());
        assert_eq!(4, rows2.num_rows());

        assert!(rows1.row(0).unwrap() > rows2.row(0).unwrap());
        assert!(rows1.row(1).unwrap() < rows2.row(1).unwrap());
        assert!(rows1.row(2).unwrap() < rows2.row(2).unwrap());
        assert!(rows1.row(3).unwrap() > rows2.row(3).unwrap());
    }
}
