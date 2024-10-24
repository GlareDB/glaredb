use std::sync::Arc;

use rayexec_error::{not_implemented, RayexecError, Result};

use crate::array::Array;
use crate::bitmap::Bitmap;
use crate::datatype::DataType;
use crate::executor::builder::{
    ArrayBuilder,
    ArrayDataBuffer,
    BooleanBuffer,
    GermanVarlenBuffer,
    PrimitiveBuffer,
};
use crate::executor::physical_type::PhysicalType;
use crate::row::ScalarRow;
use crate::scalar::interval::Interval;
use crate::scalar::ScalarValue;
use crate::selection::SelectionVector;

/// A batch of same-length arrays.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    /// Columns that make up this batch.
    cols: Vec<Array>,

    /// Number of rows in this batch. Needed to allow for a batch that has no
    /// columns but a non-zero number of rows.
    num_rows: usize,
}

impl Batch {
    pub const fn empty() -> Self {
        Batch {
            cols: Vec::new(),
            num_rows: 0,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        Batch {
            cols: Vec::new(),
            num_rows,
        }
    }

    /// Create a new batch from some number of arrays.
    ///
    /// All arrays should have the same logical length.
    pub fn try_new(cols: impl IntoIterator<Item = Array>) -> Result<Self> {
        let cols: Vec<_> = cols.into_iter().collect();
        let len = match cols.first() {
            Some(arr) => arr.logical_len(),
            None => return Ok(Self::empty()),
        };

        for (idx, col) in cols.iter().enumerate() {
            if col.logical_len() != len {
                return Err(RayexecError::new(format!(
                    "Expected column length to be {len}, got {}. Column idx: {idx}",
                    col.logical_len()
                )));
            }
        }

        Ok(Batch {
            cols,
            num_rows: len,
        })
    }

    // TODO: Owned variant
    pub fn project(&self, indices: &[usize]) -> Self {
        let cols = indices.iter().map(|idx| self.cols[*idx].clone()).collect();

        Batch {
            cols,
            num_rows: self.num_rows,
        }
    }

    pub fn slice(&self, offset: usize, count: usize) -> Self {
        let cols = self.cols.iter().map(|c| c.slice(offset, count)).collect();
        Batch {
            cols,
            num_rows: count,
        }
    }

    /// Selects rows in the batch.
    ///
    /// This accepts an Arc selection as it'll be cloned for each array in the
    /// batch.
    pub fn select(&self, selection: Arc<SelectionVector>) -> Batch {
        let cols = self
            .cols
            .iter()
            .map(|c| {
                let mut col = c.clone();
                col.select_mut(selection.clone());
                col
            })
            .collect();

        Batch {
            cols,
            num_rows: selection.as_ref().num_rows(),
        }
    }

    /// Get the row at some index.
    pub fn row(&self, idx: usize) -> Option<ScalarRow> {
        if idx >= self.num_rows {
            return None;
        }

        // Non-zero number of rows, but no actual columns. Just return an empty
        // row.
        if self.cols.is_empty() {
            return Some(ScalarRow::empty());
        }

        let row = self.cols.iter().map(|col| col.logical_value(idx).unwrap());

        Some(ScalarRow::from_iter(row))
    }

    pub fn try_from_rows(rows: &[ScalarRow], datatypes: &[DataType]) -> Result<Batch> {
        let mut arrays = Vec::with_capacity(rows.len());

        for (col_idx, datatype) in datatypes.iter().enumerate() {
            let arr = array_from_rows(datatype, rows, col_idx)?;
            arrays.push(arr);
        }

        Batch::try_new(arrays)
    }

    pub fn column(&self, idx: usize) -> Option<&Array> {
        self.cols.get(idx)
    }

    pub fn columns(&self) -> &[Array] {
        &self.cols
    }

    pub fn num_columns(&self) -> usize {
        self.cols.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn into_arrays(self) -> Vec<Array> {
        self.cols
    }
}

fn array_from_rows(datatype: &DataType, rows: &[ScalarRow], col: usize) -> Result<Array> {
    fn fmt_err(other: &ScalarValue, expected: &DataType) -> RayexecError {
        RayexecError::new(format!("Unexpected value: {other}, expected: {expected}"))
    }

    match datatype.physical_type()? {
        PhysicalType::UntypedNull => {
            not_implemented!("Scalar value to untyped null")
        }
        PhysicalType::Boolean => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: BooleanBuffer::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Boolean(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Int8 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<i8>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Int8(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Int16 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<i16>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Int16(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Int32 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<i32>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Date32(v) => Ok(v),
                    ScalarValue::Int32(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Int64 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<i64>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Decimal64(v) => Ok(&v.value),
                    ScalarValue::Date64(v) => Ok(v),
                    ScalarValue::Int64(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Int128 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<i128>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Decimal128(v) => Ok(&v.value),
                    ScalarValue::Int128(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::UInt8 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<u8>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::UInt8(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::UInt16 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<u16>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::UInt16(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::UInt32 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<u32>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::UInt32(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::UInt64 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<u64>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::UInt64(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::UInt128 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<u128>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::UInt128(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Float32 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<f32>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Float32(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Float64 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<f64>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Float64(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Interval => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: PrimitiveBuffer::<Interval>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Interval(v) => Ok(v),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Utf8 => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: GermanVarlenBuffer::<str>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Utf8(v) => Ok(v.as_ref()),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
        PhysicalType::Binary => {
            let builder = ArrayBuilder {
                datatype: datatype.clone(),
                buffer: GermanVarlenBuffer::<[u8]>::with_len(rows.len()),
            };
            let arr = array_from_row_and_builder(
                builder,
                |scalar| match scalar {
                    ScalarValue::Binary(v) => Ok(v.as_ref()),
                    other => Err(fmt_err(other, datatype)),
                },
                rows,
                col,
            )?;
            Ok(arr)
        }
    }
}

fn array_from_row_and_builder<B, F>(
    mut builder: ArrayBuilder<B>,
    get_value: F,
    rows: &[ScalarRow],
    col: usize,
) -> Result<Array>
where
    F: for<'a> Fn(&'a ScalarValue) -> Result<&'a B::Type>,
    B: ArrayDataBuffer,
{
    let mut validity = Bitmap::new_with_all_true(rows.len());

    for (idx, row) in rows.iter().enumerate() {
        let val = &row.columns[col];
        if val == &ScalarValue::Null {
            validity.set_unchecked(idx, false);
            continue;
        }

        let val = get_value(val)?;
        builder.buffer.put(idx, val);
    }

    let validity = if validity.is_all_true() {
        None
    } else {
        Some(validity.into())
    };

    Ok(Array {
        datatype: builder.datatype,
        selection: None,
        validity,
        data: builder.buffer.into_data(),
    })
}
