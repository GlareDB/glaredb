pub mod array_data;
pub mod exp;
pub mod flat;
pub mod selection;
pub mod validity;

mod shared_or_owned;

use std::fmt::Debug;
use std::sync::Arc;

use half::f16;
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use shared_or_owned::SharedOrOwned;

use crate::arrays::bitmap::Bitmap;
use crate::arrays::datatype::DataType;
use crate::arrays::executor::builder::{
    ArrayBuilder,
    BooleanBuffer,
    GermanVarlenBuffer,
    PrimitiveBuffer,
};
use crate::arrays::executor::physical_type::{
    PhysicalAny,
    PhysicalBinary_2,
    PhysicalBool_2,
    PhysicalF16_2,
    PhysicalF32_2,
    PhysicalF64_2,
    PhysicalI128_2,
    PhysicalI16_2,
    PhysicalI32_2,
    PhysicalI64_2,
    PhysicalI8_2,
    PhysicalInterval_2,
    PhysicalType2,
    PhysicalU128_2,
    PhysicalU16_2,
    PhysicalU32_2,
    PhysicalU64_2,
    PhysicalU8_2,
    PhysicalUtf8_2,
};
use crate::arrays::executor::scalar::UnaryExecutor2;
use crate::arrays::scalar::decimal::{Decimal128Scalar, Decimal64Scalar};
use crate::arrays::scalar::interval::Interval;
use crate::arrays::scalar::timestamp::TimestampScalar;
use crate::arrays::scalar::ScalarValue;
use crate::arrays::selection::SelectionVector;
use crate::arrays::storage::{
    AddressableStorage,
    BooleanStorage,
    ContiguousVarlenStorage,
    GermanVarlenStorage,
    ListStorage,
    PrimitiveStorage,
    UntypedNullStorage,
};

/// Validity mask for physical storage.
pub type PhysicalValidity = SharedOrOwned<Bitmap>;

/// Logical row selection.
pub type LogicalSelection = SharedOrOwned<SelectionVector>;

#[derive(Debug, Clone, PartialEq)]
pub struct Array2 {
    /// Data type of the array.
    pub(crate) datatype: DataType,
    /// Selection of rows for the array.
    ///
    /// If set, this provides logical row mapping on top of the underlying data.
    /// If not set, then there's a one-to-one mapping between the logical row
    /// and and row in the underlying data.
    pub(crate) selection: Option<LogicalSelection>,
    /// Option validity mask.
    ///
    /// This indicates the validity of the underlying data. This does not take
    /// into account the selection vector, and always maps directly to the data.
    pub(crate) validity: Option<PhysicalValidity>,
    /// The physical data.
    pub(crate) data: ArrayData2,
}

impl Array2 {
    pub fn new_untyped_null_array(len: usize) -> Self {
        // Note that we're adding a bitmap here even though the data already
        // returns NULL. This allows the executors (especially for aggregates)
        // to solely look at the bitmap to determine if a row should executed
        // on.
        let validity = Bitmap::new_with_all_false(1);
        let selection = SelectionVector::repeated(len, 0);
        let data = UntypedNullStorage(1);

        Array2 {
            datatype: DataType::Null,
            selection: Some(selection.into()),
            validity: Some(validity.into()),
            data: data.into(),
        }
    }

    /// Creates a new typed array with all values being set to null.
    pub fn new_typed_null_array(datatype: DataType, len: usize) -> Result<Self> {
        // Create physical array data of length 1, and use a selection vector to
        // extend it out to the desired size.
        let data = datatype.physical_type2()?.zeroed_array_data(1);
        let validity = Bitmap::new_with_all_false(1);
        let selection = SelectionVector::repeated(len, 0);

        Ok(Array2 {
            datatype,
            selection: Some(selection.into()),
            validity: Some(validity.into()),
            data,
        })
    }

    pub fn new_with_array_data(datatype: DataType, data: impl Into<ArrayData2>) -> Self {
        Array2 {
            datatype,
            selection: None,
            validity: None,
            data: data.into(),
        }
    }

    pub fn new_with_validity_and_array_data(
        datatype: DataType,
        validity: impl Into<PhysicalValidity>,
        data: impl Into<ArrayData2>,
    ) -> Self {
        Array2 {
            datatype,
            selection: None,
            validity: Some(validity.into()),
            data: data.into(),
        }
    }

    pub fn new_with_validity_selection_and_array_data(
        datatype: DataType,
        validity: impl Into<PhysicalValidity>,
        selection: impl Into<LogicalSelection>,
        data: impl Into<ArrayData2>,
    ) -> Self {
        Array2 {
            datatype,
            selection: Some(selection.into()),
            validity: Some(validity.into()),
            data: data.into(),
        }
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    pub fn has_selection(&self) -> bool {
        self.selection.is_some()
    }

    pub fn selection_vector(&self) -> Option<&SelectionVector> {
        self.selection.as_ref().map(|v| v.as_ref())
    }

    /// Sets the validity for a value at a given physical index.
    pub fn set_physical_validity(&mut self, idx: usize, valid: bool) {
        match &mut self.validity {
            Some(validity) => {
                let validity = validity.get_mut();
                validity.set_unchecked(idx, valid);
            }
            None => {
                // Initialize validity.
                let len = self.data.len();
                let mut validity = Bitmap::new_with_all_true(len);
                validity.set_unchecked(idx, valid);

                self.validity = Some(validity.into())
            }
        }
    }

    // TODO: Validating variant too.
    pub fn put_selection(&mut self, selection: impl Into<LogicalSelection>) {
        self.selection = Some(selection.into())
    }

    pub fn make_shared(&mut self) {
        if let Some(validity) = &mut self.validity {
            validity.make_shared();
        }
        if let Some(selection) = &mut self.selection {
            selection.make_shared()
        }
    }

    /// Updates this array's selection vector.
    ///
    /// Takes into account any existing selection. This allows for repeated
    /// selection (filtering) against the same array.
    // TODO: Add test for selecting on logically empty array.
    pub fn select_mut(&mut self, selection: impl Into<LogicalSelection>) {
        let selection = selection.into();
        match self.selection_vector() {
            Some(existing) => {
                let selection = existing.select(selection.as_ref());
                self.selection = Some(selection.into())
            }
            None => {
                // No existing selection, we can just use the provided vector
                // directly.
                self.selection = Some(selection)
            }
        }
    }

    pub fn logical_len(&self) -> usize {
        match self.selection_vector() {
            Some(v) => v.num_rows(),
            None => self.data.len(),
        }
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref().map(|v| v.as_ref())
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.logical_len() {
            return None;
        }

        let idx = match self.selection_vector() {
            Some(v) => v.get_opt(idx)?,
            None => idx,
        };

        if let Some(validity) = &self.validity {
            return Some(validity.as_ref().value(idx));
        }

        Some(true)
    }

    /// Returns the array data.
    ///
    /// ArrayData can be cheaply cloned.
    pub fn array_data(&self) -> &ArrayData2 {
        &self.data
    }

    pub fn into_array_data(self) -> ArrayData2 {
        self.data
    }

    /// Gets the physical type of the array.
    pub fn physical_type(&self) -> PhysicalType2 {
        match self.data.physical_type() {
            PhysicalType2::Binary => match self.datatype {
                DataType::Utf8 => PhysicalType2::Utf8,
                _ => PhysicalType2::Binary,
            },
            other => other,
        }
    }

    /// Get the value at a logical index.
    ///
    /// Takes into account the validity and selection vector.
    pub fn logical_value(&self, idx: usize) -> Result<ScalarValue> {
        let idx = match self.selection_vector() {
            Some(v) => v
                .get_opt(idx)
                .ok_or_else(|| RayexecError::new(format!("Logical index {idx} out of bounds")))?,
            None => idx,
        };

        if let Some(validity) = &self.validity {
            if !validity.as_ref().value(idx) {
                return Ok(ScalarValue::Null);
            }
        }

        self.physical_scalar(idx)
    }

    /// Takes an array fully materializes the selection.
    ///
    /// The resulting array's logical and physical indices will be the same.
    ///
    /// This mostly exists for arrow ipc since arrow doesn't have a concept of
    /// selection vectors. We'll probably a non-conforming ipc mode where we write
    /// the selection vector as just another buffer to reduce the need for this.
    pub fn unselect(&self) -> Result<Self> {
        if self.selection.is_none() {
            // Return array as-is. This currently copies the validity
            // bitmap, unsure if we care. The array data should behind an
            // arc, or just really cheap to clone.
            return Ok(self.clone());
        }

        match self.array_data() {
            ArrayData2::UntypedNull(_) => Ok(Array2 {
                datatype: self.datatype.clone(),
                selection: None,
                validity: None,
                data: UntypedNullStorage(self.logical_len()).into(),
            }),
            ArrayData2::Boolean(_) => UnaryExecutor2::execute::<PhysicalBool_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: BooleanBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Int8(_) => UnaryExecutor2::execute::<PhysicalI8_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Int16(_) => UnaryExecutor2::execute::<PhysicalI16_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Int32(_) => UnaryExecutor2::execute::<PhysicalI32_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Int64(_) => UnaryExecutor2::execute::<PhysicalI64_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Int128(_) => UnaryExecutor2::execute::<PhysicalI128_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::UInt8(_) => UnaryExecutor2::execute::<PhysicalU8_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::UInt16(_) => UnaryExecutor2::execute::<PhysicalU16_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::UInt32(_) => UnaryExecutor2::execute::<PhysicalU32_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::UInt64(_) => UnaryExecutor2::execute::<PhysicalU64_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::UInt128(_) => UnaryExecutor2::execute::<PhysicalU128_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Float16(_) => UnaryExecutor2::execute::<PhysicalF16_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Float32(_) => UnaryExecutor2::execute::<PhysicalF32_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Float64(_) => UnaryExecutor2::execute::<PhysicalF64_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Interval(_) => UnaryExecutor2::execute::<PhysicalInterval_2, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData2::Binary(_) => {
                // Use the german varlen storage for all output varlen arrays,
                // even if the input use using some other variant.
                //
                // TODO: We could special case german varlen input and clone the
                // data while just selecting the appropriate metadata. Instead
                // this will just copy everything.
                if self.datatype().is_utf8() {
                    UnaryExecutor2::execute::<PhysicalUtf8_2, _, _>(
                        self,
                        ArrayBuilder {
                            datatype: self.datatype.clone(),
                            buffer: GermanVarlenBuffer::<str>::with_len(self.logical_len()),
                        },
                        |v, buf| buf.put(v),
                    )
                } else {
                    UnaryExecutor2::execute::<PhysicalBinary_2, _, _>(
                        self,
                        ArrayBuilder {
                            datatype: self.datatype.clone(),
                            buffer: GermanVarlenBuffer::<[u8]>::with_len(self.logical_len()),
                        },
                        |v, buf| buf.put(v),
                    )
                }
            }
            ArrayData2::List(_) => Err(RayexecError::new("Cannot yet unselect list arrays")),
        }
    }

    /// Gets the scalar value at the physical index.
    ///
    /// Ignores validity and selectivitity.
    pub fn physical_scalar(&self, idx: usize) -> Result<ScalarValue> {
        Ok(match &self.datatype {
            DataType::Null => match &self.data {
                ArrayData2::UntypedNull(_) => ScalarValue::Null,
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Boolean => match &self.data {
                ArrayData2::Boolean(arr) => arr.as_ref().as_ref().value(idx).into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float16 => match &self.data {
                ArrayData2::Float16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float32 => match &self.data {
                ArrayData2::Float32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float64 => match &self.data {
                ArrayData2::Float64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int8 => match &self.data {
                ArrayData2::Int8(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int16 => match &self.data {
                ArrayData2::Int16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int32 => match &self.data {
                ArrayData2::Int32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int64 => match &self.data {
                ArrayData2::Int64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int128 => match &self.data {
                ArrayData2::Int64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt8 => match &self.data {
                ArrayData2::UInt8(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt16 => match &self.data {
                ArrayData2::UInt16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt32 => match &self.data {
                ArrayData2::UInt32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt64 => match &self.data {
                ArrayData2::UInt64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt128 => match &self.data {
                ArrayData2::UInt64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Decimal64(m) => match &self.data {
                ArrayData2::Int64(arr) => ScalarValue::Decimal64(Decimal64Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Decimal128(m) => match &self.data {
                ArrayData2::Int128(arr) => ScalarValue::Decimal128(Decimal128Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Date32 => match &self.data {
                ArrayData2::Int32(arr) => ScalarValue::Date32(arr.as_ref().as_ref()[idx]),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Date64 => match &self.data {
                ArrayData2::Int64(arr) => ScalarValue::Date64(arr.as_ref().as_ref()[idx]),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Timestamp(m) => match &self.data {
                ArrayData2::Int64(arr) => ScalarValue::Timestamp(TimestampScalar {
                    unit: m.unit,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Interval => match &self.data {
                ArrayData2::Interval(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Utf8 => {
                let v = match &self.data {
                    ArrayData2::Binary(BinaryData::Binary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData2::Binary(BinaryData::LargeBinary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData2::Binary(BinaryData::German(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    _other => return Err(array_not_valid_for_type_err(&self.datatype)),
                };
                let s = std::str::from_utf8(v).context("binary data not valid utf8")?;
                s.into()
            }
            DataType::Binary => {
                let v = match &self.data {
                    ArrayData2::Binary(BinaryData::Binary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData2::Binary(BinaryData::LargeBinary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData2::Binary(BinaryData::German(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    _other => return Err(array_not_valid_for_type_err(&self.datatype)),
                };
                v.into()
            }
            DataType::Struct(_) => not_implemented!("get value: struct"),
            DataType::List(_) => match &self.data {
                ArrayData2::List(list) => {
                    let meta = list
                        .metadata
                        .as_slice()
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("Out of bounds"))?;

                    let vals = (meta.offset..meta.offset + meta.len)
                        .map(|idx| list.array.physical_scalar(idx as usize))
                        .collect::<Result<Vec<_>>>()?;

                    ScalarValue::List(vals)
                }
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
        })
    }

    /// Checks if a scalar value is logically equal to a value in the array.
    pub fn scalar_value_logically_eq(&self, scalar: &ScalarValue, row: usize) -> Result<bool> {
        if row >= self.logical_len() {
            return Err(RayexecError::new("Row out of bounds"));
        }

        match scalar {
            ScalarValue::Null => {
                UnaryExecutor2::value_at::<PhysicalAny>(self, row).map(|arr_val| arr_val.is_none())
            } // None == NULL
            ScalarValue::Boolean(v) => {
                UnaryExecutor2::value_at::<PhysicalBool_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int8(v) => {
                UnaryExecutor2::value_at::<PhysicalI8_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int16(v) => {
                UnaryExecutor2::value_at::<PhysicalI16_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int32(v) => {
                UnaryExecutor2::value_at::<PhysicalI32_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int64(v) => {
                UnaryExecutor2::value_at::<PhysicalI64_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int128(v) => {
                UnaryExecutor2::value_at::<PhysicalI128_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt8(v) => {
                UnaryExecutor2::value_at::<PhysicalU8_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt16(v) => {
                UnaryExecutor2::value_at::<PhysicalU16_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt32(v) => {
                UnaryExecutor2::value_at::<PhysicalU32_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt64(v) => {
                UnaryExecutor2::value_at::<PhysicalU64_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt128(v) => {
                UnaryExecutor2::value_at::<PhysicalU128_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Float32(v) => {
                UnaryExecutor2::value_at::<PhysicalF32_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Float64(v) => {
                UnaryExecutor2::value_at::<PhysicalF64_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Date32(v) => {
                UnaryExecutor2::value_at::<PhysicalI32_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Date64(v) => {
                UnaryExecutor2::value_at::<PhysicalI64_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Interval(v) => UnaryExecutor2::value_at::<PhysicalInterval_2>(self, row)
                .map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                }),
            ScalarValue::Utf8(v) => {
                UnaryExecutor2::value_at::<PhysicalUtf8_2>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == v.as_ref(),
                    None => false,
                })
            }
            ScalarValue::Binary(v) => {
                UnaryExecutor2::value_at::<PhysicalBinary_2>(self, row).map(|arr_val| match arr_val
                {
                    Some(arr_val) => arr_val == v.as_ref(),
                    None => false,
                })
            }
            ScalarValue::Timestamp(v) => {
                UnaryExecutor2::value_at::<PhysicalI64_2>(self, row).map(|arr_val| {
                    // Assumes time unit is the same
                    match arr_val {
                        Some(arr_val) => arr_val == v.value,
                        None => false,
                    }
                })
            }
            ScalarValue::Decimal64(v) => {
                UnaryExecutor2::value_at::<PhysicalI64_2>(self, row).map(|arr_val| {
                    // Assumes precision/scale are the same.
                    match arr_val {
                        Some(arr_val) => arr_val == v.value,
                        None => false,
                    }
                })
            }
            ScalarValue::Decimal128(v) => {
                UnaryExecutor2::value_at::<PhysicalI128_2>(self, row).map(|arr_val| {
                    // Assumes precision/scale are the same.
                    match arr_val {
                        Some(arr_val) => arr_val == v.value,
                        None => false,
                    }
                })
            }

            other => not_implemented!("scalar value eq: {other}"),
        }
    }

    pub fn try_slice(&self, offset: usize, count: usize) -> Result<Self> {
        if offset + count > self.logical_len() {
            return Err(RayexecError::new("Slice out of bounds"));
        }
        Ok(self.slice(offset, count))
    }

    pub fn slice(&self, offset: usize, count: usize) -> Self {
        let selection = match self.selection_vector() {
            Some(sel) => sel.slice_unchecked(offset, count),
            None => SelectionVector::with_range(offset..(offset + count)),
        };

        Array2 {
            datatype: self.datatype.clone(),
            selection: Some(selection.into()),
            validity: self.validity.clone(),
            data: self.data.clone(),
        }
    }
}

fn array_not_valid_for_type_err(datatype: &DataType) -> RayexecError {
    RayexecError::new(format!("Array data not valid for data type: {datatype}"))
}

impl<F> FromIterator<Option<F>> for Array2
where
    F: Default,
    Array2: FromIterator<F>,
{
    fn from_iter<T: IntoIterator<Item = Option<F>>>(iter: T) -> Self {
        // TODO: Make a bit more performant, this is used for more than just
        // tests now.
        let vals: Vec<_> = iter.into_iter().collect();
        let mut validity = Bitmap::new_with_all_true(vals.len());

        let mut new_vals = Vec::with_capacity(vals.len());
        for (idx, val) in vals.into_iter().enumerate() {
            match val {
                Some(val) => new_vals.push(val),
                None => {
                    new_vals.push(F::default());
                    validity.set_unchecked(idx, false);
                }
            }
        }

        let mut array = Array2::from_iter(new_vals);
        array.validity = Some(validity.into());

        array
    }
}

impl FromIterator<String> for Array2 {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut german = GermanVarlenStorage::with_metadata_capacity(lower);

        for s in iter {
            german.try_push(s.as_bytes()).unwrap();
        }

        Array2 {
            datatype: DataType::Utf8,
            selection: None,
            validity: None,
            data: ArrayData2::Binary(BinaryData::German(Arc::new(german))),
        }
    }
}

impl<'a> FromIterator<&'a str> for Array2 {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut german = GermanVarlenStorage::with_metadata_capacity(lower);

        for s in iter {
            german.try_push(s.as_bytes()).unwrap();
        }

        Array2 {
            datatype: DataType::Utf8,
            selection: None,
            validity: None,
            data: ArrayData2::Binary(BinaryData::German(Arc::new(german))),
        }
    }
}

macro_rules! impl_primitive_from_iter {
    ($prim:ty, $variant:ident) => {
        impl FromIterator<$prim> for Array2 {
            fn from_iter<T: IntoIterator<Item = $prim>>(iter: T) -> Self {
                let vals: Vec<_> = iter.into_iter().collect();
                Array2 {
                    datatype: DataType::$variant,
                    selection: None,
                    validity: None,
                    data: ArrayData2::$variant(Arc::new(vals.into())),
                }
            }
        }
    };
}

impl_primitive_from_iter!(i8, Int8);
impl_primitive_from_iter!(i16, Int16);
impl_primitive_from_iter!(i32, Int32);
impl_primitive_from_iter!(i64, Int64);
impl_primitive_from_iter!(i128, Int128);
impl_primitive_from_iter!(u8, UInt8);
impl_primitive_from_iter!(u16, UInt16);
impl_primitive_from_iter!(u32, UInt32);
impl_primitive_from_iter!(u64, UInt64);
impl_primitive_from_iter!(u128, UInt128);
impl_primitive_from_iter!(f16, Float16);
impl_primitive_from_iter!(f32, Float32);
impl_primitive_from_iter!(f64, Float64);

impl FromIterator<bool> for Array2 {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let vals: Bitmap = iter.into_iter().collect();
        Array2 {
            datatype: DataType::Boolean,
            selection: None,
            validity: None,
            data: ArrayData2::Boolean(Arc::new(vals.into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrayData2 {
    UntypedNull(UntypedNullStorage),
    Boolean(Arc<BooleanStorage>),
    Float16(Arc<PrimitiveStorage<f16>>),
    Float32(Arc<PrimitiveStorage<f32>>),
    Float64(Arc<PrimitiveStorage<f64>>),
    Int8(Arc<PrimitiveStorage<i8>>),
    Int16(Arc<PrimitiveStorage<i16>>),
    Int32(Arc<PrimitiveStorage<i32>>),
    Int64(Arc<PrimitiveStorage<i64>>),
    Int128(Arc<PrimitiveStorage<i128>>),
    UInt8(Arc<PrimitiveStorage<u8>>),
    UInt16(Arc<PrimitiveStorage<u16>>),
    UInt32(Arc<PrimitiveStorage<u32>>),
    UInt64(Arc<PrimitiveStorage<u64>>),
    UInt128(Arc<PrimitiveStorage<u128>>),
    Interval(Arc<PrimitiveStorage<Interval>>),
    Binary(BinaryData),
    List(Arc<ListStorage>),
}

impl ArrayData2 {
    pub fn physical_type(&self) -> PhysicalType2 {
        match self {
            Self::UntypedNull(_) => PhysicalType2::UntypedNull,
            Self::Boolean(_) => PhysicalType2::Boolean,
            Self::Float16(_) => PhysicalType2::Float16,
            Self::Float32(_) => PhysicalType2::Float32,
            Self::Float64(_) => PhysicalType2::Float64,
            Self::Int8(_) => PhysicalType2::Int8,
            Self::Int16(_) => PhysicalType2::Int16,
            Self::Int32(_) => PhysicalType2::Int32,
            Self::Int64(_) => PhysicalType2::Int64,
            Self::Int128(_) => PhysicalType2::Int128,
            Self::UInt8(_) => PhysicalType2::UInt8,
            Self::UInt16(_) => PhysicalType2::UInt16,
            Self::UInt32(_) => PhysicalType2::UInt32,
            Self::UInt64(_) => PhysicalType2::UInt64,
            Self::UInt128(_) => PhysicalType2::UInt128,
            Self::Interval(_) => PhysicalType2::Interval,
            Self::Binary(_) => PhysicalType2::Binary,
            Self::List(_) => PhysicalType2::List,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::UntypedNull(s) => s.len(),
            Self::Boolean(s) => s.len(),
            Self::Float16(s) => s.len(),
            Self::Float32(s) => s.len(),
            Self::Float64(s) => s.len(),
            Self::Int8(s) => s.len(),
            Self::Int16(s) => s.len(),
            Self::Int32(s) => s.len(),
            Self::Int64(s) => s.len(),
            Self::Int128(s) => s.len(),
            Self::UInt8(s) => s.len(),
            Self::UInt16(s) => s.len(),
            Self::UInt32(s) => s.len(),
            Self::UInt64(s) => s.len(),
            Self::UInt128(s) => s.len(),
            Self::Interval(s) => s.len(),
            Self::Binary(bin) => match bin {
                BinaryData::Binary(s) => s.len(),
                BinaryData::LargeBinary(s) => s.len(),
                BinaryData::German(s) => s.len(),
            },
            ArrayData2::List(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryData {
    Binary(Arc<ContiguousVarlenStorage<i32>>),
    LargeBinary(Arc<ContiguousVarlenStorage<i64>>),
    German(Arc<GermanVarlenStorage>),
}

impl BinaryData {
    /// Get the binary data size for the array.
    ///
    /// This will not include metadata size in the calculation.
    pub fn binary_data_size_bytes(&self) -> usize {
        match self {
            Self::Binary(s) => s.data_size_bytes(),
            Self::LargeBinary(s) => s.data_size_bytes(),
            Self::German(s) => s.data_size_bytes(),
        }
    }
}

impl From<UntypedNullStorage> for ArrayData2 {
    fn from(value: UntypedNullStorage) -> Self {
        ArrayData2::UntypedNull(value)
    }
}

impl From<BooleanStorage> for ArrayData2 {
    fn from(value: BooleanStorage) -> Self {
        ArrayData2::Boolean(value.into())
    }
}

impl From<PrimitiveStorage<f16>> for ArrayData2 {
    fn from(value: PrimitiveStorage<f16>) -> Self {
        ArrayData2::Float16(value.into())
    }
}

impl From<PrimitiveStorage<f32>> for ArrayData2 {
    fn from(value: PrimitiveStorage<f32>) -> Self {
        ArrayData2::Float32(value.into())
    }
}

impl From<PrimitiveStorage<f64>> for ArrayData2 {
    fn from(value: PrimitiveStorage<f64>) -> Self {
        ArrayData2::Float64(value.into())
    }
}

impl From<PrimitiveStorage<i8>> for ArrayData2 {
    fn from(value: PrimitiveStorage<i8>) -> Self {
        ArrayData2::Int8(value.into())
    }
}

impl From<PrimitiveStorage<i16>> for ArrayData2 {
    fn from(value: PrimitiveStorage<i16>) -> Self {
        ArrayData2::Int16(value.into())
    }
}

impl From<PrimitiveStorage<i32>> for ArrayData2 {
    fn from(value: PrimitiveStorage<i32>) -> Self {
        ArrayData2::Int32(value.into())
    }
}

impl From<PrimitiveStorage<i64>> for ArrayData2 {
    fn from(value: PrimitiveStorage<i64>) -> Self {
        ArrayData2::Int64(value.into())
    }
}

impl From<PrimitiveStorage<i128>> for ArrayData2 {
    fn from(value: PrimitiveStorage<i128>) -> Self {
        ArrayData2::Int128(value.into())
    }
}

impl From<PrimitiveStorage<u8>> for ArrayData2 {
    fn from(value: PrimitiveStorage<u8>) -> Self {
        ArrayData2::UInt8(value.into())
    }
}

impl From<PrimitiveStorage<u16>> for ArrayData2 {
    fn from(value: PrimitiveStorage<u16>) -> Self {
        ArrayData2::UInt16(value.into())
    }
}

impl From<PrimitiveStorage<u32>> for ArrayData2 {
    fn from(value: PrimitiveStorage<u32>) -> Self {
        ArrayData2::UInt32(value.into())
    }
}

impl From<PrimitiveStorage<u64>> for ArrayData2 {
    fn from(value: PrimitiveStorage<u64>) -> Self {
        ArrayData2::UInt64(value.into())
    }
}

impl From<PrimitiveStorage<u128>> for ArrayData2 {
    fn from(value: PrimitiveStorage<u128>) -> Self {
        ArrayData2::UInt128(value.into())
    }
}

impl From<PrimitiveStorage<Interval>> for ArrayData2 {
    fn from(value: PrimitiveStorage<Interval>) -> Self {
        ArrayData2::Interval(value.into())
    }
}

impl From<GermanVarlenStorage> for ArrayData2 {
    fn from(value: GermanVarlenStorage) -> Self {
        ArrayData2::Binary(BinaryData::German(Arc::new(value)))
    }
}

impl From<ListStorage> for ArrayData2 {
    fn from(value: ListStorage) -> Self {
        ArrayData2::List(Arc::new(value))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn select_mut_no_change() {
        let mut arr = Array2::from_iter(["a", "b", "c"]);
        let selection = SelectionVector::with_range(0..3);

        arr.select_mut(selection);

        assert_eq!(ScalarValue::from("a"), arr.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("b"), arr.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("c"), arr.logical_value(2).unwrap());
    }

    #[test]
    fn select_mut_prune_rows() {
        let mut arr = Array2::from_iter(["a", "b", "c"]);
        let selection = SelectionVector::from_iter([0, 2]);

        arr.select_mut(selection);

        assert_eq!(ScalarValue::from("a"), arr.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("c"), arr.logical_value(1).unwrap());
        assert!(arr.logical_value(2).is_err());
    }

    #[test]
    fn select_mut_expand_rows() {
        let mut arr = Array2::from_iter(["a", "b", "c"]);
        let selection = SelectionVector::from_iter([0, 1, 1, 2]);

        arr.select_mut(selection);

        assert_eq!(ScalarValue::from("a"), arr.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("b"), arr.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("b"), arr.logical_value(2).unwrap());
        assert_eq!(ScalarValue::from("c"), arr.logical_value(3).unwrap());
        assert!(arr.logical_value(4).is_err());
    }

    #[test]
    fn select_mut_existing_selection() {
        let mut arr = Array2::from_iter(["a", "b", "c"]);
        let selection = SelectionVector::from_iter([0, 2]);

        // => ["a", "c"]
        arr.select_mut(selection);

        let selection = SelectionVector::from_iter([1, 1, 0]);
        arr.select_mut(selection);

        assert_eq!(ScalarValue::from("c"), arr.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("c"), arr.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("a"), arr.logical_value(2).unwrap());
        assert!(arr.logical_value(3).is_err());
    }

    #[test]
    fn scalar_value_logical_eq_i32() {
        let arr = Array2::from_iter([1, 2, 3]);
        let scalar = ScalarValue::Int32(2);

        assert!(!arr.scalar_value_logically_eq(&scalar, 0).unwrap());
        assert!(arr.scalar_value_logically_eq(&scalar, 1).unwrap());
    }

    #[test]
    fn scalar_value_logical_eq_null() {
        let arr = Array2::from_iter([Some(1), None, Some(3)]);
        let scalar = ScalarValue::Null;

        assert!(!arr.scalar_value_logically_eq(&scalar, 0).unwrap());
        assert!(arr.scalar_value_logically_eq(&scalar, 1).unwrap());
    }
}
