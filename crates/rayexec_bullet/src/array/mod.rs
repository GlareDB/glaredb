use crate::bitmap::Bitmap;
use crate::datatype::DataType;
use crate::executor::builder::{ArrayBuilder, BooleanBuffer, GermanVarlenBuffer, PrimitiveBuffer};
use crate::executor::physical_type::{
    PhysicalBinary, PhysicalBool, PhysicalF32, PhysicalF64, PhysicalI128, PhysicalI16, PhysicalI32,
    PhysicalI64, PhysicalI8, PhysicalInterval, PhysicalType, PhysicalU128, PhysicalU16,
    PhysicalU32, PhysicalU64, PhysicalU8, PhysicalUtf8,
};
use crate::executor::scalar::UnaryExecutor;
use crate::scalar::interval::Interval;
use crate::scalar::timestamp::TimestampScalar;
use crate::scalar::{
    decimal::{Decimal128Scalar, Decimal64Scalar},
    ScalarValue,
};
use crate::selection::SelectionVector;
use crate::storage::{
    AddressableStorage, BooleanStorage, ContiguousVarlenStorage, GermanVarlenStorage,
    PrimitiveStorage, SharedHeapStorage, UntypedNullStorage,
};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use std::fmt::Debug;
use std::sync::Arc;

/// Wrapper around a selection vector allowing for owned or shared vectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    Owned(SelectionVector),
    Shared(Arc<SelectionVector>),
}

impl AsRef<SelectionVector> for Selection {
    fn as_ref(&self) -> &SelectionVector {
        match self {
            Selection::Owned(v) => v,
            Self::Shared(v) => v.as_ref(),
        }
    }
}

impl From<SelectionVector> for Selection {
    fn from(value: SelectionVector) -> Self {
        Selection::Owned(value)
    }
}

impl From<Arc<SelectionVector>> for Selection {
    fn from(value: Arc<SelectionVector>) -> Self {
        Selection::Shared(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Array {
    /// Data type of the array.
    pub(crate) datatype: DataType,
    /// Selection of rows for the array.
    ///
    /// If set, this provides logical row mapping on top of the underlying data.
    /// If not set, then there's a one-to-one mapping between the logical row
    /// and and row in the underlying data.
    pub(crate) selection: Option<Selection>,
    /// Option validity mask.
    ///
    /// This indicates the validity of the underlying data. This does not take
    /// into account the selection vector, and always maps directly to the data.
    pub(crate) validity: Option<Bitmap>,
    /// The physical data.
    pub(crate) data: ArrayData,
}

impl Array {
    pub fn new_untyped_null_array(len: usize) -> Self {
        // Note that we're adding a bitmap here even though the data already
        // returns NULL. This allows the executors (especially for aggregates)
        // to solely look at the bitmap to determine if a row should executed
        // on.
        let validity = Bitmap::new_with_all_false(1);
        let selection = SelectionVector::repeated(len, 0);
        let data = UntypedNullStorage(1);

        Array {
            datatype: DataType::Null,
            selection: Some(selection.into()),
            validity: Some(validity),
            data: data.into(),
        }
    }

    /// Creates a new typed array with all values being set to null.
    pub fn new_typed_null_array(datatype: DataType, len: usize) -> Result<Self> {
        // Create physical array data of length 1, and use a selection vector to
        // extend it out to the desired size.
        let data = datatype.physical_type()?.zeroed_array_data(1);
        let validity = Bitmap::new_with_all_false(1);
        let selection = SelectionVector::repeated(len, 0);

        Ok(Array {
            datatype,
            selection: Some(selection.into()),
            validity: Some(validity),
            data,
        })
    }

    pub fn new_with_array_data(datatype: DataType, data: impl Into<ArrayData>) -> Self {
        Array {
            datatype,
            selection: None,
            validity: None,
            data: data.into(),
        }
    }

    pub fn new_with_validity_and_array_data(
        datatype: DataType,
        validity: Bitmap,
        data: impl Into<ArrayData>,
    ) -> Self {
        Array {
            datatype,
            selection: None,
            validity: Some(validity),
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
            Some(validity) => validity.set_unchecked(idx, valid),
            None => {
                // Initialize validity.
                let len = self.data.len();
                let mut validity = Bitmap::new_with_all_true(len);
                validity.set_unchecked(idx, valid);

                self.validity = Some(validity)
            }
        }
    }

    // TODO: Validating variant too.
    pub fn put_selection(&mut self, selection: impl Into<Selection>) {
        self.selection = Some(selection.into())
    }

    /// Updates this array's selection vector.
    ///
    /// Takes into account any existing selection. This allows for repeated
    /// selection (filtering) against the same array.
    pub fn select_mut(&mut self, selection: &Selection) {
        match self.selection_vector() {
            Some(existing) => {
                // Existing selection, need to create a new vector that selects
                // from the existing vector.
                let input_sel = selection.as_ref();
                let mut new_sel = SelectionVector::with_capacity(input_sel.num_rows());

                for input_loc in input_sel.iter_locations() {
                    new_sel.push_location(existing.get_unchecked(input_loc));
                }

                self.selection = Some(new_sel.into())
            }
            None => {
                // No existing selection, we can just use the provided vector
                // directly.
                self.selection = Some(selection.clone())
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
        self.validity.as_ref()
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.logical_len() {
            return None;
        }

        let idx = match self.selection_vector() {
            Some(v) => v.get(idx)?,
            None => idx,
        };

        if let Some(validity) = &self.validity {
            return Some(validity.value_unchecked(idx));
        }

        Some(true)
    }

    pub fn array_data(&self) -> &ArrayData {
        &self.data
    }

    pub fn physical_type(&self) -> PhysicalType {
        match self.data.physical_type() {
            PhysicalType::Binary => match self.datatype {
                DataType::Utf8 | DataType::LargeUtf8 => PhysicalType::Utf8,
                _ => PhysicalType::Binary,
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
                .get(idx)
                .ok_or_else(|| RayexecError::new(format!("Logical index {idx} out of bounds")))?,
            None => idx,
        };

        if let Some(validity) = &self.validity {
            if !validity.value_unchecked(idx) {
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
            ArrayData::UntypedNull(_) => Ok(Array {
                datatype: self.datatype.clone(),
                selection: None,
                validity: None,
                data: UntypedNullStorage(self.logical_len()).into(),
            }),
            ArrayData::Boolean(_) => UnaryExecutor::execute::<PhysicalBool, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: BooleanBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Int8(_) => UnaryExecutor::execute::<PhysicalI8, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Int16(_) => UnaryExecutor::execute::<PhysicalI16, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Int32(_) => UnaryExecutor::execute::<PhysicalI32, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Int64(_) => UnaryExecutor::execute::<PhysicalI64, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Int128(_) => UnaryExecutor::execute::<PhysicalI128, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::UInt8(_) => UnaryExecutor::execute::<PhysicalU8, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::UInt16(_) => UnaryExecutor::execute::<PhysicalU16, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::UInt32(_) => UnaryExecutor::execute::<PhysicalU32, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::UInt64(_) => UnaryExecutor::execute::<PhysicalU64, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::UInt128(_) => UnaryExecutor::execute::<PhysicalU128, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Float32(_) => UnaryExecutor::execute::<PhysicalF32, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Float64(_) => UnaryExecutor::execute::<PhysicalF64, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Interval(_) => UnaryExecutor::execute::<PhysicalInterval, _, _>(
                self,
                ArrayBuilder {
                    datatype: self.datatype.clone(),
                    buffer: PrimitiveBuffer::with_len(self.logical_len()),
                },
                |v, buf| buf.put(&v),
            ),
            ArrayData::Binary(_) => {
                // Use the german varlen storage for all output varlen arrays,
                // even if the input use using some other variant.
                //
                // TODO: We could special case german varlen input and clone the
                // data while just selecting the appropriate metadata. Instead
                // this will just copy everything.
                if self.datatype().is_utf8() {
                    UnaryExecutor::execute::<PhysicalUtf8, _, _>(
                        self,
                        ArrayBuilder {
                            datatype: self.datatype.clone(),
                            buffer: GermanVarlenBuffer::<str>::with_len(self.logical_len()),
                        },
                        |v, buf| buf.put(v),
                    )
                } else {
                    UnaryExecutor::execute::<PhysicalBinary, _, _>(
                        self,
                        ArrayBuilder {
                            datatype: self.datatype.clone(),
                            buffer: GermanVarlenBuffer::<[u8]>::with_len(self.logical_len()),
                        },
                        |v, buf| buf.put(v),
                    )
                }
            }
        }
    }

    /// Gets the scalar value at the physical index.
    ///
    /// Ignores validity and selectivitity.
    pub fn physical_scalar(&self, idx: usize) -> Result<ScalarValue> {
        Ok(match &self.datatype {
            DataType::Null => match &self.data {
                ArrayData::UntypedNull(_) => ScalarValue::Null,
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Boolean => match &self.data {
                ArrayData::Boolean(arr) => arr.as_ref().as_ref().value_unchecked(idx).into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float32 => match &self.data {
                ArrayData::Float32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float64 => match &self.data {
                ArrayData::Float64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int8 => match &self.data {
                ArrayData::Int8(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int16 => match &self.data {
                ArrayData::Int16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int32 => match &self.data {
                ArrayData::Int32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int64 => match &self.data {
                ArrayData::Int64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int128 => match &self.data {
                ArrayData::Int64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt8 => match &self.data {
                ArrayData::UInt8(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt16 => match &self.data {
                ArrayData::UInt16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt32 => match &self.data {
                ArrayData::UInt32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt64 => match &self.data {
                ArrayData::UInt64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt128 => match &self.data {
                ArrayData::UInt64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Decimal64(m) => match &self.data {
                ArrayData::Int64(arr) => ScalarValue::Decimal64(Decimal64Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Decimal128(m) => match &self.data {
                ArrayData::Int128(arr) => ScalarValue::Decimal128(Decimal128Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Date32 => match &self.data {
                ArrayData::Int32(arr) => ScalarValue::Date32(arr.as_ref().as_ref()[idx]),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Date64 => match &self.data {
                ArrayData::Int64(arr) => ScalarValue::Date64(arr.as_ref().as_ref()[idx]),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Timestamp(m) => match &self.data {
                ArrayData::Int64(arr) => ScalarValue::Timestamp(TimestampScalar {
                    unit: m.unit,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Interval => match &self.data {
                ArrayData::Interval(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Utf8 | DataType::LargeUtf8 => {
                let v = match &self.data {
                    ArrayData::Binary(BinaryData::Binary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::LargeBinary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::SharedHeap(arr)) => arr
                        .get(idx)
                        .map(|b| b.as_ref())
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::German(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    _other => return Err(array_not_valid_for_type_err(&self.datatype)),
                };
                let s = std::str::from_utf8(v).context("binary data not valid utf8")?;
                s.into()
            }
            DataType::Binary | DataType::LargeBinary => {
                let v = match &self.data {
                    ArrayData::Binary(BinaryData::Binary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::LargeBinary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::SharedHeap(arr)) => arr
                        .get(idx)
                        .map(|b| b.as_ref())
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::German(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    _other => return Err(array_not_valid_for_type_err(&self.datatype)),
                };
                v.into()
            }
            DataType::Struct(_) => not_implemented!("get value: struct"),
            DataType::List(_) => not_implemented!("get value: list"),
        })
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

        Array {
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

impl<F> FromIterator<Option<F>> for Array
where
    F: Default,
    Array: FromIterator<F>,
{
    fn from_iter<T: IntoIterator<Item = Option<F>>>(iter: T) -> Self {
        // TODO: Whatever. Just need it for tests.
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

        let mut array = Array::from_iter(new_vals);
        array.validity = Some(validity);

        array
    }
}

impl FromIterator<String> for Array {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut german = GermanVarlenStorage::with_metadata_capacity(lower);

        for s in iter {
            german.try_push(s.as_bytes()).unwrap();
        }

        Array {
            datatype: DataType::Utf8,
            selection: None,
            validity: None,
            data: ArrayData::Binary(BinaryData::German(Arc::new(german))),
        }
    }
}

impl<'a> FromIterator<&'a str> for Array {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut german = GermanVarlenStorage::with_metadata_capacity(lower);

        for s in iter {
            german.try_push(s.as_bytes()).unwrap();
        }

        Array {
            datatype: DataType::Utf8,
            selection: None,
            validity: None,
            data: ArrayData::Binary(BinaryData::German(Arc::new(german))),
        }
    }
}

macro_rules! impl_primitive_from_iter {
    ($prim:ty, $variant:ident) => {
        impl FromIterator<$prim> for Array {
            fn from_iter<T: IntoIterator<Item = $prim>>(iter: T) -> Self {
                let vals: Vec<_> = iter.into_iter().collect();
                Array {
                    datatype: DataType::$variant,
                    selection: None,
                    validity: None,
                    data: ArrayData::$variant(Arc::new(vals.into())),
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
impl_primitive_from_iter!(f32, Float32);
impl_primitive_from_iter!(f64, Float64);

impl FromIterator<bool> for Array {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let vals: Bitmap = iter.into_iter().collect();
        Array {
            datatype: DataType::Boolean,
            selection: None,
            validity: None,
            data: ArrayData::Boolean(Arc::new(vals.into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrayData {
    UntypedNull(UntypedNullStorage),
    Boolean(Arc<BooleanStorage>),
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryData {
    Binary(Arc<ContiguousVarlenStorage<i32>>),
    LargeBinary(Arc<ContiguousVarlenStorage<i64>>),
    SharedHeap(Arc<SharedHeapStorage>),
    German(Arc<GermanVarlenStorage>),
}

impl ArrayData {
    pub fn physical_type(&self) -> PhysicalType {
        match self {
            Self::UntypedNull(_) => PhysicalType::UntypedNull,
            Self::Boolean(_) => PhysicalType::Boolean,
            Self::Float32(_) => PhysicalType::Float32,
            Self::Float64(_) => PhysicalType::Float64,
            Self::Int8(_) => PhysicalType::Int8,
            Self::Int16(_) => PhysicalType::Int16,
            Self::Int32(_) => PhysicalType::Int32,
            Self::Int64(_) => PhysicalType::Int64,
            Self::Int128(_) => PhysicalType::Int128,
            Self::UInt8(_) => PhysicalType::UInt8,
            Self::UInt16(_) => PhysicalType::UInt16,
            Self::UInt32(_) => PhysicalType::UInt32,
            Self::UInt64(_) => PhysicalType::UInt64,
            Self::UInt128(_) => PhysicalType::UInt128,
            Self::Interval(_) => PhysicalType::Interval,
            Self::Binary(_) => PhysicalType::Binary,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::UntypedNull(s) => s.len(),
            Self::Boolean(s) => s.len(),
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
                BinaryData::SharedHeap(s) => s.len(),
                BinaryData::German(s) => s.len(),
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<UntypedNullStorage> for ArrayData {
    fn from(value: UntypedNullStorage) -> Self {
        ArrayData::UntypedNull(value)
    }
}

impl From<BooleanStorage> for ArrayData {
    fn from(value: BooleanStorage) -> Self {
        ArrayData::Boolean(value.into())
    }
}

impl From<PrimitiveStorage<f32>> for ArrayData {
    fn from(value: PrimitiveStorage<f32>) -> Self {
        ArrayData::Float32(value.into())
    }
}

impl From<PrimitiveStorage<f64>> for ArrayData {
    fn from(value: PrimitiveStorage<f64>) -> Self {
        ArrayData::Float64(value.into())
    }
}

impl From<PrimitiveStorage<i8>> for ArrayData {
    fn from(value: PrimitiveStorage<i8>) -> Self {
        ArrayData::Int8(value.into())
    }
}

impl From<PrimitiveStorage<i16>> for ArrayData {
    fn from(value: PrimitiveStorage<i16>) -> Self {
        ArrayData::Int16(value.into())
    }
}

impl From<PrimitiveStorage<i32>> for ArrayData {
    fn from(value: PrimitiveStorage<i32>) -> Self {
        ArrayData::Int32(value.into())
    }
}

impl From<PrimitiveStorage<i64>> for ArrayData {
    fn from(value: PrimitiveStorage<i64>) -> Self {
        ArrayData::Int64(value.into())
    }
}

impl From<PrimitiveStorage<i128>> for ArrayData {
    fn from(value: PrimitiveStorage<i128>) -> Self {
        ArrayData::Int128(value.into())
    }
}

impl From<PrimitiveStorage<u8>> for ArrayData {
    fn from(value: PrimitiveStorage<u8>) -> Self {
        ArrayData::UInt8(value.into())
    }
}

impl From<PrimitiveStorage<u16>> for ArrayData {
    fn from(value: PrimitiveStorage<u16>) -> Self {
        ArrayData::UInt16(value.into())
    }
}

impl From<PrimitiveStorage<u32>> for ArrayData {
    fn from(value: PrimitiveStorage<u32>) -> Self {
        ArrayData::UInt32(value.into())
    }
}

impl From<PrimitiveStorage<u64>> for ArrayData {
    fn from(value: PrimitiveStorage<u64>) -> Self {
        ArrayData::UInt64(value.into())
    }
}

impl From<PrimitiveStorage<u128>> for ArrayData {
    fn from(value: PrimitiveStorage<u128>) -> Self {
        ArrayData::UInt128(value.into())
    }
}

impl From<PrimitiveStorage<Interval>> for ArrayData {
    fn from(value: PrimitiveStorage<Interval>) -> Self {
        ArrayData::Interval(value.into())
    }
}

impl From<SharedHeapStorage> for ArrayData {
    fn from(value: SharedHeapStorage) -> Self {
        ArrayData::Binary(BinaryData::SharedHeap(Arc::new(value)))
    }
}

impl From<GermanVarlenStorage> for ArrayData {
    fn from(value: GermanVarlenStorage) -> Self {
        ArrayData::Binary(BinaryData::German(Arc::new(value)))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn select_mut_no_change() {
        let mut arr = Array::from_iter(["a", "b", "c"]);
        let selection = SelectionVector::with_range(0..3);

        arr.select_mut(&selection.into());

        assert_eq!(ScalarValue::from("a"), arr.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("b"), arr.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("c"), arr.logical_value(2).unwrap());
    }

    #[test]
    fn select_mut_prune_rows() {
        let mut arr = Array::from_iter(["a", "b", "c"]);
        let selection = SelectionVector::from_iter([0, 2]);

        arr.select_mut(&selection.into());

        assert_eq!(ScalarValue::from("a"), arr.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("c"), arr.logical_value(1).unwrap());
        assert!(arr.logical_value(2).is_err());
    }

    #[test]
    fn select_mut_expand_rows() {
        let mut arr = Array::from_iter(["a", "b", "c"]);
        let selection = SelectionVector::from_iter([0, 1, 1, 2]);

        arr.select_mut(&selection.into());

        assert_eq!(ScalarValue::from("a"), arr.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("b"), arr.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("b"), arr.logical_value(2).unwrap());
        assert_eq!(ScalarValue::from("c"), arr.logical_value(3).unwrap());
        assert!(arr.logical_value(4).is_err());
    }

    #[test]
    fn select_mut_existing_selection() {
        let mut arr = Array::from_iter(["a", "b", "c"]);
        let selection = SelectionVector::from_iter([0, 2]);

        // => ["a", "c"]
        arr.select_mut(&selection.into());

        let selection = SelectionVector::from_iter([1, 1, 0]);
        arr.select_mut(&selection.into());

        assert_eq!(ScalarValue::from("c"), arr.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from("c"), arr.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from("a"), arr.logical_value(2).unwrap());
        assert!(arr.logical_value(3).is_err());
    }
}
