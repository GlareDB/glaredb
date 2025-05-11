pub mod array_buffer;
pub mod execution_format;
pub mod physical_type;
pub mod selection;
pub mod validity;

use std::fmt::Debug;

use array_buffer::{
    AnyArrayBuffer,
    ArrayBufferDowncast,
    ArrayBufferType,
    ConstantBuffer,
    DictionaryBuffer,
    EmptyBuffer,
    ListBuffer,
    ListChildBuffer,
    ListItemMetadata,
};
use execution_format::{ExecutionFormat, ExecutionFormatMut};
use glaredb_error::{DbError, Result, not_implemented};
use half::f16;
use physical_type::{
    Addressable,
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalInterval,
    PhysicalType,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUtf8,
    ScalarStorage,
};
use validity::Validity;

use super::cache::MaybeCache;
use super::compute::copy::copy_rows_array;
use super::scalar::ScalarValue;
use crate::arrays::compute::set_list_value::set_list_value_raw;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::scalar::BorrowedScalarValue;
use crate::arrays::scalar::decimal::{Decimal64Scalar, Decimal128Scalar};
use crate::arrays::scalar::interval::Interval;
use crate::arrays::scalar::timestamp::TimestampScalar;
use crate::buffer::buffer_manager::{AsRawBufferManager, BufferManager, DefaultBufferManager};
use crate::buffer::db_vec::DbVec;
use crate::util::iter::{IntoExactSizeIterator, TryFromExactSizeIterator};

#[derive(Debug)]
pub struct Array {
    /// Data type of the array.
    pub(crate) datatype: DataType,
    /// Determines the validity at each row in the array.
    ///
    /// This should match the logical length of the underlying data buffer.
    pub(crate) validity: Validity,
    /// Holds the underlying array data.
    ///
    /// When making shared:
    /// - `ConstantBuffer` should remain owned, its child made shared.
    /// - `DictionaryBuffer` should remain owned, its child made shared.
    /// - All other should have the top level buffer made shared.
    ///
    /// `ConstantBuffer` and `DictionaryBuffer` should always remain unique,
    /// with its child being owned or shared.
    pub(crate) data: AnyArrayBuffer,
}

impl Array {
    /// Create a new array with the given capacity.
    ///
    /// This will take care of initalizing the data buffer depending on the
    /// datatype. All buffers will be "owned".
    pub fn new(
        manager: &impl AsRawBufferManager,
        datatype: DataType,
        capacity: usize,
    ) -> Result<Self> {
        let data = AnyArrayBuffer::new_for_datatype(manager, &datatype, capacity)?;
        let validity = Validity::new_all_valid(capacity);

        Ok(Array {
            datatype,
            validity,
            data,
        })
    }

    /// Try to create a new array from other.
    ///
    /// This will make the underlying array from other "managed", which will
    /// then be cloned into this array. No array data will be allocated for this
    /// array.
    pub fn new_from_other(_manager: &impl BufferManager, other: &mut Self) -> Result<Self> {
        Ok(Array {
            datatype: other.datatype.clone(),
            validity: other.validity.clone(),
            data: make_array_buffer_shared_and_clone(&mut other.data)?,
        })
    }

    /// Create a new array backed by a constant value.
    ///
    /// This internally creates an array of size 1 with all values pointing to
    /// that same element.
    pub fn new_constant(
        manager: &impl BufferManager,
        value: &BorrowedScalarValue,
        len: usize,
    ) -> Result<Self> {
        let mut arr = Self::new(manager, value.datatype(), 1)?;
        arr.set_value(0, value)?;

        let buffer = ConstantBuffer {
            row_idx: 0,
            len,
            buffer: arr.data,
        };

        let validity = if arr.validity.is_valid(0) {
            Validity::new_all_valid(len)
        } else {
            Validity::new_all_invalid(len)
        };

        Ok(Array {
            datatype: value.datatype(),
            validity,
            data: AnyArrayBuffer::new_unique(buffer),
        })
    }

    /// Creates a new array backed by a constant value from another array.
    pub fn new_constant_from_other(
        manager: &impl BufferManager,
        other: &mut Self,
        row_reference: usize,
        len: usize,
    ) -> Result<Self> {
        // TODO: For nested types, we should get a shared reference to the
        // buffer instead of getting the value.
        let value = other.get_value(row_reference)?;
        Self::new_constant(manager, &value, len)
    }

    pub fn swap(&mut self, other: &mut Self) -> Result<()> {
        if self.datatype != other.datatype {
            return Err(DbError::new("Cannot swap arrays with different data types")
                .with_field("self", self.datatype.clone())
                .with_field("other", other.datatype.clone()));
        }

        std::mem::swap(self, other);

        Ok(())
    }

    pub fn clone(&mut self) -> Result<Self> {
        let cloned = make_array_buffer_shared_and_clone(&mut self.data)?;
        let validity = self.validity.clone();

        Ok(Array {
            datatype: self.datatype.clone(),
            validity,
            data: cloned,
        })
    }

    /// Try to clone the data from the other array into this one, possibly
    /// caching the existing buffers.
    ///
    /// This will attempt to make the data from the other array buffer shared,
    /// and set it on this array. The existing array data will potentially be
    /// cached for later reuse.
    pub fn clone_from_other(
        &mut self,
        other: &mut Self,
        cache: &mut impl MaybeCache,
    ) -> Result<()> {
        if self.datatype != other.datatype {
            return Err(
                DbError::new("Cannot clone arrays with different data types")
                    .with_field("self", self.datatype.clone())
                    .with_field("other", other.datatype.clone()),
            );
        }

        let cloned = make_array_buffer_shared_and_clone(&mut other.data)?;
        let existing = std::mem::replace(&mut self.data, cloned);
        cache.maybe_cache(existing);

        self.validity = other.validity.clone();
        Ok(())
    }

    /// Try to clone a constant from the other array.
    ///
    /// Attempts to cache the existing the buffer.
    pub fn clone_constant_from(
        &mut self,
        other: &mut Self,
        row: usize,
        len: usize,
        _cache: &mut impl MaybeCache,
    ) -> Result<()> {
        if self.datatype != other.datatype {
            return Err(
                DbError::new("Cannot clone arrays with different data types")
                    .with_field("self", self.datatype.clone())
                    .with_field("other", other.datatype.clone()),
            );
        }

        let new_validity = if other.validity.is_valid(row) {
            Validity::new_all_valid(len)
        } else {
            Validity::new_all_invalid(len)
        };

        match other.data.buffer_type {
            ArrayBufferType::Constant => {
                let constant = ConstantBuffer::downcast_mut(&mut other.data)?;
                let buffer = ConstantBuffer {
                    row_idx: constant.row_idx, // Use existing row reference since it points to the right row already.
                    len,
                    buffer: constant.buffer.make_shared_and_clone(),
                };

                self.validity = new_validity;
                // TODO: Cache buffer
                self.data = AnyArrayBuffer::new_unique(buffer);
                Ok(())
            }
            ArrayBufferType::Dictionary => {
                let dictionary = DictionaryBuffer::downcast_mut(&mut other.data)?;
                let buffer = ConstantBuffer {
                    row_idx: dictionary.selection.as_slice()[row], // Use row relative to the selection.
                    len,
                    buffer: dictionary.buffer.make_shared_and_clone(),
                };

                self.validity = new_validity;
                // TODO: Cache buffer
                self.data = AnyArrayBuffer::new_unique(buffer);
                Ok(())
            }
            _ => {
                let buffer = ConstantBuffer {
                    row_idx: row,
                    len,
                    buffer: other.data.make_shared_and_clone(),
                };

                self.validity = new_validity;
                // TODO: Cache buffer
                self.data = AnyArrayBuffer::new_unique(buffer);
                Ok(())
            }
        }
    }

    pub fn logical_len(&self) -> usize {
        self.data.logical_len()
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    pub fn data(&self) -> &AnyArrayBuffer {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut AnyArrayBuffer {
        &mut self.data
    }

    pub fn validity_mut(&mut self) -> &mut Validity {
        &mut self.validity
    }

    /// Gets a mutable reference to bothe the array buffer, and the validity.
    pub fn data_and_validity_mut(&mut self) -> (&mut AnyArrayBuffer, &mut Validity) {
        (&mut self.data, &mut self.validity)
    }

    /// Gets the physical type for this array's data type.
    pub fn physical_type(&self) -> Result<PhysicalType> {
        self.datatype.physical_type()
    }

    /// Replaces the existing validity mask.
    ///
    /// The validity mask needs to match the length of the underlying data
    /// buffer.
    pub fn put_validity(&mut self, validity: Validity) -> Result<()> {
        if validity.len() != self.data.logical_len() {
            return Err(DbError::new("Invalid validity length")
                .with_field("got", validity.len())
                .with_field("want", self.data.logical_len()));
        }
        self.validity = validity;

        Ok(())
    }

    /// Selects indices from the array.
    ///
    /// This will convert the underlying array buffer into a dictionary buffer.
    pub fn select(
        &mut self,
        manager: &impl AsRawBufferManager,
        selection: impl IntoExactSizeIterator<Item = usize> + Clone,
    ) -> Result<()> {
        match self.data.buffer_type {
            ArrayBufferType::Constant => {
                let constant = ConstantBuffer::downcast_mut(&mut self.data)?;
                // Selection on top of constant array produces an array of just
                // the same constants. Just update the len to match the
                // selection.
                let new_len = selection.into_exact_size_iter().len();
                constant.len = new_len;

                // Update validity too to match the new length.
                let validity = if self.validity.is_valid(0) {
                    Validity::new_all_valid(new_len)
                } else {
                    Validity::new_all_invalid(new_len)
                };

                self.validity = validity;

                Ok(())
            }
            ArrayBufferType::Dictionary => {
                let dictionary = DictionaryBuffer::downcast_mut(&mut self.data)?;
                // Select the existing selection. We don't want to deal with
                // nested dictionaries.
                let sel_cloned = selection.clone().into_exact_size_iter();

                // Select the selection.
                let mut new_sel = DbVec::<usize>::new_from_iter(manager, sel_cloned)?;
                let src = dictionary.selection.as_slice();
                let dest = new_sel.as_slice_mut();
                for v in dest {
                    *v = src[*v];
                }

                dictionary.selection = new_sel;

                // Update validity based on selection.
                self.validity = self.validity.select(selection);

                Ok(())
            }
            _ => {
                // For everything else, create a dictionary.
                let dict_sel = DbVec::<usize>::new_from_iter(manager, selection.clone())?;
                let validity = self.validity.select(selection);

                let buffer =
                    std::mem::replace(&mut self.data, AnyArrayBuffer::new_unique(EmptyBuffer));
                let dict = DictionaryBuffer {
                    selection: dict_sel,
                    buffer,
                };

                self.data = AnyArrayBuffer::new_unique(dict);
                self.validity = validity;

                Ok(())
            }
        }
    }

    /// Selects from some other array and puts the selection in this array.
    ///
    /// This first "clones" the other array data into self, then applies a
    /// selection on top of the shared array data.
    ///
    /// This will attempt to put the current array buffer in provided cache.
    pub fn select_from_other(
        &mut self,
        manager: &impl AsRawBufferManager,
        other: &mut Self,
        selection: impl IntoExactSizeIterator<Item = usize> + Clone,
        cache: &mut impl MaybeCache,
    ) -> Result<()> {
        self.clone_from_other(other, cache)?;
        self.select(manager, selection)
    }

    /// Copy rows from self to another array.
    ///
    /// `mapping` provides a mapping of source indices to destination indices in
    /// (source, dest) pairs.
    pub fn copy_rows(
        &self,
        mapping: impl IntoIterator<Item = (usize, usize)>,
        dest: &mut Self,
    ) -> Result<()> {
        copy_rows_array(self, mapping, dest)
    }

    pub fn get_value(&self, idx: usize) -> Result<BorrowedScalarValue> {
        if idx >= self.logical_len() {
            return Err(DbError::new("Index out of bounds")
                .with_field("idx", idx)
                .with_field("capacity", self.logical_len()));
        }

        if !self.validity.is_valid(idx) {
            return Ok(ScalarValue::Null);
        }

        get_physical_value(&self.datatype, &self.validity, &self.data, idx)
    }

    /// Set a scalar value at a given index.
    pub fn set_value(&mut self, idx: usize, val: &BorrowedScalarValue) -> Result<()> {
        if idx >= self.logical_len() {
            return Err(DbError::new("Index out of bounds")
                .with_field("idx", idx)
                .with_field("capacity", self.logical_len()));
        }

        set_physical_value(val, &self.datatype, &mut self.validity, &mut self.data, idx)
    }
}

/// Helper for making an array's buffer shared then cloning it.
///
/// This special cases on buffer types.
fn make_array_buffer_shared_and_clone(data: &mut AnyArrayBuffer) -> Result<AnyArrayBuffer> {
    match data.buffer_type {
        ArrayBufferType::Constant => {
            // Array buffer is constant, clone the child buffer so we
            // can freely select on it.
            let constant = ConstantBuffer::downcast_mut(data)?;
            let child = constant.buffer.make_shared_and_clone();

            Ok(AnyArrayBuffer::new_unique(ConstantBuffer {
                row_idx: constant.row_idx,
                len: constant.len,
                buffer: child,
            }))
        }
        ArrayBufferType::Dictionary => {
            // See constant
            let dictionary = DictionaryBuffer::downcast_mut(data)?;
            let child = dictionary.buffer.make_shared_and_clone();

            // TODO: Pass in manager.
            // TODO: Or possibly wrap the selection in an `OwnedOrShared`.
            let selection =
                DbVec::new_from_slice(&DefaultBufferManager, dictionary.selection.as_slice())?;

            Ok(AnyArrayBuffer::new_unique(DictionaryBuffer {
                selection,
                buffer: child,
            }))
        }
        _ => {
            // Everything else just clone the top-level buffer.
            let data = data.make_shared_and_clone();
            Ok(data)
        }
    }
}

/// Helper for getting a physical value value from an array.
fn get_physical_value<'a>(
    datatype: &DataType,
    validity: &Validity,
    buffer: &'a AnyArrayBuffer,
    row_idx: usize,
) -> Result<BorrowedScalarValue<'a>> {
    if !validity.is_valid(row_idx) {
        return Ok(BorrowedScalarValue::Null);
    }

    fn get_value_inner<S>(buffer: &AnyArrayBuffer, row_idx: usize) -> Result<&S::StorageType>
    where
        S: ScalarStorage,
    {
        match S::downcast_execution_format(buffer)? {
            ExecutionFormat::Flat(buf) => Ok(S::addressable(buf).get(row_idx).unwrap()),
            ExecutionFormat::Selection(buf) => {
                let sel_idx = buf.selection.get(row_idx).unwrap();
                Ok(S::addressable(buf.buffer).get(sel_idx).unwrap())
            }
        }
    }

    match datatype.id {
        DataTypeId::Boolean => {
            let v = get_value_inner::<PhysicalBool>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Boolean(*v))
        }
        DataTypeId::Int8 => {
            let v = get_value_inner::<PhysicalI8>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Int8(*v))
        }
        DataTypeId::Int16 => {
            let v = get_value_inner::<PhysicalI16>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Int16(*v))
        }
        DataTypeId::Int32 => {
            let v = get_value_inner::<PhysicalI32>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Int32(*v))
        }
        DataTypeId::Int64 => {
            let v = get_value_inner::<PhysicalI64>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Int64(*v))
        }
        DataTypeId::Int128 => {
            let v = get_value_inner::<PhysicalI128>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Int128(*v))
        }
        DataTypeId::UInt8 => {
            let v = get_value_inner::<PhysicalU8>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::UInt8(*v))
        }
        DataTypeId::UInt16 => {
            let v = get_value_inner::<PhysicalU16>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::UInt16(*v))
        }
        DataTypeId::UInt32 => {
            let v = get_value_inner::<PhysicalU32>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::UInt32(*v))
        }
        DataTypeId::UInt64 => {
            let v = get_value_inner::<PhysicalU64>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::UInt64(*v))
        }
        DataTypeId::UInt128 => {
            let v = get_value_inner::<PhysicalU128>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::UInt128(*v))
        }
        DataTypeId::Float16 => {
            let v = get_value_inner::<PhysicalF16>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Float16(*v))
        }
        DataTypeId::Float32 => {
            let v = get_value_inner::<PhysicalF32>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Float32(*v))
        }
        DataTypeId::Float64 => {
            let v = get_value_inner::<PhysicalF64>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Float64(*v))
        }
        DataTypeId::Decimal64 => {
            let m = datatype.try_get_decimal_type_meta()?;
            let v = get_value_inner::<PhysicalI64>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Decimal64(Decimal64Scalar {
                precision: m.precision,
                scale: m.scale,
                value: *v,
            }))
        }
        DataTypeId::Decimal128 => {
            let m = datatype.try_get_decimal_type_meta()?;
            let v = get_value_inner::<PhysicalI128>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Decimal128(Decimal128Scalar {
                precision: m.precision,
                scale: m.scale,
                value: *v,
            }))
        }
        DataTypeId::Interval => {
            let v = get_value_inner::<PhysicalInterval>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Interval(*v))
        }
        DataTypeId::Timestamp => {
            let m = datatype.try_get_timestamp_type_meta()?;
            let v = get_value_inner::<PhysicalI64>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Timestamp(TimestampScalar {
                unit: m.unit,
                value: *v,
            }))
        }
        DataTypeId::Date32 => {
            let v = get_value_inner::<PhysicalI32>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Date32(*v))
        }
        DataTypeId::Date64 => {
            let v = get_value_inner::<PhysicalI64>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Date64(*v))
        }
        DataTypeId::Utf8 => {
            let v = get_value_inner::<PhysicalUtf8>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Utf8(v.into()))
        }
        DataTypeId::Binary => {
            let v = get_value_inner::<PhysicalBinary>(buffer, row_idx)?;
            Ok(BorrowedScalarValue::Binary(v.into()))
        }
        DataTypeId::List => {
            let m = datatype.try_get_list_type_meta()?;
            let list = ListBuffer::downcast_execution_format(buffer)?.into_selection_format()?;
            let sel_idx = list.selection.get(row_idx).unwrap();
            let meta = list.buffer.metadata.as_slice()[sel_idx];

            let mut vals = Vec::with_capacity(meta.len as usize);

            for child_idx in meta.offset..(meta.offset + meta.len) {
                let val = get_physical_value(
                    &m.datatype,
                    &list.buffer.child.validity,
                    &list.buffer.child.buffer,
                    child_idx as usize,
                )?;
                vals.push(val);
            }

            Ok(BorrowedScalarValue::List(vals))
        }

        other => not_implemented!("get value for scalar type: {other:?}"),
    }
}

fn set_physical_value(
    value: &BorrowedScalarValue,
    datatype: &DataType,
    validity: &mut Validity,
    buffer: &mut AnyArrayBuffer,
    row_idx: usize,
) -> Result<()> {
    if value.is_null() {
        validity.set_invalid(row_idx);
        return Ok(());
    }
    validity.set_valid(row_idx);

    fn set_value_inner<S>(
        buffer: &mut AnyArrayBuffer,
        val: &S::StorageType,
        row_idx: usize,
    ) -> Result<()>
    where
        S: MutableScalarStorage,
    {
        match S::downcast_execution_format_mut(buffer)? {
            ExecutionFormatMut::Flat(buf) => {
                S::addressable_mut(buf).put(row_idx, val);
                Ok(())
            }
            ExecutionFormatMut::Selection(_) => {
                // TODO: We may want to allow this.
                //
                // Either:
                //
                // - Allow setting a single row value to update multiple rows.
                //   E.g. setting a value for a constant array would change all
                //   rows to that constant.
                // - Or change the array buffer to adapt to the new value. E.g.
                //   a constant array would become a dictionary, and a
                //   dictionary would have a new value appended to the end of
                //   the underlying buffer.
                Err(DbError::new(
                    "Cannot set value for array buffer with selection",
                ))
            }
        }
    }

    match value {
        BorrowedScalarValue::Null => {
            // Checked above.
            unreachable!()
        }
        BorrowedScalarValue::Boolean(val) => {
            set_value_inner::<PhysicalBool>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Int8(val) => {
            set_value_inner::<PhysicalI8>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Int16(val) => {
            set_value_inner::<PhysicalI16>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Int32(val) => {
            set_value_inner::<PhysicalI32>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Int64(val) => {
            set_value_inner::<PhysicalI64>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Int128(val) => {
            set_value_inner::<PhysicalI128>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::UInt8(val) => {
            set_value_inner::<PhysicalU8>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::UInt16(val) => {
            set_value_inner::<PhysicalU16>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::UInt32(val) => {
            set_value_inner::<PhysicalU32>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::UInt64(val) => {
            set_value_inner::<PhysicalU64>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::UInt128(val) => {
            set_value_inner::<PhysicalU128>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Float16(val) => {
            set_value_inner::<PhysicalF16>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Float32(val) => {
            set_value_inner::<PhysicalF32>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Float64(val) => {
            set_value_inner::<PhysicalF64>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Decimal64(val) => {
            set_value_inner::<PhysicalI64>(buffer, &val.value, row_idx)?;
        }
        BorrowedScalarValue::Decimal128(val) => {
            set_value_inner::<PhysicalI128>(buffer, &val.value, row_idx)?;
        }
        BorrowedScalarValue::Date32(val) => {
            set_value_inner::<PhysicalI32>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Date64(val) => {
            set_value_inner::<PhysicalI64>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Timestamp(val) => {
            set_value_inner::<PhysicalI64>(buffer, &val.value, row_idx)?;
        }
        BorrowedScalarValue::Interval(val) => {
            set_value_inner::<PhysicalInterval>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Utf8(val) => {
            set_value_inner::<PhysicalUtf8>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::Binary(val) => {
            set_value_inner::<PhysicalBinary>(buffer, val, row_idx)?;
        }
        BorrowedScalarValue::List(val) => {
            set_list_value_raw(datatype, validity, buffer, val, row_idx)?
        }
        BorrowedScalarValue::Struct(_) => not_implemented!("set value for struct"),
    }

    Ok(())
}

/// Implements `try_from_iter` for primitive types.
///
/// Note these create arrays using Nop buffer manager and so really only
/// suitable for tests right now.
macro_rules! impl_primitive_from_iter {
    ($prim:ty, $phys:ty, $typ_func:ident) => {
        impl TryFromExactSizeIterator<$prim> for Array {
            type Error = DbError;

            fn try_from_iter<T: crate::util::iter::IntoExactSizeIterator<Item = $prim>>(
                iter: T,
            ) -> Result<Self, Self::Error> {
                let iter = iter.into_exact_size_iter();
                let manager = DefaultBufferManager;

                let mut array = Array::new(&manager, DataType::$typ_func(), iter.len())?;
                let slice = <$phys>::get_addressable_mut(&mut array.data)?;

                for (src, dest) in iter.zip(slice.slice) {
                    *dest = src
                }

                Ok(array)
            }
        }
    };
}

impl_primitive_from_iter!(bool, PhysicalBool, boolean);

impl_primitive_from_iter!(i8, PhysicalI8, int8);
impl_primitive_from_iter!(i16, PhysicalI16, int16);
impl_primitive_from_iter!(i32, PhysicalI32, int32);
impl_primitive_from_iter!(i64, PhysicalI64, int64);
impl_primitive_from_iter!(i128, PhysicalI128, int128);

impl_primitive_from_iter!(u8, PhysicalU8, uint8);
impl_primitive_from_iter!(u16, PhysicalU16, uint16);
impl_primitive_from_iter!(u32, PhysicalU32, uint32);
impl_primitive_from_iter!(u64, PhysicalU64, uint64);
impl_primitive_from_iter!(u128, PhysicalU128, uint128);

impl_primitive_from_iter!(f16, PhysicalF16, float16);
impl_primitive_from_iter!(f32, PhysicalF32, float32);
impl_primitive_from_iter!(f64, PhysicalF64, float64);

impl_primitive_from_iter!(Interval, PhysicalInterval, interval);

/// Trait that provides `AsRef<str>` for use with creating arrays from an
/// iterator.
///
/// We don't use `AsRef<str>` directly as the implementation of
/// `TryFromExactSizedIterator` could conflict with other types (the above impls
/// for primitives). A separate trait just lets us limit it to just `&str` and
/// `String`.
pub trait AsRefStr: AsRef<str> {}

impl AsRefStr for &str {}
impl AsRefStr for String {}

impl<S> TryFromExactSizeIterator<S> for Array
where
    S: AsRefStr,
{
    type Error = DbError;

    fn try_from_iter<T: crate::util::iter::IntoExactSizeIterator<Item = S>>(
        iter: T,
    ) -> Result<Self, Self::Error> {
        let iter = iter.into_exact_size_iter();
        let manager = DefaultBufferManager;

        let mut array = Array::new(&manager, DataType::utf8(), iter.len())?;
        let mut buf = PhysicalUtf8::get_addressable_mut(&mut array.data)?;

        for (idx, v) in iter.enumerate() {
            buf.put(idx, v.as_ref());
        }

        Ok(array)
    }
}

/// From iterator implementation that creates an array from optionally valid
/// values. Some is treated as valid, None as invalid.
impl<V> TryFromExactSizeIterator<Option<V>> for Array
where
    V: Default,
    Array: TryFromExactSizeIterator<V, Error = DbError>,
{
    type Error = DbError;

    fn try_from_iter<T: crate::util::iter::IntoExactSizeIterator<Item = Option<V>>>(
        iter: T,
    ) -> Result<Self, Self::Error> {
        let iter = iter.into_exact_size_iter();
        let len = iter.len();

        let mut validity = Validity::new_all_valid(len);

        // New iterator that just uses the default value for missing values, and
        // sets the validity as appropriate.
        let iter = iter.enumerate().map(|(idx, v)| {
            if v.is_none() {
                validity.set_invalid(idx);
            }
            v.unwrap_or_default()
        });

        let mut array = Self::try_from_iter(iter)?;
        array.put_validity(validity)?;

        Ok(array)
    }
}

impl<'a, T> TryFromExactSizeIterator<&'a [T]> for Array
where
    T: Copy, // TODO
    Array: TryFromExactSizeIterator<T, Error = DbError>,
{
    type Error = DbError;

    fn try_from_iter<I>(iter: I) -> Result<Self, Self::Error>
    where
        I: IntoExactSizeIterator<Item = &'a [T]>,
    {
        // TODO: Don't do this.
        let vals: Vec<&[T]> = iter.into_exact_size_iter().collect();

        // Generate metadatas.
        let mut metadata =
            unsafe { DbVec::<ListItemMetadata>::new_uninit(&DefaultBufferManager, vals.len())? };
        let m_slice = metadata.as_slice_mut();
        let mut offset = 0;

        for (idx, val) in vals.iter().enumerate() {
            let m = ListItemMetadata {
                offset: offset as i32,
                len: val.len() as i32,
            };
            m_slice[idx] = m;
            offset += val.len();
        }

        // Generate the list child buffer.

        // TODO: Don't do this.
        let vals: Vec<T> = vals.into_iter().flatten().copied().collect();
        let array: Array = TryFromExactSizeIterator::<T>::try_from_iter(vals)?;

        let datatype = DataType::list(array.datatype);

        let child = ListChildBuffer {
            validity: array.validity,
            buffer: array.data,
        };
        let validity = Validity::new_all_valid(m_slice.len());
        let data = AnyArrayBuffer::new_unique(ListBuffer { metadata, child });

        Ok(Array {
            datatype,
            validity,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::cache::NopCache;
    use crate::arrays::compute::make_list::make_list;
    use crate::arrays::scalar::ScalarValue;
    use crate::generate_array;
    use crate::testutil::arrays::assert_arrays_eq;

    #[test]
    fn try_from_create_i64_list_array() {
        let vals: [&[i64]; 2] = [&[1, 2], &[3, 4, 5]];
        let arr = Array::try_from_iter(vals).unwrap();

        let expected_dt = DataType::list(DataType::int64());
        assert_eq!(expected_dt, arr.datatype);
        assert_eq!(2, arr.logical_len());
    }

    #[test]
    fn try_from_create_i64_with_null_list_array() {
        let vals: [&[Option<i64>]; 2] = [&[Some(1), None], &[None, Some(4), Some(5)]];
        let arr = Array::try_from_iter(vals).unwrap();

        let expected_dt = DataType::list(DataType::int64());
        assert_eq!(expected_dt, arr.datatype);
        assert_eq!(2, arr.logical_len());
    }

    #[test]
    fn try_from_create_str_list_array() {
        let vals: [&[&str]; 2] = [&["a", "b"], &["c", "d", "e"]];
        let arr = Array::try_from_iter(vals).unwrap();

        let expected_dt = DataType::list(DataType::utf8());
        assert_eq!(expected_dt, arr.datatype);
        assert_eq!(2, arr.logical_len());
    }

    #[test]
    fn try_new_constant_utf8() {
        let arr = Array::new_constant(&DefaultBufferManager, &"a".into(), 4).unwrap();
        let expected = Array::try_from_iter(["a", "a", "a", "a"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn try_new_from_other_simple() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let new_arr = Array::new_from_other(&DefaultBufferManager, &mut arr).unwrap();

        let expected = Array::try_from_iter(["a", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
        assert_arrays_eq(&expected, &new_arr);
    }

    #[test]
    fn try_new_from_other_dictionary() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => '["b", "a", "a", "b"]'
        arr.select(&DefaultBufferManager, [1, 0, 0, 1]).unwrap();

        let new_arr = Array::new_from_other(&DefaultBufferManager, &mut arr).unwrap();

        let expected = Array::try_from_iter(["b", "a", "a", "b"]).unwrap();
        assert_arrays_eq(&expected, &arr);
        assert_arrays_eq(&expected, &new_arr);
    }

    #[test]
    fn try_new_from_other_constant() {
        let mut arr = Array::new_constant(&DefaultBufferManager, &"cat".into(), 4).unwrap();
        let new_arr = Array::new_from_other(&DefaultBufferManager, &mut arr).unwrap();

        let expected = Array::try_from_iter(["cat", "cat", "cat", "cat"]).unwrap();

        assert_arrays_eq(&expected, &new_arr);
    }

    #[test]
    fn select_no_change() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&DefaultBufferManager, [0, 1, 2]).unwrap();

        let expected = Array::try_from_iter(["a", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_prune_rows() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&DefaultBufferManager, [0, 2]).unwrap();

        let expected = Array::try_from_iter(["a", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_expand_rows() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&DefaultBufferManager, [0, 1, 1, 2]).unwrap();

        let expected = Array::try_from_iter(["a", "b", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_existing_selection() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => ["a", "c"]
        arr.select(&DefaultBufferManager, [0, 2]).unwrap();

        // => ["c", "c", "a"]
        arr.select(&DefaultBufferManager, [1, 1, 0]).unwrap();

        let expected = Array::try_from_iter(["c", "c", "a"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_constant() {
        let mut arr = Array::new_constant(&DefaultBufferManager, &"dog".into(), 3).unwrap();
        arr.select(&DefaultBufferManager, [0, 1, 2, 0, 1, 2])
            .unwrap();

        let expected = Array::try_from_iter(["dog", "dog", "dog", "dog", "dog", "dog"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_after_making_array_data_shared() {
        // Try to select from an array that has its array data managed by having
        // it be created from an existing array.

        let mut arr1 = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut arr2 = Array::new_from_other(&DefaultBufferManager, &mut arr1).unwrap();
        // => [2, 1, 3]
        arr2.select(&DefaultBufferManager, [1, 0, 2]).unwrap();

        let expected = Array::try_from_iter([2, 1, 3]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }

    #[test]
    fn select_after_making_constant_array_data_shared() {
        // Same as above, just with a constant array.

        let mut arr1 = Array::new_constant(&DefaultBufferManager, &14.into(), 3).unwrap();
        let mut arr2 = Array::new_from_other(&DefaultBufferManager, &mut arr1).unwrap();
        // => [14, 14, 14]
        arr2.select(&DefaultBufferManager, [1, 0, 2]).unwrap();

        let expected = Array::try_from_iter([14, 14, 14]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }

    #[test]
    fn get_value_simple() {
        let arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let val = arr.get_value(1).unwrap();
        assert_eq!(BorrowedScalarValue::Utf8("b".into()), val);
    }

    #[test]
    fn get_value_null() {
        let arr = Array::try_from_iter([Some("a"), None, Some("c")]).unwrap();

        let val = arr.get_value(0).unwrap();
        assert_eq!(BorrowedScalarValue::Utf8("a".into()), val);

        let val = arr.get_value(1).unwrap();
        assert_eq!(BorrowedScalarValue::Null, val);
    }

    #[test]
    fn get_value_with_selection() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => ["a", "c"]
        arr.select(&DefaultBufferManager, [0, 2]).unwrap();
        let val = arr.get_value(1).unwrap();

        assert_eq!(BorrowedScalarValue::Utf8("c".into()), val);
    }

    #[test]
    fn get_value_with_selection_null() {
        let mut arr = Array::try_from_iter([Some("a"), Some("b"), None]).unwrap();
        // => [NULL, "a"]
        arr.select(&DefaultBufferManager, [2, 0]).unwrap();

        let val = arr.get_value(0).unwrap();
        assert_eq!(BorrowedScalarValue::Null, val);

        let val = arr.get_value(1).unwrap();
        assert_eq!(BorrowedScalarValue::Utf8("a".into()), val);
    }

    #[test]
    fn get_value_constant() {
        let arr = Array::new_constant(&DefaultBufferManager, &"cat".into(), 4).unwrap();
        let val = arr.get_value(2).unwrap();
        assert_eq!(BorrowedScalarValue::Utf8("cat".into()), val);
    }

    #[test]
    fn set_value_simple() {
        let mut arr = generate_array!(["a", "b", "c"]);
        let val: ScalarValue = "cat".into();
        arr.set_value(1, &val).unwrap();

        let expected = generate_array!(["a", "cat", "c"]);
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn set_value_null() {
        let mut arr = generate_array!(["a", "b", "c"]);
        arr.set_value(1, &ScalarValue::Null).unwrap();

        let expected = generate_array!([Some("a"), None, Some("c")]);
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn get_value_list_i32() {
        let mut lists =
            Array::new(&DefaultBufferManager, DataType::list(DataType::int32()), 4).unwrap();

        make_list(
            &[
                Array::try_from_iter([Some(1), Some(2), None, Some(4)]).unwrap(),
                Array::try_from_iter([5, 6, 7, 8]).unwrap(),
            ],
            0..4,
            &mut lists,
        )
        .unwrap();

        let expected0 = BorrowedScalarValue::List(vec![1.into(), 5.into()]);
        let v0 = lists.get_value(0).unwrap();
        assert_eq!(expected0, v0);

        let expected2 = BorrowedScalarValue::List(vec![BorrowedScalarValue::Null, 7.into()]);
        let v2 = lists.get_value(2).unwrap();
        assert_eq!(expected2, v2);
    }

    #[test]
    fn clone_constant_from_other_i32_valid() {
        let mut arr = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut arr2 = Array::new(&DefaultBufferManager, DataType::int32(), 16).unwrap();

        arr2.clone_constant_from(&mut arr, 1, 8, &mut NopCache)
            .unwrap();

        let expected = Array::try_from_iter([2, 2, 2, 2, 2, 2, 2, 2]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }

    #[test]
    fn clone_constant_from_other_i32_null() {
        let mut arr = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
        let mut arr2 = Array::new(&DefaultBufferManager, DataType::int32(), 16).unwrap();

        arr2.clone_constant_from(&mut arr, 1, 8, &mut NopCache)
            .unwrap();

        let expected = Array::try_from_iter(vec![None as Option<i32>; 8]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }

    #[test]
    fn clone_constant_from_other_i32_dictionary() {
        let mut arr = Array::try_from_iter([1, 2, 3]).unwrap();
        // => [2, 3, 1]
        arr.select(&DefaultBufferManager, [1, 2, 0]).unwrap();
        let mut arr2 = Array::new(&DefaultBufferManager, DataType::int32(), 16).unwrap();

        arr2.clone_constant_from(&mut arr, 1, 8, &mut NopCache)
            .unwrap();

        let expected = Array::try_from_iter([3, 3, 3, 3, 3, 3, 3, 3]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }
}
