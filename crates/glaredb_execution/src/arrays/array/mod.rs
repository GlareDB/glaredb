pub mod array_buffer;
pub mod flat;
pub mod physical_type;
pub mod selection;
pub mod validity;

use std::fmt::Debug;

use array_buffer::{
    ArrayBuffer,
    ArrayBufferType,
    ConstantBuffer,
    DictionaryBuffer,
    ScalarBuffer,
    SharedOrOwned,
};
use flat::FlattenedArray;
use glaredb_error::{not_implemented, RayexecError, Result};
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
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
    ScalarStorage,
};
use validity::Validity;

use super::cache::MaybeCache;
use super::compute::copy::copy_rows_array;
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::decimal::{Decimal128Scalar, Decimal64Scalar};
use crate::arrays::scalar::interval::Interval;
use crate::arrays::scalar::timestamp::TimestampScalar;
use crate::arrays::scalar::BorrowedScalarValue;
use crate::buffer::buffer_manager::{AsRawBufferManager, BufferManager, NopBufferManager};
use crate::buffer::typed::TypedBuffer;
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
    pub(crate) data: ArrayBuffer,
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
        let data = ArrayBuffer::try_new_for_datatype(manager, &datatype, capacity)?;
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
            data: other.data.make_shared_and_clone(),
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
            row_reference: 0,
            len,
            child_buffer: Box::new(arr.data),
        };

        let validity = if arr.validity.is_valid(0) {
            Validity::new_all_valid(len)
        } else {
            Validity::new_all_invalid(len)
        };

        Ok(Array {
            datatype: value.datatype(),
            validity,
            data: ArrayBuffer::new(buffer),
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

    /// Create a new typed null array.
    ///
    /// This will create an array that contains only nulls but retains the
    /// requested datatype.
    ///
    /// A buffer of size 1 will be created with all values pointing to the same
    /// element.
    pub fn new_typed_null(
        manager: &impl BufferManager,
        datatype: DataType,
        len: usize,
    ) -> Result<Self> {
        let data = ArrayBuffer::try_new_for_datatype(manager, &datatype, 1)?;
        let buffer = ConstantBuffer {
            row_reference: 0,
            len,
            child_buffer: Box::new(data),
        };

        Ok(Array {
            datatype,
            validity: Validity::new_all_invalid(len),
            data: ArrayBuffer::new(buffer),
        })
    }

    pub fn swap(&mut self, other: &mut Self) -> Result<()> {
        if self.datatype != other.datatype {
            return Err(
                RayexecError::new("Cannot swap arrays with different data types")
                    .with_field("self", self.datatype.clone())
                    .with_field("other", other.datatype.clone()),
            );
        }

        std::mem::swap(self, other);

        Ok(())
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
                RayexecError::new("Cannot clone arrays with different data types")
                    .with_field("self", self.datatype.clone())
                    .with_field("other", other.datatype.clone()),
            );
        }

        let other_data = other.data.make_shared_and_clone();
        let existing = std::mem::replace(&mut self.data, other_data);
        cache.maybe_cache(existing);
        self.validity = other.validity.clone();

        Ok(())
    }

    /// Try to clone a constant from the other array, attempting to cache the
    /// current buffer.
    ///
    /// Attempts to cache the existing the buffer.
    pub fn clone_constant_from(
        &mut self,
        other: &mut Self,
        row: usize,
        len: usize,
        cache: &mut impl MaybeCache,
    ) -> Result<()> {
        if self.datatype != other.datatype {
            return Err(
                RayexecError::new("Cannot clone arrays with different data types")
                    .with_field("self", self.datatype.clone())
                    .with_field("other", other.datatype.clone()),
            );
        }

        let new_validity = if other.validity.is_valid(row) {
            Validity::new_all_valid(len)
        } else {
            Validity::new_all_invalid(len)
        };

        match other.data.as_mut() {
            ArrayBufferType::Constant(constant) => {
                let child_buffer = constant.child_buffer.make_shared_and_clone();
                let buffer = ConstantBuffer {
                    row_reference: constant.row_reference, // Use existing row reference since it points to the right row already.
                    len,
                    child_buffer: Box::new(child_buffer),
                };

                self.validity = new_validity;
                let existing = std::mem::replace(&mut self.data, ArrayBuffer::new(buffer));
                cache.maybe_cache(existing);

                Ok(())
            }
            ArrayBufferType::Dictionary(dict) => {
                let child_buffer = dict.child_buffer.make_shared_and_clone();
                let buffer = ConstantBuffer {
                    row_reference: dict.selection.as_slice()[row], // Ensure row reference relative to selection.
                    len,
                    child_buffer: Box::new(child_buffer),
                };

                self.validity = new_validity;
                let existing = std::mem::replace(&mut self.data, ArrayBuffer::new(buffer));
                cache.maybe_cache(existing);

                Ok(())
            }
            _ => {
                let child_buffer = other.data.make_shared_and_clone();
                let buffer = ConstantBuffer {
                    row_reference: row,
                    len,
                    child_buffer: Box::new(child_buffer),
                };

                self.validity = new_validity;
                let existing = std::mem::replace(&mut self.data, ArrayBuffer::new(buffer));
                cache.maybe_cache(existing);

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

    pub fn data(&self) -> &ArrayBuffer {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut ArrayBuffer {
        &mut self.data
    }

    /// Gets a mutable reference to bothe the array buffer, and the validity.
    pub fn data_and_validity_mut(&mut self) -> (&mut ArrayBuffer, &mut Validity) {
        (&mut self.data, &mut self.validity)
    }

    /// Gets the physical type for this array's data type.
    pub fn physical_type(&self) -> PhysicalType {
        self.datatype.physical_type()
    }

    /// Replaces the existing validity mask.
    ///
    /// The validity mask needs to match the length of the underlying data
    /// buffer.
    pub fn put_validity(&mut self, validity: Validity) -> Result<()> {
        if validity.len() != self.data.logical_len() {
            return Err(RayexecError::new("Invalid validity length")
                .with_field("got", validity.len())
                .with_field("want", self.data.logical_len()));
        }
        self.validity = validity;

        Ok(())
    }

    /// If we should flatten the array prior to executing an operation on the
    /// array.
    pub fn should_flatten_for_execution(&self) -> bool {
        matches!(
            self.data.as_ref(),
            ArrayBufferType::Constant(_) | ArrayBufferType::Dictionary(_)
        )
    }

    pub fn flatten(&self) -> Result<FlattenedArray> {
        FlattenedArray::from_array(self)
    }

    /// Selects indice from the array.
    ///
    /// This will convert the underlying array buffer into a dictionary buffer.
    pub fn select(
        &mut self,
        manager: &impl AsRawBufferManager,
        selection: impl IntoExactSizeIterator<Item = usize> + Clone,
    ) -> Result<()> {
        match self.data.as_mut() {
            ArrayBufferType::Constant(constant) => {
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
            ArrayBufferType::Dictionary(dict) => {
                // Select the existing selection. We don't want to deal with
                // nested dictionaries.
                let sel_cloned = selection.clone().into_exact_size_iter();
                let new_len = sel_cloned.len();

                let mut new_sel = TypedBuffer::try_with_capacity(manager, new_len)?;
                let existing_sel = dict.selection.as_slice();

                for (sel_idx, dest) in sel_cloned.zip(new_sel.as_slice_mut()) {
                    *dest = existing_sel[sel_idx];
                }

                dict.selection = SharedOrOwned::owned(new_sel);

                // Update validity based on selection.
                self.validity = self.validity.select(selection);

                Ok(())
            }
            _ => {
                // For everything else, make array buffer a dictionary.
                let selection = selection.into_exact_size_iter();
                let mut buf_selection = TypedBuffer::try_with_capacity(manager, selection.len())?;

                for (src, dest) in selection.zip(buf_selection.as_slice_mut()) {
                    *dest = src;
                }

                // Update validity.
                self.validity = self
                    .validity
                    .select(buf_selection.as_slice().iter().copied());

                // Some hacks below, just swapping around the buffers.
                let uninit = ArrayBuffer::new(ScalarBuffer {
                    physical_type: PhysicalType::UntypedNull,
                    raw: SharedOrOwned::Uninit,
                });
                let buffer = std::mem::replace(&mut self.data, uninit);

                let dictionary = DictionaryBuffer {
                    selection: SharedOrOwned::owned(buf_selection),
                    child_buffer: Box::new(buffer),
                };

                self.data = ArrayBuffer::new(dictionary);

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
            return Err(RayexecError::new("Index out of bounds")
                .with_field("idx", idx)
                .with_field("capacity", self.logical_len()));
        }

        let flat = self.flatten()?;
        let data_idx = flat.selection.get(idx).expect("Index to be in bounds");

        get_value_inner(
            &self.datatype,
            flat.array_buffer,
            flat.validity,
            idx,
            data_idx,
        )
    }

    /// Set a scalar value at a given index.
    pub fn set_value(&mut self, idx: usize, val: &BorrowedScalarValue) -> Result<()> {
        // TODO: Handle constant, dictionary
        //
        // - Constant => convert to dictionary
        // - Dictionary => add new value, update selection to point to value

        if self.should_flatten_for_execution() {
            not_implemented!("set value for dictionary/constant arrays")
        }

        if idx >= self.logical_len() {
            return Err(RayexecError::new("Index out of bounds")
                .with_field("idx", idx)
                .with_field("capacity", self.logical_len()));
        }

        self.validity.set_valid(idx);

        match val {
            BorrowedScalarValue::Null => {
                self.validity.set_invalid(idx);
            }
            BorrowedScalarValue::Boolean(val) => {
                PhysicalBool::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Int8(val) => {
                PhysicalI8::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Int16(val) => {
                PhysicalI16::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Int32(val) => {
                PhysicalI32::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Int64(val) => {
                PhysicalI64::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Int128(val) => {
                PhysicalI128::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::UInt8(val) => {
                PhysicalU8::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::UInt16(val) => {
                PhysicalU16::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::UInt32(val) => {
                PhysicalU32::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::UInt64(val) => {
                PhysicalU64::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::UInt128(val) => {
                PhysicalU128::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Float16(val) => {
                PhysicalF16::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Float32(val) => {
                PhysicalF32::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Float64(val) => {
                PhysicalF64::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Decimal64(val) => {
                PhysicalI64::get_addressable_mut(&mut self.data)?.put(idx, &val.value);
            }
            BorrowedScalarValue::Decimal128(val) => {
                PhysicalI128::get_addressable_mut(&mut self.data)?.put(idx, &val.value);
            }
            BorrowedScalarValue::Date32(val) => {
                PhysicalI32::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Date64(val) => {
                PhysicalI64::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Timestamp(val) => {
                PhysicalI64::get_addressable_mut(&mut self.data)?.put(idx, &val.value);
            }
            BorrowedScalarValue::Interval(val) => {
                PhysicalInterval::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Utf8(val) => {
                PhysicalUtf8::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::Binary(val) => {
                PhysicalBinary::get_addressable_mut(&mut self.data)?.put(idx, val);
            }
            BorrowedScalarValue::List(_) => {
                not_implemented!("set value for list")
                // let secondary = self.data.try_as_mut()?.get_secondary_mut().get_list_mut()?;

                // // Ensure we have space to push.
                // let rem_cap = secondary.child.capacity() - secondary.entries;
                // if rem_cap < list.len() {
                //     // TODO: Just resize secondary.
                //     return Err(RayexecError::new(
                //         "Secondary list buffer does not have required capacity",
                //     )
                //     .with_field("remaining", rem_cap)
                //     .with_field("need", list.len()));
                // }

                // for (child_idx, val) in (secondary.entries..).zip(list) {
                //     secondary.child.set_value(child_idx, val)?;
                // }

                // // Now update entry count in child. Original value is our offset
                // // index.
                // let start_offset = secondary.entries;
                // secondary.entries += list.len();

                // // Set metadata pointing to new list.
                // PhysicalList::get_addressable_mut(self.data.try_as_mut()?)?.put(
                //     idx,
                //     &ListItemMetadata {
                //         offset: start_offset as i32,
                //         len: list.len() as i32,
                //     },
                // );
            }
            BorrowedScalarValue::Struct(_) => not_implemented!("set value for struct"),
        }

        Ok(())
    }
}

/// Helper for getting the value from an array.
///
/// The provided buffer/validity should come from a flattened array. We don't
/// want to deal with selections here.
fn get_value_inner<'a>(
    datatype: &DataType,
    buffer: &'a ArrayBuffer,
    validity: &Validity,
    logical_idx: usize,
    data_idx: usize,
) -> Result<BorrowedScalarValue<'a>> {
    if !validity.is_valid(logical_idx) {
        return Ok(BorrowedScalarValue::Null);
    }

    match datatype {
        DataType::Boolean => {
            let v = PhysicalBool::get_addressable(buffer)?
                .get(data_idx)
                .unwrap();
            Ok(BorrowedScalarValue::Boolean(*v))
        }
        DataType::Int8 => {
            let v = PhysicalI8::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Int8(*v))
        }
        DataType::Int16 => {
            let v = PhysicalI16::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Int16(*v))
        }
        DataType::Int32 => {
            let v = PhysicalI32::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Int32(*v))
        }
        DataType::Int64 => {
            let v = PhysicalI64::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Int64(*v))
        }
        DataType::Int128 => {
            let v = PhysicalI128::get_addressable(buffer)?
                .get(data_idx)
                .unwrap();
            Ok(BorrowedScalarValue::Int128(*v))
        }
        DataType::UInt8 => {
            let v = PhysicalU8::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::UInt8(*v))
        }
        DataType::UInt16 => {
            let v = PhysicalU16::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::UInt16(*v))
        }
        DataType::UInt32 => {
            let v = PhysicalU32::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::UInt32(*v))
        }
        DataType::UInt64 => {
            let v = PhysicalU64::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::UInt64(*v))
        }
        DataType::UInt128 => {
            let v = PhysicalU128::get_addressable(buffer)?
                .get(data_idx)
                .unwrap();
            Ok(BorrowedScalarValue::UInt128(*v))
        }
        DataType::Float16 => {
            let v = PhysicalF16::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Float16(*v))
        }
        DataType::Float32 => {
            let v = PhysicalF32::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Float32(*v))
        }
        DataType::Float64 => {
            let v = PhysicalF64::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Float64(*v))
        }
        DataType::Decimal64(m) => {
            let v = PhysicalI64::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Decimal64(Decimal64Scalar {
                precision: m.precision,
                scale: m.scale,
                value: *v,
            }))
        }
        DataType::Decimal128(m) => {
            let v = PhysicalI128::get_addressable(buffer)?
                .get(data_idx)
                .unwrap();
            Ok(BorrowedScalarValue::Decimal128(Decimal128Scalar {
                precision: m.precision,
                scale: m.scale,
                value: *v,
            }))
        }
        DataType::Interval => {
            let v = PhysicalInterval::get_addressable(buffer)?
                .get(data_idx)
                .unwrap();
            Ok(BorrowedScalarValue::Interval(*v))
        }
        DataType::Timestamp(m) => {
            let v = PhysicalI64::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Timestamp(TimestampScalar {
                unit: m.unit,
                value: *v,
            }))
        }
        DataType::Date32 => {
            let v = PhysicalI32::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Date32(*v))
        }
        DataType::Date64 => {
            let v = PhysicalI64::get_addressable(buffer)?.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Date64(*v))
        }
        DataType::Utf8 => {
            let addressable = PhysicalUtf8::get_addressable(buffer)?;
            let v = addressable.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Utf8(v.into()))
        }
        DataType::Binary => {
            let addressable = PhysicalBinary::get_addressable(buffer)?;
            let v = addressable.get(data_idx).unwrap();
            Ok(BorrowedScalarValue::Binary(v.into()))
        }
        DataType::List(m) => {
            let list_buf = match buffer.as_ref() {
                ArrayBufferType::List(list_buf) => list_buf,
                _ => return Err(RayexecError::new("Expected list buffer")),
            };

            let meta = list_buf.metadata.as_slice()[data_idx];
            let mut vals = Vec::with_capacity(meta.len as usize);

            for child_idx in meta.offset..(meta.offset + meta.len) {
                let v = get_value_inner(
                    &m.datatype,
                    &list_buf.child_buffer,
                    &list_buf.child_validity,
                    child_idx as usize,
                    child_idx as usize,
                )?;
                vals.push(v);
            }

            Ok(BorrowedScalarValue::List(vals))
        }

        other => not_implemented!("get value for scalar type: {other:?}"),
    }
}

/// Implements `try_from_iter` for primitive types.
///
/// Note these create arrays using Nop buffer manager and so really only
/// suitable for tests right now.
macro_rules! impl_primitive_from_iter {
    ($prim:ty, $phys:ty, $typ_variant:ident) => {
        impl TryFromExactSizeIterator<$prim> for Array {
            type Error = RayexecError;

            fn try_from_iter<T: crate::util::iter::IntoExactSizeIterator<Item = $prim>>(
                iter: T,
            ) -> Result<Self, Self::Error> {
                let iter = iter.into_exact_size_iter();
                let manager = NopBufferManager;

                let mut array = Array::new(&manager, DataType::$typ_variant, iter.len())?;
                let slice = <$phys>::get_addressable_mut(&mut array.data)?;

                for (src, dest) in iter.zip(slice.slice) {
                    *dest = src
                }

                Ok(array)
            }
        }
    };
}

impl_primitive_from_iter!(bool, PhysicalBool, Boolean);

impl_primitive_from_iter!(i8, PhysicalI8, Int8);
impl_primitive_from_iter!(i16, PhysicalI16, Int16);
impl_primitive_from_iter!(i32, PhysicalI32, Int32);
impl_primitive_from_iter!(i64, PhysicalI64, Int64);
impl_primitive_from_iter!(i128, PhysicalI128, Int128);

impl_primitive_from_iter!(u8, PhysicalU8, UInt8);
impl_primitive_from_iter!(u16, PhysicalU16, UInt16);
impl_primitive_from_iter!(u32, PhysicalU32, UInt32);
impl_primitive_from_iter!(u64, PhysicalU64, UInt64);
impl_primitive_from_iter!(u128, PhysicalU128, UInt128);

impl_primitive_from_iter!(f16, PhysicalF16, Float16);
impl_primitive_from_iter!(f32, PhysicalF32, Float32);
impl_primitive_from_iter!(f64, PhysicalF64, Float64);

impl_primitive_from_iter!(Interval, PhysicalInterval, Interval);

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
    type Error = RayexecError;

    fn try_from_iter<T: crate::util::iter::IntoExactSizeIterator<Item = S>>(
        iter: T,
    ) -> Result<Self, Self::Error> {
        let iter = iter.into_exact_size_iter();
        let manager = NopBufferManager;

        let mut array = Array::new(&manager, DataType::Utf8, iter.len())?;
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
    Array: TryFromExactSizeIterator<V, Error = RayexecError>,
{
    type Error = RayexecError;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::cache::NopCache;
    use crate::arrays::compute::make_list::make_list_from_values;
    use crate::arrays::datatype::ListTypeMeta;
    use crate::testutil::arrays::assert_arrays_eq;

    #[test]
    fn try_new_constant_utf8() {
        let arr = Array::new_constant(&NopBufferManager, &"a".into(), 4).unwrap();
        let expected = Array::try_from_iter(["a", "a", "a", "a"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn try_new_from_other_simple() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let new_arr = Array::new_from_other(&NopBufferManager, &mut arr).unwrap();

        let expected = Array::try_from_iter(["a", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
        assert_arrays_eq(&expected, &new_arr);
    }

    #[test]
    fn try_new_from_other_dictionary() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => '["b", "a", "a", "b"]'
        arr.select(&NopBufferManager, [1, 0, 0, 1]).unwrap();

        let new_arr = Array::new_from_other(&NopBufferManager, &mut arr).unwrap();

        let expected = Array::try_from_iter(["b", "a", "a", "b"]).unwrap();
        assert_arrays_eq(&expected, &arr);
        assert_arrays_eq(&expected, &new_arr);
    }

    #[test]
    fn try_new_from_other_constant() {
        let mut arr = Array::new_constant(&NopBufferManager, &"cat".into(), 4).unwrap();
        let new_arr = Array::new_from_other(&NopBufferManager, &mut arr).unwrap();

        let expected = Array::try_from_iter(["cat", "cat", "cat", "cat"]).unwrap();

        assert_arrays_eq(&expected, &new_arr);
    }

    #[test]
    fn try_new_typed_null_array() {
        let arr = Array::new_typed_null(&NopBufferManager, DataType::Int32, 4).unwrap();
        let expected = Array::try_from_iter::<[Option<i32>; 4]>([None, None, None, None]).unwrap();
        assert_arrays_eq(&expected, &arr);

        assert_eq!(BorrowedScalarValue::Null, arr.get_value(2).unwrap());
    }

    #[test]
    fn select_no_change() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&NopBufferManager, [0, 1, 2]).unwrap();

        let expected = Array::try_from_iter(["a", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_prune_rows() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&NopBufferManager, [0, 2]).unwrap();

        let expected = Array::try_from_iter(["a", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_expand_rows() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&NopBufferManager, [0, 1, 1, 2]).unwrap();

        let expected = Array::try_from_iter(["a", "b", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_existing_selection() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => ["a", "c"]
        arr.select(&NopBufferManager, [0, 2]).unwrap();

        // => ["c", "c", "a"]
        arr.select(&NopBufferManager, [1, 1, 0]).unwrap();

        let expected = Array::try_from_iter(["c", "c", "a"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_constant() {
        let mut arr = Array::new_constant(&NopBufferManager, &"dog".into(), 3).unwrap();
        arr.select(&NopBufferManager, [0, 1, 2, 0, 1, 2]).unwrap();

        let expected = Array::try_from_iter(["dog", "dog", "dog", "dog", "dog", "dog"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_after_making_array_data_shared() {
        // Try to select from an array that has its array data managed by having
        // it be created from an existing array.

        let mut arr1 = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut arr2 = Array::new_from_other(&NopBufferManager, &mut arr1).unwrap();
        // => [2, 1, 3]
        arr2.select(&NopBufferManager, [1, 0, 2]).unwrap();

        let expected = Array::try_from_iter([2, 1, 3]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }

    #[test]
    fn select_after_making_constant_array_data_shared() {
        // Same as above, just with a constant array.

        let mut arr1 = Array::new_constant(&NopBufferManager, &14.into(), 3).unwrap();
        let mut arr2 = Array::new_from_other(&NopBufferManager, &mut arr1).unwrap();
        // => [14, 14, 14]
        arr2.select(&NopBufferManager, [1, 0, 2]).unwrap();

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
        arr.select(&NopBufferManager, [0, 2]).unwrap();
        let val = arr.get_value(1).unwrap();

        assert_eq!(BorrowedScalarValue::Utf8("c".into()), val);
    }

    #[test]
    fn get_value_with_selection_null() {
        let mut arr = Array::try_from_iter([Some("a"), Some("b"), None]).unwrap();
        // => [NULL, "a"]
        arr.select(&NopBufferManager, [2, 0]).unwrap();

        let val = arr.get_value(0).unwrap();
        assert_eq!(BorrowedScalarValue::Null, val);

        let val = arr.get_value(1).unwrap();
        assert_eq!(BorrowedScalarValue::Utf8("a".into()), val);
    }

    #[test]
    fn get_value_constant() {
        let arr = Array::new_constant(&NopBufferManager, &"cat".into(), 4).unwrap();
        let val = arr.get_value(2).unwrap();
        assert_eq!(BorrowedScalarValue::Utf8("cat".into()), val);
    }

    #[test]
    fn get_value_list_i32() {
        let mut lists = Array::new(
            &NopBufferManager,
            DataType::List(ListTypeMeta::new(DataType::Int32)),
            4,
        )
        .unwrap();

        make_list_from_values(
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
        let mut arr2 = Array::new(&NopBufferManager, DataType::Int32, 16).unwrap();

        arr2.clone_constant_from(&mut arr, 1, 8, &mut NopCache)
            .unwrap();

        let expected = Array::try_from_iter([2, 2, 2, 2, 2, 2, 2, 2]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }

    #[test]
    fn clone_constant_from_other_i32_null() {
        let mut arr = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
        let mut arr2 = Array::new(&NopBufferManager, DataType::Int32, 16).unwrap();

        arr2.clone_constant_from(&mut arr, 1, 8, &mut NopCache)
            .unwrap();

        let expected = Array::try_from_iter(vec![None as Option<i32>; 8]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }

    #[test]
    fn clone_constant_from_other_i32_dictionary() {
        let mut arr = Array::try_from_iter([1, 2, 3]).unwrap();
        // => [2, 3, 1]
        arr.select(&NopBufferManager, [1, 2, 0]).unwrap();
        let mut arr2 = Array::new(&NopBufferManager, DataType::Int32, 16).unwrap();

        arr2.clone_constant_from(&mut arr, 1, 8, &mut NopCache)
            .unwrap();

        let expected = Array::try_from_iter([3, 3, 3, 3, 3, 3, 3, 3]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }
}
