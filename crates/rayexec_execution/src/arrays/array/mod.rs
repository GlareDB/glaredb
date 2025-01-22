pub mod array_buffer;
pub mod array_data;
pub mod buffer_manager;
pub mod flat;
pub mod physical_type;
pub mod selection;
pub mod string_view;
pub mod validity;

mod raw;
mod shared_or_owned;

use std::fmt::Debug;
use std::sync::Arc;

use array_buffer::{
    ArrayBuffer,
    ConstantBuffer,
    DictionaryBuffer,
    ListBuffer,
    ListItemMetadata,
    SecondaryBuffer,
};
use array_data::ArrayData;
use buffer_manager::{BufferManager, NopBufferManager};
use flat::FlatArrayView;
use half::f16;
use physical_type::{
    Addressable,
    AddressableMut,
    MutablePhysicalStorage,
    PhysicalBinary,
    PhysicalBool,
    PhysicalConstant,
    PhysicalDictionary,
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
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use shared_or_owned::SharedOrOwned;
use stdutil::iter::TryFromExactSizeIterator;
use string_view::StringViewHeap;
use validity::Validity;

use crate::arrays::bitmap::Bitmap;
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::decimal::{Decimal128Scalar, Decimal64Scalar};
use crate::arrays::scalar::interval::Interval;
use crate::arrays::scalar::timestamp::TimestampScalar;
use crate::arrays::scalar::ScalarValue;
use crate::arrays::selection::SelectionVector;

/// Validity mask for physical storage.
pub type PhysicalValidity = SharedOrOwned<Bitmap>;

/// Logical row selection.
pub type LogicalSelection = SharedOrOwned<SelectionVector>;

#[derive(Debug)]
pub(crate) struct ArrayNextInner<B: BufferManager> {
    /// Determines the validity at each row in the array.
    ///
    /// This should match the length of array data.
    pub(crate) validity: Validity,
    /// Holds the underlying array data.
    pub(crate) data: ArrayData<B>,
}

// TODO: Remove Clone, PartialEq
#[derive(Debug)]
pub struct Array<B: BufferManager = NopBufferManager> {
    /// Data type of the array.
    pub(crate) datatype: DataType,
    /// Contents of the refactored array internals.
    ///
    /// Will be flattened and `selection2`, `validity2`, `data2` will be removed
    /// once everything's switched over.
    pub(crate) next: Option<ArrayNextInner<B>>,
}

impl<B> Array<B>
where
    B: BufferManager,
{
    /// Create a new array with the given capacity.
    ///
    /// This will take care of initalizing the primary and secondary data
    /// buffers depending on the type.
    pub fn try_new(manager: &Arc<B>, datatype: DataType, capacity: usize) -> Result<Self> {
        let buffer = array_buffer_for_datatype(manager, &datatype, capacity)?;
        let validity = Validity::new_all_valid(capacity);

        Ok(Array {
            datatype,
            next: Some(ArrayNextInner {
                validity,
                data: ArrayData::owned(buffer),
            }),
        })
    }

    /// Try to create a new array from other.
    pub fn try_new_from_other(manager: &Arc<B>, other: &mut Self) -> Result<Self> {
        let managed = other.next_mut().data.make_managed(manager)?;
        let data = ArrayData::managed(managed);
        let validity = other.next().validity.clone();

        Ok(Array {
            datatype: other.datatype().clone(),
            next: Some(ArrayNextInner { validity, data }),
        })
    }

    /// Create a new array backed by a constant value.
    pub fn try_new_constant(manager: &Arc<B>, value: &ScalarValue, len: usize) -> Result<Self> {
        let mut arr = Self::try_new(manager, value.datatype(), 1)?;
        arr.set_value(0, value)?;

        let next = arr.next.unwrap();

        let mut buf = ArrayBuffer::with_primary_capacity::<PhysicalConstant>(manager, len)?;
        buf.put_secondary_buffer(SecondaryBuffer::Constant(ConstantBuffer {
            row_reference: 0,
            validity: next.validity,
            buffer: next.data,
        }));

        Ok(Array {
            datatype: value.datatype(),
            next: Some(ArrayNextInner {
                validity: Validity::new_all_valid(len),
                data: ArrayData::owned(buf),
            }),
        })
    }

    /// Create a new typed null array.
    pub fn try_new_typed_null(manager: &Arc<B>, datatype: DataType, len: usize) -> Result<Self> {
        let val_buffer = array_buffer_for_datatype(manager, &datatype, 1)?;
        let validity = Validity::new_all_invalid(1);

        let mut buf = ArrayBuffer::with_primary_capacity::<PhysicalConstant>(manager, len)?;
        buf.put_secondary_buffer(SecondaryBuffer::Constant(ConstantBuffer {
            row_reference: 0,
            validity,
            buffer: ArrayData::owned(val_buffer),
        }));

        Ok(Array {
            datatype,
            next: Some(ArrayNextInner {
                validity: Validity::new_all_valid(len),
                data: ArrayData::owned(buf),
            }),
        })
    }

    // TODO: Remove
    #[allow(dead_code)]
    pub(crate) fn next(&self) -> &ArrayNextInner<B> {
        self.next.as_ref().expect("next to be set")
    }

    // TODO: Remove
    #[allow(dead_code)]
    pub(crate) fn next_mut(&mut self) -> &mut ArrayNextInner<B> {
        self.next.as_mut().expect("next to be set")
    }

    pub fn capacity(&self) -> usize {
        self.next().data.primary_capacity()
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    /// Gets the physical type for this array's data type.
    ///
    /// Note for arrays that have previously been `select`ed, this will report
    /// the original physical type of the data type, and _not_ dictionary.
    pub fn physical_type(&self) -> PhysicalType {
        self.datatype.physical_type()
    }

    pub fn put_validity(&mut self, validity: Validity) -> Result<()> {
        let next = self.next_mut();

        if validity.len() != next.data.primary_capacity() {
            return Err(RayexecError::new("Invalid validity length")
                .with_field("got", validity.len())
                .with_field("want", next.data.primary_capacity()));
        }
        next.validity = validity;

        Ok(())
    }

    pub fn is_dictionary(&self) -> bool {
        self.next.as_ref().unwrap().data.physical_type() == PhysicalType::Dictionary
    }

    pub fn is_constant(&self) -> bool {
        self.next.as_ref().unwrap().data.physical_type() == PhysicalType::Constant
    }

    pub fn flat_view(&self) -> Result<FlatArrayView<B>> {
        FlatArrayView::from_array(self)
    }

    /// Selects indice from the array.
    ///
    /// This will convert the underlying array buffer into a dictionary buffer.
    pub fn select(
        &mut self,
        manager: &Arc<B>,
        selection: impl stdutil::iter::IntoExactSizeIterator<Item = usize>,
    ) -> Result<()> {
        let is_dictionary = self.is_dictionary();
        let is_constant = self.is_constant();
        let next = self.next_mut();

        if is_constant {
            // Easy, just create a new primary data buffer with the size of the selection.
            let sel = selection.into_iter();

            let mut new_buf =
                ArrayBuffer::with_primary_capacity::<PhysicalConstant>(manager, sel.len())?;

            std::mem::swap(
                next.data.try_as_mut()?.get_secondary_mut(), // TODO: Should just clone the pointer if managed.
                new_buf.get_secondary_mut(),
            );

            next.data = ArrayData::owned(new_buf);

            debug_assert!(matches!(
                next.data.get_secondary(),
                SecondaryBuffer::Constant(_)
            ));

            return Ok(());
        }

        if is_dictionary {
            // Already dictionary, select the selection.
            let sel = selection.into_iter();
            let mut new_buf =
                ArrayBuffer::with_primary_capacity::<PhysicalDictionary>(manager, sel.len())?;

            let old_sel = next.data.try_as_slice::<PhysicalDictionary>()?;
            let new_sel = new_buf.try_as_slice_mut::<PhysicalDictionary>()?;

            for (sel_idx, sel_buf) in sel.zip(new_sel) {
                let idx = old_sel[sel_idx];
                *sel_buf = idx;
            }

            // Now swap the secondary buffers, the dictionary buffer will now be
            // on `new_buf`.
            std::mem::swap(
                next.data.try_as_mut()?.get_secondary_mut(), // TODO: Should just clone the pointer if managed.
                new_buf.get_secondary_mut(),
            );

            // And set the new buf, old buf gets dropped.
            next.data = ArrayData::owned(new_buf);

            debug_assert!(matches!(
                next.data.get_secondary(),
                SecondaryBuffer::Dictionary(_)
            ));

            return Ok(());
        }

        let sel = selection.into_iter();
        let mut new_buf =
            ArrayBuffer::with_primary_capacity::<PhysicalDictionary>(manager, sel.len())?;

        let new_buf_slice = new_buf.try_as_slice_mut::<PhysicalDictionary>()?;

        // Set all selection indices in the new array buffer.
        for (sel_idx, sel_buf) in sel.zip(new_buf_slice) {
            *sel_buf = sel_idx
        }

        // TODO: Probably verify selection all in bounds.

        // Now replace the original buffer, and put the original buffer in the
        // secondary buffer.
        let orig_validity = std::mem::replace(
            &mut next.validity,
            Validity::new_all_valid(new_buf.primary_capacity()),
        );
        let orig_buffer = std::mem::replace(&mut next.data, ArrayData::owned(new_buf));
        // TODO: Should just clone the pointer if managed.
        next.data
            .try_as_mut()?
            .put_secondary_buffer(SecondaryBuffer::Dictionary(DictionaryBuffer {
                validity: orig_validity,
                buffer: orig_buffer,
            }));

        debug_assert!(matches!(
            next.data.get_secondary(),
            SecondaryBuffer::Dictionary(_)
        ));

        Ok(())
    }

    /// Resets self to prepare for writing to the array.
    ///
    /// This will:
    /// - Reset validity to all 'valid'.
    /// - Create or reuse a writeable buffer for array data. No guarantees are
    ///   made about the contents of the buffer.
    ///
    /// Bfuffer values _must_ be written for a row before attempting to read a
    /// value for that row after calling this function. Underlying storage may
    /// be cleared resulting in stale metadata (and thus invalid reads).
    pub fn reset_for_write(&mut self, manager: &Arc<B>) -> Result<()> {
        let cap = self.capacity();
        let next = self.next.as_mut().unwrap();

        next.validity = Validity::new_all_valid(cap);

        // Check if dictionary first since we want to try to get the underlying
        // buffer from that. We should only have layer of "dictionary", so we
        // shouldn't need to recurse.
        if next.data.as_ref().physical_type() == PhysicalType::Dictionary {
            let secondary = next.data.try_as_mut()?.get_secondary_mut();
            let dict = match std::mem::replace(secondary, SecondaryBuffer::None) {
                SecondaryBuffer::Dictionary(dict) => dict,
                other => {
                    return Err(RayexecError::new(format!(
                        "Expected dictionary secondary buffer, got {other:?}",
                    )))
                }
            };

            // TODO: Not sure what to do if capacities don't match. Currently
            // dictionaries are only created through 'select' and the index
            // buffer gets initialized to the length of the selection.
            next.data = dict.buffer;
        }

        if let Err(()) = next.data.try_reset_for_write() {
            // Need to create a new buffer and set that.
            let buffer = array_buffer_for_datatype(manager, &self.datatype, cap)?;
            next.data = ArrayData::owned(buffer)
        }

        // Reset secondary buffers.
        match next.data.try_as_mut()?.get_secondary_mut() {
            SecondaryBuffer::StringViewHeap(heap) => {
                heap.clear();
                // All metadata is stale. Panics may occur if attempting to read
                // prior to writing new values for a row.
            }
            SecondaryBuffer::List(list) => {
                list.entries = 0;
                // Child array keeps its capacity, it'll be overwritten. List
                // item metadata will become stale, but technically won't error.
            }
            SecondaryBuffer::Dictionary(_) => (),
            SecondaryBuffer::Constant(_) => (),
            SecondaryBuffer::None => (),
        }

        Ok(())
    }

    /// "Clones" some other array into this array.
    ///
    /// This will try to make the buffer from the other array managed to make it
    /// cheaply cloneable and shared with this array.
    ///
    /// Array capacities and datatypes must be the same for both arrays.
    pub fn try_clone_from(&mut self, manager: &B, other: &mut Self) -> Result<()> {
        if self.datatype != other.datatype {
            return Err(RayexecError::new(
                "Attempted clone array from other array with different data types",
            )
            .with_field("own_datatype", self.datatype.clone())
            .with_field("other_datatype", other.datatype.clone()));
        }

        // TODO: Do we want this check? Dictionaries right now can have differing capacities based
        // on selection inputs.
        // if self.capacity() != other.capacity() {
        //     return Err(RayexecError::new(
        //         "Attempted to clone into array from other array with different capacity",
        //     )
        //     .with_field("own_capacity", self.capacity())
        //     .with_field("other_capacity", other.capacity()));
        // }

        let managed = other.next_mut().data.make_managed(manager)?;
        self.next_mut().data.set_managed(managed)?;
        self.next_mut().validity = other.next().validity.clone();

        Ok(())
    }

    /// Try to clone a row from another array into this array, turning this
    /// array into a constant array with `count` length.
    pub fn try_clone_row_from(
        &mut self,
        manager: &Arc<B>,
        other: &mut Self,
        row: usize,
        count: usize,
    ) -> Result<()> {
        if self.datatype != other.datatype {
            return Err(RayexecError::new(
                "Attempted clone row from other array with different data types",
            )
            .with_field("own_datatype", self.datatype.clone())
            .with_field("other_datatype", other.datatype.clone()));
        }

        let managed = other.next_mut().data.make_managed(manager)?;
        let mut buf = ArrayBuffer::with_primary_capacity::<PhysicalConstant>(manager, count)?;
        buf.put_secondary_buffer(SecondaryBuffer::Constant(ConstantBuffer {
            row_reference: row,
            validity: other.next().validity.clone(), // TODO: We could avoid a full clone here.
            buffer: ArrayData::managed(managed),
        }));

        let next = self.next_mut();
        next.validity = Validity::new_all_valid(count);
        next.data = ArrayData::owned(buf);

        Ok(())
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
        match self.datatype.physical_type() {
            PhysicalType::Boolean => copy_rows::<PhysicalBool, _>(self, mapping, dest)?,
            PhysicalType::Int8 => copy_rows::<PhysicalI8, _>(self, mapping, dest)?,
            PhysicalType::Int16 => copy_rows::<PhysicalI16, _>(self, mapping, dest)?,
            PhysicalType::Int32 => copy_rows::<PhysicalI32, _>(self, mapping, dest)?,
            PhysicalType::Int64 => copy_rows::<PhysicalI64, _>(self, mapping, dest)?,
            PhysicalType::Int128 => copy_rows::<PhysicalI128, _>(self, mapping, dest)?,
            PhysicalType::UInt8 => copy_rows::<PhysicalU8, _>(self, mapping, dest)?,
            PhysicalType::UInt16 => copy_rows::<PhysicalU16, _>(self, mapping, dest)?,
            PhysicalType::UInt32 => copy_rows::<PhysicalU32, _>(self, mapping, dest)?,
            PhysicalType::UInt64 => copy_rows::<PhysicalU64, _>(self, mapping, dest)?,
            PhysicalType::UInt128 => copy_rows::<PhysicalU128, _>(self, mapping, dest)?,
            PhysicalType::Float16 => copy_rows::<PhysicalF16, _>(self, mapping, dest)?,
            PhysicalType::Float32 => copy_rows::<PhysicalF32, _>(self, mapping, dest)?,
            PhysicalType::Float64 => copy_rows::<PhysicalF64, _>(self, mapping, dest)?,
            PhysicalType::Interval => copy_rows::<PhysicalInterval, _>(self, mapping, dest)?,
            PhysicalType::Utf8 => copy_rows::<PhysicalUtf8, _>(self, mapping, dest)?,
            _ => unimplemented!(),
        }

        Ok(())
    }

    pub fn get_value(&self, idx: usize) -> Result<ScalarValue> {
        if idx >= self.capacity() {
            return Err(RayexecError::new("Index out of bounds")
                .with_field("idx", idx)
                .with_field("capacity", self.capacity()));
        }

        let flat = self.flat_view()?;
        let idx = flat.selection.get(idx).expect("Index to be in bounds");

        if !flat.validity.is_valid(idx) {
            return Ok(ScalarValue::Null);
        }

        match &self.datatype {
            DataType::Boolean => {
                let v = PhysicalBool::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Boolean(*v))
            }
            DataType::Int8 => {
                let v = PhysicalI8::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Int8(*v))
            }
            DataType::Int16 => {
                let v = PhysicalI16::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Int16(*v))
            }
            DataType::Int32 => {
                let v = PhysicalI32::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Int32(*v))
            }
            DataType::Int64 => {
                let v = PhysicalI64::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Int64(*v))
            }
            DataType::Int128 => {
                let v = PhysicalI128::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Int128(*v))
            }
            DataType::UInt8 => {
                let v = PhysicalU8::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::UInt8(*v))
            }
            DataType::UInt16 => {
                let v = PhysicalU16::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::UInt16(*v))
            }
            DataType::UInt32 => {
                let v = PhysicalU32::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::UInt32(*v))
            }
            DataType::UInt64 => {
                let v = PhysicalU64::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::UInt64(*v))
            }
            DataType::UInt128 => {
                let v = PhysicalU128::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::UInt128(*v))
            }
            DataType::Float16 => {
                let v = PhysicalF16::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Float16(*v))
            }
            DataType::Float32 => {
                let v = PhysicalF32::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Float32(*v))
            }
            DataType::Float64 => {
                let v = PhysicalF64::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Float64(*v))
            }
            DataType::Decimal64(m) => {
                let v = PhysicalI64::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Decimal64(Decimal64Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: *v,
                }))
            }
            DataType::Decimal128(m) => {
                let v = PhysicalI128::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Decimal128(Decimal128Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: *v,
                }))
            }
            DataType::Interval => {
                let v = PhysicalInterval::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Interval(*v))
            }
            DataType::Timestamp(m) => {
                let v = PhysicalI64::get_addressable(flat.array_buffer)?
                    .get(idx)
                    .unwrap();
                Ok(ScalarValue::Timestamp(TimestampScalar {
                    unit: m.unit,
                    value: *v,
                }))
            }
            DataType::Utf8 => {
                let addressable = PhysicalUtf8::get_addressable(flat.array_buffer)?;
                let v = addressable.get(idx).unwrap();
                Ok(ScalarValue::Utf8(v.into()))
            }
            DataType::Binary => {
                let addressable = PhysicalBinary::get_addressable(flat.array_buffer)?;
                let v = addressable.get(idx).unwrap();
                Ok(ScalarValue::Binary(v.into()))
            }
            DataType::List(_) => {
                let addressable = PhysicalList::get_addressable(flat.array_buffer)?;
                let meta = addressable.get(idx).unwrap();
                let list_buf = flat.array_buffer.get_secondary().get_list()?;

                // TODO: Could be slow.
                let mut vals = Vec::with_capacity(meta.len as usize);
                for child_idx in meta.offset..(meta.offset + meta.len) {
                    let v = list_buf.child.get_value(child_idx as usize)?;
                    vals.push(v);
                }

                Ok(ScalarValue::List(vals))
            }

            other => not_implemented!("get value for scalar type: {other:?}"),
        }
    }

    /// Set a scalar value at a given index.
    pub fn set_value(&mut self, idx: usize, val: &ScalarValue) -> Result<()> {
        // TODO: Handle constant, dictionary
        //
        // - Constant => convert to dictionary
        // - Dictionary => add new value, update selection to point to value

        if self.is_dictionary() || self.is_constant() {
            not_implemented!("set value for dictionary/constant arrays")
        }

        if idx >= self.capacity() {
            return Err(RayexecError::new("Index out of bounds")
                .with_field("idx", idx)
                .with_field("capacity", self.capacity()));
        }

        let next = self.next_mut();
        next.validity.set_valid(idx);
        let data = next.data.try_as_mut()?;

        match val {
            ScalarValue::Null => {
                next.validity.set_invalid(idx);
            }
            ScalarValue::Boolean(val) => {
                PhysicalBool::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Int8(val) => {
                PhysicalI8::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Int16(val) => {
                PhysicalI16::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Int32(val) => {
                PhysicalI32::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Int64(val) => {
                PhysicalI64::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Int128(val) => {
                PhysicalI128::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::UInt8(val) => {
                PhysicalU8::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::UInt16(val) => {
                PhysicalU16::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::UInt32(val) => {
                PhysicalU32::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::UInt64(val) => {
                PhysicalU64::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::UInt128(val) => {
                PhysicalU128::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Float16(val) => {
                PhysicalF16::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Float32(val) => {
                PhysicalF32::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Float64(val) => {
                PhysicalF64::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Decimal64(val) => {
                PhysicalI64::get_addressable_mut(data)?.put(idx, &val.value);
            }
            ScalarValue::Decimal128(val) => {
                PhysicalI128::get_addressable_mut(data)?.put(idx, &val.value);
            }
            ScalarValue::Date32(val) => {
                PhysicalI32::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Date64(val) => {
                PhysicalI64::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Timestamp(val) => {
                PhysicalI64::get_addressable_mut(data)?.put(idx, &val.value);
            }
            ScalarValue::Interval(val) => {
                PhysicalInterval::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Utf8(val) => {
                PhysicalUtf8::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::Binary(val) => {
                PhysicalBinary::get_addressable_mut(data)?.put(idx, val);
            }
            ScalarValue::List(list) => {
                let secondary = next.data.try_as_mut()?.get_secondary_mut().get_list_mut()?;

                // Ensure we have space to push.
                let rem_cap = secondary.child.capacity() - secondary.entries;
                if rem_cap < list.len() {
                    // TODO: Just resize secondary.
                    return Err(RayexecError::new(
                        "Secondary list buffer does not have required capacity",
                    )
                    .with_field("remaining", rem_cap)
                    .with_field("need", list.len()));
                }

                for (child_idx, val) in (secondary.entries..).zip(list) {
                    secondary.child.set_value(child_idx, val)?;
                }

                // Now update entry count in child. Original value is our offset
                // index.
                let start_offset = secondary.entries;
                secondary.entries += list.len();

                // Set metadata pointing to new list.
                PhysicalList::get_addressable_mut(next.data.try_as_mut()?)?.put(
                    idx,
                    &ListItemMetadata {
                        offset: start_offset as i32,
                        len: list.len() as i32,
                    },
                );
            }
            ScalarValue::Struct(_) => not_implemented!("set value for struct"),
        }

        Ok(())
    }
}

impl Array {
    #[deprecated]
    pub fn logical_len(&self) -> usize {
        unimplemented!()
    }

    #[deprecated]
    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        unimplemented!()
    }

    /// Get the value at a logical index.
    ///
    /// Takes into account the validity and selection vector.
    #[deprecated]
    pub fn logical_value(&self, idx: usize) -> Result<ScalarValue> {
        unimplemented!()
        // let idx = match self.selection_vector() {
        //     Some(v) => v
        //         .get_opt(idx)
        //         .ok_or_else(|| RayexecError::new(format!("Logical index {idx} out of bounds")))?,
        //     None => idx,
        // };

        // if let Some(validity) = &self.validity2 {
        //     if !validity.as_ref().value(idx) {
        //         return Ok(ScalarValue::Null);
        //     }
        // }

        // self.physical_scalar(idx)
    }
}

/// Helper for copying rows.
fn copy_rows<S, B>(
    from: &Array<B>,
    mapping: impl IntoIterator<Item = (usize, usize)>,
    to: &mut Array<B>,
) -> Result<()>
where
    S: MutablePhysicalStorage,
    B: BufferManager,
{
    let from_flat = from.flat_view()?;
    let from_storage = S::get_addressable(from_flat.array_buffer)?;

    let next = to.next_mut();
    let to_data = next.data.try_as_mut()?;
    let mut to_storage = S::get_addressable_mut(to_data)?;

    if from_flat.validity.all_valid() && next.validity.all_valid() {
        for (from_idx, to_idx) in mapping.into_iter() {
            let from_idx = from_flat.selection.get(from_idx).unwrap();
            let v = from_storage.get(from_idx).unwrap();
            to_storage.put(to_idx, v);
        }
    } else {
        for (from_idx, to_idx) in mapping.into_iter() {
            let from_idx = from_flat.selection.get(from_idx).unwrap();
            if from_flat.validity.is_valid(from_idx) {
                let v = from_storage.get(from_idx).unwrap();
                to_storage.put(to_idx, v);
            } else {
                next.validity.set_invalid(to_idx);
            }
        }
    }

    Ok(())
}

/// Create a new array buffer for a datatype.
fn array_buffer_for_datatype<B>(
    manager: &Arc<B>,
    datatype: &DataType,
    capacity: usize,
) -> Result<ArrayBuffer<B>>
where
    B: BufferManager,
{
    let buffer = match datatype.physical_type() {
        PhysicalType::UntypedNull => {
            ArrayBuffer::with_primary_capacity::<PhysicalUntypedNull>(manager, capacity)?
        }
        PhysicalType::Boolean => {
            ArrayBuffer::with_primary_capacity::<PhysicalBool>(manager, capacity)?
        }
        PhysicalType::Int8 => ArrayBuffer::with_primary_capacity::<PhysicalI8>(manager, capacity)?,
        PhysicalType::Int16 => {
            ArrayBuffer::with_primary_capacity::<PhysicalI16>(manager, capacity)?
        }
        PhysicalType::Int32 => {
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(manager, capacity)?
        }
        PhysicalType::Int64 => {
            ArrayBuffer::with_primary_capacity::<PhysicalI64>(manager, capacity)?
        }
        PhysicalType::Int128 => {
            ArrayBuffer::with_primary_capacity::<PhysicalI128>(manager, capacity)?
        }
        PhysicalType::UInt8 => ArrayBuffer::with_primary_capacity::<PhysicalU8>(manager, capacity)?,
        PhysicalType::UInt16 => {
            ArrayBuffer::with_primary_capacity::<PhysicalU16>(manager, capacity)?
        }
        PhysicalType::UInt32 => {
            ArrayBuffer::with_primary_capacity::<PhysicalU32>(manager, capacity)?
        }
        PhysicalType::UInt64 => {
            ArrayBuffer::with_primary_capacity::<PhysicalU64>(manager, capacity)?
        }
        PhysicalType::UInt128 => {
            ArrayBuffer::with_primary_capacity::<PhysicalU128>(manager, capacity)?
        }
        PhysicalType::Float16 => {
            ArrayBuffer::with_primary_capacity::<PhysicalF16>(manager, capacity)?
        }
        PhysicalType::Float32 => {
            ArrayBuffer::with_primary_capacity::<PhysicalF32>(manager, capacity)?
        }
        PhysicalType::Float64 => {
            ArrayBuffer::with_primary_capacity::<PhysicalF64>(manager, capacity)?
        }
        PhysicalType::Interval => {
            ArrayBuffer::with_primary_capacity::<PhysicalInterval>(manager, capacity)?
        }
        PhysicalType::Utf8 => {
            let mut buffer = ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(manager, capacity)?;
            buffer.put_secondary_buffer(SecondaryBuffer::StringViewHeap(StringViewHeap::new()));
            buffer
        }
        PhysicalType::List => {
            let inner_type = match &datatype {
                DataType::List(m) => m.datatype.as_ref().clone(),
                other => {
                    return Err(RayexecError::new(format!(
                        "Expected list datatype, got {other}"
                    )))
                }
            };

            let child = Array::try_new(manager, inner_type, capacity)?;

            let mut buffer = ArrayBuffer::with_primary_capacity::<PhysicalList>(manager, capacity)?;
            buffer.put_secondary_buffer(SecondaryBuffer::List(ListBuffer::new(child)));

            buffer
        }
        other => not_implemented!("create array buffer for physical type {other}"),
    };

    Ok(buffer)
}

/// Implements `try_from_iter` for primitive types.
///
/// Note these create arrays using Nop buffer manager and so really only
/// suitable for tests right now.
macro_rules! impl_primitive_from_iter {
    ($prim:ty, $phys:ty, $typ_variant:ident) => {
        impl TryFromExactSizeIterator<$prim> for Array {
            type Error = RayexecError;

            fn try_from_iter<T: stdutil::iter::IntoExactSizeIterator<Item = $prim>>(
                iter: T,
            ) -> Result<Self, Self::Error> {
                let iter = iter.into_iter();

                let manager = Arc::new(NopBufferManager);

                let mut array = Array::try_new(&manager, DataType::$typ_variant, iter.len())?;
                let slice = array
                    .next
                    .as_mut()
                    .unwrap()
                    .data
                    .try_as_mut()?
                    .try_as_slice_mut::<$phys>()?;

                for (dest, v) in slice.iter_mut().zip(iter) {
                    *dest = v;
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

impl<S> TryFromExactSizeIterator<S> for Array<NopBufferManager>
where
    S: AsRefStr,
{
    type Error = RayexecError;

    fn try_from_iter<T: stdutil::iter::IntoExactSizeIterator<Item = S>>(
        iter: T,
    ) -> Result<Self, Self::Error> {
        let iter = iter.into_iter();
        let len = iter.len();

        let mut buffer =
            ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(&Arc::new(NopBufferManager), len)?;
        buffer.put_secondary_buffer(SecondaryBuffer::StringViewHeap(StringViewHeap::new()));

        let mut addressable = buffer.try_as_string_view_addressable_mut()?;

        for (idx, v) in iter.enumerate() {
            addressable.put(idx, v.as_ref());
        }

        Ok(Array {
            datatype: DataType::Utf8,
            next: Some(ArrayNextInner {
                validity: Validity::new_all_valid(len),
                data: ArrayData::owned(buffer),
            }),
        })
    }
}

/// From iterator implementation that creates an array from optionally valid
/// values. Some is treated as valid, None as invalid.
impl<V> TryFromExactSizeIterator<Option<V>> for Array<NopBufferManager>
where
    V: Default,
    Array<NopBufferManager>: TryFromExactSizeIterator<V, Error = RayexecError>,
{
    type Error = RayexecError;

    fn try_from_iter<T: stdutil::iter::IntoExactSizeIterator<Item = Option<V>>>(
        iter: T,
    ) -> Result<Self, Self::Error> {
        let iter = iter.into_iter();
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
    use crate::arrays::compute::make_list::make_list_from_values;
    use crate::arrays::datatype::ListTypeMeta;
    use crate::arrays::testutil::assert_arrays_eq;

    #[test]
    fn new_constant_array() {
        let arr = Array::try_new_constant(&Arc::new(NopBufferManager), &"a".into(), 4).unwrap();
        let expected = Array::try_from_iter(["a", "a", "a", "a"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn new_typed_null_array() {
        let arr =
            Array::try_new_typed_null(&Arc::new(NopBufferManager), DataType::Int32, 4).unwrap();
        let expected = Array::try_from_iter::<[Option<i32>; 4]>([None, None, None, None]).unwrap();
        assert_arrays_eq(&expected, &arr);

        assert_eq!(ScalarValue::Null, arr.get_value(2).unwrap());
    }

    #[test]
    fn new_from_other_simple() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let new_arr = Array::try_new_from_other(&Arc::new(NopBufferManager), &mut arr).unwrap();

        let expected = Array::try_from_iter(["a", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
        assert_arrays_eq(&expected, &new_arr);
    }

    #[test]
    fn new_from_other_dictionary() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => '["b", "a", "a", "b"]'
        arr.select(&Arc::new(NopBufferManager), [1, 0, 0, 1])
            .unwrap();

        let new_arr = Array::try_new_from_other(&Arc::new(NopBufferManager), &mut arr).unwrap();

        let expected = Array::try_from_iter(["b", "a", "a", "b"]).unwrap();
        assert_arrays_eq(&expected, &arr);
        assert_arrays_eq(&expected, &new_arr);
    }

    #[test]
    fn select_no_change() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&Arc::new(NopBufferManager), [0, 1, 2]).unwrap();

        let expected = Array::try_from_iter(["a", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_prune_rows() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&Arc::new(NopBufferManager), [0, 2]).unwrap();

        let expected = Array::try_from_iter(["a", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_expand_rows() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        arr.select(&Arc::new(NopBufferManager), [0, 1, 1, 2])
            .unwrap();

        let expected = Array::try_from_iter(["a", "b", "b", "c"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_existing_selection() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => ["a", "c"]
        arr.select(&Arc::new(NopBufferManager), [0, 2]).unwrap();

        // => ["c", "c", "a"]
        arr.select(&Arc::new(NopBufferManager), [1, 1, 0]).unwrap();

        let expected = Array::try_from_iter(["c", "c", "a"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn select_constant() {
        let mut arr =
            Array::try_new_constant(&Arc::new(NopBufferManager), &"dog".into(), 3).unwrap();
        arr.select(&Arc::new(NopBufferManager), [0, 1, 2, 0, 1, 2])
            .unwrap();

        let expected = Array::try_from_iter(["dog", "dog", "dog", "dog", "dog", "dog"]).unwrap();
        assert_arrays_eq(&expected, &arr);
    }

    #[test]
    fn get_value_simple() {
        let arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let val = arr.get_value(1).unwrap();
        assert_eq!(ScalarValue::Utf8("b".into()), val);
    }

    #[test]
    fn get_value_null() {
        let arr = Array::try_from_iter([Some("a"), None, Some("c")]).unwrap();

        let val = arr.get_value(0).unwrap();
        assert_eq!(ScalarValue::Utf8("a".into()), val);

        let val = arr.get_value(1).unwrap();
        assert_eq!(ScalarValue::Null, val);
    }

    #[test]
    fn get_value_with_selection() {
        let mut arr = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => ["a", "c"]
        arr.select(&Arc::new(NopBufferManager), [0, 2]).unwrap();
        let val = arr.get_value(1).unwrap();

        assert_eq!(ScalarValue::Utf8("c".into()), val);
    }

    #[test]
    fn get_value_constant() {
        let arr = Array::try_new_constant(&Arc::new(NopBufferManager), &"cat".into(), 4).unwrap();
        let val = arr.get_value(2).unwrap();
        assert_eq!(ScalarValue::Utf8("cat".into()), val);
    }

    #[test]
    fn get_value_list_i32() {
        let mut lists = Array::try_new(
            &Arc::new(NopBufferManager),
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

        let expected0 = ScalarValue::List(vec![1.into(), 5.into()]);
        let v0 = lists.get_value(0).unwrap();
        assert_eq!(expected0, v0);

        let expected2 = ScalarValue::List(vec![ScalarValue::Null, 7.into()]);
        let v2 = lists.get_value(2).unwrap();
        assert_eq!(expected2, v2);
    }

    #[test]
    fn copy_rows_simple() {
        let from = Array::try_from_iter(["a", "b", "c"]).unwrap();
        let mut to = Array::try_from_iter(["d", "d", "d"]).unwrap();

        from.copy_rows([(0, 1), (1, 2)], &mut to).unwrap();

        let expected = Array::try_from_iter(["d", "a", "b"]).unwrap();

        assert_arrays_eq(&expected, &to);
    }

    #[test]
    fn copy_rows_from_dict() {
        let mut from = Array::try_from_iter(["a", "b", "c"]).unwrap();
        // => '["b", "a", "c"]
        from.select(&Arc::new(NopBufferManager), [1, 0, 2]).unwrap();

        let mut to = Array::try_from_iter(["d", "d", "d"]).unwrap();

        from.copy_rows([(0, 1), (1, 2)], &mut to).unwrap();

        let expected = Array::try_from_iter(["d", "b", "a"]).unwrap();

        assert_arrays_eq(&expected, &to);
    }

    #[test]
    fn reset_after_clone_from() {
        let mut a1 = Array::try_from_iter(["a", "bb", "ccc"]).unwrap();
        let mut a2 = Array::try_from_iter(["d", "ee", "fff"]).unwrap();

        a1.try_clone_from(&NopBufferManager, &mut a2).unwrap();

        let expected = Array::try_from_iter(["d", "ee", "fff"]).unwrap();
        assert_arrays_eq(&expected, &a1);
        assert_arrays_eq(&expected, &a2);

        a1.reset_for_write(&Arc::new(NopBufferManager)).unwrap();

        // Ensure we can write to it.
        let mut strings = a1
            .next_mut()
            .data
            .try_as_mut()
            .unwrap()
            .try_as_string_view_addressable_mut()
            .unwrap();

        strings.put(0, "hello");
        strings.put(1, "world");
        strings.put(2, "goodbye");

        let expected = Array::try_from_iter(["hello", "world", "goodbye"]).unwrap();
        assert_arrays_eq(&expected, &a1);
    }

    #[test]
    fn reset_resets_validity() {
        let mut a = Array::try_from_iter([Some("a"), None, Some("c")]).unwrap();
        assert!(!a.next().validity.all_valid());

        a.reset_for_write(&Arc::new(NopBufferManager)).unwrap();
        assert!(a.next().validity.all_valid());
    }

    #[test]
    fn try_clone_row_from_i32_valid() {
        let manager = Arc::new(NopBufferManager);

        let mut arr = Array::try_from_iter([1, 2, 3]).unwrap();
        let mut arr2 = Array::try_new(&manager, DataType::Int32, 16).unwrap();

        arr2.try_clone_row_from(&manager, &mut arr, 1, 8).unwrap();

        let expected = Array::try_from_iter([2, 2, 2, 2, 2, 2, 2, 2]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }

    #[test]
    fn try_clone_row_from_i32_null() {
        let manager = Arc::new(NopBufferManager);

        let mut arr = Array::try_from_iter([Some(1), None, Some(3)]).unwrap();
        let mut arr2 = Array::try_new(&manager, DataType::Int32, 16).unwrap();

        arr2.try_clone_row_from(&manager, &mut arr, 1, 8).unwrap();

        let expected = Array::try_from_iter(vec![None as Option<i32>; 8]).unwrap();
        assert_arrays_eq(&expected, &arr2);
    }
}
