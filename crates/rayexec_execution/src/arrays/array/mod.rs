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

use array_buffer::{ArrayBuffer, DictionaryBuffer, ListBuffer, ListItemMetadata, SecondaryBuffer};
use array_data::ArrayData;
use buffer_manager::{BufferManager, NopBufferManager};
use flat::FlatArrayView;
use half::f16;
use physical_type::{
    Addressable,
    AddressableMut,
    MutablePhysicalStorage,
    PhysicalAny,
    PhysicalBinary,
    PhysicalBool,
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
use crate::arrays::executor::scalar::UnaryExecutor;
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

#[derive(Debug)]
pub(crate) struct ArrayNextInner<B: BufferManager> {
    pub(crate) validity: Validity,
    pub(crate) data: ArrayData<B>,
}

// TODO: Remove Clone, PartialEq
#[derive(Debug)]
pub struct Array<B: BufferManager = NopBufferManager> {
    /// Data type of the array.
    pub(crate) datatype: DataType,
    /// Selection of rows for the array.
    ///
    /// If set, this provides logical row mapping on top of the underlying data.
    /// If not set, then there's a one-to-one mapping between the logical row
    /// and and row in the underlying data.
    // TODO: Remove
    pub(crate) selection2: Option<LogicalSelection>,
    /// Option validity mask.
    ///
    /// This indicates the validity of the underlying data. This does not take
    /// into account the selection vector, and always maps directly to the data.
    // TODO: Remove
    pub(crate) validity2: Option<PhysicalValidity>,
    /// The physical data.
    // TODO: Remove
    pub(crate) data2: ArrayData2,

    /// Contents of the refactored array internals.
    ///
    /// Will be flattened and `selection2`, `validity2`, `data2` will be removed
    /// once everything's switched over.
    pub(crate) next: Option<ArrayNextInner<B>>,
}

// TODO: Remove
impl Clone for Array {
    fn clone(&self) -> Self {
        Array {
            datatype: self.datatype.clone(),
            selection2: self.selection2.clone(),
            validity2: self.validity2.clone(),
            data2: self.data2.clone(),
            next: None,
        }
    }
}

// TODO: Remove
impl PartialEq for Array {
    fn eq(&self, other: &Self) -> bool {
        self.datatype == other.datatype
            && self.selection2 == other.selection2
            && self.validity2 == other.validity2
            && self.data2 == other.data2
    }
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
            selection2: None,
            validity2: None,
            data2: ArrayData2::UntypedNull(UntypedNullStorage(capacity)),
            next: Some(ArrayNextInner {
                validity,
                data: ArrayData::owned(buffer),
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
        if let Some(next) = &self.next {
            return next.data.primary_capacity();
        }

        // TODO: Remove, just using to not break things completely yet.
        match self.selection2.as_ref().map(|v| v.as_ref()) {
            Some(v) => v.num_rows(),
            None => self.data2.len(),
        }
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    pub fn physical_type(&self) -> PhysicalType {
        self.datatype.physical_type().unwrap()
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
        let next = self.next_mut();

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

    /// Copy rows from self to another array.
    ///
    /// `mapping` provides a mapping of source indices to destination indices in
    /// (source, dest) pairs.
    pub fn copy_rows(
        &self,
        mapping: impl stdutil::iter::IntoExactSizeIterator<Item = (usize, usize)>,
        dest: &mut Self,
    ) -> Result<()> {
        match self.datatype.physical_type()? {
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

            _ => not_implemented!("get value for scalar type"),
        }
    }

    /// Set a scalar value at a given index.
    pub fn set_value(&mut self, idx: usize, val: &ScalarValue) -> Result<()> {
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
            selection2: Some(selection.into()),
            validity2: Some(validity.into()),
            data2: data.into(),
            next: None,
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
            selection2: Some(selection.into()),
            validity2: Some(validity.into()),
            data2: data,
            next: None,
        })
    }

    pub fn new_with_array_data(datatype: DataType, data: impl Into<ArrayData2>) -> Self {
        Array {
            datatype,
            selection2: None,
            validity2: None,
            data2: data.into(),
            next: None,
        }
    }

    pub fn new_with_validity_and_array_data(
        datatype: DataType,
        validity: impl Into<PhysicalValidity>,
        data: impl Into<ArrayData2>,
    ) -> Self {
        Array {
            datatype,
            selection2: None,
            validity2: Some(validity.into()),
            data2: data.into(),
            next: None,
        }
    }

    pub fn has_selection(&self) -> bool {
        self.selection2.is_some()
    }

    pub fn selection_vector(&self) -> Option<&SelectionVector> {
        self.selection2.as_ref().map(|v| v.as_ref())
    }

    /// Sets the validity for a value at a given physical index.
    pub fn set_physical_validity(&mut self, idx: usize, valid: bool) {
        match &mut self.validity2 {
            Some(validity) => {
                let validity = validity.get_mut();
                validity.set_unchecked(idx, valid);
            }
            None => {
                // Initialize validity.
                let len = self.data2.len();
                let mut validity = Bitmap::new_with_all_true(len);
                validity.set_unchecked(idx, valid);

                self.validity2 = Some(validity.into())
            }
        }
    }

    // TODO: Validating variant too.
    pub fn put_selection(&mut self, selection: impl Into<LogicalSelection>) {
        self.selection2 = Some(selection.into())
    }

    pub fn make_shared(&mut self) {
        if let Some(validity) = &mut self.validity2 {
            validity.make_shared();
        }
        if let Some(selection) = &mut self.selection2 {
            selection.make_shared()
        }
    }

    /// Updates this array's selection vector.
    ///
    /// Takes into account any existing selection. This allows for repeated
    /// selection (filtering) against the same array.
    // TODO: Add test for selecting on logically empty array.
    #[deprecated]
    pub fn select_mut2(&mut self, selection: impl Into<LogicalSelection>) {
        let selection = selection.into();
        match self.selection_vector() {
            Some(existing) => {
                let selection = existing.select(selection.as_ref());
                self.selection2 = Some(selection.into())
            }
            None => {
                // No existing selection, we can just use the provided vector
                // directly.
                self.selection2 = Some(selection)
            }
        }
    }

    pub fn logical_len(&self) -> usize {
        match self.selection_vector() {
            Some(v) => v.num_rows(),
            None => self.data2.len(),
        }
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity2.as_ref().map(|v| v.as_ref())
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.logical_len() {
            return None;
        }

        let idx = match self.selection_vector() {
            Some(v) => v.get_opt(idx)?,
            None => idx,
        };

        if let Some(validity) = &self.validity2 {
            return Some(validity.as_ref().value(idx));
        }

        Some(true)
    }

    /// Returns the array data.
    ///
    /// ArrayData can be cheaply cloned.
    pub fn array_data(&self) -> &ArrayData2 {
        &self.data2
    }

    pub fn into_array_data(self) -> ArrayData2 {
        self.data2
    }

    /// Gets the physical type of the array.
    pub fn physical_type2(&self) -> PhysicalType {
        match self.data2.physical_type() {
            PhysicalType::Binary => match self.datatype {
                DataType::Utf8 => PhysicalType::Utf8,
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
                .get_opt(idx)
                .ok_or_else(|| RayexecError::new(format!("Logical index {idx} out of bounds")))?,
            None => idx,
        };

        if let Some(validity) = &self.validity2 {
            if !validity.as_ref().value(idx) {
                return Ok(ScalarValue::Null);
            }
        }

        self.physical_scalar(idx)
    }

    /// Gets the scalar value at the physical index.
    ///
    /// Ignores validity and selectivitity.
    pub fn physical_scalar(&self, idx: usize) -> Result<ScalarValue> {
        Ok(match &self.datatype {
            DataType::Null => match &self.data2 {
                ArrayData2::UntypedNull(_) => ScalarValue::Null,
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Boolean => match &self.data2 {
                ArrayData2::Boolean(arr) => arr.as_ref().as_ref().value(idx).into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float16 => match &self.data2 {
                ArrayData2::Float16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float32 => match &self.data2 {
                ArrayData2::Float32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float64 => match &self.data2 {
                ArrayData2::Float64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int8 => match &self.data2 {
                ArrayData2::Int8(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int16 => match &self.data2 {
                ArrayData2::Int16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int32 => match &self.data2 {
                ArrayData2::Int32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int64 => match &self.data2 {
                ArrayData2::Int64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int128 => match &self.data2 {
                ArrayData2::Int64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt8 => match &self.data2 {
                ArrayData2::UInt8(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt16 => match &self.data2 {
                ArrayData2::UInt16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt32 => match &self.data2 {
                ArrayData2::UInt32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt64 => match &self.data2 {
                ArrayData2::UInt64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt128 => match &self.data2 {
                ArrayData2::UInt64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Decimal64(m) => match &self.data2 {
                ArrayData2::Int64(arr) => ScalarValue::Decimal64(Decimal64Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Decimal128(m) => match &self.data2 {
                ArrayData2::Int128(arr) => ScalarValue::Decimal128(Decimal128Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Date32 => match &self.data2 {
                ArrayData2::Int32(arr) => ScalarValue::Date32(arr.as_ref().as_ref()[idx]),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Date64 => match &self.data2 {
                ArrayData2::Int64(arr) => ScalarValue::Date64(arr.as_ref().as_ref()[idx]),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Timestamp(m) => match &self.data2 {
                ArrayData2::Int64(arr) => ScalarValue::Timestamp(TimestampScalar {
                    unit: m.unit,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Interval => match &self.data2 {
                ArrayData2::Interval(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Utf8 => {
                let v = match &self.data2 {
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
                let v = match &self.data2 {
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
            DataType::List(_) => match &self.data2 {
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
                UnaryExecutor::value_at2::<PhysicalAny>(self, row).map(|arr_val| arr_val.is_none())
            } // None == NULL
            ScalarValue::Boolean(v) => {
                UnaryExecutor::value_at2::<PhysicalBool>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int8(v) => {
                UnaryExecutor::value_at2::<PhysicalI8>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int16(v) => {
                UnaryExecutor::value_at2::<PhysicalI16>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int32(v) => {
                UnaryExecutor::value_at2::<PhysicalI32>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int64(v) => {
                UnaryExecutor::value_at2::<PhysicalI64>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Int128(v) => {
                UnaryExecutor::value_at2::<PhysicalI128>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt8(v) => {
                UnaryExecutor::value_at2::<PhysicalU8>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt16(v) => {
                UnaryExecutor::value_at2::<PhysicalU16>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt32(v) => {
                UnaryExecutor::value_at2::<PhysicalU32>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt64(v) => {
                UnaryExecutor::value_at2::<PhysicalU64>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::UInt128(v) => {
                UnaryExecutor::value_at2::<PhysicalU128>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Float32(v) => {
                UnaryExecutor::value_at2::<PhysicalF32>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Float64(v) => {
                UnaryExecutor::value_at2::<PhysicalF64>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Date32(v) => {
                UnaryExecutor::value_at2::<PhysicalI32>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Date64(v) => {
                UnaryExecutor::value_at2::<PhysicalI64>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                })
            }
            ScalarValue::Interval(v) => UnaryExecutor::value_at2::<PhysicalInterval>(self, row)
                .map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == *v,
                    None => false,
                }),
            ScalarValue::Utf8(v) => {
                UnaryExecutor::value_at2::<PhysicalUtf8>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == v.as_ref(),
                    None => false,
                })
            }
            ScalarValue::Binary(v) => {
                UnaryExecutor::value_at2::<PhysicalBinary>(self, row).map(|arr_val| match arr_val {
                    Some(arr_val) => arr_val == v.as_ref(),
                    None => false,
                })
            }
            ScalarValue::Timestamp(v) => {
                UnaryExecutor::value_at2::<PhysicalI64>(self, row).map(|arr_val| {
                    // Assumes time unit is the same
                    match arr_val {
                        Some(arr_val) => arr_val == v.value,
                        None => false,
                    }
                })
            }
            ScalarValue::Decimal64(v) => {
                UnaryExecutor::value_at2::<PhysicalI64>(self, row).map(|arr_val| {
                    // Assumes precision/scale are the same.
                    match arr_val {
                        Some(arr_val) => arr_val == v.value,
                        None => false,
                    }
                })
            }
            ScalarValue::Decimal128(v) => {
                UnaryExecutor::value_at2::<PhysicalI128>(self, row).map(|arr_val| {
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

        Array {
            datatype: self.datatype.clone(),
            selection2: Some(selection.into()),
            validity2: self.validity2.clone(),
            data2: self.data2.clone(),
            next: None,
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

        let mut array = Array::from_iter(new_vals);
        array.validity2 = Some(validity.into());

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
            selection2: None,
            validity2: None,
            data2: ArrayData2::Binary(BinaryData::German(Arc::new(german))),
            next: None,
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
            selection2: None,
            validity2: None,
            data2: ArrayData2::Binary(BinaryData::German(Arc::new(german))),
            next: None,
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
                    selection2: None,
                    validity2: None,
                    data2: ArrayData2::$variant(Arc::new(vals.into())),
                    next: None,
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

impl FromIterator<bool> for Array {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let vals: Bitmap = iter.into_iter().collect();
        Array {
            datatype: DataType::Boolean,
            selection2: None,
            validity2: None,
            data2: ArrayData2::Boolean(Arc::new(vals.into())),
            next: None,
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
    pub fn physical_type(&self) -> PhysicalType {
        match self {
            Self::UntypedNull(_) => PhysicalType::UntypedNull,
            Self::Boolean(_) => PhysicalType::Boolean,
            Self::Float16(_) => PhysicalType::Float16,
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
            Self::List(_) => PhysicalType::List,
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

/// Helper for copying rows.
fn copy_rows<S, B>(
    from: &Array<B>,
    mapping: impl stdutil::iter::IntoExactSizeIterator<Item = (usize, usize)>,
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
    let buffer = match datatype.physical_type()? {
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

impl<'a> AsRefStr for &'a str {}
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
            selection2: None,
            validity2: None,
            data2: ArrayData2::UntypedNull(UntypedNullStorage(len)),
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
    use crate::arrays::testutil::assert_arrays_eq;

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
}
