use std::ops::Deref;

use iterutil::exact_size::IntoExactSizeIterator;
use rayexec_bullet::scalar::ScalarValue;
use rayexec_error::{not_implemented, RayexecError, Result};

use super::buffer::addressable::MutableAddressableStorage;
use super::buffer::dictionary::DictionaryBuffer;
use super::buffer::physical_type::{PhysicalDictionary, PhysicalType};
use super::buffer::{ArrayBuffer, SecondaryBuffers};
use super::buffer_manager::{BufferManager, NopBufferManager};
use super::datatype::DataType;
use super::flat_array::FlatArrayView;
use super::validity::Validity;
use crate::arrays::buffer::physical_type::{PhysicalI32, PhysicalI8, PhysicalUtf8};
use crate::arrays::buffer::string_view::StringViewHeap;

#[derive(Debug)]
pub struct Array<B: BufferManager = NopBufferManager> {
    /// Data type of the array.
    pub(crate) datatype: DataType,
    /// Array validity mask.
    pub(crate) validity: Validity,
    /// Buffer containing the underlying array data.
    pub(crate) data: ArrayData<B>,
}

impl<B> Array<B>
where
    B: BufferManager,
{
    /// Create a new array with the given capacity.
    ///
    /// This will take care of initalizing the primary and secondary data
    /// buffers depending on the type.
    pub fn new(manager: &B, datatype: DataType, cap: usize) -> Result<Self> {
        let data = match datatype.physical_type() {
            PhysicalType::Int8 => ArrayBuffer::with_capacity::<PhysicalI8>(manager, cap)?,
            PhysicalType::Int32 => ArrayBuffer::with_capacity::<PhysicalI32>(manager, cap)?,
            PhysicalType::Utf8 => {
                let heap = StringViewHeap::new();
                ArrayBuffer::with_len_and_child_buffer::<PhysicalUtf8>(manager, cap, heap)?
            }
            other => not_implemented!("init array buffer: {other}"),
        };

        let validity = Validity::new_all_valid(cap);

        Ok(Array {
            datatype,
            validity,
            data: ArrayData::owned(data),
        })
    }

    pub fn new_with_buffer(datatype: DataType, buffer: ArrayBuffer<B>) -> Self {
        let validity = Validity::new_all_valid(buffer.capacity());
        Array {
            datatype,
            validity,
            data: ArrayData::owned(buffer),
        }
    }

    pub fn new_with_validity(
        datatype: DataType,
        buffer: ArrayBuffer<B>,
        validity: Validity,
    ) -> Result<Self> {
        if validity.len() != buffer.capacity() {
            return Err(
                RayexecError::new("Validty length does not match buffer length")
                    .with_field("validity_len", validity.len())
                    .with_field("buffer_len", buffer.capacity()),
            );
        }

        Ok(Array {
            datatype,
            validity,
            data: ArrayData::owned(buffer),
        })
    }

    pub fn make_managed_from(&mut self, manager: &B, other: &mut Self) -> Result<()> {
        if self.datatype != other.datatype {
            return Err(
                RayexecError::new("Attempted to make array managed with data from other array with different data types")
                    .with_field("own_datatype", self.datatype.clone())
                    .with_field("other_datatype", other.datatype.clone()));
        }

        let managed = other.data.make_managed(manager)?;
        self.data = ArrayData::managed(managed);
        self.validity = other.validity.clone();

        Ok(())
    }

    pub fn flat_view(&self) -> Result<FlatArrayView<'_, B>> {
        FlatArrayView::from_array(self)
    }

    pub fn validity(&self) -> &Validity {
        &self.validity
    }

    pub fn data(&self) -> &ArrayData<B> {
        &self.data
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Selects indice from the array.
    ///
    /// This will convert the underlying array buffer into a dictionary buffer.
    pub fn select(
        &mut self,
        manager: &B,
        selection: impl IntoExactSizeIterator<Item = usize>,
    ) -> Result<()> {
        if self.is_dictionary() {
            // Already dictionary, select the selection.
            let sel = selection.into_iter();
            let mut new_buf = ArrayBuffer::with_capacity::<PhysicalDictionary>(manager, sel.len())?;

            let old_sel = self.data.try_as_slice::<PhysicalDictionary>()?;
            let new_sel = new_buf.try_as_slice_mut::<PhysicalDictionary>()?;

            for (sel_idx, sel_buf) in sel.zip(new_sel) {
                let idx = old_sel[sel_idx];
                *sel_buf = idx;
            }

            // Now swap the secondary buffers, the dictionary buffer will now be
            // on `new_buf`.
            std::mem::swap(
                self.data.try_as_mut()?.secondary_buffers_mut(), // TODO: Should just clone the pointer if managed.
                new_buf.secondary_buffers_mut(),
            );

            // And set the new buf, old buf gets dropped.
            self.data = ArrayData::owned(new_buf);

            return Ok(());
        }

        let sel = selection.into_iter();
        let mut new_buf = ArrayBuffer::with_capacity::<PhysicalDictionary>(manager, sel.len())?;

        let new_buf_slice = new_buf.try_as_slice_mut::<PhysicalDictionary>()?;

        // Set all selection indices in the new array buffer.
        for (sel_idx, sel_buf) in sel.zip(new_buf_slice) {
            *sel_buf = sel_idx
        }

        // TODO: Probably verify selection all in bounds.

        // Now replace the original buffer, and put the original buffer in the
        // secondary buffer.
        let orig_validity = std::mem::replace(
            &mut self.validity,
            Validity::new_all_valid(new_buf.capacity()),
        );
        let orig_buffer = std::mem::replace(&mut self.data, ArrayData::owned(new_buf));
        // TODO: Should just clone the pointer if managed.
        *self.data.try_as_mut()?.secondary_buffers_mut() =
            SecondaryBuffers::Dictionary(DictionaryBuffer::new(orig_buffer, orig_validity));

        Ok(())
    }

    /// If this array is a dictionary array.
    pub fn is_dictionary(&self) -> bool {
        self.data.physical_type() == PhysicalType::Dictionary
    }

    pub fn get_dictionary_buffer(&self) -> Option<&DictionaryBuffer<B>> {
        match self.data.secondary_buffers() {
            SecondaryBuffers::Dictionary(buf) => Some(buf),
            _ => None,
        }
    }

    /// Sets a scalar value at a given index.
    pub fn set_value(&mut self, val: &ScalarValue, idx: usize) -> Result<()> {
        if idx >= self.capacity() {
            return Err(RayexecError::new("Index out of bounds")
                .with_field("idx", idx)
                .with_field("capacity", self.capacity()));
        }

        let data = self.data.try_as_mut()?;

        match val {
            ScalarValue::Null => {
                self.validity.set_invalid(idx);
                return Ok(());
            }
            ScalarValue::Int8(v) => {
                data.try_as_slice_mut::<PhysicalI8>()?[idx] = *v;
            }
            ScalarValue::Int32(v) => {
                data.try_as_slice_mut::<PhysicalI32>()?[idx] = *v;
            }
            ScalarValue::Utf8(v) => {
                let mut string_buf = data.try_as_string_view_storage_mut()?;
                string_buf.put(idx, v.as_ref());
            }

            other => not_implemented!("set scalar: {other:?}"),
        }

        if !self.validity.is_valid(idx) {
            self.validity.set_valid(idx);
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ArrayData<B: BufferManager = NopBufferManager> {
    inner: ArrayDataInner<B>,
}

#[derive(Debug)]
enum ArrayDataInner<B: BufferManager> {
    Managed(B::CowPtr<ArrayBuffer<B>>),
    Owned(ArrayBuffer<B>),
    Uninit,
}

impl<B> ArrayData<B>
where
    B: BufferManager,
{
    pub fn owned(buffer: ArrayBuffer<B>) -> Self {
        ArrayData {
            inner: ArrayDataInner::Owned(buffer),
        }
    }

    pub fn managed(buffer: B::CowPtr<ArrayBuffer<B>>) -> Self {
        ArrayData {
            inner: ArrayDataInner::Managed(buffer),
        }
    }

    pub fn is_managed(&self) -> bool {
        matches!(self.inner, ArrayDataInner::Managed(_))
    }

    pub fn is_owned(&self) -> bool {
        matches!(self.inner, ArrayDataInner::Owned(_))
    }

    /// Try to make the array managed by the buffer manager.
    ///
    /// Does nothing if the array is already managed.
    ///
    /// Returns an error if the array cannot be made to be managed. The array is
    /// still valid (and remains in the 'owned' state).
    ///
    /// A cloned pointer to the newly managed array will be returned.
    pub fn make_managed(&mut self, manager: &B) -> Result<B::CowPtr<ArrayBuffer<B>>> {
        match &mut self.inner {
            ArrayDataInner::Managed(m) => Ok(m.clone()), // Already managed.
            ArrayDataInner::Owned(_) => {
                let orig = std::mem::replace(&mut self.inner, ArrayDataInner::Uninit);
                let array = match orig {
                    ArrayDataInner::Owned(array) => array,
                    _ => unreachable!("variant already checked"),
                };

                match manager.make_cow(array) {
                    Ok(managed) => {
                        self.inner = ArrayDataInner::Managed(managed);
                        match &self.inner {
                            ArrayDataInner::Managed(m) => Ok(m.clone()),
                            _ => unreachable!("variant just set"),
                        }
                    }
                    Err(orig) => {
                        // Manager rejected it, put it back as owned and return
                        // an error.
                        self.inner = ArrayDataInner::Owned(orig);
                        Err(RayexecError::new("Failed to make batch array managed"))
                    }
                }
            }
            ArrayDataInner::Uninit => panic!("array in uninit state"),
        }
    }

    pub fn try_as_mut(&mut self) -> Result<&mut ArrayBuffer<B>> {
        match &mut self.inner {
            ArrayDataInner::Managed(_) => Err(RayexecError::new(
                "Mut references from managed arrays not yet supported",
            )),
            ArrayDataInner::Owned(array) => Ok(array),
            ArrayDataInner::Uninit => panic!("array in uninit state"),
        }
    }
}
impl<B> AsRef<ArrayBuffer<B>> for ArrayData<B>
where
    B: BufferManager,
{
    fn as_ref(&self) -> &ArrayBuffer<B> {
        match &self.inner {
            ArrayDataInner::Managed(m) => m.as_ref(),
            ArrayDataInner::Owned(array) => array,
            ArrayDataInner::Uninit => panic!("array in uninit state"),
        }
    }
}

impl<B> Deref for ArrayData<B>
where
    B: BufferManager,
{
    type Target = ArrayBuffer<B>;

    fn deref(&self) -> &Self::Target {
        ArrayData::as_ref(&self)
    }
}
