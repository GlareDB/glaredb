use std::any::Any;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use glaredb_error::{DbError, Result, not_implemented};
use half::f16;

use super::execution_format::{
    ExecutionFormat,
    ExecutionFormatMut,
    SelectionFormat,
    SelectionFormatMut,
};
use super::physical_type::{
    BinaryViewAddressable,
    BinaryViewAddressableMut,
    StringViewAddressable,
    StringViewAddressableMut,
};
use super::selection::Selection;
use super::validity::Validity;
use crate::arrays::array::physical_type::{
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
    PhysicalUntypedNull,
    PhysicalUtf8,
    ScalarStorage,
    UntypedNull,
};
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::interval::Interval;
use crate::arrays::string::{MAX_INLINE_LEN, StringView};
use crate::buffer::buffer_manager::AsRawBufferManager;
use crate::buffer::db_vec::DbVec;
use crate::buffer::raw::RawBuffer;
use crate::buffer::typed::TypedBuffer;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayBufferType {
    Empty,
    Scalar,
    String,
    Constant,
    Dictionary,
    List,
    Struct,
}

pub trait ArrayBuffer: Sized + Sync + Send + 'static {
    const BUFFER_TYPE: ArrayBufferType;

    /// Return the logical length of the buffer.
    fn logical_len(&self) -> usize;
}

pub trait ArrayBufferDowncast: ArrayBuffer {
    /// Downcast from an `AnyArrayBuffer` to a reference to the concrete array
    /// buffer type.
    ///
    /// Errors if the provided buffer isn't actually this type.
    fn downcast_ref(buffer: &AnyArrayBuffer) -> Result<&Self>;

    /// Downcast from an `AnyArrayBuffer` to a mutable reference to the concrete
    /// array buffer type.
    ///
    /// Errors if the provided buffer isn't actually this type.
    fn downcast_mut(buffer: &mut AnyArrayBuffer) -> Result<&mut Self>;

    fn downcast_execution_format(buffer: &AnyArrayBuffer) -> Result<ExecutionFormat<'_, Self>>;

    fn downcast_execution_format_mut(
        buffer: &mut AnyArrayBuffer,
    ) -> Result<ExecutionFormatMut<'_, Self>>;
}

impl<A> ArrayBufferDowncast for A
where
    A: ArrayBuffer,
{
    fn downcast_ref(buffer: &AnyArrayBuffer) -> Result<&Self> {
        buffer
            .buffer
            .as_ref()
            .downcast_ref::<A>()
            .ok_or_else(|| DbError::new("failed to downcast array buffer"))
    }

    fn downcast_mut(buffer: &mut AnyArrayBuffer) -> Result<&mut Self> {
        let buffer = buffer
            .buffer
            .as_mut()
            .ok_or_else(|| DbError::new("Buffer is shared, cannot get mutable reference"))?;
        buffer
            .downcast_mut::<A>()
            .ok_or_else(|| DbError::new("failed to downcast array buffer (mut)"))
    }

    fn downcast_execution_format(buffer: &AnyArrayBuffer) -> Result<ExecutionFormat<'_, Self>> {
        match buffer.buffer_type {
            ArrayBufferType::Constant => {
                let constant = ConstantBuffer::downcast_ref(buffer)?;
                let selection = Selection::constant(constant.len, constant.row_idx);
                let child_buffer = Self::downcast_ref(&constant.buffer)?;

                Ok(ExecutionFormat::Selection(SelectionFormat {
                    selection,
                    buffer: child_buffer,
                }))
            }
            ArrayBufferType::Dictionary => {
                let dictionary = DictionaryBuffer::downcast_ref(buffer)?;
                let selection = Selection::slice(unsafe { dictionary.selection.as_slice() });
                let child_buffer = Self::downcast_ref(&dictionary.buffer)?;

                Ok(ExecutionFormat::Selection(SelectionFormat {
                    selection,
                    buffer: child_buffer,
                }))
            }
            _ => {
                // No transformation needed for execution. Use this buffer as-is.
                // TODO: Could be cool if we could do something for lists.
                let buffer = Self::downcast_ref(buffer)?;
                Ok(ExecutionFormat::Flat(buffer))
            }
        }
    }

    fn downcast_execution_format_mut(
        buffer: &mut AnyArrayBuffer,
    ) -> Result<ExecutionFormatMut<'_, Self>> {
        match buffer.buffer_type {
            ArrayBufferType::Constant => {
                let constant = ConstantBuffer::downcast_mut(buffer)?;
                let selection = Selection::constant(constant.len, constant.row_idx);
                let child_buffer = Self::downcast_mut(&mut constant.buffer)?;

                Ok(ExecutionFormatMut::Selection(SelectionFormatMut {
                    selection,
                    buffer: child_buffer,
                }))
            }
            ArrayBufferType::Dictionary => {
                let dictionary = DictionaryBuffer::downcast_mut(buffer)?;
                let selection = Selection::slice(unsafe { dictionary.selection.as_slice() });
                let child_buffer = Self::downcast_mut(&mut dictionary.buffer)?;

                Ok(ExecutionFormatMut::Selection(SelectionFormatMut {
                    selection,
                    buffer: child_buffer,
                }))
            }
            _ => {
                // No transformation needed for execution. Use this buffer as-is.
                // TODO: Could be cool if we could do something for lists.
                let buffer = Self::downcast_mut(buffer)?;
                Ok(ExecutionFormatMut::Flat(buffer))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AnyArrayBufferVTable {
    logical_len_fn: fn(&(dyn Any + Sync + Send)) -> usize,
}

trait ArrayBufferVTable: ArrayBuffer {
    const VTABLE: &'static AnyArrayBufferVTable;
}

impl<A> ArrayBufferVTable for A
where
    A: ArrayBuffer,
{
    const VTABLE: &'static AnyArrayBufferVTable = &AnyArrayBufferVTable {
        logical_len_fn: |buffer| {
            let buffer = buffer.downcast_ref::<Self>().unwrap();
            buffer.logical_len()
        },
    };
}

#[derive(Debug)]
pub struct AnyArrayBuffer {
    pub(crate) buffer: OwnedOrShared<dyn Any + Sync + Send>,
    pub(crate) vtable: &'static AnyArrayBufferVTable,
    pub(crate) buffer_type: ArrayBufferType,
}

impl AnyArrayBuffer {
    pub(crate) fn new_for_datatype(
        manager: &impl AsRawBufferManager,
        datatype: &DataType,
        capacity: usize,
    ) -> Result<Self> {
        match datatype.physical_type() {
            PhysicalType::UntypedNull => Ok(Self::new_unique(
                ScalarBuffer::<UntypedNull>::try_new(manager, capacity)?,
            )),
            PhysicalType::Boolean => Ok(Self::new_unique(ScalarBuffer::<bool>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Int8 => Ok(Self::new_unique(ScalarBuffer::<i8>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Int16 => Ok(Self::new_unique(ScalarBuffer::<i16>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Int32 => Ok(Self::new_unique(ScalarBuffer::<i32>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Int64 => Ok(Self::new_unique(ScalarBuffer::<i64>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Int128 => Ok(Self::new_unique(ScalarBuffer::<i128>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::UInt8 => Ok(Self::new_unique(ScalarBuffer::<u8>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::UInt16 => Ok(Self::new_unique(ScalarBuffer::<u16>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::UInt32 => Ok(Self::new_unique(ScalarBuffer::<u32>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::UInt64 => Ok(Self::new_unique(ScalarBuffer::<u64>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::UInt128 => Ok(Self::new_unique(ScalarBuffer::<u128>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Float16 => Ok(Self::new_unique(ScalarBuffer::<f16>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Float32 => Ok(Self::new_unique(ScalarBuffer::<f32>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Float64 => Ok(Self::new_unique(ScalarBuffer::<f64>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Interval => Ok(Self::new_unique(ScalarBuffer::<Interval>::try_new(
                manager, capacity,
            )?)),
            PhysicalType::Binary => Ok(Self::new_unique(StringBuffer::try_new(manager, capacity)?)),
            PhysicalType::Utf8 => Ok(Self::new_unique(StringBuffer::try_new(manager, capacity)?)),
            PhysicalType::List => Ok(Self::new_unique(ListBuffer::try_new(manager, capacity)?)),
            PhysicalType::Struct => Ok(Self::new_unique(StructBuffer::try_new(manager, capacity)?)),
        }
    }

    pub(crate) fn new_unique<A>(array: A) -> Self
    where
        A: ArrayBuffer,
    {
        let boxed: Box<dyn Any + Sync + Send> = Box::new(array);
        let buffer = OwnedOrShared::new_unique(boxed);

        AnyArrayBuffer {
            buffer,
            vtable: A::VTABLE,
            buffer_type: A::BUFFER_TYPE,
        }
    }

    /// Make the underlying buffer shared.
    ///
    /// Does nothing if the buffer is already shared.
    ///
    /// This will indiscriminately make the top-level buffer shared. More
    /// selective sharing should happen in `Array`.
    pub(crate) fn make_shared(&mut self) {
        self.buffer.make_shared()
    }

    /// Makes this buffer shared, and clones it.
    pub(crate) fn make_shared_and_clone(&mut self) -> Self {
        self.buffer.make_shared();
        let shared = match &self.buffer.0 {
            OwnedOrSharedPtr::Shared(shared) => {
                OwnedOrShared(OwnedOrSharedPtr::Shared(shared.clone()))
            }
            _ => unreachable!(),
        };

        AnyArrayBuffer {
            buffer: shared,
            vtable: self.vtable,
            buffer_type: self.buffer_type,
        }
    }

    pub(crate) fn try_clone_shared(&self) -> Result<Self> {
        let shared = match &self.buffer.0 {
            OwnedOrSharedPtr::Shared(shared) => {
                OwnedOrShared(OwnedOrSharedPtr::Shared(shared.clone()))
            }
            _ => return Err(DbError::new("Array buffer not shared")),
        };

        Ok(AnyArrayBuffer {
            buffer: shared,
            vtable: self.vtable,
            buffer_type: self.buffer_type,
        })
    }

    pub(crate) fn logical_len(&self) -> usize {
        (self.vtable.logical_len_fn)(self.buffer.as_ref())
    }
}

/// A pointer to T that is either uniquely owned (Box) or shared (Arc).
#[derive(Debug)]
pub struct OwnedOrShared<T: ?Sized>(OwnedOrSharedPtr<T>);

#[derive(Debug)]
enum OwnedOrSharedPtr<T: ?Sized> {
    /// Unique ownership, mutable access is possible.
    Unique(Box<T>),
    /// Shared ownership.
    Shared(Arc<T>),
    /// Unreachable intermediated state when switching from unique to shared.
    Uninit,
}

impl<T> OwnedOrShared<T>
where
    T: ?Sized,
{
    /// Create a new uniquely‐owned value.
    pub fn new_unique(value: impl Into<Box<T>>) -> Self {
        OwnedOrShared(OwnedOrSharedPtr::Unique(value.into()))
    }

    /// Make this reference shared.
    pub fn make_shared(&mut self) {
        match self.0 {
            OwnedOrSharedPtr::Shared(_) => (), // Nothing to do.
            OwnedOrSharedPtr::Unique(_) => {
                // Need to swap out and wrap in an arc.
                let orig = std::mem::replace(&mut self.0, OwnedOrSharedPtr::Uninit);
                let owned = match orig {
                    OwnedOrSharedPtr::Unique(owned) => owned,
                    _ => unreachable!(),
                };

                let shared = Arc::from(owned);
                self.0 = OwnedOrSharedPtr::Shared(shared);
            }
            OwnedOrSharedPtr::Uninit => unreachable!(),
        }
    }

    /// Get an reference to the inner T.
    pub fn as_ref(&self) -> &T {
        match &self.0 {
            OwnedOrSharedPtr::Unique(v) => v.as_ref(),
            OwnedOrSharedPtr::Shared(v) => v.as_ref(),
            OwnedOrSharedPtr::Uninit => unreachable!(),
        }
    }

    /// If this is still unique, get a mutable reference to it or return None.
    pub fn as_mut(&mut self) -> Option<&mut T> {
        match &mut self.0 {
            OwnedOrSharedPtr::Unique(b) => Some(&mut *b),
            OwnedOrSharedPtr::Shared(_) => None,
            OwnedOrSharedPtr::Uninit => unreachable!(),
        }
    }
}

/// A buffer that doesn't hold anything.
#[derive(Debug)]
pub struct EmptyBuffer;

impl ArrayBuffer for EmptyBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::Empty;

    fn logical_len(&self) -> usize {
        0
    }
}

#[derive(Debug)]
pub struct ScalarBuffer<T: Default + Copy + 'static> {
    pub(crate) buffer: DbVec<T>,
}

impl<T> ScalarBuffer<T>
where
    T: Default + Copy + 'static,
{
    pub fn try_new(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        Ok(ScalarBuffer {
            buffer: DbVec::new_uninit(manager, capacity)?,
        })
    }
}

impl<T> ArrayBuffer for ScalarBuffer<T>
where
    T: Default + Copy + Sync + Send + 'static,
{
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::Scalar;

    fn logical_len(&self) -> usize {
        self.buffer.len()
    }
}

#[derive(Debug)]
pub struct StringBuffer {
    pub(crate) metadata: DbVec<StringView>,
    pub(crate) buffer: StringViewBuffer,
}

impl StringBuffer {
    pub fn try_new(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        let metadata = DbVec::new_uninit(manager, capacity)?;
        let buffer = StringViewBuffer::with_capacity(manager, 0)?;

        Ok(StringBuffer { metadata, buffer })
    }
}

impl ArrayBuffer for StringBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::String;

    fn logical_len(&self) -> usize {
        self.metadata.len()
    }
}

#[derive(Debug)]
pub struct ListBuffer {}

impl ListBuffer {
    pub fn try_new(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        unimplemented!()
    }
}

impl ArrayBuffer for ListBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::List;

    fn logical_len(&self) -> usize {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct StructBuffer {}

impl StructBuffer {
    pub fn try_new(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        unimplemented!()
    }
}

impl ArrayBuffer for StructBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::Struct;

    fn logical_len(&self) -> usize {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct DictionaryBuffer {
    // TODO: Should this be shareable?
    pub(crate) selection: DbVec<usize>,
    pub(crate) buffer: AnyArrayBuffer,
}

impl DictionaryBuffer {}

impl ArrayBuffer for DictionaryBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::Dictionary;

    fn logical_len(&self) -> usize {
        self.selection.len()
    }
}

#[derive(Debug)]
pub struct ConstantBuffer {
    /// Row index we're referencing in the array buffer.
    pub(crate) row_idx: usize,
    /// Logical length of the buffer.
    pub(crate) len: usize,
    /// The array buffer.
    ///
    /// May either be owned, or shared with another array.
    pub(crate) buffer: AnyArrayBuffer,
}

impl ArrayBuffer for ConstantBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::Constant;

    fn logical_len(&self) -> usize {
        self.len
    }
}

#[derive(Debug)]
pub struct StringViewBuffer {
    bytes_filled: usize,
    buffer: DbVec<u8>,
}

impl StringViewBuffer {
    pub fn with_capacity(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        let buffer = DbVec::new_uninit(manager, capacity)?;
        Ok(StringViewBuffer {
            bytes_filled: 0,
            buffer,
        })
    }

    pub fn clear(&mut self) {
        self.bytes_filled = 0;
    }

    pub fn get<'a>(&'a self, view: &'a StringView) -> &'a [u8] {
        if view.is_inline() {
            let inline = view.as_inline();
            &inline.inline[0..inline.len as usize]
        } else {
            let reference = view.as_reference();
            debug_assert_eq!(0, reference.buffer_idx);
            let offset = reference.offset as usize;
            let len = reference.len as usize;
            unsafe { &self.buffer.as_slice()[offset..(offset + len)] }
        }
    }

    pub fn get_mut<'a>(&'a mut self, view: &'a mut StringView) -> &'a mut [u8] {
        if view.is_inline() {
            let inline = view.as_inline_mut();
            &mut inline.inline[0..inline.len as usize]
        } else {
            let reference = view.as_reference();
            debug_assert_eq!(0, reference.buffer_idx);
            let offset = reference.offset as usize;
            let len = reference.len as usize;
            unsafe { &mut self.buffer.as_slice_mut()[offset..(offset + len)] }
        }
    }

    /// Push bytes as a new row value.
    pub fn push_bytes_as_row(&mut self, value: &[u8]) -> Result<StringView> {
        if value.len() <= MAX_INLINE_LEN {
            Ok(StringView::new_inline(value))
        } else {
            let remaining = self.buffer.len() - self.bytes_filled;
            if remaining < value.len() {
                let additional = value.len() - remaining;
                let reserve_amount = Self::compute_amount_to_reserve(
                    self.buffer.len(),
                    self.bytes_filled,
                    additional,
                );
                self.buffer.resize(self.buffer.len() + reserve_amount)?;
            }

            let offset = self.bytes_filled;
            self.bytes_filled += value.len();

            // Copy entire value to buffer.
            let buf = unsafe { &mut self.buffer.as_slice_mut()[offset..(offset + value.len())] };
            buf.copy_from_slice(value);

            Ok(StringView::new_reference(value, 0, offset as i32))
        }
    }

    /// Compute how much we need to reserve for a buffer for it to fit `additional`
    /// number of bytes.
    const fn compute_amount_to_reserve(
        curr_cap: usize,
        curr_filled: usize,
        additional: usize,
    ) -> usize {
        let mut new_size = if curr_cap == 0 { 16 } else { curr_cap };
        loop {
            if new_size - curr_filled >= additional {
                // This is enough to fit `additional` bytes.
                break;
            }
            // Otherwise, double the size.
            new_size *= 2;
        }
        new_size
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata {
    pub offset: i32,
    pub len: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::DefaultBufferManager;

    #[test]
    fn downcast_basic() {
        // Sanity check the downcast stuff.
        let buffer =
            AnyArrayBuffer::new_for_datatype(&DefaultBufferManager, &DataType::Int32, 4).unwrap();
        ScalarBuffer::<i32>::downcast_ref(&buffer).unwrap();
    }

    #[test]
    fn string_view_buffer_push_inlined() {
        let mut buffer = StringViewBuffer::with_capacity(&DefaultBufferManager, 0).unwrap();
        let m = buffer.push_bytes_as_row(&[0, 1, 2, 3]).unwrap();
        assert!(m.is_inline());

        let got = buffer.get(&m);
        assert_eq!(&[0, 1, 2, 3], got);
    }

    #[test]
    fn string_view_buffer_push_referenced() {
        let mut buffer = StringViewBuffer::with_capacity(&DefaultBufferManager, 0).unwrap();
        let m1 = buffer.push_bytes_as_row(&vec![4; 32]).unwrap();
        assert!(!m1.is_inline());

        let got = buffer.get(&m1);
        assert_eq!(&vec![4; 32], got);

        let m2 = buffer.push_bytes_as_row(&vec![5; 32]).unwrap();
        assert!(!m2.is_inline());

        let got = buffer.get(&m2);
        assert_eq!(&vec![5; 32], got);

        // Ensure first wasn't overwritten.
        let got = buffer.get(&m1);
        assert_eq!(&vec![4; 32], got);
    }

    #[test]
    fn compute_reserve_size() {
        // Current cap sufficient.
        let amount = StringViewBuffer::compute_amount_to_reserve(16, 0, 1);
        assert_eq!(16, amount);

        // Current cap sufficient, including filled bytes.
        let amount = StringViewBuffer::compute_amount_to_reserve(16, 15, 1);
        assert_eq!(16, amount);

        // Need to double.
        let amount = StringViewBuffer::compute_amount_to_reserve(16, 0, 17);
        assert_eq!(32, amount);

        // Need to double, taking into account existing filled bytes.
        let amount = StringViewBuffer::compute_amount_to_reserve(16, 15, 2);
        assert_eq!(32, amount);

        // Need to double more than once.
        let amount = StringViewBuffer::compute_amount_to_reserve(16, 0, 33);
        assert_eq!(64, amount);

        // Need to double more than once, taking into account filled bytes.
        let amount = StringViewBuffer::compute_amount_to_reserve(16, 15, 18);
        assert_eq!(64, amount);
    }
}
