use std::any::Any;
use std::sync::Arc;

use glaredb_error::{DbError, Result, not_implemented};
use half::f16;

use super::execution_format::{
    ExecutionFormat,
    ExecutionFormatMut,
    SelectionFormat,
    SelectionFormatMut,
};
use super::selection::Selection;
use super::validity::Validity;
use crate::arrays::array::physical_type::{PhysicalType, UntypedNull};
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::interval::Interval;
use crate::arrays::string::{MAX_INLINE_LEN, StringView};
use crate::buffer::buffer_manager::AsRawBufferManager;
use crate::buffer::db_vec::DbVec;

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

    /// Resize this buffer.
    ///
    /// `len` represents the final logical length.
    fn resize(&mut self, len: usize) -> Result<()>;
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
                let selection = Selection::slice(dictionary.selection.as_slice());
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
                let selection = Selection::slice(dictionary.selection.as_slice());
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
    resize_fn: fn(&mut (dyn Any + Sync + Send), len: usize) -> Result<()>,
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
        resize_fn: |buffer, len| {
            let buffer = buffer.downcast_mut::<Self>().unwrap();
            buffer.resize(len)
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
            PhysicalType::List => Ok(Self::new_unique(ListBuffer::try_new(
                manager, datatype, capacity,
            )?)),
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

    pub(crate) fn resize(&mut self, len: usize) -> Result<()> {
        let buffer = self
            .buffer
            .as_mut()
            .ok_or_else(|| DbError::new("Cannot resized shared array buffer"))?;
        (self.vtable.resize_fn)(buffer, len)
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

    /// If this is still unique, get a mutable reference to it or return None.
    pub fn as_mut(&mut self) -> Option<&mut T> {
        match &mut self.0 {
            OwnedOrSharedPtr::Unique(b) => Some(&mut *b),
            OwnedOrSharedPtr::Shared(_) => None,
            OwnedOrSharedPtr::Uninit => unreachable!(),
        }
    }
}

impl<T: ?Sized> AsRef<T> for OwnedOrShared<T> {
    fn as_ref(&self) -> &T {
        match &self.0 {
            OwnedOrSharedPtr::Unique(v) => v.as_ref(),
            OwnedOrSharedPtr::Shared(v) => v.as_ref(),
            OwnedOrSharedPtr::Uninit => unreachable!(),
        }
    }
}

/// A buffer that doesn't hold anything.
///
/// Mostly for mem::swap.
#[derive(Debug)]
pub struct EmptyBuffer;

impl ArrayBuffer for EmptyBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::Empty;

    fn logical_len(&self) -> usize {
        0
    }

    fn resize(&mut self, _len: usize) -> Result<()> {
        Err(DbError::new("Cannot resize Empty buffer"))
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
            buffer: unsafe { DbVec::new_uninit(manager, capacity)? },
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

    fn resize(&mut self, len: usize) -> Result<()> {
        unsafe { self.buffer.resize_uninit(len) }
    }
}

#[derive(Debug)]
pub struct StringBuffer {
    pub(crate) metadata: DbVec<StringView>,
    pub(crate) buffer: StringViewBuffer,
}

impl StringBuffer {
    pub fn try_new(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        let metadata = unsafe { DbVec::new_uninit(manager, capacity)? };
        let buffer = StringViewBuffer::with_capacity(manager, 0)?;

        Ok(StringBuffer { metadata, buffer })
    }
}

impl ArrayBuffer for StringBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::String;

    fn logical_len(&self) -> usize {
        self.metadata.len()
    }

    fn resize(&mut self, len: usize) -> Result<()> {
        unsafe { self.metadata.resize_uninit(len) }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata {
    /// Offset in the child buffer where this list element starts.
    pub offset: i32,
    /// Length of this list element.
    pub len: i32,
}

#[derive(Debug)]
pub struct ListChildBuffer {
    /// Validities for all values in the child.
    ///
    /// This is used to determine the number of values being stored in the child
    /// buffer.
    pub(crate) validity: Validity,
    pub(crate) buffer: AnyArrayBuffer,
}

#[derive(Debug)]
pub struct ListBuffer {
    pub(crate) metadata: DbVec<ListItemMetadata>,
    pub(crate) child: ListChildBuffer,
}

impl ListBuffer {
    pub fn try_new(
        manager: &impl AsRawBufferManager,
        datatype: &DataType,
        capacity: usize,
    ) -> Result<Self> {
        let list_meta = datatype.try_get_list_type_meta()?;
        let child = ListChildBuffer {
            validity: Validity::new_all_valid(0),
            buffer: AnyArrayBuffer::new_for_datatype(manager, &list_meta.datatype, 0)?,
        };

        let metadata = unsafe { DbVec::new_uninit(manager, capacity)? };

        Ok(ListBuffer { metadata, child })
    }
}

impl ArrayBuffer for ListBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::List;

    fn logical_len(&self) -> usize {
        self.metadata.len()
    }

    fn resize(&mut self, len: usize) -> Result<()> {
        unsafe { self.metadata.resize_uninit(len) }
    }
}

#[derive(Debug)]
pub struct StructBuffer {}

impl StructBuffer {
    pub fn try_new(_manager: &impl AsRawBufferManager, _capacity: usize) -> Result<Self> {
        not_implemented!("create struct buffer")
    }
}

impl ArrayBuffer for StructBuffer {
    const BUFFER_TYPE: ArrayBufferType = ArrayBufferType::Struct;

    fn logical_len(&self) -> usize {
        // TODO
        0
    }

    fn resize(&mut self, _len: usize) -> Result<()> {
        not_implemented!("resize struct buffer")
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

    fn resize(&mut self, _len: usize) -> Result<()> {
        // TODO: Why not?
        Err(DbError::new("Cannot resize dictionary buffer"))
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

    fn resize(&mut self, len: usize) -> Result<()> {
        self.len = len;
        Ok(())
    }
}

#[derive(Debug)]
pub struct StringViewBuffer {
    buffer: DbVec<u8>,
}

impl StringViewBuffer {
    pub fn with_capacity(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        let buffer = DbVec::with_capacity(manager, capacity)?;
        Ok(StringViewBuffer { buffer })
    }

    pub fn clear(&mut self) {
        // SAFETY: We're truncating, no possibility of making unitialized memory
        // accessible.
        unsafe {
            // TODO: Can we actually error?
            let _ = self.buffer.resize_uninit(0);
        }
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
            &self.buffer.as_slice()[offset..(offset + len)]
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
            &mut self.buffer.as_slice_mut()[offset..(offset + len)]
        }
    }

    /// Push bytes as a new row value.
    pub fn push_bytes_as_row(&mut self, value: &[u8]) -> Result<StringView> {
        if value.len() <= MAX_INLINE_LEN {
            Ok(StringView::new_inline(value))
        } else {
            let offset = self.buffer.len();
            self.buffer.push_slice(value)?;

            // Copy entire value to buffer.
            let buf = &mut self.buffer.as_slice_mut()[offset..(offset + value.len())];
            buf.copy_from_slice(value);

            Ok(StringView::new_reference(value, 0, offset as i32))
        }
    }
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
    fn string_view_push_clear_push() {
        let mut buffer = StringViewBuffer::with_capacity(&DefaultBufferManager, 0).unwrap();
        let _ = buffer
            .push_bytes_as_row(b"hellohellohellohellohellohello")
            .unwrap();
        buffer.clear();
        let m = buffer
            .push_bytes_as_row(b"worldworldworldworldworldworld")
            .unwrap();

        assert_eq!(b"worldworldworldworldworldworld", buffer.get(&m));
    }
}
