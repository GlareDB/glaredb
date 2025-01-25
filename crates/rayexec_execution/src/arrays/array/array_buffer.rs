use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use stdutil::convert::TryAsMut;

use super::buffer_manager::BufferManager;
use super::cache::Cached;
use super::raw::{RawBuffer, TypedRawBuffer};
use super::string_view::{
    BinaryViewAddressable,
    BinaryViewAddressableMut,
    StringViewAddressable,
    StringViewAddressableMut,
    StringViewHeap,
    StringViewMetadataUnion,
};
use super::validity::Validity;
use super::Array;
use crate::arrays::array::physical_type::{
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
    PhysicalList,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::datatype::DataType;

/// Abstraction layer for holding shared or owned array data.
///
/// Arrays typically start out with all owned data allow for mutability. If
/// another array needs to reference it (e.g. for selection), then the
/// underlying buffers will be transitioned to shared references.
#[derive(Debug)]
pub(crate) struct ArrayBuffer<B: BufferManager> {
    inner: ArrayBufferType<B>,
}

impl<B> ArrayBuffer<B>
where
    B: BufferManager,
{
    pub fn new(inner: impl Into<ArrayBufferType<B>>) -> Self {
        ArrayBuffer {
            inner: inner.into(),
        }
    }

    /// Creates a new array buffer for the given datatype.
    ///
    /// This will never produce a Constant or Dictionary buffer.
    pub fn try_new_for_datatype(manager: &B, datatype: &DataType, capacity: usize) -> Result<Self> {
        let inner = match datatype.physical_type() {
            PhysicalType::UntypedNull => {
                ScalarBuffer::try_new::<PhysicalUntypedNull>(manager, capacity)?.into()
            }
            PhysicalType::Boolean => {
                ScalarBuffer::try_new::<PhysicalBool>(manager, capacity)?.into()
            }
            PhysicalType::Int8 => ScalarBuffer::try_new::<PhysicalI8>(manager, capacity)?.into(),
            PhysicalType::Int16 => ScalarBuffer::try_new::<PhysicalI16>(manager, capacity)?.into(),
            PhysicalType::Int32 => ScalarBuffer::try_new::<PhysicalI32>(manager, capacity)?.into(),
            PhysicalType::Int64 => ScalarBuffer::try_new::<PhysicalI64>(manager, capacity)?.into(),
            PhysicalType::Int128 => {
                ScalarBuffer::try_new::<PhysicalI128>(manager, capacity)?.into()
            }
            PhysicalType::UInt8 => ScalarBuffer::try_new::<PhysicalU8>(manager, capacity)?.into(),
            PhysicalType::UInt16 => ScalarBuffer::try_new::<PhysicalU16>(manager, capacity)?.into(),
            PhysicalType::UInt32 => ScalarBuffer::try_new::<PhysicalU32>(manager, capacity)?.into(),
            PhysicalType::UInt64 => ScalarBuffer::try_new::<PhysicalU64>(manager, capacity)?.into(),
            PhysicalType::UInt128 => {
                ScalarBuffer::try_new::<PhysicalU128>(manager, capacity)?.into()
            }
            PhysicalType::Float16 => {
                ScalarBuffer::try_new::<PhysicalF16>(manager, capacity)?.into()
            }
            PhysicalType::Float32 => {
                ScalarBuffer::try_new::<PhysicalF32>(manager, capacity)?.into()
            }
            PhysicalType::Float64 => {
                ScalarBuffer::try_new::<PhysicalF64>(manager, capacity)?.into()
            }
            PhysicalType::Interval => {
                ScalarBuffer::try_new::<PhysicalInterval>(manager, capacity)?.into()
            }
            PhysicalType::Utf8 => StringBuffer::try_new::<PhysicalUtf8>(manager, capacity)?.into(),
            PhysicalType::Binary => {
                StringBuffer::try_new::<PhysicalBinary>(manager, capacity)?.into()
            }
            PhysicalType::List => {
                let inner = match datatype {
                    DataType::List(m) => m.datatype.as_ref().clone(),
                    other => {
                        return Err(RayexecError::new(format!(
                            "Expected list datatype, got {other}"
                        )))
                    }
                };
                ListBuffer::try_new(manager, inner, capacity)?.into()
            }
            _ => unimplemented!(),
        };

        Ok(ArrayBuffer { inner })
    }

    pub fn make_shared_and_clone(&mut self) -> Result<Self> {
        Ok(ArrayBuffer {
            inner: self.inner.make_shared_and_clone(),
        })
    }

    pub fn into_inner(self) -> ArrayBufferType<B> {
        self.inner
    }
}

impl<B> AsRef<ArrayBufferType<B>> for ArrayBuffer<B>
where
    B: BufferManager,
{
    fn as_ref(&self) -> &ArrayBufferType<B> {
        &self.inner
    }
}

impl<B> AsMut<ArrayBufferType<B>> for ArrayBuffer<B>
where
    B: BufferManager,
{
    fn as_mut(&mut self) -> &mut ArrayBufferType<B> {
        &mut self.inner
    }
}

impl<B> Deref for ArrayBuffer<B>
where
    B: BufferManager,
{
    type Target = ArrayBufferType<B>;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<B> DerefMut for ArrayBuffer<B>
where
    B: BufferManager,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

#[derive(Debug)]
pub(crate) enum ArrayBufferType<B: BufferManager> {
    Scalar(ScalarBuffer<B>),
    Constant(ConstantBuffer<B>),
    String(StringBuffer<B>),
    Dictionary(DictionaryBuffer<B>),
    List(ListBuffer<B>),
}

impl<B> ArrayBufferType<B>
where
    B: BufferManager,
{
    pub fn logical_len(&self) -> usize {
        match self {
            Self::Scalar(buf) => buf.logical_len(),
            Self::Constant(buf) => buf.logical_len(),
            Self::String(buf) => buf.logical_len(),
            Self::Dictionary(buf) => buf.logical_len(),
            Self::List(buf) => buf.logical_len(),
        }
    }

    fn make_shared_and_clone(&mut self) -> Self {
        match self {
            Self::Scalar(buf) => Self::Scalar(buf.make_shared_and_clone()),
            Self::Constant(buf) => Self::Constant(buf.make_shared_and_clone()),
            Self::String(buf) => Self::String(buf.make_shared_and_clone()),
            Self::Dictionary(buf) => Self::Dictionary(buf.make_shared_and_clone()),
            Self::List(buf) => Self::List(buf.make_shared_and_clone()),
        }
    }
}

impl<B: BufferManager> From<ScalarBuffer<B>> for ArrayBufferType<B> {
    fn from(value: ScalarBuffer<B>) -> Self {
        Self::Scalar(value)
    }
}

impl<B: BufferManager> From<ConstantBuffer<B>> for ArrayBufferType<B> {
    fn from(value: ConstantBuffer<B>) -> Self {
        Self::Constant(value)
    }
}

impl<B: BufferManager> From<StringBuffer<B>> for ArrayBufferType<B> {
    fn from(value: StringBuffer<B>) -> Self {
        Self::String(value)
    }
}

impl<B: BufferManager> From<DictionaryBuffer<B>> for ArrayBufferType<B> {
    fn from(value: DictionaryBuffer<B>) -> Self {
        Self::Dictionary(value)
    }
}

impl<B: BufferManager> From<ListBuffer<B>> for ArrayBufferType<B> {
    fn from(value: ListBuffer<B>) -> Self {
        Self::List(value)
    }
}

#[derive(Debug)]
pub(crate) struct ScalarBuffer<B: BufferManager> {
    pub(crate) physical_type: PhysicalType,
    pub(crate) raw: SharedOrOwned<RawBuffer<B>>,
}

impl<B> ScalarBuffer<B>
where
    B: BufferManager,
{
    pub fn try_as_slice<S>(&self) -> Result<&[S::StorageType]>
    where
        S: ScalarStorage,
        S::StorageType: Sized,
    {
        if self.physical_type != S::PHYSICAL_TYPE {
            return Err(RayexecError::new("Physical type doesn't match for cast")
                .with_field("need", self.physical_type)
                .with_field("have", S::PHYSICAL_TYPE));
        }

        let buf = unsafe { self.raw.as_slice::<S::StorageType>() };
        Ok(buf)
    }

    pub fn try_as_slice_mut<S>(&mut self) -> Result<&mut [S::StorageType]>
    where
        S: ScalarStorage,
        S::StorageType: Sized,
    {
        if self.physical_type != S::PHYSICAL_TYPE {
            return Err(RayexecError::new("Physical type doesn't match for cast")
                .with_field("need", self.physical_type)
                .with_field("have", S::PHYSICAL_TYPE));
        }

        let raw = self.raw.try_as_mut()?;
        let buf = unsafe { raw.as_slice_mut::<S::StorageType>() };

        Ok(buf)
    }

    /// Create a new scalar buffer for storing sized primitive values.
    fn try_new<S>(manager: &B, capacity: usize) -> Result<Self>
    where
        S: ScalarStorage,
        S::StorageType: Sized,
    {
        let raw = RawBuffer::try_with_capacity::<S::StorageType>(manager, capacity)?;
        Ok(ScalarBuffer {
            physical_type: S::PHYSICAL_TYPE,
            raw: SharedOrOwned::owned(raw),
        })
    }

    fn logical_len(&self) -> usize {
        self.raw.capacity
    }

    fn make_shared_and_clone(&mut self) -> Self {
        let raw = self.raw.make_shared_and_clone();

        ScalarBuffer {
            physical_type: self.physical_type,
            raw,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ConstantBuffer<B: BufferManager> {
    pub(crate) row_reference: usize,
    pub(crate) len: usize,
    pub(crate) child_buffer: SharedOrOwned<ArrayBuffer<B>>,
}

impl<B> ConstantBuffer<B>
where
    B: BufferManager,
{
    fn logical_len(&self) -> usize {
        self.len
    }

    fn make_shared_and_clone(&mut self) -> Self {
        ConstantBuffer {
            row_reference: self.row_reference,
            len: self.len,
            child_buffer: self.child_buffer.make_shared_and_clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct StringBuffer<B: BufferManager> {
    pub(crate) metadata: SharedOrOwned<TypedRawBuffer<StringViewMetadataUnion, B>>,
    pub(crate) heap: SharedOrOwned<StringViewHeap>,
}

impl<B> StringBuffer<B>
where
    B: BufferManager,
{
    pub fn try_as_string_view(&self) -> Result<StringViewAddressable> {
        let heap = &self.heap;
        if !heap.is_utf8() {
            return Err(RayexecError::new("Cannot view raw binary as strings"));
        }

        Ok(StringViewAddressable {
            metadata: self.metadata.as_slice(),
            heap,
        })
    }

    pub fn try_as_string_view_mut(&mut self) -> Result<StringViewAddressableMut> {
        let heap = self.heap.try_as_mut()?;
        if !heap.is_utf8() {
            return Err(RayexecError::new("Cannot view raw binary as strings"));
        }

        Ok(StringViewAddressableMut {
            metadata: self.metadata.try_as_mut()?.as_slice_mut(),
            heap,
        })
    }

    pub fn as_binary_view(&self) -> BinaryViewAddressable {
        // Note that we don't check if this is utf8 or not. We always allow
        // getting binary slices even when we're dealing with strings.
        BinaryViewAddressable {
            metadata: self.metadata.as_slice(),
            heap: &self.heap,
        }
    }

    pub fn try_as_binary_view_mut(&mut self) -> Result<BinaryViewAddressableMut> {
        let heap = self.heap.try_as_mut()?;
        // Unlike binary view, we don't want to allow mutable access to string
        // data without validating it.
        if heap.is_utf8() {
            return Err(RayexecError::new(
                "Cannot view modify raw binary for string data",
            ));
        }

        Ok(BinaryViewAddressableMut {
            metadata: self.metadata.try_as_mut()?.as_slice_mut(),
            heap,
        })
    }

    fn try_new<S>(manager: &B, capacity: usize) -> Result<Self>
    where
        S: ScalarStorage,
    {
        let metadata = TypedRawBuffer::try_with_capacity(manager, capacity)?;
        let is_utf8 = match S::PHYSICAL_TYPE {
            PhysicalType::Utf8 => true,
            PhysicalType::Binary => false,
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected physical type for string buffer: {other}",
                )))
            }
        };
        let heap = StringViewHeap::new(is_utf8);

        Ok(StringBuffer {
            metadata: SharedOrOwned::owned(metadata),
            heap: SharedOrOwned::owned(heap),
        })
    }

    fn logical_len(&self) -> usize {
        self.metadata.capacity()
    }

    fn make_shared_and_clone(&mut self) -> Self {
        let metadata = self.metadata.make_shared_and_clone();
        let heap = self.heap.make_shared_and_clone();

        StringBuffer { metadata, heap }
    }
}

#[derive(Debug)]
pub(crate) struct DictionaryBuffer<B: BufferManager> {
    pub(crate) selection: SharedOrOwned<TypedRawBuffer<usize, B>>,
    pub(crate) child_buffer: SharedOrOwned<ArrayBuffer<B>>,
}

impl<B> DictionaryBuffer<B>
where
    B: BufferManager,
{
    fn make_shared_and_clone(&mut self) -> Self {
        DictionaryBuffer {
            selection: self.selection.make_shared_and_clone(),
            child_buffer: self.child_buffer.make_shared_and_clone(),
        }
    }

    fn logical_len(&self) -> usize {
        self.selection.capacity()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata {
    pub offset: i32,
    pub len: i32,
}

#[derive(Debug)]
pub struct ListBuffer<B: BufferManager> {
    pub(crate) metadata: SharedOrOwned<TypedRawBuffer<ListItemMetadata, B>>,
    /// Number of "filled" entries in the child array.
    ///
    /// This differs from the child's capacity as we need to be able
    /// incrementally push back values.
    ///
    /// This is only looked at when writing values to the child array. Reads can
    /// ignore this as all required info is in the entry metadata.
    #[allow(dead_code)]
    pub(crate) entries: usize,
    pub(crate) child: SharedOrOwned<Array<B>>,
}

impl<B> ListBuffer<B>
where
    B: BufferManager,
{
    fn try_new(manager: &B, inner_type: DataType, capacity: usize) -> Result<Self> {
        let metadata = TypedRawBuffer::try_with_capacity(manager, capacity)?;
        let child = Array::try_new(manager, inner_type, capacity)?;

        Ok(ListBuffer {
            metadata: SharedOrOwned::owned(metadata),
            entries: 0,
            child: SharedOrOwned::owned(child),
        })
    }

    fn logical_len(&self) -> usize {
        self.metadata.capacity()
    }

    fn make_shared_and_clone(&mut self) -> Self {
        let metadata = self.metadata.make_shared_and_clone();
        let child = self.child.make_shared_and_clone();

        ListBuffer {
            metadata,
            entries: self.entries,
            child,
        }
    }
}

// TODO: This could result in double boxing.
#[derive(Debug)]
pub(crate) enum SharedOrOwned<T> {
    Shared(Arc<T>),
    Owned(Box<T>),
    Uninit,
}

impl<T> SharedOrOwned<T> {
    pub(crate) fn owned(v: impl Into<Box<T>>) -> Self {
        SharedOrOwned::Owned(v.into())
    }

    pub(crate) const fn is_owned(&self) -> bool {
        matches!(self, Self::Owned(_))
    }

    pub(crate) fn make_shared_and_clone(&mut self) -> Self {
        self.make_shared();
        self.try_clone_shared().expect("to be in shared variant")
    }

    pub(crate) fn make_shared(&mut self) {
        match self {
            Self::Shared(_) => (), // Nothing to do.
            Self::Owned(_) => {
                // Need to swap out and wrap in an arc.
                let orig = std::mem::replace(self, SharedOrOwned::Uninit);
                let owned = match orig {
                    SharedOrOwned::Owned(owned) => owned,
                    _ => unreachable!(),
                };

                let shared = Arc::new(*owned);
                *self = SharedOrOwned::Shared(shared);
            }
            Self::Uninit => panic!("invalid state"),
        }
    }

    pub(crate) fn try_clone_shared(&self) -> Result<Self> {
        match self {
            Self::Owned(_) => Err(RayexecError::new("Cannot clone owned value")),
            Self::Shared(v) => Ok(Self::Shared(v.clone())),
            Self::Uninit => panic!("invalid state"),
        }
    }
}

impl<T> AsRef<T> for SharedOrOwned<T> {
    fn as_ref(&self) -> &T {
        match self {
            Self::Shared(v) => v.as_ref(),
            Self::Owned(v) => v.as_ref(),
            Self::Uninit => panic!("invalid state"),
        }
    }
}

impl<T> Deref for SharedOrOwned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> TryAsMut<T> for SharedOrOwned<T> {
    type Error = RayexecError;

    fn try_as_mut(&mut self) -> Result<&mut T, Self::Error> {
        match self {
            Self::Owned(v) => Ok(v.as_mut()),
            Self::Shared(_) => Err(RayexecError::new("Cannot get mutable refernce")),
            Self::Uninit => panic!("invalid state"),
        }
    }
}
