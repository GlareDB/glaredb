use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use glaredb_error::{DbError, Result, not_implemented};

use super::physical_type::{
    BinaryViewAddressable,
    BinaryViewAddressableMut,
    StringViewAddressable,
    StringViewAddressableMut,
};
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
};
use crate::arrays::datatype::DataType;
use crate::arrays::string::{MAX_INLINE_LEN, StringView};
use crate::buffer::buffer_manager::AsRawBufferManager;
use crate::buffer::raw::RawBuffer;
use crate::buffer::typed::TypedBuffer;
use crate::util::convert::TryAsMut;

/// Abstraction layer for holding shared or owned array data.
///
/// Arrays typically start out with all owned data allow for mutability. If
/// another array needs to reference it (e.g. for selection), then the
/// underlying buffers will be transitioned to shared references.
#[derive(Debug)]
pub struct ArrayBuffer {
    inner: ArrayBufferType,
}

impl ArrayBuffer {
    pub(crate) fn new(inner: impl Into<ArrayBufferType>) -> Self {
        ArrayBuffer {
            inner: inner.into(),
        }
    }

    /// Creates a new array buffer for the given datatype.
    ///
    /// This will never produce a Constant or Dictionary buffer.
    pub(crate) fn try_new_for_datatype(
        manager: &impl AsRawBufferManager,
        datatype: &DataType,
        capacity: usize,
    ) -> Result<Self> {
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
                        return Err(DbError::new(format!("Expected list datatype, got {other}")));
                    }
                };
                ListBuffer::try_new(manager, inner, capacity)?.into()
            }
            other => not_implemented!("new array buffer for physical type: {other}"),
        };

        Ok(ArrayBuffer { inner })
    }

    /// Make all underlying buffers shared and returns an array buffer
    /// containing only shared references.
    pub(crate) fn make_shared_and_clone(&mut self) -> Self {
        ArrayBuffer {
            inner: self.inner.make_shared_and_clone(),
        }
    }

    /// Make all underlying buffers shared.
    pub(crate) fn make_shared(&mut self) {
        self.inner.make_shared();
    }

    /// Try to clone this buffer.
    ///
    /// This will error if any buffer is not already shared.
    pub(crate) fn try_clone_shared(&self) -> Result<Self> {
        Ok(ArrayBuffer {
            inner: self.inner.try_clone_shared()?,
        })
    }

    pub(crate) fn into_inner(self) -> ArrayBufferType {
        self.inner
    }
}

impl AsRef<ArrayBufferType> for ArrayBuffer {
    fn as_ref(&self) -> &ArrayBufferType {
        &self.inner
    }
}

impl AsMut<ArrayBufferType> for ArrayBuffer {
    fn as_mut(&mut self) -> &mut ArrayBufferType {
        &mut self.inner
    }
}

impl Deref for ArrayBuffer {
    type Target = ArrayBufferType;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl DerefMut for ArrayBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayBufferKind {
    Scalar,
    Constant,
    String,
    Dictionary,
    List,
}

impl ArrayBufferKind {
    pub const fn as_str(&self) -> &str {
        match self {
            Self::Scalar => "Scalar",
            Self::Constant => "Constant",
            Self::String => "String",
            Self::Dictionary => "Dictionary",
            Self::List => "List",
        }
    }
}

impl fmt::Display for ArrayBufferKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug)]
pub enum ArrayBufferType {
    Scalar(ScalarBuffer),
    Constant(ConstantBuffer),
    String(StringBuffer),
    Dictionary(DictionaryBuffer),
    List(ListBuffer),
}

impl ArrayBufferType {
    pub const fn kind(&self) -> ArrayBufferKind {
        match self {
            Self::Scalar(_) => ArrayBufferKind::Scalar,
            Self::Constant(_) => ArrayBufferKind::Constant,
            Self::String(_) => ArrayBufferKind::String,
            Self::Dictionary(_) => ArrayBufferKind::Dictionary,
            Self::List(_) => ArrayBufferKind::List,
        }
    }

    pub fn physical_type(&self) -> PhysicalType {
        match self {
            Self::Scalar(b) => b.physical_type,
            Self::Constant(b) => b.child_buffer.physical_type(),
            Self::String(b) => {
                if b.is_utf8 {
                    PhysicalType::Utf8
                } else {
                    PhysicalType::Binary
                }
            }
            Self::Dictionary(b) => b.child_buffer.physical_type(),
            Self::List(_) => PhysicalType::List,
        }
    }

    pub fn get_list_buffer(&self) -> Result<&ListBuffer> {
        match self {
            Self::List(b) => Ok(b),
            other => Err(DbError::new(format!(
                "Expected list buffer, got {}",
                other.kind()
            ))),
        }
    }

    pub fn get_list_buffer_mut(&mut self) -> Result<&mut ListBuffer> {
        match self {
            Self::List(b) => Ok(b),
            other => Err(DbError::new(format!(
                "Expected list buffer, got {}",
                other.kind()
            ))),
        }
    }

    pub fn get_scalar_buffer(&self) -> Result<&ScalarBuffer> {
        match self {
            Self::Scalar(b) => Ok(b),
            other => Err(DbError::new(format!(
                "Expected scalar buffer, got {}",
                other.kind()
            ))),
        }
    }

    pub fn get_scalar_buffer_mut(&mut self) -> Result<&mut ScalarBuffer> {
        match self {
            Self::Scalar(b) => Ok(b),
            other => Err(DbError::new(format!(
                "Expected scalar buffer, got {}",
                other.kind()
            ))),
        }
    }

    pub fn get_string_buffer(&self) -> Result<&StringBuffer> {
        match self {
            Self::String(b) => Ok(b),
            other => Err(DbError::new(format!(
                "Expected string buffer, got {}",
                other.kind()
            ))),
        }
    }

    pub fn get_string_buffer_mut(&mut self) -> Result<&mut StringBuffer> {
        match self {
            Self::String(b) => Ok(b),
            other => Err(DbError::new(format!(
                "Expected string buffer, got {}",
                other.kind()
            ))),
        }
    }

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

    fn make_shared(&mut self) {
        match self {
            Self::Scalar(buf) => buf.make_shared(),
            Self::Constant(buf) => buf.make_shared(),
            Self::String(buf) => buf.make_shared(),
            Self::Dictionary(buf) => buf.make_shared(),
            Self::List(buf) => buf.make_shared(),
        }
    }

    fn try_clone_shared(&self) -> Result<Self> {
        Ok(match self {
            Self::Scalar(buf) => Self::Scalar(buf.try_clone_shared()?),
            Self::Constant(buf) => Self::Constant(buf.try_clone_shared()?),
            Self::String(buf) => Self::String(buf.try_clone_shared()?),
            Self::Dictionary(buf) => Self::Dictionary(buf.try_clone_shared()?),
            Self::List(buf) => Self::List(buf.try_clone_shared()?),
        })
    }
}

impl From<ScalarBuffer> for ArrayBufferType {
    fn from(value: ScalarBuffer) -> Self {
        Self::Scalar(value)
    }
}

impl From<ConstantBuffer> for ArrayBufferType {
    fn from(value: ConstantBuffer) -> Self {
        Self::Constant(value)
    }
}

impl From<StringBuffer> for ArrayBufferType {
    fn from(value: StringBuffer) -> Self {
        Self::String(value)
    }
}

impl From<DictionaryBuffer> for ArrayBufferType {
    fn from(value: DictionaryBuffer) -> Self {
        Self::Dictionary(value)
    }
}

impl From<ListBuffer> for ArrayBufferType {
    fn from(value: ListBuffer) -> Self {
        Self::List(value)
    }
}

#[derive(Debug)]
pub struct ScalarBuffer {
    pub(crate) physical_type: PhysicalType,
    pub(crate) raw: SharedOrOwned<RawBuffer>,
}

impl ScalarBuffer {
    pub fn try_reserve<S>(&mut self, additional: usize) -> Result<()>
    where
        S: ScalarStorage,
        S::StorageType: Sized,
    {
        let raw = self.raw.try_as_mut()?;
        unsafe { raw.reserve::<S::StorageType>(additional) }
    }

    pub fn try_as_slice<S>(&self) -> Result<&[S::StorageType]>
    where
        S: ScalarStorage,
        S::StorageType: Sized,
    {
        if self.physical_type != S::PHYSICAL_TYPE {
            return Err(DbError::new("Physical type doesn't match for cast")
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
            return Err(DbError::new("Physical type doesn't match for cast")
                .with_field("need", self.physical_type)
                .with_field("have", S::PHYSICAL_TYPE));
        }

        let raw = self.raw.try_as_mut()?;
        let buf = unsafe { raw.as_slice_mut::<S::StorageType>() };

        Ok(buf)
    }

    /// Create a new scalar buffer for storing sized primitive values.
    fn try_new<S>(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self>
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

    fn make_shared(&mut self) {
        self.raw.make_shared();
    }

    fn make_shared_and_clone(&mut self) -> Self {
        let raw = self.raw.make_shared_and_clone();

        ScalarBuffer {
            physical_type: self.physical_type,
            raw,
        }
    }

    fn try_clone_shared(&self) -> Result<Self> {
        Ok(ScalarBuffer {
            physical_type: self.physical_type,
            raw: self.raw.try_clone_shared()?,
        })
    }
}

#[derive(Debug)]
pub struct ConstantBuffer {
    pub(crate) row_reference: usize,
    pub(crate) len: usize,
    pub(crate) child_buffer: Box<ArrayBuffer>,
}

impl ConstantBuffer {
    fn logical_len(&self) -> usize {
        self.len
    }

    fn make_shared(&mut self) {
        self.child_buffer.make_shared();
    }

    fn make_shared_and_clone(&mut self) -> Self {
        ConstantBuffer {
            row_reference: self.row_reference,
            len: self.len,
            child_buffer: Box::new(self.child_buffer.make_shared_and_clone()),
        }
    }

    fn try_clone_shared(&self) -> Result<Self> {
        Ok(ConstantBuffer {
            row_reference: self.row_reference,
            len: self.len,
            child_buffer: Box::new(self.child_buffer.try_clone_shared()?),
        })
    }
}

/// Uses string views to represent strings and binary data.
///
/// The string view buffer index is always set to '0' within this buffer.
#[derive(Debug)]
pub struct StringBuffer {
    pub(crate) is_utf8: bool,
    pub(crate) metadata: SharedOrOwned<TypedBuffer<StringView>>,
    pub(crate) buffer: SharedOrOwned<StringViewBuffer>,
}

impl StringBuffer {
    pub fn try_reserve(&mut self, additional: usize) -> Result<()> {
        self.metadata.try_as_mut()?.reserve_additional(additional)
    }

    pub fn try_as_string_view(&self) -> Result<StringViewAddressable> {
        let buffer = &self.buffer;
        if !self.is_utf8 {
            return Err(DbError::new("Cannot view raw binary as strings"));
        }

        Ok(StringViewAddressable {
            metadata: self.metadata.as_slice(),
            buffer,
        })
    }

    pub fn try_as_string_view_mut(&mut self) -> Result<StringViewAddressableMut> {
        let buffer = self.buffer.try_as_mut()?;
        if !self.is_utf8 {
            return Err(DbError::new("Cannot view raw binary as strings"));
        }

        Ok(StringViewAddressableMut {
            metadata: self.metadata.try_as_mut()?.as_slice_mut(),
            buffer,
        })
    }

    pub fn as_binary_view(&self) -> BinaryViewAddressable {
        // Note that we don't check if this is utf8 or not. We always allow
        // getting binary slices even when we're dealing with strings.
        BinaryViewAddressable {
            metadata: self.metadata.as_slice(),
            buffer: &self.buffer,
        }
    }

    pub fn try_as_binary_view_mut(&mut self) -> Result<BinaryViewAddressableMut> {
        let buffer = self.buffer.try_as_mut()?;
        // TODO: Probably do want this check. Currently skipping this for easier
        // row decoding between binary/utf8 data.
        // // Unlike binary view, we don't want to allow mutable access to string
        // // data without validating it.
        // if self.is_utf8 {
        //     return Err(RayexecError::new(
        //         "Cannot view modify raw binary for string data",
        //     ));
        // }

        Ok(BinaryViewAddressableMut {
            metadata: self.metadata.try_as_mut()?.as_slice_mut(),
            buffer,
        })
    }

    fn try_new<S>(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self>
    where
        S: ScalarStorage,
    {
        let metadata = TypedBuffer::try_with_capacity(manager, capacity)?;
        let is_utf8 = match S::PHYSICAL_TYPE {
            PhysicalType::Utf8 => true,
            PhysicalType::Binary => false,
            other => {
                return Err(DbError::new(format!(
                    "Unexpected physical type for string buffer: {other}",
                )));
            }
        };
        let buffer = StringViewBuffer::with_capacity(manager, 0)?;

        Ok(StringBuffer {
            is_utf8,
            metadata: SharedOrOwned::owned(metadata),
            buffer: SharedOrOwned::owned(buffer),
        })
    }

    fn logical_len(&self) -> usize {
        self.metadata.capacity()
    }

    fn make_shared(&mut self) {
        self.metadata.make_shared();
        self.buffer.make_shared();
    }

    fn make_shared_and_clone(&mut self) -> Self {
        let metadata = self.metadata.make_shared_and_clone();
        let buffer = self.buffer.make_shared_and_clone();

        StringBuffer {
            is_utf8: self.is_utf8,
            metadata,
            buffer,
        }
    }

    fn try_clone_shared(&self) -> Result<Self> {
        Ok(StringBuffer {
            is_utf8: self.is_utf8,
            metadata: self.metadata.try_clone_shared()?,
            buffer: self.buffer.try_clone_shared()?,
        })
    }
}

#[derive(Debug)]
pub struct StringViewBuffer {
    bytes_filled: usize,
    buffer: TypedBuffer<u8>,
}

impl StringViewBuffer {
    pub fn with_capacity(manager: &impl AsRawBufferManager, capacity: usize) -> Result<Self> {
        let buffer = TypedBuffer::try_with_capacity(manager, capacity)?;
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
            let remaining = self.buffer.capacity() - self.bytes_filled;
            if remaining < value.len() {
                let additional = value.len() - remaining;
                let reserve_amount = Self::compute_amount_to_reserve(
                    self.buffer.capacity(),
                    self.bytes_filled,
                    additional,
                );
                self.buffer.reserve_additional(reserve_amount)?;
            }

            let offset = self.bytes_filled;
            self.bytes_filled += value.len();

            // Copy entire value to buffer.
            let buf = &mut self.buffer.as_mut()[offset..(offset + value.len())];
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

#[derive(Debug)]
pub struct DictionaryBuffer {
    pub(crate) selection: SharedOrOwned<TypedBuffer<usize>>,
    pub(crate) child_buffer: Box<ArrayBuffer>,
}

impl DictionaryBuffer {
    fn make_shared(&mut self) {
        self.selection.make_shared();
        self.child_buffer.make_shared();
    }

    fn make_shared_and_clone(&mut self) -> Self {
        DictionaryBuffer {
            selection: self.selection.make_shared_and_clone(),
            child_buffer: Box::new(self.child_buffer.make_shared_and_clone()),
        }
    }

    fn try_clone_shared(&self) -> Result<Self> {
        Ok(DictionaryBuffer {
            selection: self.selection.try_clone_shared()?,
            child_buffer: Box::new(self.child_buffer.try_clone_shared()?),
        })
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
pub struct ListBuffer {
    pub(crate) metadata: SharedOrOwned<TypedBuffer<ListItemMetadata>>,
    /// Current offset when adding values to the list.
    #[allow(dead_code)]
    pub(crate) current_offset: usize,
    pub(crate) child_physical_type: PhysicalType,
    pub(crate) child_validity: SharedOrOwned<Validity>, // TODO: Does this need to be wrapped? // TODO: With what?
    pub(crate) child_buffer: Box<ArrayBuffer>,
}

impl ListBuffer {
    pub fn try_new(
        manager: &impl AsRawBufferManager,
        inner_type: DataType,
        capacity: usize,
    ) -> Result<Self> {
        let metadata = TypedBuffer::try_with_capacity(manager, capacity)?;
        let child_buffer = ArrayBuffer::try_new_for_datatype(manager, &inner_type, capacity)?;
        let child_validity = Validity::new_all_valid(capacity);

        Ok(ListBuffer {
            metadata: SharedOrOwned::owned(metadata),
            current_offset: 0,
            child_physical_type: inner_type.physical_type(),
            child_validity: SharedOrOwned::owned(child_validity),
            child_buffer: Box::new(child_buffer),
        })
    }

    /// Try to resize the child buffers in order to hold `additional` elements.
    pub fn try_reserve_child_buffers(&mut self, additional: usize) -> Result<()> {
        // Resize array buffer.
        match &mut self.child_buffer.inner {
            ArrayBufferType::Scalar(scalar) => match self.child_physical_type {
                PhysicalType::UntypedNull => {
                    scalar.try_reserve::<PhysicalUntypedNull>(additional)?
                }
                PhysicalType::Boolean => scalar.try_reserve::<PhysicalBool>(additional)?,
                PhysicalType::Int8 => scalar.try_reserve::<PhysicalI8>(additional)?,
                PhysicalType::Int16 => scalar.try_reserve::<PhysicalI16>(additional)?,
                PhysicalType::Int32 => scalar.try_reserve::<PhysicalI32>(additional)?,
                PhysicalType::Int64 => scalar.try_reserve::<PhysicalI64>(additional)?,
                PhysicalType::Int128 => scalar.try_reserve::<PhysicalI128>(additional)?,
                PhysicalType::UInt8 => scalar.try_reserve::<PhysicalU8>(additional)?,
                PhysicalType::UInt16 => scalar.try_reserve::<PhysicalU16>(additional)?,
                PhysicalType::UInt32 => scalar.try_reserve::<PhysicalU32>(additional)?,
                PhysicalType::UInt64 => scalar.try_reserve::<PhysicalU64>(additional)?,
                PhysicalType::UInt128 => scalar.try_reserve::<PhysicalU128>(additional)?,
                PhysicalType::Float16 => scalar.try_reserve::<PhysicalF16>(additional)?,
                PhysicalType::Float32 => scalar.try_reserve::<PhysicalF32>(additional)?,
                PhysicalType::Float64 => scalar.try_reserve::<PhysicalF64>(additional)?,
                PhysicalType::Interval => scalar.try_reserve::<PhysicalInterval>(additional)?,
                other => {
                    return Err(DbError::new(format!(
                        "Unexpected physical type in list array attempting to resize scalar buffer: {other}"
                    )));
                }
            },
            ArrayBufferType::String(string) => string.try_reserve(additional)?,
            ArrayBufferType::List(_) => {
                not_implemented!("Reserve additional capacity for nested arrays")
            }
            ArrayBufferType::Constant(_) => {
                return Err(DbError::new(
                    "Unexpected constant array in list array buffer during resize",
                ));
            }
            ArrayBufferType::Dictionary(_) => {
                return Err(DbError::new(
                    "Unexpected dictionary array in list array buffer during resize",
                ));
            }
        }

        // Resize validity.
        let mut validity = Validity::new_all_valid(self.child_validity.len() + additional);
        if !self.child_validity.all_valid() {
            for (idx, valid) in self.child_validity.iter().enumerate() {
                if !valid {
                    validity.set_invalid(idx);
                }
            }
        }
        self.child_validity = SharedOrOwned::owned(validity);

        Ok(())
    }

    pub(crate) fn logical_len(&self) -> usize {
        self.metadata.capacity()
    }

    fn make_shared(&mut self) {
        self.metadata.make_shared();
        self.child_validity.make_shared();
        self.child_buffer.make_shared();
    }

    fn make_shared_and_clone(&mut self) -> Self {
        let metadata = self.metadata.make_shared_and_clone();
        let child_buffer = self.child_buffer.make_shared_and_clone();
        let child_validity = self.child_validity.make_shared_and_clone();

        ListBuffer {
            metadata,
            current_offset: self.current_offset,
            child_physical_type: self.child_physical_type,
            child_buffer: Box::new(child_buffer),
            child_validity,
        }
    }

    fn try_clone_shared(&self) -> Result<Self> {
        Ok(ListBuffer {
            metadata: self.metadata.try_clone_shared()?,
            current_offset: self.current_offset,
            child_physical_type: self.child_physical_type,
            child_buffer: Box::new(self.child_buffer.try_clone_shared()?),
            child_validity: self.child_validity.try_clone_shared()?,
        })
    }
}

// TODO: This could result in double boxing.
#[derive(Debug)]
pub enum SharedOrOwned<T> {
    Shared(Arc<T>),
    Owned(Box<T>),
    Uninit,
}

impl<T> SharedOrOwned<T> {
    pub(crate) fn owned(v: impl Into<Box<T>>) -> Self {
        SharedOrOwned::Owned(v.into())
    }

    pub const fn is_owned(&self) -> bool {
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
            Self::Owned(_) => Err(DbError::new("Cannot clone owned value")),
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
    type Error = DbError;

    fn try_as_mut(&mut self) -> Result<&mut T, Self::Error> {
        match self {
            Self::Owned(v) => Ok(v.as_mut()),
            Self::Shared(_) => Err(DbError::new("Cannot get mutable reference")),
            Self::Uninit => panic!("invalid state"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;

    #[test]
    fn string_view_buffer_push_inlined() {
        let mut buffer = StringViewBuffer::with_capacity(&NopBufferManager, 0).unwrap();
        let m = buffer.push_bytes_as_row(&[0, 1, 2, 3]).unwrap();
        assert!(m.is_inline());

        let got = buffer.get(&m);
        assert_eq!(&[0, 1, 2, 3], got);
    }

    #[test]
    fn string_view_buffer_push_referenced() {
        let mut buffer = StringViewBuffer::with_capacity(&NopBufferManager, 0).unwrap();
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
