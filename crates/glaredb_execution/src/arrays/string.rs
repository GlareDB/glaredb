//! Various string types.

use std::fmt;

pub const MAX_INLINE_LEN: usize = 12;
pub const PREFIX_LEN: usize = 4;

/// StringView is an arrow-compatible string for utf8 buffers.
///
/// This is currently the default string type we use for arrays.
#[derive(Clone, Copy)]
#[repr(C)]
pub union StringView {
    inline: StringViewInline,
    reference: StringViewReference,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct StringViewInline {
    pub len: i32,
    pub inline: [u8; 12],
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct StringViewReference {
    pub len: i32,
    pub prefix: [u8; 4],
    pub buffer_idx: i32,
    pub offset: i32,
}

impl StringView {
    pub const EMPTY: Self = StringView {
        inline: StringViewInline {
            len: 0,
            inline: [0; 12],
        },
    };

    const _SIZE_ASSERTION: () = assert!(std::mem::size_of::<Self>() == 16);

    /// Creates a new inline view.
    ///
    /// Panics if `data` contains more than 12 bytes.
    pub fn new_inline(data: &[u8]) -> Self {
        let len = data.len();
        assert!(len <= MAX_INLINE_LEN);
        let mut inline = StringViewInline {
            len: len as i32,
            inline: [0; 12],
        };

        inline.inline[0..len].copy_from_slice(data);

        StringView { inline }
    }

    /// Creates a new view that references another buffer.
    ///
    /// `data` should be the complete value.
    ///
    /// Panics if `data` contains fewer than 12 bytes.
    pub fn new_reference(data: &[u8], buffer_idx: i32, offset: i32) -> Self {
        let len = data.len();
        assert!(len > MAX_INLINE_LEN);
        let mut reference = StringViewReference {
            len: len as i32,
            prefix: [0; 4],
            buffer_idx,
            offset,
        };

        reference.prefix.copy_from_slice(&data[0..4]);

        StringView { reference }
    }

    pub const fn is_inline(&self) -> bool {
        // SAFETYE: i32 len is first field in both, safe to access from either
        // variant.
        unsafe { self.inline.len <= 12 }
    }

    pub const fn is_reference(&self) -> bool {
        unsafe { self.inline.len > 12 }
    }

    pub fn data_len(&self) -> i32 {
        // SAFETY: `len` field is in the same place in both variants.
        unsafe { self.inline.len }
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        // SAFETY: Const assertion guarantees this is 16 bytes.
        unsafe { std::mem::transmute::<Self, [u8; 16]>(self) }
    }

    pub const fn from_bytes(buf: [u8; 16]) -> Self {
        // SAFETY: Const assersion guarantees this is 16 bytes.
        //
        // Note that this doesn't guarantee validity of the data, just that
        // these bytes can always be represented as the union.
        unsafe { std::mem::transmute::<[u8; 16], Self>(buf) }
    }

    pub const fn as_inline(&self) -> &StringViewInline {
        debug_assert!(self.is_inline());
        unsafe { &self.inline }
    }

    pub const fn as_reference(&self) -> &StringViewReference {
        debug_assert!(!self.is_inline());
        unsafe { &self.reference }
    }

    pub const fn as_inline_mut(&mut self) -> &mut StringViewInline {
        debug_assert!(self.is_inline());
        unsafe { &mut self.inline }
    }

    pub const fn as_reference_mut(&mut self) -> &mut StringViewReference {
        debug_assert!(!self.is_inline());
        unsafe { &mut self.reference }
    }
}

impl fmt::Debug for StringView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_inline() {
            write!(f, "{:?}", self.as_inline())
        } else {
            write!(f, "{:?}", self.as_reference())
        }
    }
}

/// An alternative string representation that can eithe represent a string
/// inline or reference a string via a pointer.
///
/// This is used when using row collections where the pointer points to an
/// allocated heap block. We use this over `StringView` to avoid needing to
/// recompute buffer indices when merging multiple collections together.
///
/// Note that this _will not_ deallocate on drop.
#[derive(Clone, Copy)]
#[repr(C)]
pub union StringPtr {
    inline: StringPtrInline,
    reference: StringPtrReference,
}

/// Helper for converting from an inline string view to a string ptr.
impl From<StringViewInline> for StringPtr {
    fn from(value: StringViewInline) -> Self {
        StringPtr {
            inline: StringPtrInline {
                len: value.len,
                inline: value.inline,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct StringPtrInline {
    pub len: i32,
    pub inline: [u8; 12],
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct StringPtrReference {
    pub len: i32,
    pub prefix: [u8; 4],
    pub ptr: *const u8,
}

impl StringPtr {
    pub const EMPTY: Self = StringPtr {
        inline: StringPtrInline {
            len: 0,
            inline: [0; 12],
        },
    };

    const _SIZE_ASSERTION: () = assert!(std::mem::size_of::<Self>() == 16);

    /// Creates a new inline view.
    ///
    /// Panics if `data` contains more than 12 bytes.
    pub fn new_inline(data: &[u8]) -> Self {
        let len = data.len();
        assert!(len <= MAX_INLINE_LEN);
        let mut inline = StringPtrInline {
            len: len as i32,
            inline: [0; 12],
        };

        inline.inline[0..len].copy_from_slice(data);

        StringPtr { inline }
    }

    /// Creates a new view that references the string data via a pointer.
    ///
    /// `data` should be the complete value. The pointer will be derived from
    /// this slice.
    ///
    /// Panics if `data` contains fewer than 12 bytes.
    pub fn new_reference(data: &[u8]) -> Self {
        let len = data.len();
        assert!(len > MAX_INLINE_LEN);
        let mut reference = StringPtrReference {
            len: len as i32,
            prefix: [0; 4],
            ptr: data.as_ptr(),
        };

        reference.prefix.copy_from_slice(&data[0..4]);

        StringPtr { reference }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            if self.is_inline() {
                &self.inline.inline[0..(self.inline.len as usize)]
            } else {
                std::slice::from_raw_parts(self.reference.ptr, self.reference.len as usize)
            }
        }
    }

    pub const fn is_inline(&self) -> bool {
        // SAFETYE: i32 len is first field in both, safe to access from either
        // variant.
        unsafe { self.inline.len <= 12 }
    }

    pub const fn is_reference(&self) -> bool {
        unsafe { self.inline.len > 12 }
    }

    pub fn data_len(&self) -> i32 {
        // SAFETY: `len` field is in the same place in both variants.
        unsafe { self.inline.len }
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        // SAFETY: Const assertion guarantees this is 16 bytes.
        unsafe { std::mem::transmute::<Self, [u8; 16]>(self) }
    }

    pub const fn from_bytes(buf: [u8; 16]) -> Self {
        // SAFETY: Const assersion guarantees this is 16 bytes.
        //
        // Note that this doesn't guarantee validity of the data, just that
        // these bytes can always be represented as the union.
        unsafe { std::mem::transmute::<[u8; 16], Self>(buf) }
    }

    pub const fn as_inline(&self) -> &StringPtrInline {
        debug_assert!(self.is_inline());
        unsafe { &self.inline }
    }

    pub const fn as_reference(&self) -> &StringPtrReference {
        debug_assert!(!self.is_inline());
        unsafe { &self.reference }
    }

    pub const fn as_inline_mut(&mut self) -> &mut StringPtrInline {
        debug_assert!(self.is_inline());
        unsafe { &mut self.inline }
    }

    pub const fn as_reference_mut(&mut self) -> &mut StringPtrReference {
        debug_assert!(!self.is_inline());
        unsafe { &mut self.reference }
    }
}

impl fmt::Debug for StringPtr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_inline() {
            write!(f, "{:?}", self.as_inline())
        } else {
            write!(f, "{:?}", self.as_reference())
        }
    }
}
