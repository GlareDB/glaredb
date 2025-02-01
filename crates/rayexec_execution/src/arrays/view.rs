use std::fmt;

pub const MAX_INLINE_LEN: usize = 12;
pub const PREFIX_LEN: usize = 4;

#[derive(Clone, Copy)]
#[repr(C)]
pub union StringView {
    inline: ViewInline,
    reference: ViewReference,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ViewInline {
    pub len: i32,
    pub inline: [u8; 12],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ViewReference {
    pub len: i32,
    pub prefix: [u8; 4],
    pub buffer_idx: i32,
    pub offset: i32,
}

impl StringView {
    const _SIZE_ASSERTION: () = assert!(std::mem::size_of::<Self>() == 16);

    /// Creates a new inline view.
    ///
    /// Panics if `data` contains more than 12 bytes.
    pub fn new_inline(data: &[u8]) -> Self {
        let len = data.len();
        assert!(len <= MAX_INLINE_LEN);
        let mut inline = ViewInline {
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
        let mut reference = ViewReference {
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

    pub const fn as_inline(&self) -> &ViewInline {
        debug_assert!(self.is_inline());
        unsafe { &self.inline }
    }

    pub const fn as_reference(&self) -> &ViewReference {
        debug_assert!(!self.is_inline());
        unsafe { &self.reference }
    }

    pub const fn as_inline_mut(&mut self) -> &mut ViewInline {
        debug_assert!(self.is_inline());
        unsafe { &mut self.inline }
    }

    pub const fn as_reference_mut(&mut self) -> &mut ViewReference {
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
