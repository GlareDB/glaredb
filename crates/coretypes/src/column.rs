use bitvec::{slice::BitSlice, vec::BitVec};
use paste::paste;
use std::fmt;
use std::iter::Iterator;
use std::marker::PhantomData;

pub trait NativeType: Sync + Send + fmt::Debug {}

impl NativeType for bool {}
impl NativeType for i8 {}
impl NativeType for i16 {}
impl NativeType for i32 {}
impl NativeType for i64 {}
impl NativeType for f32 {}
impl NativeType for f64 {}
impl NativeType for str {}
impl NativeType for [u8] {}

pub trait FixedLengthType: NativeType + Copy {}

impl FixedLengthType for bool {}
impl FixedLengthType for i8 {}
impl FixedLengthType for i16 {}
impl FixedLengthType for i32 {}
impl FixedLengthType for i64 {}
impl FixedLengthType for f32 {}
impl FixedLengthType for f64 {}

pub trait VarLengthType: NativeType {}

impl VarLengthType for str {}
impl VarLengthType for [u8] {}

#[derive(Debug)]
pub struct FixedLengthVec<T> {
    vec: Vec<T>,
}

impl<T: FixedLengthType> FixedLengthVec<T> {
    pub fn copy_insert(&mut self, idx: usize, item: &T) {
        self.vec.insert(idx, *item)
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        self.vec.get(idx)
    }
}

impl<T: FixedLengthType> Default for FixedLengthVec<T> {
    fn default() -> Self {
        FixedLengthVec { vec: Vec::new() }
    }
}

pub type BoolVec = FixedLengthVec<bool>;
pub type I8Vec = FixedLengthVec<i8>;
pub type I16Vec = FixedLengthVec<i16>;
pub type I32Vec = FixedLengthVec<i32>;
pub type I64Vec = FixedLengthVec<i64>;
pub type F32Vec = FixedLengthVec<f32>;
pub type F64Vec = FixedLengthVec<f64>;

pub trait BytesRef: VarLengthType + AsRef<[u8]> {
    /// Convert a slice of bytes to a reference to self. The entire slice must
    /// be used.
    fn from_bytes(buf: &[u8]) -> &Self;
}

impl BytesRef for str {
    fn from_bytes(buf: &[u8]) -> &Self {
        // System should only ever be dealing with utf8.
        std::str::from_utf8(buf).unwrap()
    }
}

impl BytesRef for [u8] {
    fn from_bytes(buf: &[u8]) -> &Self {
        buf
    }
}

#[derive(Debug)]
pub struct VarLengthVec<T: ?Sized> {
    offsets: Vec<usize>,
    data: Vec<u8>,
    phantom: PhantomData<T>,
}

impl<T: BytesRef + ?Sized> Default for VarLengthVec<T> {
    fn default() -> Self {
        // Offsets vector always has one more than the number of items. Helps
        // with calculating the range of bytes for each item.
        let offsets = vec![0];
        VarLengthVec {
            offsets,
            data: Vec::new(),
            phantom: PhantomData,
        }
    }
}

impl<T: BytesRef + ?Sized> VarLengthVec<T> {
    pub fn copy_insert(&mut self, idx: usize, item: &T) {
        let buf = item.as_ref();

        self.data.resize(self.data.len() + buf.len(), 0);

        let start = self.offsets[idx];
        let end = start + buf.len();

        self.data.copy_within(start..end, end);
        self.data[start..end].copy_from_slice(buf);
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        let start = *self.offsets.get(idx)?;
        let end = self.offsets[idx + 1]; // Offsets always has one more than number of items.

        let buf = &self.data[start..end];
        Some(T::from_bytes(buf))
    }
}

pub type StrVec = VarLengthVec<str>;
pub type BinaryVec = VarLengthVec<[u8]>;

#[derive(Debug)]
pub enum ColumnVec {
    Bool(BoolVec),
    I8(I8Vec),
    I16(I16Vec),
    I32(I32Vec),
    I64(I64Vec),
    F32(F32Vec),
    F64(F64Vec),
    Str(StrVec),
    Binary(BinaryVec),
}

/// Implement various constructors for each variant.
macro_rules! impl_constructor {
    ($variant:ident) => {
        paste! {
            pub fn [<new_ $variant:lower _vec>]() -> Self {
                ColumnVec::$variant([<$variant Vec>]::default())
            }
        }
    };
}

/// Implement `try_as_..._vec` and `try_as_..._vec_mut` methods to downcast to
/// the concrete vector type.
macro_rules! impl_try_as_dispatch {
    ($variant:ident) => {
        // Produces:
        // pub fn try_as_bool_vec(&self) -> Option<&BoolVec>
        // pub fn try_as_bool_vec_mut(&mut self) -> Option<&mut BoolVec>
        paste! {
            pub fn [<try_as_ $variant:lower _vec>](&self) -> Option<&[<$variant Vec>]> {
                match self {
                    Self::$variant(v) => Some(v),
                    _ => None,
                }
            }

            pub fn [<try_as_ $variant:lower _vec_mut>](&mut self) -> Option<&mut [<$variant Vec>]> {
                match self {
                    Self::$variant(v) => Some(v),
                    _ => None,
                }
            }
        }
    };
}

impl ColumnVec {
    impl_constructor!(Bool);
    impl_constructor!(I8);
    impl_constructor!(I16);
    impl_constructor!(I32);
    impl_constructor!(I64);
    impl_constructor!(F32);
    impl_constructor!(F64);
    impl_constructor!(Str);
    impl_constructor!(Binary);

    impl_try_as_dispatch!(Bool);
    impl_try_as_dispatch!(I8);
    impl_try_as_dispatch!(I16);
    impl_try_as_dispatch!(I32);
    impl_try_as_dispatch!(I64);
    impl_try_as_dispatch!(F32);
    impl_try_as_dispatch!(F64);
    impl_try_as_dispatch!(Str);
    impl_try_as_dispatch!(Binary);
}
