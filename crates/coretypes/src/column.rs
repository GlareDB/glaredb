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
        self.vec.insert(idx, *item);
    }

    pub fn copy_push(&mut self, item: &T) {
        self.vec.push(*item);
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        self.vec.get(idx)
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.vec.iter()
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

        let data_len = self.data.len();
        self.data.resize(data_len + buf.len(), 0);

        let start = self.offsets[idx];
        let new_end = start + buf.len();

        self.data.copy_within(start..data_len, new_end);
        self.data[start..new_end].copy_from_slice(buf);

        // Insert new end offset, update existing offsets after this new
        // insertion.
        self.offsets.insert(idx + 1, new_end);
        for offset in self.offsets.iter_mut().skip(idx + 2) {
            *offset += buf.len();
        }
    }

    pub fn copy_push(&mut self, item: &T) {
        self.data.extend_from_slice(item.as_ref());
        let next_offset = self.data.len();
        self.offsets.push(next_offset);
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        let start = *self.offsets.get(idx)?;
        let end = *self.offsets.get(idx + 1)?;

        let buf = &self.data[start..end];
        Some(T::from_bytes(buf))
    }

    pub fn iter(&self) -> VarLengthIterator<'_, T> {
        VarLengthIterator::from_vec(self)
    }
}

pub struct VarLengthIterator<'a, T: ?Sized> {
    vec: &'a VarLengthVec<T>,
    idx: usize,
}

impl<'a, T: ?Sized> VarLengthIterator<'a, T> {
    fn from_vec(vec: &'a VarLengthVec<T>) -> Self {
        VarLengthIterator { vec, idx: 0 }
    }
}

impl<'a, T: BytesRef + ?Sized> Iterator for VarLengthIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.vec.get(self.idx)?;
        self.idx += 1;
        Some(item)
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
    ($($variant:ident),*) => {
        $(
            // pub fn new_bool_vec() -> Self
            paste! {
                pub fn [<new_ $variant:lower _vec>]() -> Self {
                    ColumnVec::$variant([<$variant Vec>]::default())
                }
            }
        )*
    };
}

/// Implement `try_as_..._vec` and `try_as_..._vec_mut` methods to downcast to
/// the concrete vector type.
macro_rules! impl_try_as_dispatch {
    ($($variant:ident),*) => {
        $(
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
        )*
    };
}

impl ColumnVec {
    impl_constructor!(Bool, I8, I16, I32, I64, F32, F64, Str, Binary);
    impl_try_as_dispatch!(Bool, I8, I16, I32, I64, F32, F64, Str, Binary);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_push_get() {
        let mut v = ColumnVec::new_i32_vec();
        let v = v.try_as_i32_vec_mut().unwrap();

        v.copy_push(&0);
        v.copy_push(&1);
        v.copy_push(&2);

        let item = v.get(1).unwrap();
        assert_eq!(1, *item);
    }

    #[test]
    fn varlen_push_get_insert() {
        let mut v = ColumnVec::new_str_vec();
        let v = v.try_as_str_vec_mut().unwrap();

        v.copy_push("one");
        v.copy_push("two");
        v.copy_push("three");

        let item = v.get(1).unwrap();
        assert_eq!("two", item);

        v.copy_insert(1, "four");

        let item = v.get(1).unwrap();
        assert_eq!("four", item);
    }

    #[test]
    fn varlen_iter() {
        let mut v = ColumnVec::new_str_vec();
        let v = v.try_as_str_vec_mut().unwrap();

        let vals = vec!["one", "two", "three"];
        for val in vals.iter() {
            v.copy_push(val);
        }

        let got: Vec<_> = v.iter().collect();
        assert_eq!(vals, got);
    }
}
