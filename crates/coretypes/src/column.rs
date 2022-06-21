use crate::datatype::{DataType, DataValue};
use bitvec::{slice::BitSlice, vec::BitVec};
use paste::paste;
use std::fmt;
use std::iter::Iterator;
use std::marker::PhantomData;

#[derive(Debug, thiserror::Error)]
pub enum ColumnError {
    #[error("lengths mismatch: {0} != {1}")]
    LengthMismatch(usize, usize),
    #[error("invalid value")]
    InvalidValue,
}

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

pub trait FixedLengthType: NativeType + Default + Copy {}

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

#[derive(Debug, Clone)]
pub struct FixedLengthVec<T> {
    vec: Vec<T>,
}

impl<T: FixedLengthType> FixedLengthVec<T> {
    pub fn with_capacity(cap: usize) -> Self {
        FixedLengthVec {
            vec: Vec::with_capacity(cap),
        }
    }

    pub fn copy_insert(&mut self, idx: usize, item: &T) {
        self.vec.insert(idx, *item);
    }

    pub fn copy_push(&mut self, item: &T) {
        self.vec.push(*item);
    }

    pub fn push_default(&mut self) {
        self.vec.push(T::default())
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        self.vec.get(idx)
    }

    pub fn len(&self) -> usize {
        self.vec.len()
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
    varlen_type: PhantomData<T>,
}

impl<T: BytesRef + ?Sized> Default for VarLengthVec<T> {
    fn default() -> Self {
        // Offsets vector always has one more than the number of items. Helps
        // with calculating the range of bytes for each item.
        let offsets = vec![0];
        VarLengthVec {
            offsets,
            data: Vec::new(),
            varlen_type: PhantomData,
        }
    }
}

impl<T: BytesRef + ?Sized> VarLengthVec<T> {
    pub fn with_capacity(cap: usize) -> Self {
        let mut offsets = Vec::with_capacity(cap + 1);
        offsets.push(0);
        let data = Vec::with_capacity(cap); // TODO: Determine suitable cap here.
        VarLengthVec {
            offsets,
            data,
            varlen_type: PhantomData,
        }
    }

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

    pub fn push_default(&mut self) {
        let next_offset = self.data.len();
        self.offsets.push(next_offset);
    }

    pub fn len(&self) -> usize {
        // Offsets always has one more than then number of items held.
        self.offsets.len() - 1
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

impl<T: ?Sized> Clone for VarLengthVec<T> {
    fn clone(&self) -> Self {
        VarLengthVec {
            offsets: self.offsets.clone(),
            data: self.data.clone(),
            varlen_type: PhantomData,
        }
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

/// Column vector variants for all types supported by the system.
#[derive(Debug, Clone)]
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
macro_rules! cvec_constructor {
    ($($variant:ident),*) => {
        $(
            // pub fn new_bool_vec() -> Self
            // pub fn new_bool_vec_with_capacity(cap: usize) -> Self
            paste! {
                pub fn [<new_ $variant:lower _vec>]() -> Self {
                    ColumnVec::$variant([<$variant Vec>]::default())
                }

                pub fn [<new_ $variant:lower _vec_with_capacity>](cap: usize) -> Self {
                    ColumnVec::$variant([<$variant Vec>]::with_capacity(cap))
                }
            }
        )*
    };
}

/// Implement common methods across all variants of vectors that each return the
/// same type.
macro_rules! cvec_common {
    ($($variant:ident),*) => {
        /// Return the length of the vector.
        pub fn len(&self) -> usize {
            match self {
                $(
                    Self::$variant(v) => v.len(),
                )*
            }
        }

        pub fn push_default(&mut self) {
            match self {
                $(
                    Self::$variant(v) => v.push_default(),
                )*
            }
        }
    };
}

/// Implement `try_as_..._vec` and `try_as_..._vec_mut` methods to downcast to
/// the concrete vector type.
macro_rules! cvec_try_as_dispatch {
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
    cvec_constructor!(Bool, I8, I16, I32, I64, F32, F64, Str, Binary);
    cvec_common!(Bool, I8, I16, I32, I64, F32, F64, Str, Binary);
    cvec_try_as_dispatch!(Bool, I8, I16, I32, I64, F32, F64, Str, Binary);
}

macro_rules! impl_from_typed_vec {
    ($($variant:ident),*) => {
        $(
            paste! {
                impl From<[<$variant Vec>]> for ColumnVec {
                    fn from(val: [<$variant Vec>]) -> ColumnVec {
                        ColumnVec::$variant(val)
                    }
                }
            }
        )*
    };
}

impl_from_typed_vec!(Bool, I8, I16, I32, I64, F32, F64, Str, Binary);

/// A wrapper around a column vec allowing for value nullability.
#[derive(Debug, Clone)]
pub struct NullableColumnVec {
    /// Validity of each value in the column vector. A '1' indicates not null, a
    /// '0' indicates null.
    validity: BitVec,
    values: ColumnVec,
}

impl NullableColumnVec {
    pub fn with_capacity(cap: usize, datatype: &DataType) -> NullableColumnVec {
        let validity = BitVec::with_capacity(cap);
        let values = match datatype {
            DataType::Bool => ColumnVec::new_bool_vec_with_capacity(cap),
            DataType::Int8 => ColumnVec::new_i8_vec_with_capacity(cap),
            DataType::Int16 => ColumnVec::new_i16_vec_with_capacity(cap),
            DataType::Int32 => ColumnVec::new_i32_vec_with_capacity(cap),
            DataType::Int64 => ColumnVec::new_i64_vec_with_capacity(cap),
            DataType::Float32 => ColumnVec::new_f32_vec_with_capacity(cap),
            DataType::Float64 => ColumnVec::new_f64_vec_with_capacity(cap),
            DataType::Date64 => ColumnVec::new_i64_vec_with_capacity(cap),
            DataType::Utf8 => ColumnVec::new_str_vec_with_capacity(cap),
            DataType::Binary => ColumnVec::new_binary_vec_with_capacity(cap),
        };
        NullableColumnVec { validity, values }
    }

    /// Create a new nullable column vec with each value being marked as valid.
    pub fn new_all_valid(values: ColumnVec) -> NullableColumnVec {
        let validity = BitVec::repeat(true, values.len());
        NullableColumnVec { validity, values }
    }

    /// Create a new nullable column vec from the provides values and validity.
    /// Their lengths must match.
    pub fn from_values_and_validity(
        values: ColumnVec,
        validity: BitVec,
    ) -> Result<NullableColumnVec, ColumnError> {
        if values.len() != validity.len() {
            return Err(ColumnError::LengthMismatch(values.len(), validity.len()));
        }
        Ok(NullableColumnVec { validity, values })
    }

    pub fn push_value(&mut self, value: &DataValue) -> Result<(), ColumnError> {
        if value.is_null() {
            self.validity.push(false);
            self.values.push_default();
            return Ok(());
        }
        match (&mut self.values, value) {
            (ColumnVec::Bool(vec), DataValue::Bool(val)) => vec.copy_push(val),
            (ColumnVec::I8(vec), DataValue::Int8(val)) => vec.copy_push(val),
            (ColumnVec::I16(vec), DataValue::Int16(val)) => vec.copy_push(val),
            (ColumnVec::I32(vec), DataValue::Int32(val)) => vec.copy_push(val),
            (ColumnVec::I64(vec), DataValue::Int64(val)) => vec.copy_push(val),
            (ColumnVec::F32(vec), DataValue::Float32(val)) => vec.copy_push(val),
            (ColumnVec::F64(vec), DataValue::Float64(val)) => vec.copy_push(val),
            (ColumnVec::I64(vec), DataValue::Date64(val)) => vec.copy_push(val),
            (ColumnVec::Str(vec), DataValue::Utf8(val)) => vec.copy_push(val),
            (ColumnVec::Binary(vec), DataValue::Binary(val)) => vec.copy_push(val),
            _ => return Err(ColumnError::InvalidValue),
        }
        self.validity.push(true);
        Ok(())
    }

    pub fn get_values(&self) -> &ColumnVec {
        &self.values
    }

    pub fn get_validity(&self) -> &BitVec {
        &self.validity
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }
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
