//! Vectors using native types.
use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};

pub trait NativeType: Sync + Send + PartialEq + PartialOrd + std::fmt::Debug {}

impl NativeType for bool {}
impl NativeType for i8 {}
impl NativeType for i16 {}
impl NativeType for i32 {}
impl NativeType for i64 {}
impl NativeType for f32 {}
impl NativeType for f64 {}
impl NativeType for str {}
impl NativeType for [u8] {}

pub trait FixedLengthType: NativeType + Default + Copy + 'static {}

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

/// A vector holding fixed length values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixedLengthVec<T> {
    validity: BitVec,
    values: Vec<T>,
}

impl<T: FixedLengthType> FixedLengthVec<T> {
    pub fn empty() -> Self {
        Self::with_capacity(0)
    }

    pub fn one(val: Option<T>) -> Self {
        let mut v = Self::with_capacity(1);
        v.push(val);
        v
    }

    pub fn with_capacity(cap: usize) -> Self {
        FixedLengthVec {
            validity: BitVec::with_capacity(cap),
            values: Vec::with_capacity(cap),
        }
    }

    pub fn from_iter_all_valid<I>(values: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        let values: Vec<_> = values.into_iter().collect();
        let validity = BitVec::repeat(true, values.len());
        FixedLengthVec { validity, values }
    }

    pub fn from_values_and_validity(values: Vec<T>, validity: BitVec) -> Result<Self> {
        if values.len() != validity.len() {
            return Err(anyhow!("values and validity are not the same length"));
        }
        Ok(FixedLengthVec { validity, values })
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn push(&mut self, val: Option<T>) {
        match val {
            Some(val) => {
                self.validity.push(true);
                self.values.push(val)
            }
            None => {
                self.validity.push(false);
                self.values.push(T::default());
            }
        }
    }

    /// Insert a value at some index. Panics if the index is out of bounds.
    pub fn insert(&mut self, idx: usize, val: Option<T>) {
        match val {
            Some(val) => {
                self.validity.insert(idx, true);
                self.values.insert(idx, val);
            }
            None => {
                self.validity.insert(idx, false);
                self.values.insert(idx, T::default());
            }
        }
    }

    /// Get a value at some index.
    ///
    /// Returns `None` if the index falls outside of the vector.
    ///
    /// Returns `Some(None)` if the value at index is not valid.
    pub fn get(&self, idx: usize) -> Option<Option<&T>> {
        if *self.validity.get(idx)? {
            Some(Some(self.values.get(idx).unwrap()))
        } else {
            Some(None)
        }
    }

    /// Get value at index, ignoring its validity.
    pub fn get_value(&self, idx: usize) -> Option<&T> {
        self.values.get(idx)
    }

    pub fn retain(&mut self, selectivity: &BitVec) {
        assert_eq!(self.len(), selectivity.len());

        let mut iter = selectivity.iter();
        self.validity.retain(|_, _| *iter.next().unwrap());

        let mut iter = selectivity.iter();
        self.values.retain(|_| *iter.next().unwrap());
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&T>> {
        zip_with_validity(self.values.iter(), &self.validity)
    }

    pub fn iter_values(&self) -> impl Iterator<Item = &T> {
        self.values.iter()
    }
}

/// Vector type for storing variable length values.
///
/// Normally should use `BinaryVec` or `Utf8Vec` instead of this.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarLengthVec {
    validity: BitVec,
    offsets: Vec<usize>,
    data: Vec<u8>,
}

impl VarLengthVec {
    pub fn empty() -> Self {
        Self::with_capacity(0)
    }

    pub fn one<T: AsRef<[u8]> + ?Sized>(val: Option<&T>) -> Self {
        let mut v = Self::with_capacity(0);
        v.push(val);
        v
    }

    pub fn with_capacity(cap: usize) -> Self {
        let mut offsets = Vec::with_capacity(cap + 1);
        offsets.push(0);
        let data = Vec::with_capacity(cap); // TODO: Determine suitable cap here.
        let validity = BitVec::with_capacity(cap);
        VarLengthVec {
            validity,
            offsets,
            data,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Push a value onto the vector. Copies the bytes from val.
    pub fn push<T: AsRef<[u8]> + ?Sized>(&mut self, val: Option<&T>) {
        match val {
            Some(val) => {
                self.validity.push(true);
                self.data.extend_from_slice(val.as_ref());
                let next_offset = self.data.len();
                self.offsets.push(next_offset);
            }
            None => {
                self.validity.push(false);
                self.offsets.push(self.data.len());
            }
        }
    }

    /// Insert a value into the vector. Copies the bytes from val. Panics if
    /// index is out of bounds.
    pub fn insert<T: AsRef<[u8]> + ?Sized>(&mut self, idx: usize, val: Option<&T>) {
        match val {
            Some(val) => {
                self.validity.insert(idx, true);

                let buf = val.as_ref();

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
            None => {
                self.validity.insert(idx, false);
                let offset = self.offsets[idx];
                self.offsets.insert(idx + 1, offset);
            }
        }
    }

    /// Get a value from the vector.
    ///
    /// Returns None if index is not in bounds.
    ///
    /// Returns Some(None) if the value is not valid.
    pub fn get<T: BytesRef + ?Sized>(&self, idx: usize) -> Option<Option<&T>> {
        if *self.validity.get(idx)? {
            let start = *self.offsets.get(idx)?;
            let end = *self.offsets.get(idx + 1)?;
            let buf = &self.data[start..end];
            Some(Some(T::from_bytes(buf)))
        } else {
            Some(None)
        }
    }

    /// Get a value, ignoring its validity.
    pub fn get_value<T: BytesRef + ?Sized>(&self, idx: usize) -> Option<&T> {
        let start = *self.offsets.get(idx)?;
        let end = *self.offsets.get(idx + 1)?;
        let buf = &self.data[start..end];
        Some(T::from_bytes(buf))
    }

    pub fn retain(&mut self, selectivity: &BitVec) {
        assert_eq!(self.len(), selectivity.len());

        // Retain values.

        let mut new_offsets = Vec::with_capacity(selectivity.count_ones() + 1);

        let mut offsets = self.offsets.iter();
        let mut curr_offset = *offsets.next().unwrap();
        new_offsets.push(curr_offset);

        let data_len = self.data.len();
        let mut offset_sub = 0;
        for (sel, &next_offset) in selectivity.iter().zip(offsets) {
            let next_offset = next_offset - offset_sub;
            if *sel {
                new_offsets.push(next_offset);
                curr_offset = next_offset;
            } else {
                self.data.copy_within(next_offset..data_len, curr_offset);
                offset_sub += next_offset - curr_offset;
            }
        }

        self.offsets = new_offsets;

        // Retain validity.

        let mut iter = selectivity.iter();
        self.validity.retain(|_, _| *iter.next().unwrap());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryVec(VarLengthVec);

impl BinaryVec {
    pub fn empty() -> Self {
        VarLengthVec::empty().into()
    }

    pub fn with_capacity(cap: usize) -> Self {
        VarLengthVec::with_capacity(cap).into()
    }

    pub fn one(val: Option<&[u8]>) -> Self {
        VarLengthVec::one(val).into()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn push(&mut self, val: Option<&[u8]>) {
        self.0.push(val)
    }

    pub fn insert(&mut self, idx: usize, val: Option<&[u8]>) {
        self.0.insert(idx, val)
    }

    pub fn get(&self, idx: usize) -> Option<Option<&[u8]>> {
        self.0.get(idx)
    }

    pub fn get_value(&self, idx: usize) -> Option<&[u8]> {
        self.0.get_value(idx)
    }

    pub fn retain(&mut self, selectivity: &BitVec) {
        self.0.retain(selectivity);
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        zip_with_validity(self.iter_values(), &self.0.validity)
    }

    pub fn iter_values(&self) -> BinaryIter<'_> {
        BinaryIter {
            vec: &self.0,
            idx: 0,
        }
    }
}

impl From<VarLengthVec> for BinaryVec {
    fn from(v: VarLengthVec) -> Self {
        BinaryVec(v)
    }
}

pub struct BinaryIter<'a> {
    vec: &'a VarLengthVec,
    idx: usize,
}

impl<'a> Iterator for BinaryIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.vec.get_value(self.idx)?;
        self.idx += 1;
        Some(item)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Utf8Vec(VarLengthVec);

impl Utf8Vec {
    pub fn empty() -> Self {
        VarLengthVec::empty().into()
    }

    pub fn with_capacity(cap: usize) -> Self {
        VarLengthVec::with_capacity(cap).into()
    }

    pub fn one(val: Option<&str>) -> Self {
        VarLengthVec::one(val).into()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn push(&mut self, val: Option<&str>) {
        self.0.push(val)
    }

    pub fn insert(&mut self, idx: usize, val: Option<&str>) {
        self.0.insert(idx, val)
    }

    pub fn get(&self, idx: usize) -> Option<Option<&str>> {
        self.0.get(idx)
    }

    pub fn get_value(&self, idx: usize) -> Option<&str> {
        self.0.get_value(idx)
    }

    pub fn retain(&mut self, selectivity: &BitVec) {
        self.0.retain(selectivity);
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&str>> {
        zip_with_validity(self.iter_values(), &self.0.validity)
    }

    pub fn iter_values(&self) -> Utf8Iter<'_> {
        Utf8Iter {
            vec: &self.0,
            idx: 0,
        }
    }
}

impl From<VarLengthVec> for Utf8Vec {
    fn from(v: VarLengthVec) -> Self {
        Utf8Vec(v)
    }
}

impl From<Vec<&str>> for Utf8Vec {
    fn from(vals: Vec<&str>) -> Self {
        let mut v = Utf8Vec::empty();
        for val in vals.into_iter() {
            v.push(Some(val));
        }
        v
    }
}

pub struct Utf8Iter<'a> {
    vec: &'a VarLengthVec,
    idx: usize,
}

impl<'a> Iterator for Utf8Iter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.vec.get_value(self.idx)?;
        self.idx += 1;
        Some(item)
    }
}

pub type BoolVec = FixedLengthVec<bool>; // TODO: Change to bitvec.
pub type Int8Vec = FixedLengthVec<i8>;
pub type Int16Vec = FixedLengthVec<i16>;
pub type Int32Vec = FixedLengthVec<i32>;
pub type Int64Vec = FixedLengthVec<i64>;
pub type Float32Vec = FixedLengthVec<f32>;
pub type Float64Vec = FixedLengthVec<f64>;

/// Zip an iterator with a validity vector.
///
/// Assumes the iter is the same length as the bitvec.
fn zip_with_validity<'a, I, T>(iter: I, validity: &'a BitVec) -> impl Iterator<Item = Option<&'a T>>
where
    I: Iterator<Item = &'a T>,
    T: 'a + ?Sized,
{
    validity
        .iter()
        .by_vals()
        .zip(iter)
        .map(|(valid, value)| if valid { Some(value) } else { None })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitvec::prelude::*;

    #[test]
    fn varlen_mutation() {
        let mut v = Utf8Vec::with_capacity(256);
        v.push(Some("one"));
        v.push(Some("two"));
        v.push(Some("four"));

        v.insert(2, Some("three"));

        assert_eq!(Some("two"), v.get(1).unwrap());
        assert_eq!(Some("three"), v.get(2).unwrap());
        assert_eq!(Some("four"), v.get(3).unwrap());

        v.insert(2, None);

        assert_eq!(Some("two"), v.get(1).unwrap());
        assert_eq!(None, v.get(2).unwrap());
        assert_eq!(Some("three"), v.get(3).unwrap());
        assert_eq!(Some("four"), v.get(4).unwrap());
    }

    #[test]
    fn varlen_iter() {
        let vals = vec!["one", "two", "three", "four"];
        let vec: Utf8Vec = vals.clone().into();

        for (expected, got) in vals.iter().zip(vec.iter()) {
            assert_eq!(Some(*expected), got);
        }
    }

    #[test]
    fn varlen_retain() {
        let mut vec: Utf8Vec = vec!["one", "two", "three", "four"].into();

        // All
        let bm = bitvec![1, 1, 1, 1];
        vec.retain(&bm);
        assert_eq!(4, vec.len());
        let got: Vec<_> = vec.iter().collect();
        assert_eq!(
            vec![Some("one"), Some("two"), Some("three"), Some("four")],
            got
        );

        // Some
        let bm = bitvec![1, 0, 0, 1];
        vec.retain(&bm);
        assert_eq!(2, vec.len());
        let got: Vec<_> = vec.iter().collect();
        assert_eq!(vec![Some("one"), Some("four")], got);

        // None
        let bm = bitvec![0, 0];
        vec.retain(&bm);
        assert_eq!(0, vec.len());
    }
}
