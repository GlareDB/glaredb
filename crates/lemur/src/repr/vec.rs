use crate::repr::ordfloat::{OrdF32, OrdF64};
use crate::repr::sort::{GroupRanges, SortPermutation};
use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};
use std::ops::Range;

pub trait NativeType: Sync + Send + PartialEq + PartialOrd + Ord + std::fmt::Debug {}

impl NativeType for bool {}
impl NativeType for i8 {}
impl NativeType for i16 {}
impl NativeType for i32 {}
impl NativeType for i64 {}
impl NativeType for OrdF32 {}
impl NativeType for OrdF64 {}
impl NativeType for str {}
impl NativeType for [u8] {}

pub trait FixedLengthType: NativeType + Default + Copy + 'static {}

impl FixedLengthType for bool {}
impl FixedLengthType for i8 {}
impl FixedLengthType for i16 {}
impl FixedLengthType for i32 {}
impl FixedLengthType for i64 {}
impl FixedLengthType for OrdF32 {}
impl FixedLengthType for OrdF64 {}

pub trait VarLengthType: NativeType {}

impl VarLengthType for str {}
impl VarLengthType for [u8] {}

/// Types that can be converted to and from binary slices.
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

    pub fn from_parts(validity: BitVec, values: Vec<T>) -> Self {
        assert_eq!(validity.len(), values.len());
        FixedLengthVec { validity, values }
    }

    pub fn split_off(&mut self, at: usize) -> Self {
        let validity = self.validity.split_off(at);
        let values = self.values.split_off(at);
        FixedLengthVec { validity, values }
    }

    pub fn get_validity(&self) -> &BitVec {
        &self.validity
    }

    pub fn push(&mut self, val: Option<T>) {
        match val {
            Some(val) => {
                self.validity.push(true);
                self.values.push(val)
            }
            None => {
                self.validity.push(false);
                self.values.push(T::default())
            }
        }
    }

    pub fn append(&mut self, mut other: Self) {
        self.validity.append(&mut other.validity);
        self.values.append(&mut other.values);
    }

    pub fn group_ranges_at(&self, range: &Range<usize>) -> GroupRanges {
        let values = self.values.get(range.start..range.end).unwrap();
        GroupRanges::from_slice(values)
    }

    /// Apply a permutation to some part of the vector.
    pub fn apply_permutation_at(&mut self, range: &Range<usize>, perm: &SortPermutation) {
        let values = self.values.get_mut(range.start..range.end).unwrap();
        let validity = self
            .validity
            .as_mut_bitslice()
            .get_mut(range.start..range.end)
            .unwrap();

        perm.apply_to(values);
        perm.apply_to_bitslice(validity);
    }

    /// Sort each group defined in the provided ranges. Sort permutations
    /// will be returned for each group.
    ///
    /// Panics if any of the ranges is out of bounds.
    pub fn sort_each_group(&mut self, groups: &GroupRanges) -> Vec<SortPermutation> {
        let mut perms = Vec::with_capacity(groups.ranges.len());

        for range in groups.ranges.iter() {
            // TODO: Sort nulls last.

            let vals = self.values.get_mut(range.start..range.end).unwrap();
            let perm = SortPermutation::from_slice(vals);
            let validity = self
                .validity
                .as_mut_bitslice()
                .get_mut(range.start..range.end)
                .unwrap();
            perm.apply_to_bitslice(validity);

            perms.push(perm);
        }

        perms
    }

    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn is_empty(&self) -> bool {
        self.validity.is_empty()
    }

    pub fn resize_null(&mut self, len: usize) {
        self.validity.resize(len, false);
        self.values.resize(len, T::default());
    }

    pub fn broadcast_single(&mut self, len: usize) -> Result<()> {
        if self.len() != 1 {
            return Err(anyhow!("can only broadcast single value vectors"));
        }
        self.validity.resize(len, self.validity[0]);
        self.values.resize(len, self.values[0]);
        Ok(())
    }

    pub fn get_value(&self, idx: usize) -> Option<&T> {
        self.values.get(idx)
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&T>> {
        zip_with_validity(self.iter_values(), &self.validity)
    }

    pub fn iter_values(&self) -> impl Iterator<Item = &T> {
        self.values.iter()
    }

    pub fn iter_validity(&self) -> impl Iterator<Item = bool> + '_ {
        self.validity.iter().by_vals()
    }
}

impl<'a, T: FixedLengthType> FromIterator<Option<&'a T>> for FixedLengthVec<T> {
    fn from_iter<I: IntoIterator<Item=Option<&'a T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut vec = Self::with_capacity(lower);

        for val in iter {
            vec.push(val.cloned());
        }

        vec
    }
}

/// Vector type for storing variable length values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VarLengthVec {
    validity: BitVec,
    offsets: Vec<usize>,
    sizes: Vec<usize>,
    data: Vec<u8>,
}

impl VarLengthVec {
    pub fn empty() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let mut offsets = Vec::with_capacity(cap + 1);
        offsets.push(0);
        let data = Vec::with_capacity(cap); // TODO: Determine suitable cap here.
        let sizes = Vec::with_capacity(cap);
        VarLengthVec {
            validity: BitVec::with_capacity(cap),
            offsets,
            sizes,
            data,
        }
    }

    pub fn one<T>(val: Option<&T>) -> Self
    where
        T: BytesRef + ?Sized,
    {
        let mut v = Self::with_capacity(1);
        v.push(val);
        v
    }

    pub fn get_validity(&self) -> &BitVec {
        &self.validity
    }

    pub fn push<T: BytesRef + ?Sized>(&mut self, val: Option<&T>) {
        match val {
            Some(val) => {
                self.validity.push(true);
                self.data.extend_from_slice(val.as_ref());
                let next_offset = self.data.len();
                self.offsets.push(next_offset);
                self.sizes.push(val.as_ref().len());
            }
            None => {
                self.validity.push(false);
                self.offsets.push(self.data.len());
                self.sizes.push(0);
            }
        }
    }

    pub fn group_ranges_at(&self, range: &Range<usize>) -> GroupRanges {
        // TODO: Implement.

        GroupRanges::single_group(range.len())
    }

    /// See `FixedLengthVec::apply_permutation_at` for more info.
    pub fn apply_permutation_at(&mut self, range: &Range<usize>, perm: &SortPermutation) {
        let offsets = self.offsets.get_mut(range.start..range.end).unwrap();
        let sizes = self.sizes.get_mut(range.start..range.end).unwrap();
        let validity = self
            .validity
            .as_mut_bitslice()
            .get_mut(range.start..range.end)
            .unwrap();

        perm.apply_to(offsets);
        perm.apply_to(sizes);
        perm.apply_to_bitslice(validity);
        // Data does not need to be rearranged as the `offsets` and `sizes` vecs
        // are enough for us to index into data.
    }

    /// See `FixedLengthVec::sort_each_group` for more info.
    pub fn sort_each_group(&mut self, groups: &GroupRanges) -> Vec<SortPermutation> {
        // TODO: Implement

        groups
            .iter_lens()
            .map(SortPermutation::same_order)
            .collect()
    }

    pub fn append(&mut self, mut other: Self) {
        self.validity.append(&mut other.validity);
        self.sizes.append(&mut other.sizes);

        // Remove unused space in the original data vector.
        self.data.truncate(self.offsets[self.offsets.len() - 1]);
        let data_offset = self.data.len();

        self.data.append(&mut other.data);

        other
            .offsets
            .iter_mut()
            .for_each(|offset| *offset += data_offset);
        // Offsets vec always has a trailing offset indicating the "next" offset
        // to use. Remove from both before appending.
        other.offsets.pop();
        self.offsets.pop();

        self.offsets.append(&mut other.offsets);
        self.offsets.push(self.data.len());
    }

    pub fn resize_null(&mut self, len: usize) {
        self.validity.resize(len, false);
        self.sizes.resize(len, 0);

        let last_offset = self.offsets.pop().unwrap_or(0);
        self.offsets.resize(len, last_offset);
        self.offsets.push(last_offset);
    }

    pub fn broadcast_single(&mut self, len: usize) -> Result<()> {
        println!("broadcasting: {:?}, len: {}", self, self.len());
        if self.len() != 1 {
            return Err(anyhow!("can only broadcast single value vectors"));
        }
        self.validity.resize(len, self.validity[0]);
        self.sizes.resize(len, self.sizes[0]);
        let last_offset = self.offsets.pop().unwrap();
        self.offsets.resize(len, self.offsets[0]);
        self.offsets.push(last_offset);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a value, ignoring its validity.
    pub fn get_value<T: BytesRef + ?Sized>(&self, idx: usize) -> Option<&T> {
        let start = *self.offsets.get(idx)?;
        let size = *self.sizes.get(idx)?;
        let buf = &self.data[start..start + size];
        Some(T::from_bytes(buf))
    }

    pub fn iter_validity(&self) -> impl Iterator<Item = bool> + '_ {
        self.validity.iter().by_vals()
    }
}

impl<'a, T: BytesRef + ?Sized + 'a> FromIterator<Option<&'a T>> for VarLengthVec {
    fn from_iter<I: IntoIterator<Item = Option<&'a T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut vec = Self::with_capacity(lower);

        for val in iter {
            vec.push(val);
        }

        vec
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Utf8Vec(VarLengthVec);

impl Utf8Vec {
    pub fn empty() -> Self {
        Self(VarLengthVec::empty())
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self(VarLengthVec::with_capacity(cap))
    }

    pub fn one(val: Option<&str>) -> Self {
        Self(VarLengthVec::one(val))
    }

    pub fn push(&mut self, val: Option<&str>) {
        self.0.push(val)
    }

    pub fn get_validity(&self) -> &BitVec {
        self.0.get_validity()
    }

    pub fn group_ranges_at(&self, range: &Range<usize>) -> GroupRanges {
        self.0.group_ranges_at(range)
    }

    pub fn apply_permutation_at(&mut self, range: &Range<usize>, perm: &SortPermutation) {
        self.0.apply_permutation_at(range, perm)
    }

    pub fn sort_each_group(&mut self, groups: &GroupRanges) -> Vec<SortPermutation> {
        self.0.sort_each_group(groups)
    }

    pub fn append(&mut self, other: Self) {
        self.0.append(other.0)
    }

    pub fn resize_null(&mut self, len: usize) {
        self.0.resize_null(len)
    }

    pub fn broadcast_single(&mut self, len: usize) -> Result<()> {
        self.0.broadcast_single(len)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get_value(&self, idx: usize) -> Option<&str> {
        self.0.get_value(idx)
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

    pub fn iter_validity(&self) -> impl Iterator<Item = bool> + '_ {
        self.0.iter_validity()
    }
}

impl<'a> FromIterator<Option<&'a str>> for Utf8Vec {
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        Self(VarLengthVec::from_iter(iter))
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinaryVec(VarLengthVec);

impl BinaryVec {
    // TODO: Implement
    pub fn empty() -> Self {
        Self(VarLengthVec::empty())
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self(VarLengthVec::with_capacity(cap))
    }

    pub fn one(val: Option<&[u8]>) -> Self {
        Self(VarLengthVec::one(val))
    }

    pub fn from_iter<'a>(iter: impl IntoIterator<Item = Option<&'a [u8]>>) -> Self {
        Self(VarLengthVec::from_iter(iter))
    }

    pub fn push(&mut self, val: Option<&[u8]>) {
        self.0.push(val)
    }

    pub fn get_validity(&self) -> &BitVec {
        self.0.get_validity()
    }

    pub fn group_ranges_at(&self, range: &Range<usize>) -> GroupRanges {
        self.0.group_ranges_at(range)
    }

    pub fn apply_permutation_at(&mut self, range: &Range<usize>, perm: &SortPermutation) {
        self.0.apply_permutation_at(range, perm)
    }

    pub fn sort_each_group(&mut self, groups: &GroupRanges) -> Vec<SortPermutation> {
        self.0.sort_each_group(groups)
    }

    pub fn append(&mut self, other: Self) {
        self.0.append(other.0)
    }

    pub fn resize_null(&mut self, len: usize) {
        self.0.resize_null(len)
    }

    pub fn broadcast_single(&mut self, len: usize) -> Result<()> {
        self.0.broadcast_single(len)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get_value(&self, idx: usize) -> Option<&[u8]> {
        self.0.get_value(idx)
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

    pub fn iter_validity(&self) -> impl Iterator<Item = bool> + '_ {
        self.0.iter_validity()
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

    #[test]
    fn fixedlen_apply_permutation() {
        let mut v = Int32Vec::empty();
        v.push(Some(1));
        v.push(Some(2));
        v.push(Some(3));
        let perm = SortPermutation::reverse_order(v.len());
        v.apply_permutation_at(&Range { start: 0, end: 3 }, &perm);

        let out: Vec<_> = v.iter().map(|v| v.cloned()).collect();
        assert_eq!(vec![Some(3), Some(2), Some(1)], out);
    }

    #[test]
    fn varlen_push() {
        let mut vec = Utf8Vec::empty();
        vec.push(Some("hello"));
    }
}
