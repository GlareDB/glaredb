use bitvec::vec::BitVec;
use anyhow::{Result};
use crate::repr::sort::{SortPermutation, GroupRanges};
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::ops::Range;

use crate::repr::vec::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    Bool,
    Int8,
    Int32,
    Utf8,
}

impl ValueType {
    pub fn is_numeric(&self) -> bool {
        matches!(self, ValueType::Int8 | ValueType::Int32)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueVec {
    Bool(BoolVec),
    Int8(Int8Vec),
    Int32(Int32Vec),
    Utf8(Utf8Vec),
}

impl ValueVec {
    pub fn bools(vals: &[bool]) -> Self {
        BoolVec::from_iter(vals.iter().map(|v| Some(v))).into()
    }

    pub fn int8s(vals: &[i8]) -> Self {
        Int8Vec::from_iter(vals.iter().map(|v| Some(v))).into()
    }

    pub fn int32s(vals: &[i32]) -> Self {
        Int32Vec::from_iter(vals.iter().map(|v| Some(v))).into()
    }

    pub fn utf8s(vals: &[&str]) -> Self {
        Utf8Vec::from_iter(vals.iter().map(|v| Some(*v))).into()
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            ValueVec::Bool(_) => ValueType::Bool,
            ValueVec::Int8(_) => ValueType::Int8,
            ValueVec::Int32(_) => ValueType::Int32,
            ValueVec::Utf8(_) => ValueType::Utf8,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ValueVec::Bool(v) => v.len(),
            ValueVec::Int8(v) => v.len(),
            ValueVec::Int32(v) => v.len(),
            ValueVec::Utf8(v) => v.len(),
        }
    }

    pub fn is_singular_value(&self) -> bool {
        self.len() == 1
    }

    pub fn filter(&self, mask: &BitVec) -> Self {
        match self {
            ValueVec::Bool(v) => BoolVec::from_iter(make_filter_iter(v.iter(), &mask)).into(),
            ValueVec::Int8(v) => Int8Vec::from_iter(make_filter_iter(v.iter(), &mask)).into(),
            ValueVec::Int32(v) => Int32Vec::from_iter(make_filter_iter(v.iter(), &mask)).into(),
            ValueVec::Utf8(v) => Utf8Vec::from_iter(make_filter_iter(v.iter(), &mask)).into(),
        }
    }

    /// Try to append other to the end of self.
    ///
    /// `other` will be returned unchanged if the vector types don't match.
    pub fn try_append(&mut self, other: Self) -> Result<(), Self> {
        match (self, other) {
            (ValueVec::Bool(v1), ValueVec::Bool(v2)) => v1.append(v2),
            (ValueVec::Int8(v1), ValueVec::Int8(v2)) => v1.append(v2),
            (ValueVec::Int32(v1), ValueVec::Int32(v2)) => v1.append(v2),
            (ValueVec::Utf8(v1), ValueVec::Utf8(v2)) => v1.append(v2),
            (_, other) => return Err(other),
        }
        Ok(())
    }

    pub fn group_ranges_at(&self, range: &Range<usize>) -> GroupRanges {
        match self {
            ValueVec::Bool(v) => v.group_ranges_at(range),
            ValueVec::Int8(v) => v.group_ranges_at(range),
            ValueVec::Int32(v) => v.group_ranges_at(range),
            ValueVec::Utf8(v) => v.group_ranges_at(range),
        }
    }

    pub fn apply_permutation_at(&mut self, range: &Range<usize>, perm: &SortPermutation) {
        match self {
            ValueVec::Bool(v) => v.apply_permutation_at(range, perm),
            ValueVec::Int8(v) => v.apply_permutation_at(range, perm),
            ValueVec::Int32(v) => v.apply_permutation_at(range, perm),
            ValueVec::Utf8(v) => v.apply_permutation_at(range, perm),
        }
    }

    pub fn sort_each_group(&mut self, groups: &GroupRanges) -> Vec<SortPermutation> {
        match self {
            ValueVec::Bool(v) => v.sort_each_group(groups),
            ValueVec::Int8(v) => v.sort_each_group(groups),
            ValueVec::Int32(v) => v.sort_each_group(groups),
            ValueVec::Utf8(v) => v.sort_each_group(groups),
        }
    }

    pub fn broadcast_single(&mut self, len: usize) -> Result<()> {
        match self {
            ValueVec::Bool(v) => v.broadcast_single(len),
            ValueVec::Int8(v) => v.broadcast_single(len),
            ValueVec::Int32(v) => v.broadcast_single(len),
            ValueVec::Utf8(v) => v.broadcast_single(len),
        }
    }

    pub fn from_vec_repeat_each_value(&self, n: usize) -> Self {
        match self {
            ValueVec::Bool(v) => BoolVec::from_iter(make_repeat_value_iter(v.iter(), n)).into(),
            ValueVec::Int8(v) => Int8Vec::from_iter(make_repeat_value_iter(v.iter(), n)).into(),
            ValueVec::Int32(v) => Int32Vec::from_iter(make_repeat_value_iter(v.iter(), n)).into(),
            ValueVec::Utf8(v) => Utf8Vec::from_iter(make_repeat_value_iter(v.iter(), n)).into(),
        }
    }
}

impl From<BoolVec> for ValueVec {
    fn from(v: BoolVec) -> Self {
        ValueVec::Bool(v)
    }
}

impl From<Int8Vec> for ValueVec {
    fn from(v: Int8Vec) -> Self {
        ValueVec::Int8(v)
    }
}

impl From<Int32Vec> for ValueVec {
    fn from(v: Int32Vec) -> Self {
        ValueVec::Int32(v)
    }
}

impl From<Utf8Vec> for ValueVec {
    fn from(v: Utf8Vec) -> Self {
        ValueVec::Utf8(v)
    }
}

impl PartialEq for ValueVec {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        match (self, other) {
            (ValueVec::Bool(a), ValueVec::Bool(b)) => a.iter().zip(b.iter()).all(|(a, b)| a == b),
            (ValueVec::Int8(a), ValueVec::Int8(b)) => a.iter().zip(b.iter()).all(|(a, b)| a == b),
            (ValueVec::Int32(a), ValueVec::Int32(b)) => a.iter().zip(b.iter()).all(|(a, b)| a == b),
            (ValueVec::Utf8(a), ValueVec::Utf8(b)) => a.iter().zip(b.iter()).all(|(a, b)| a == b),
            _ => false,
        }
    }
}

/// Create an iterator that filters based on the mask.
fn make_filter_iter<'a, I: 'a, T>(iter: I, mask: &'a BitVec) -> impl Iterator<Item = T> + 'a
where
    I: Iterator<Item = T>,
{
    iter.zip(mask.iter().by_vals())
        .filter(|(_, b)| *b)
        .map(|(v, _)| v)
}

fn make_repeat_value_iter<I, T>(iter: I, n: usize) -> impl Iterator<Item = T>
where
    T: Clone,
    I: Iterator<Item = T>,
{
    iter.flat_map(move |v| std::iter::repeat(v).take(n))
}
