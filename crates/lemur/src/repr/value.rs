
use crate::repr::sort::{GroupRanges, SortPermutation};
use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::ops::Range;

use crate::repr::vec::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueType {
    Unknown,
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

/// A single, nullable scalar value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    /// A `Null` value is a value that does not carry any type information.
    ///
    /// When a null value is found during a user insert, it will be immediately
    /// parsed into this. Before query execution, this should be converted into
    /// value with type information (e.g. by infering from the rest of the query
    /// or consulting a catalog).
    Null,
    Bool(Option<bool>),
    Int8(Option<i8>),
    Int32(Option<i32>),
    Utf8(Option<String>),
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Unknown,
            Value::Bool(_) => ValueType::Bool,
            Value::Int8(_) => ValueType::Int8,
            Value::Int32(_) => ValueType::Int32,
            Value::Utf8(_) => ValueType::Utf8,
        }
    }

    /// Repeat self `n` times, returning a vector.
    pub fn repeat(&self, n: usize) -> Result<ValueVec> {
        Ok(match self {
            Value::Bool(v) => {
                let mut vec = BoolVec::with_capacity(n);
                vec.push(*v);
                vec.broadcast_single(n).unwrap();
                vec.into()
            }
            Value::Int8(v) => {
                let mut vec = Int8Vec::with_capacity(n);
                vec.push(*v);
                vec.broadcast_single(n).unwrap();
                vec.into()
            }
            Value::Int32(v) => {
                let mut vec = Int32Vec::with_capacity(n);
                vec.push(*v);
                vec.broadcast_single(n).unwrap();
                vec.into()
            }
            Value::Utf8(v) => {
                let mut vec = Utf8Vec::with_capacity(n);
                vec.push(v.as_ref().map(|s| s.as_str()));
                vec.broadcast_single(n).unwrap();
                vec.into()
            }
            Value::Null => {
                return Err(anyhow!(
                    "cannot create a repeating vector from a null value with no type information"
                ))
            }
        })
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null
            | Value::Bool(None)
            | Value::Int8(None)
            | Value::Int32(None)
            | Value::Utf8(None))
    }

    /// Cast self into some other type.
    pub fn try_cast(self, ty: &ValueType) -> Result<Self> {
        if self.is_null() {
            // Nulls may be cast to anything else.
            Ok(match ty {
                ValueType::Bool => Value::Bool(None),
                ValueType::Int8 => Value::Int8(None),
                ValueType::Int32 => Value::Int32(None),
                ValueType::Utf8 => Value::Utf8(None),
                ValueType::Unknown => return Err(anyhow!("cannot cast to type unknown")),
            })
        } else {
            // TODO: Actually cast.
            Ok(match (self, ty) {
                (Value::Bool(v), ValueType::Bool) => Value::Bool(v),
                (Value::Int8(v), ValueType::Int8) => Value::Int8(v),
                (Value::Int32(v), ValueType::Int32) => Value::Int32(v),
                (Value::Utf8(v), ValueType::Utf8) => Value::Utf8(v),
                (value, ty) => return Err(anyhow!("cannot cast '{:?}' to {:?}", value, ty)),
            })
        }
    }
}

impl From<Option<bool>> for Value {
    fn from(v: Option<bool>) -> Self {
        Value::Bool(v)
    }
}

impl From<Option<i8>> for Value {
    fn from(v: Option<i8>) -> Self {
        Value::Int8(v)
    }
}

impl From<Option<i32>> for Value {
    fn from(v: Option<i32>) -> Self {
        Value::Int32(v)
    }
}

impl From<Option<String>> for Value {
    fn from(v: Option<String>) -> Self {
        Value::Utf8(v)
    }
}

/// A list of some values representing a single row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn arity(&self) -> usize {
        self.values.len()
    }
}

impl From<Vec<Value>> for Row {
    fn from(values: Vec<Value>) -> Self {
        Row { values }
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
        BoolVec::from_iter(vals.iter().map(Some)).into()
    }

    pub fn int8s(vals: &[i8]) -> Self {
        Int8Vec::from_iter(vals.iter().map(Some)).into()
    }

    pub fn int32s(vals: &[i32]) -> Self {
        Int32Vec::from_iter(vals.iter().map(Some)).into()
    }

    pub fn utf8s(vals: &[&str]) -> Self {
        Utf8Vec::from_iter(vals.iter().map(|v| Some(*v))).into()
    }

    pub fn with_capacity_for_type(ty: &ValueType, cap: usize) -> Result<Self> {
        Ok(match ty {
            ValueType::Bool => BoolVec::with_capacity(cap).into(),
            ValueType::Int8 => Int8Vec::with_capacity(cap).into(),
            ValueType::Int32 => Int32Vec::with_capacity(cap).into(),
            ValueType::Utf8 => Utf8Vec::with_capacity(cap).into(),
            ValueType::Unknown => return Err(anyhow!("cannot create vector of unknown type")),
        })
    }

    pub fn downcast_bool_vec(&self) -> Option<&BoolVec> {
        match self {
            ValueVec::Bool(v) => Some(v),
            _ => None,
        }
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

    pub fn is_empty(&self) -> bool {
        match self {
            ValueVec::Bool(v) => v.is_empty(),
            ValueVec::Int8(v) => v.is_empty(),
            ValueVec::Int32(v) => v.is_empty(),
            ValueVec::Utf8(v) => v.is_empty(),
        }
    }

    pub fn is_singular_value(&self) -> bool {
        self.len() == 1
    }

    pub fn filter(&self, mask: &BitVec) -> Self {
        match self {
            ValueVec::Bool(v) => BoolVec::from_iter(make_filter_iter(v.iter(), mask)).into(),
            ValueVec::Int8(v) => Int8Vec::from_iter(make_filter_iter(v.iter(), mask)).into(),
            ValueVec::Int32(v) => Int32Vec::from_iter(make_filter_iter(v.iter(), mask)).into(),
            ValueVec::Utf8(v) => Utf8Vec::from_iter(make_filter_iter(v.iter(), mask)).into(),
        }
    }

    pub fn try_push(&mut self, value: Value) -> Result<()> {
        match (self, value) {
            (ValueVec::Bool(vec), Value::Bool(val)) => vec.push(val),
            (ValueVec::Int8(vec), Value::Int8(val)) => vec.push(val),
            (ValueVec::Int32(vec), Value::Int32(val)) => vec.push(val),
            (ValueVec::Utf8(vec), Value::Utf8(val)) => vec.push(val.as_deref()),
            (vec, val) => {
                return Err(anyhow!(
                    "cannot push value '{:?}' onto vec of type {:?}",
                    val,
                    vec.value_type()
                ))
            }
        };
        Ok(())
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

    pub fn first_value(&self) -> Option<Value> {
        Some(match self {
            ValueVec::Bool(v) => v.iter().next()?.cloned().into(),
            ValueVec::Int8(v) => v.iter().next()?.cloned().into(),
            ValueVec::Int32(v) => v.iter().next()?.cloned().into(),
            ValueVec::Utf8(v) => v.iter().next()?.map(|s| s.to_string()).into(),
        })
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

    /// Create a new vec that repeats each value `n` times.
    pub fn repeat_each_value(&self, n: usize) -> Self {
        match self {
            ValueVec::Bool(v) => BoolVec::from_iter(make_repeat_value_iter(v.iter(), n)).into(),
            ValueVec::Int8(v) => Int8Vec::from_iter(make_repeat_value_iter(v.iter(), n)).into(),
            ValueVec::Int32(v) => Int32Vec::from_iter(make_repeat_value_iter(v.iter(), n)).into(),
            ValueVec::Utf8(v) => Utf8Vec::from_iter(make_repeat_value_iter(v.iter(), n)).into(),
        }
    }

    /// Iterate over each value in the vector as a wrapped `Value` type.
    pub fn iter_values(&self) -> Box<dyn Iterator<Item = Value> + '_> {
        match self {
            ValueVec::Bool(v) => Box::new(v.iter().map(|v| Value::from(v.cloned()))),
            ValueVec::Int8(v) => Box::new(v.iter().map(|v| Value::from(v.cloned()))),
            ValueVec::Int32(v) => Box::new(v.iter().map(|v| Value::from(v.cloned()))),
            ValueVec::Utf8(v) => Box::new(v.iter().map(|v| Value::from(v.map(|v| v.to_string())))),
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
