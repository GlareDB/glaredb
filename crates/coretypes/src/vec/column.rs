//! Module for providing a `ColumnVec`.
//!
//! The `ColumnVec` is a single type for holding all possible vector types. Most
//! of the system will make use of this type. And this is really the module that
//! ties together the underlying native types and the database system
//! `DataType`.
//!
//! There's a heavy use of macros for dispatching function calls to the
//! underlying vector.
use super::*;
use crate::datatype::{DataType, DataValue};
use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;
use paste::paste;
use serde::{Deserialize, Serialize};

/// Column vector variants for all types supported by the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnVec {
    Bool(BoolVec),
    Int8(Int8Vec),
    Int16(Int16Vec),
    Int32(Int32Vec),
    Int64(Int64Vec),
    Float32(Float32Vec),
    Float64(Float64Vec),
    Utf8(Utf8Vec),
    Binary(BinaryVec),
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

        pub fn retain(&mut self, selectivity: &BitVec) {
            match self {
                $(
                    Self::$variant(v) => v.retain(selectivity),
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

macro_rules! cvec_impl_from_typed_vec {
    ($($variant:ident),*) => {
        $(
            paste! {
                impl From<[<$variant Vec>]> for ColumnVec {
                    fn from(vec: [<$variant Vec>]) -> ColumnVec {
                        ColumnVec::$variant(vec)
                    }
                }
            }
        )*
    };
}

cvec_impl_from_typed_vec!(Bool, Int8, Int16, Int32, Int64, Float32, Float64, Utf8, Binary);

impl ColumnVec {
    pub fn one(value: &DataValue, datatype: &DataType) -> Self {
        let mut v = Self::with_capacity_for_type(1, datatype);
        v.push_value(value).unwrap();
        v
    }

    pub fn with_capacity_for_type(cap: usize, datatype: &DataType) -> Self {
        match datatype {
            DataType::Bool => ColumnVec::Bool(BoolVec::with_capacity(cap)),
            DataType::Int8 => ColumnVec::Int8(Int8Vec::with_capacity(cap)),
            DataType::Int16 => ColumnVec::Int16(Int16Vec::with_capacity(cap)),
            DataType::Int32 => ColumnVec::Int32(Int32Vec::with_capacity(cap)),
            DataType::Int64 => ColumnVec::Int64(Int64Vec::with_capacity(cap)),
            DataType::Float32 => ColumnVec::Float32(Float32Vec::with_capacity(cap)),
            DataType::Float64 => ColumnVec::Float64(Float64Vec::with_capacity(cap)),
            DataType::Date64 => ColumnVec::Int64(Int64Vec::with_capacity(cap)),
            DataType::Utf8 => ColumnVec::Utf8(Utf8Vec::with_capacity(cap)),
            DataType::Binary => ColumnVec::Binary(BinaryVec::with_capacity(cap)),
        }
    }

    pub fn push_value(&mut self, value: &DataValue) -> Result<()> {
        Ok(match (self, value) {
            (col, DataValue::Null) => col.push_null(),
            (ColumnVec::Bool(a), DataValue::Bool(b)) => a.push(Some(*b)),
            (ColumnVec::Int8(a), DataValue::Int8(b)) => a.push(Some(*b)),
            (ColumnVec::Int16(a), DataValue::Int16(b)) => a.push(Some(*b)),
            (ColumnVec::Int32(a), DataValue::Int32(b)) => a.push(Some(*b)),
            (ColumnVec::Int64(a), DataValue::Int64(b)) => a.push(Some(*b)),
            (ColumnVec::Float32(a), DataValue::Float32(b)) => a.push(Some(*b)),
            (ColumnVec::Float64(a), DataValue::Float64(b)) => a.push(Some(*b)),
            (ColumnVec::Int64(a), DataValue::Date64(b)) => a.push(Some(*b)),
            (ColumnVec::Utf8(a), DataValue::Utf8(b)) => a.push(Some(b)),
            (ColumnVec::Binary(a), DataValue::Binary(b)) => a.push(Some(b)),
            _ => return Err(anyhow!("unsupported push")),
        })
    }

    pub fn push_null(&mut self) {
        match self {
            ColumnVec::Bool(a) => a.push(None),
            ColumnVec::Int8(a) => a.push(None),
            ColumnVec::Int16(a) => a.push(None),
            ColumnVec::Int32(a) => a.push(None),
            ColumnVec::Int64(a) => a.push(None),
            ColumnVec::Float32(a) => a.push(None),
            ColumnVec::Float64(a) => a.push(None),
            ColumnVec::Utf8(a) => a.push(None),
            ColumnVec::Binary(a) => a.push(None),
        }
    }

    cvec_common!(Bool, Int8, Int16, Int32, Int64, Float32, Float64, Utf8, Binary);
    cvec_try_as_dispatch!(Bool, Int8, Int16, Int32, Int64, Float32, Float64, Utf8, Binary);
}

// Macros useful for implementing the relevant compute traits.

macro_rules! match_column_column {
    ($col1:ident, $col2:ident, $op:ident) => {
        Ok(match ($col1, $col2) {
            (ColumnVec::Bool(a), ColumnVec::Bool(b)) => a.$op(b)?.into(),
            (ColumnVec::Int8(a), ColumnVec::Int8(b)) => a.$op(b)?.into(),
            (ColumnVec::Int16(a), ColumnVec::Int16(b)) => a.$op(b)?.into(),
            (ColumnVec::Int32(a), ColumnVec::Int32(b)) => a.$op(b)?.into(),
            (ColumnVec::Int64(a), ColumnVec::Int64(b)) => a.$op(b)?.into(),
            (ColumnVec::Float32(a), ColumnVec::Float32(b)) => a.$op(b)?.into(),
            (ColumnVec::Float64(a), ColumnVec::Float64(b)) => a.$op(b)?.into(),
            (ColumnVec::Utf8(a), ColumnVec::Utf8(b)) => a.$op(b)?.into(),
            (ColumnVec::Binary(a), ColumnVec::Binary(b)) => a.$op(b)?.into(),
            _ => return Err(anyhow!("unsupported")),
        })
    };
}

macro_rules! match_column_column_numeric {
    ($col1:ident, $col2:ident, $op:ident) => {
        Ok(match ($col1, $col2) {
            (ColumnVec::Int8(a), ColumnVec::Int8(b)) => a.$op(b)?.into(),
            (ColumnVec::Int16(a), ColumnVec::Int16(b)) => a.$op(b)?.into(),
            (ColumnVec::Int32(a), ColumnVec::Int32(b)) => a.$op(b)?.into(),
            (ColumnVec::Int64(a), ColumnVec::Int64(b)) => a.$op(b)?.into(),
            (ColumnVec::Float32(a), ColumnVec::Float32(b)) => a.$op(b)?.into(),
            (ColumnVec::Float64(a), ColumnVec::Float64(b)) => a.$op(b)?.into(),
            _ => return Err(anyhow!("unsupported")),
        })
    };
}

macro_rules! match_column_column_bool {
    ($col1:ident, $col2:ident, $op:ident) => {
        Ok(match ($col1, $col2) {
            (ColumnVec::Bool(a), ColumnVec::Bool(b)) => a.$op(b)?.into(),
            _ => return Err(anyhow!("unsupported")),
        })
    };
}

macro_rules! match_column_datavalue {
    ($col:ident, $datavalue:ident, $op:ident) => {
        Ok(match ($col, $datavalue) {
            (ColumnVec::Bool(a), DataValue::Bool(b)) => a.$op(b)?.into(),
            (ColumnVec::Int8(a), DataValue::Int8(b)) => a.$op(b)?.into(),
            (ColumnVec::Int16(a), DataValue::Int16(b)) => a.$op(b)?.into(),
            (ColumnVec::Int32(a), DataValue::Int32(b)) => a.$op(b)?.into(),
            (ColumnVec::Int64(a), DataValue::Int64(b)) => a.$op(b)?.into(),
            (ColumnVec::Float32(a), DataValue::Float32(b)) => a.$op(b)?.into(),
            (ColumnVec::Float64(a), DataValue::Float64(b)) => a.$op(b)?.into(),
            (ColumnVec::Int64(a), DataValue::Date64(b)) => a.$op(b)?.into(),
            (ColumnVec::Utf8(a), DataValue::Utf8(b)) => a.$op(b)?.into(),
            (ColumnVec::Binary(a), DataValue::Binary(b)) => a.$op(b)?.into(),
            _ => return Err(anyhow!("unsupported")),
        })
    };
}

macro_rules! match_column_datavalue_numeric {
    ($col:ident, $datavalue:ident, $op:ident) => {
        Ok(match ($col, $datavalue) {
            (ColumnVec::Int8(a), DataValue::Int8(b)) => a.$op(b)?.into(),
            (ColumnVec::Int16(a), DataValue::Int16(b)) => a.$op(b)?.into(),
            (ColumnVec::Int32(a), DataValue::Int32(b)) => a.$op(b)?.into(),
            (ColumnVec::Int64(a), DataValue::Int64(b)) => a.$op(b)?.into(),
            (ColumnVec::Float32(a), DataValue::Float32(b)) => a.$op(b)?.into(),
            (ColumnVec::Float64(a), DataValue::Float64(b)) => a.$op(b)?.into(),
            (ColumnVec::Int64(a), DataValue::Date64(b)) => a.$op(b)?.into(), // TODO: How to handle?
            _ => return Err(anyhow!("unsupported")),
        })
    };
}

macro_rules! match_column_datavalue_bool {
    ($col:ident, $datavalue:ident, $op:ident) => {
        Ok(match ($col, $datavalue) {
            (ColumnVec::Bool(a), DataValue::Bool(b)) => a.$op(b)?.into(),
            _ => return Err(anyhow!("unsupported")),
        })
    };
}

// Compute trait implementations.

impl VecAdd for ColumnVec {
    fn add(&self, rhs: &Self) -> Result<Self> {
        match_column_column_numeric!(self, rhs, add)
    }
}

impl VecAdd<DataValue> for ColumnVec {
    fn add(&self, rhs: &DataValue) -> Result<Self> {
        match_column_datavalue_numeric!(self, rhs, add)
    }
}

impl VecSub for ColumnVec {
    fn sub(&self, rhs: &Self) -> Result<Self> {
        match_column_column_numeric!(self, rhs, sub)
    }
}

impl VecSub<DataValue> for ColumnVec {
    fn sub(&self, rhs: &DataValue) -> Result<Self> {
        match_column_datavalue_numeric!(self, rhs, sub)
    }
}

impl VecMul for ColumnVec {
    fn mul(&self, rhs: &Self) -> Result<Self> {
        match_column_column_numeric!(self, rhs, mul)
    }
}

impl VecMul<DataValue> for ColumnVec {
    fn mul(&self, rhs: &DataValue) -> Result<Self> {
        match_column_datavalue_numeric!(self, rhs, mul)
    }
}

impl VecDiv for ColumnVec {
    fn div(&self, rhs: &Self) -> Result<Self> {
        match_column_column_numeric!(self, rhs, div)
    }
}

impl VecDiv<DataValue> for ColumnVec {
    fn div(&self, rhs: &DataValue) -> Result<Self> {
        match_column_datavalue_numeric!(self, rhs, div)
    }
}

impl VecEq for ColumnVec {
    fn eq(&self, rhs: &Self) -> Result<BoolVec> {
        match_column_column!(self, rhs, eq)
    }
}

impl VecEq<DataValue> for ColumnVec {
    fn eq(&self, rhs: &DataValue) -> Result<BoolVec> {
        match_column_datavalue!(self, rhs, eq)
    }
}

impl VecNeq for ColumnVec {
    fn neq(&self, rhs: &Self) -> Result<BoolVec> {
        match_column_column!(self, rhs, neq)
    }
}

impl VecNeq<DataValue> for ColumnVec {
    fn neq(&self, rhs: &DataValue) -> Result<BoolVec> {
        match_column_datavalue!(self, rhs, neq)
    }
}

impl VecGt for ColumnVec {
    fn gt(&self, rhs: &Self) -> Result<BoolVec> {
        match_column_column!(self, rhs, gt)
    }
}

impl VecGt<DataValue> for ColumnVec {
    fn gt(&self, rhs: &DataValue) -> Result<BoolVec> {
        match_column_datavalue!(self, rhs, gt)
    }
}

impl VecLt for ColumnVec {
    fn lt(&self, rhs: &Self) -> Result<BoolVec> {
        match_column_column!(self, rhs, lt)
    }
}

impl VecLt<DataValue> for ColumnVec {
    fn lt(&self, rhs: &DataValue) -> Result<BoolVec> {
        match_column_datavalue!(self, rhs, lt)
    }
}

impl VecGe for ColumnVec {
    fn ge(&self, rhs: &Self) -> Result<BoolVec> {
        match_column_column!(self, rhs, ge)
    }
}

impl VecGe<DataValue> for ColumnVec {
    fn ge(&self, rhs: &DataValue) -> Result<BoolVec> {
        match_column_datavalue!(self, rhs, ge)
    }
}

impl VecLe for ColumnVec {
    fn le(&self, rhs: &Self) -> Result<BoolVec> {
        match_column_column!(self, rhs, le)
    }
}

impl VecLe<DataValue> for ColumnVec {
    fn le(&self, rhs: &DataValue) -> Result<BoolVec> {
        match_column_datavalue!(self, rhs, le)
    }
}
