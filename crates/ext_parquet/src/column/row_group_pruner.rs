use std::fmt::Debug;

use glaredb_core::storage::scan_filter::PhysicalScanFilterConstantEq;
use glaredb_error::Result;

use crate::data_type::Int96;
use crate::metadata::statistics::{Statistics, ValueStatistics};

pub trait PlainType: Debug + Default + Sync + Send + Sized {
    type Native;
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<Self::Native>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTypeBool;

impl PlainType for PlainTypeBool {
    type Native = bool;
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<bool>> {
        match stats {
            Statistics::Boolean(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTypeI32;

impl PlainType for PlainTypeI32 {
    type Native = i32;
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<i32>> {
        match stats {
            Statistics::Int32(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTypeI64;

impl PlainType for PlainTypeI64 {
    type Native = i64;
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<i64>> {
        match stats {
            Statistics::Int64(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTypeF32;

impl PlainType for PlainTypeF32 {
    type Native = f32;
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<f32>> {
        match stats {
            Statistics::Float(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTypeF64;

impl PlainType for PlainTypeF64 {
    type Native = f64;
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<f64>> {
        match stats {
            Statistics::Double(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTypeInt96;

impl PlainType for PlainTypeInt96 {
    type Native = Int96; // TODO: Remove
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<Int96>> {
        match stats {
            Statistics::Int96(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTypeByteArray;

impl PlainType for PlainTypeByteArray {
    type Native = Vec<u8>;
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<Vec<u8>>> {
        match stats {
            Statistics::ByteArray(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlainTypeFixedLenByteArray;

impl PlainType for PlainTypeFixedLenByteArray {
    type Native = Vec<u8>;
    fn statistics(stats: &Statistics) -> Option<&ValueStatistics<Vec<u8>>> {
        match stats {
            Statistics::FixedLenByteArray(s) => Some(s),
            _ => None,
        }
    }
}

pub trait RowGroupPruner<T>: Debug + Sync + Send
where
    T: PlainType,
{
    /// Returns a boolean indicating if we can prune this row group based on
    /// statistics for a column.
    fn can_prune_using_column_stats(&self, stats: &ValueStatistics<T::Native>) -> Result<bool>;
}
