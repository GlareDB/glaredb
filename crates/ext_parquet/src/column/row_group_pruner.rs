use std::fmt::Debug;

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

pub trait RowGroupPruner<T>: Debug + Sync + Send {
    /// Returns a boolean indicating if we can prune this row group based on
    /// statistics for a column.
    fn can_prune_using_column_stats(&self, stats: &ValueStatistics<T>) -> bool;
}

/// Prune base on if column contains an exact value.
#[derive(Debug)]
pub struct ColumnContainsValue<T> {
    pub value: T,
}

impl<T> RowGroupPruner<T> for ColumnContainsValue<T>
where
    T: Debug + Sync + Send + PartialEq + PartialOrd,
{
    fn can_prune_using_column_stats(&self, stats: &ValueStatistics<T>) -> bool {
        if !(stats.is_max_value_exact && stats.is_min_value_exact) {
            // Not exact values, need to think more about what that means.
            return false;
        }

        match (stats.min.as_ref(), stats.max.as_ref()) {
            (Some(min), Some(max)) => {
                // Check if value falls outside the min/max range. If it does,
                // we're safe to prune.
                min > &self.value || max < &self.value
            }
            _ => false, // Nothing we can glean from this.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_value_can_prune_with_min_max() {
        let c = ColumnContainsValue { value: 4 };
        let stats = ValueStatistics::new(Some(6), Some(12), Some(2), 0, false);

        let can_prune = c.can_prune_using_column_stats(&stats);
        assert!(can_prune);
    }
}
