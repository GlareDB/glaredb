use std::fmt::Debug;

use glaredb_core::arrays::scalar::ScalarValue;
use glaredb_core::arrays::scalar::unwrap::{NullableValue, ScalarValueUnwrap};
use glaredb_core::storage::scan_filter::{PhysicalScanFilter, PhysicalScanFilterType};
use glaredb_core::util::marker::PhantomCovariant;
use glaredb_error::Result;
use num::cast::AsPrimitive;

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
    /// Returns if a row group should be pruned based on column stats.
    fn should_prune(&self, stats: &ValueStatistics<T::Native>) -> Result<bool>;
}

/// A pruner that will never allow pruning a row group.
#[derive(Debug, Default)]
pub struct NopRowGroupPruner<T: PlainType> {
    _t: PhantomCovariant<T>,
}

impl<T> RowGroupPruner<T> for NopRowGroupPruner<T>
where
    T: PlainType,
{
    fn should_prune(&self, _stats: &ValueStatistics<<T as PlainType>::Native>) -> Result<bool> {
        Ok(false)
    }
}

/// A pruner that works on primitive values.
///
/// The scalar value unwrap should represent the unwrapping of the constants
/// we're comparing against. The plain type will be cast to the appropriate
/// scalar type.
///
/// For example, the expression 'a = 5' where both sides represent u8 values,
/// the scalar unwrap we use should be for u8, even though we're reading 32
/// values from the parquet file. The i32 value will be cast to u8 before the
/// compare.
#[derive(Debug)]
pub struct PrimitiveRowGroupPruner<T: PlainType, U: ScalarValueUnwrap> {
    /// Compare against constant value.
    const_eq_filters: Vec<ScalarValue>,
    _t: PhantomCovariant<T>,
    _u: PhantomCovariant<U>,
}

impl<T, U> PrimitiveRowGroupPruner<T, U>
where
    T: PlainType,
    U: ScalarValueUnwrap,
    U::StorageType: Copy + PartialOrd,
    T::Native: AsPrimitive<U::StorageType>,
{
    #[allow(clippy::single_match)] // For the match, we will be expanding the number of variants beyond just ConstantEq.
    pub fn new<'a>(filters: impl Iterator<Item = &'a PhysicalScanFilter>) -> Self {
        let mut pruner = PrimitiveRowGroupPruner {
            const_eq_filters: Vec::new(),
            _t: PhantomCovariant::new(),
            _u: PhantomCovariant::new(),
        };

        for filter in filters {
            match &filter.filter_type {
                PhysicalScanFilterType::ConstantEq(constant) => {
                    pruner.const_eq_filters.push(constant.clone())
                }
                _ => (), // Skip
            }
        }

        pruner
    }
}

impl<T, U> RowGroupPruner<T> for PrimitiveRowGroupPruner<T, U>
where
    T: PlainType,
    U: ScalarValueUnwrap,
    U::StorageType: Copy + PartialOrd,
    T::Native: AsPrimitive<U::StorageType>,
{
    fn should_prune(&self, stats: &ValueStatistics<<T as PlainType>::Native>) -> Result<bool> {
        if !(stats.is_max_value_exact && stats.is_min_value_exact) {
            return Ok(false);
        }

        match (stats.min.as_ref(), stats.max.as_ref()) {
            (Some(&min), Some(&max)) => {
                let min: U::StorageType = min.as_();
                let max: U::StorageType = max.as_();

                for constant in &self.const_eq_filters {
                    let constant = match U::try_unwrap(constant)? {
                        NullableValue::Value(v) => *v,
                        NullableValue::Null => return Ok(false), // I don't know, just skip pruning.
                    };

                    if min > constant {
                        // Unsatisfiable.
                        return Ok(true);
                    }

                    if max < constant {
                        // Unsatisfiable.
                        return Ok(true);
                    }

                    // Try the next constant.
                }

                // Getting here means all the filters passed the min/max range.
                // Can't prune.
                Ok(false)
            }
            _ => Ok(false), // No min/max, can't glean anything from this.
        }
    }
}
