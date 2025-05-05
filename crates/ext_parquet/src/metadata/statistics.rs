// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains definitions for working with Parquet statistics.
//!
//! Though some common methods are available on enum, use pattern match to extract
//! actual min and max values from statistics, see below:

use std::fmt;

use glaredb_error::{DbError, Result};

use crate::basic::Type;
use crate::data_type::{AsBytes, Int96};
use crate::format::Statistics as TStatistics;
use crate::util::bit_util::from_le_slice;

// Macro to generate getter functions for Statistics.
macro_rules! statistic_enum_field {
    ($self:ident, $field:ident) => {{
        match *$self {
            Statistics::Boolean(ref typed) => typed.$field,
            Statistics::Int32(ref typed) => typed.$field,
            Statistics::Int64(ref typed) => typed.$field,
            Statistics::Int96(ref typed) => typed.$field,
            Statistics::Float(ref typed) => typed.$field,
            Statistics::Double(ref typed) => typed.$field,
            Statistics::ByteArray(ref typed) => typed.$field,
            Statistics::FixedLenByteArray(ref typed) => typed.$field,
        }
    }};
}

/// Converts Thrift definition into `Statistics`.
pub fn from_thrift(
    physical_type: Type,
    thrift_stats: Option<TStatistics>,
) -> Result<Option<Statistics>> {
    Ok(match thrift_stats {
        Some(stats) => {
            // Number of nulls recorded, when it is not available, we just mark it as 0.
            let null_count = stats.null_count.unwrap_or(0);

            if null_count < 0 {
                return Err(DbError::new(format!(
                    "Statistics null count is negative {}",
                    null_count
                )));
            }

            // Generic null count.
            let null_count = null_count as u64;
            // Generic distinct count (count of distinct values occurring)
            let distinct_count = stats.distinct_count.map(|value| value as u64);
            // Whether or not statistics use deprecated min/max fields.
            let old_format = stats.min_value.is_none() && stats.max_value.is_none();
            // Generic min value as bytes.
            let min = if old_format {
                stats.min
            } else {
                stats.min_value
            };
            // Generic max value as bytes.
            let max = if old_format {
                stats.max
            } else {
                stats.max_value
            };

            // Values are encoded using PLAIN encoding definition, except that
            // variable-length byte arrays do not include a length prefix.
            //
            // Instead of using actual decoder, we manually convert values.
            let res = match physical_type {
                Type::BOOLEAN => Statistics::Boolean(ValueStatistics::new(
                    min.map(|data| data[0] != 0),
                    max.map(|data| data[0] != 0),
                    distinct_count,
                    null_count,
                    old_format,
                )),
                Type::INT32 => Statistics::Int32(ValueStatistics::new(
                    min.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                )),
                Type::INT64 => Statistics::Int64(ValueStatistics::new(
                    min.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                )),
                Type::INT96 => {
                    // INT96 statistics may not be correct, because comparison is signed
                    // byte-wise, not actual timestamps. It is recommended to ignore
                    // min/max statistics for INT96 columns.
                    let min = min.map(|data| {
                        assert_eq!(data.len(), 12);
                        from_le_slice::<Int96>(&data)
                    });
                    let max = max.map(|data| {
                        assert_eq!(data.len(), 12);
                        from_le_slice::<Int96>(&data)
                    });
                    Statistics::Int96(ValueStatistics::new(
                        min,
                        max,
                        distinct_count,
                        null_count,
                        old_format,
                    ))
                }
                Type::FLOAT => Statistics::Float(ValueStatistics::new(
                    min.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                )),
                Type::DOUBLE => Statistics::Double(ValueStatistics::new(
                    min.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                )),
                Type::BYTE_ARRAY => Statistics::ByteArray(ValueStatistics {
                    min,
                    max,
                    distinct_count,
                    null_count,
                    is_min_max_deprecated: old_format,
                    is_min_max_backwards_compatible: old_format,
                    is_max_value_exact: stats.is_max_value_exact.unwrap_or(false),
                    is_min_value_exact: stats.is_min_value_exact.unwrap_or(false),
                }),
                Type::FIXED_LEN_BYTE_ARRAY => Statistics::FixedLenByteArray(ValueStatistics {
                    min,
                    max,
                    distinct_count,
                    null_count,
                    is_min_max_deprecated: old_format,
                    is_min_max_backwards_compatible: old_format,
                    is_max_value_exact: stats.is_max_value_exact.unwrap_or(false),
                    is_min_value_exact: stats.is_min_value_exact.unwrap_or(false),
                }),
            };

            Some(res)
        }
        None => None,
    })
}

// Convert Statistics into Thrift definition.
pub fn to_thrift(stats: Option<&Statistics>) -> Option<TStatistics> {
    let stats = stats?;

    let mut thrift_stats = TStatistics {
        max: None,
        min: None,
        null_count: if stats.has_nulls() {
            Some(stats.null_count() as i64)
        } else {
            None
        },
        distinct_count: stats.distinct_count().map(|value| value as i64),
        max_value: None,
        min_value: None,
        is_max_value_exact: None,
        is_min_value_exact: None,
    };

    // Get min/max if set.
    let (min, min_exact, max, max_exact) = match (stats.min_as_bytes(), stats.max_as_bytes()) {
        (Some(min), Some(max)) => (
            Some(min.to_vec()),
            Some(stats.min_is_exact()),
            Some(max.to_vec()),
            Some(stats.max_is_exact()),
        ),
        _ => (None, None, None, None),
    };

    if stats.is_min_max_backwards_compatible() {
        // Copy to deprecated min, max values for compatibility with older readers
        thrift_stats.min.clone_from(&min);
        thrift_stats.max.clone_from(&max);
    }

    if !stats.is_min_max_deprecated() {
        thrift_stats.min_value = min;
        thrift_stats.max_value = max;
    }

    thrift_stats.is_min_value_exact = min_exact;
    thrift_stats.is_max_value_exact = max_exact;

    Some(thrift_stats)
}

/// Statistics for a column chunk and data page.
#[derive(Debug, Clone, PartialEq)]
pub enum Statistics {
    Boolean(ValueStatistics<bool>),
    Int32(ValueStatistics<i32>),
    Int64(ValueStatistics<i64>),
    Int96(ValueStatistics<Int96>),
    Float(ValueStatistics<f32>),
    Double(ValueStatistics<f64>),
    ByteArray(ValueStatistics<Vec<u8>>),
    FixedLenByteArray(ValueStatistics<Vec<u8>>),
}

impl Statistics {
    /// Returns if the min and max values are set.
    pub fn has_min_max_set(&self) -> bool {
        match self {
            Self::Boolean(v) => v.min.is_some() && v.max.is_some(),
            Self::Int32(v) => v.min.is_some() && v.max.is_some(),
            Self::Int64(v) => v.min.is_some() && v.max.is_some(),
            Self::Int96(v) => v.min.is_some() && v.max.is_some(),
            Self::Float(v) => v.min.is_some() && v.max.is_some(),
            Self::Double(v) => v.min.is_some() && v.max.is_some(),
            Self::ByteArray(v) => v.min.is_some() && v.max.is_some(),
            Self::FixedLenByteArray(v) => v.min.is_some() && v.max.is_some(),
        }
    }

    pub fn min_as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Boolean(v) => v.min.as_ref().map(|m| m.as_bytes()),
            Self::Int32(v) => v.min.as_ref().map(|m| m.as_bytes()),
            Self::Int64(v) => v.min.as_ref().map(|m| m.as_bytes()),
            Self::Int96(v) => v.min.as_ref().map(|m| m.as_bytes()),
            Self::Float(v) => v.min.as_ref().map(|m| m.as_bytes()),
            Self::Double(v) => v.min.as_ref().map(|m| m.as_bytes()),
            Self::ByteArray(v) => v.min.as_ref().map(|m| m.as_bytes()),
            Self::FixedLenByteArray(v) => v.min.as_ref().map(|m| m.as_bytes()),
        }
    }

    pub fn max_as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Boolean(v) => v.max.as_ref().map(|m| m.as_bytes()),
            Self::Int32(v) => v.max.as_ref().map(|m| m.as_bytes()),
            Self::Int64(v) => v.max.as_ref().map(|m| m.as_bytes()),
            Self::Int96(v) => v.max.as_ref().map(|m| m.as_bytes()),
            Self::Float(v) => v.max.as_ref().map(|m| m.as_bytes()),
            Self::Double(v) => v.max.as_ref().map(|m| m.as_bytes()),
            Self::ByteArray(v) => v.max.as_ref().map(|m| m.as_bytes()),
            Self::FixedLenByteArray(v) => v.max.as_ref().map(|m| m.as_bytes()),
        }
    }

    /// Returns `true` if statistics have old `min` and `max` fields set.
    /// This means that the column order is likely to be undefined, which, for old files
    /// could mean a signed sort order of values.
    ///
    /// Refer to [`ColumnOrder`](crate::basic::ColumnOrder) and
    /// [`SortOrder`](crate::basic::SortOrder) for more information.
    pub fn is_min_max_deprecated(&self) -> bool {
        statistic_enum_field![self, is_min_max_deprecated]
    }

    /// Old versions of parquet stored statistics in `min` and `max` fields, ordered
    /// using signed comparison. This resulted in an undefined ordering for unsigned
    /// quantities, such as booleans and unsigned integers.
    ///
    /// These fields were therefore deprecated in favour of `min_value` and `max_value`,
    /// which have a type-defined sort order.
    ///
    /// However, not all readers have been updated. For backwards compatibility, this method
    /// returns `true` if the statistics within this have a signed sort order, that is
    /// compatible with being stored in the deprecated `min` and `max` fields
    pub fn is_min_max_backwards_compatible(&self) -> bool {
        statistic_enum_field![self, is_min_max_backwards_compatible]
    }

    /// Returns optional value of number of distinct values occurring.
    /// When it is `None`, the value should be ignored.
    pub fn distinct_count(&self) -> Option<u64> {
        statistic_enum_field![self, distinct_count]
    }

    /// Returns number of null values for the column.
    /// Note that this includes all nulls when column is part of the complex type.
    pub fn null_count(&self) -> u64 {
        statistic_enum_field![self, null_count]
    }

    /// Returns `true` if statistics collected any null values, `false` otherwise.
    pub fn has_nulls(&self) -> bool {
        self.null_count() > 0
    }

    /// Returns `true` if the min value is set, and is an exact min value.
    pub fn min_is_exact(&self) -> bool {
        statistic_enum_field![self, is_min_value_exact]
    }

    /// Returns `true` if the max value is set, and is an exact max value.
    pub fn max_is_exact(&self) -> bool {
        statistic_enum_field![self, is_max_value_exact]
    }

    /// Returns physical type associated with statistics.
    pub fn physical_type(&self) -> Type {
        match self {
            Statistics::Boolean(_) => Type::BOOLEAN,
            Statistics::Int32(_) => Type::INT32,
            Statistics::Int64(_) => Type::INT64,
            Statistics::Int96(_) => Type::INT96,
            Statistics::Float(_) => Type::FLOAT,
            Statistics::Double(_) => Type::DOUBLE,
            Statistics::ByteArray(_) => Type::BYTE_ARRAY,
            Statistics::FixedLenByteArray(_) => Type::FIXED_LEN_BYTE_ARRAY,
        }
    }
}

impl fmt::Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statistics::Boolean(typed) => write!(f, "{typed}"),
            Statistics::Int32(typed) => write!(f, "{typed}"),
            Statistics::Int64(typed) => write!(f, "{typed}"),
            Statistics::Int96(typed) => write!(f, "{typed}"),
            Statistics::Float(typed) => write!(f, "{typed}"),
            Statistics::Double(typed) => write!(f, "{typed}"),
            Statistics::ByteArray(typed) => write!(f, "{typed}"),
            Statistics::FixedLenByteArray(typed) => write!(f, "{typed}"),
        }
    }
}

// Sure
pub trait ValueStatisticsType: fmt::Debug + AsBytes {}

impl ValueStatisticsType for bool {}
impl ValueStatisticsType for i32 {}
impl ValueStatisticsType for i64 {}
impl ValueStatisticsType for Int96 {}
impl ValueStatisticsType for f32 {}
impl ValueStatisticsType for f64 {}
impl ValueStatisticsType for Vec<u8> {}

/// Statistics for a particular `ParquetValueType`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueStatistics<T> {
    pub min: Option<T>,
    pub max: Option<T>,
    // Distinct count could be omitted in some cases
    pub distinct_count: Option<u64>,
    /// Returns optional value of number of distinct values occurring.
    pub null_count: u64,

    /// Whether the stored `max` field represents the exact maximum, or just a
    /// bound on the maximum value.
    pub is_max_value_exact: bool,
    /// Whether the stored `min` field represents the exact minimum, or just a
    /// bound on the minimum value.
    pub is_min_value_exact: bool,

    /// If `true` populate the deprecated `min` and `max` fields instead of
    /// `min_value` and `max_value`
    pub is_min_max_deprecated: bool,

    /// Old versions of parquet stored statistics in `min` and `max` fields, ordered
    /// using signed comparison. This resulted in an undefined ordering for unsigned
    /// quantities, such as booleans and unsigned integers.
    ///
    /// These fields were therefore deprecated in favour of `min_value` and `max_value`,
    /// which have a type-defined sort order.
    ///
    /// However, not all readers have been updated. For backwards compatibility, this method
    /// returns `true` if the statistics within this have a signed sort order, that is
    /// compatible with being stored in the deprecated `min` and `max` fields
    pub is_min_max_backwards_compatible: bool,
}

impl<T> ValueStatistics<T>
where
    T: ValueStatisticsType,
{
    /// Creates new typed statistics.
    pub fn new(
        min: Option<T>,
        max: Option<T>,
        distinct_count: Option<u64>,
        null_count: u64,
        is_min_max_deprecated: bool,
    ) -> Self {
        Self {
            is_max_value_exact: max.is_some(),
            is_min_value_exact: min.is_some(),
            min,
            max,
            distinct_count,
            null_count,
            is_min_max_deprecated,
            is_min_max_backwards_compatible: is_min_max_deprecated,
        }
    }
}

impl<T> fmt::Display for ValueStatistics<T>
where
    T: ValueStatisticsType,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "min: ")?;
        match self.min {
            Some(ref value) => write!(f, "{value:?}")?,
            None => write!(f, "N/A")?,
        }
        write!(f, ", max: ")?;
        match self.max {
            Some(ref value) => write!(f, "{value:?}")?,
            None => write!(f, "N/A")?,
        }
        write!(f, ", distinct_count: ")?;
        match self.distinct_count {
            Some(value) => write!(f, "{value}")?,
            None => write!(f, "N/A")?,
        }
        write!(f, ", null_count: {}", self.null_count)?;
        write!(f, ", min_max_deprecated: {}", self.is_min_max_deprecated)?;
        write!(f, ", max_value_exact: {}", self.is_max_value_exact)?;
        write!(f, ", min_value_exact: {}", self.is_min_value_exact)?;
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::AsBytes;

    #[test]
    fn test_statistics_min_max_bytes() {
        let stats = Statistics::Int32(ValueStatistics::new(Some(-123), Some(234), None, 1, false));
        assert!(stats.has_min_max_set());
        assert_eq!(stats.min_as_bytes().unwrap(), (-123).as_bytes());
        assert_eq!(stats.max_as_bytes().unwrap(), 234.as_bytes());

        let stats = Statistics::ByteArray(ValueStatistics::new(
            Some(vec![1, 2, 3]),
            Some(vec![3, 4, 5]),
            None,
            1,
            true,
        ));
        assert!(stats.has_min_max_set());
        assert_eq!(stats.min_as_bytes().unwrap(), &[1, 2, 3]);
        assert_eq!(stats.max_as_bytes().unwrap(), &[3, 4, 5]);
    }

    #[test]
    fn test_statistics_negative_null_count() {
        let thrift_stats = TStatistics {
            max: None,
            min: None,
            null_count: Some(-10),
            distinct_count: None,
            max_value: None,
            min_value: None,
            is_max_value_exact: None,
            is_min_value_exact: None,
        };

        from_thrift(Type::INT32, Some(thrift_stats)).unwrap_err();
    }

    #[test]
    fn test_statistics_thrift_none() {
        assert_eq!(from_thrift(Type::INT32, None).unwrap(), None);
        assert_eq!(from_thrift(Type::BYTE_ARRAY, None).unwrap(), None);
    }

    #[test]
    fn test_statistics_from_thrift() {
        // Helper method to check statistics conversion.
        #[track_caller]
        fn check_stats(stats: Statistics) {
            let tpe = stats.physical_type();
            let thrift_stats = to_thrift(Some(&stats));
            assert_eq!(from_thrift(tpe, thrift_stats).unwrap(), Some(stats));
        }

        check_stats(Statistics::Boolean(ValueStatistics::new(
            Some(false),
            Some(true),
            None,
            7,
            true,
        )));
        check_stats(Statistics::Boolean(ValueStatistics::new(
            Some(false),
            Some(true),
            None,
            7,
            true,
        )));
        check_stats(Statistics::Boolean(ValueStatistics::new(
            Some(false),
            Some(true),
            None,
            0,
            false,
        )));
        check_stats(Statistics::Boolean(ValueStatistics::new(
            Some(true),
            Some(true),
            None,
            7,
            true,
        )));
        check_stats(Statistics::Boolean(ValueStatistics::new(
            Some(false),
            Some(false),
            None,
            7,
            true,
        )));
        check_stats(Statistics::Boolean(ValueStatistics::new(
            None, None, None, 7, true,
        )));

        check_stats(Statistics::Int32(ValueStatistics::new(
            Some(-100),
            Some(500),
            None,
            7,
            true,
        )));
        check_stats(Statistics::Int32(ValueStatistics::new(
            Some(-100),
            Some(500),
            None,
            0,
            false,
        )));
        check_stats(Statistics::Int32(ValueStatistics::new(
            None, None, None, 7, true,
        )));

        check_stats(Statistics::Int64(ValueStatistics::new(
            Some(-100),
            Some(200),
            None,
            7,
            true,
        )));
        check_stats(Statistics::Int64(ValueStatistics::new(
            Some(-100),
            Some(200),
            None,
            0,
            false,
        )));
        check_stats(Statistics::Int64(ValueStatistics::new(
            None, None, None, 7, true,
        )));

        check_stats(Statistics::Float(ValueStatistics::new(
            Some(1.2),
            Some(3.4),
            None,
            7,
            true,
        )));
        check_stats(Statistics::Float(ValueStatistics::new(
            Some(1.2),
            Some(3.4),
            None,
            0,
            false,
        )));
        check_stats(Statistics::Float(ValueStatistics::new(
            None, None, None, 7, true,
        )));

        check_stats(Statistics::Double(ValueStatistics::new(
            Some(1.2),
            Some(3.4),
            None,
            7,
            true,
        )));
        check_stats(Statistics::Double(ValueStatistics::new(
            Some(1.2),
            Some(3.4),
            None,
            0,
            false,
        )));
        check_stats(Statistics::Double(ValueStatistics::new(
            None, None, None, 7, true,
        )));

        check_stats(Statistics::ByteArray(ValueStatistics::new(
            Some(vec![1, 2, 3]),
            Some(vec![3, 4, 5]),
            None,
            7,
            true,
        )));
        check_stats(Statistics::ByteArray(ValueStatistics::new(
            None, None, None, 7, true,
        )));

        check_stats(Statistics::FixedLenByteArray(ValueStatistics::new(
            Some(vec![1, 2, 3]),
            Some(vec![3, 4, 5]),
            None,
            7,
            true,
        )));
        check_stats(Statistics::FixedLenByteArray(ValueStatistics::new(
            None, None, None, 7, true,
        )));
    }
}
