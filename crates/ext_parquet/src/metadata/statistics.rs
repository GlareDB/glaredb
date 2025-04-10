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
use crate::data_type::private::ParquetValueType;
use crate::data_type::{ByteArray, DataType, FixedLenByteArray, Int96};
use crate::format::Statistics as TStatistics;
use crate::util::bit_util::from_le_slice;

pub(crate) mod private {
    use super::*;
    use crate::data_type::{ByteArray, FixedLenByteArray, Int96};

    pub trait MakeStatistics {
        fn make_statistics(statistics: ValueStatistics<Self>) -> Statistics
        where
            Self: Sized;
    }

    macro_rules! gen_make_statistics {
        ($value_ty:ty, $stat:ident) => {
            impl MakeStatistics for $value_ty {
                fn make_statistics(statistics: ValueStatistics<Self>) -> Statistics
                where
                    Self: Sized,
                {
                    Statistics::$stat(statistics)
                }
            }
        };
    }

    gen_make_statistics!(bool, Boolean);
    gen_make_statistics!(i32, Int32);
    gen_make_statistics!(i64, Int64);
    gen_make_statistics!(Int96, Int96);
    gen_make_statistics!(f32, Float);
    gen_make_statistics!(f64, Double);
    gen_make_statistics!(ByteArray, ByteArray);
    gen_make_statistics!(FixedLenByteArray, FixedLenByteArray);
}

// Macro to generate methods create Statistics.
macro_rules! statistics_new_func {
    ($func:ident, $vtype:ty, $stat:ident) => {
        pub fn $func(
            min: $vtype,
            max: $vtype,
            distinct: Option<u64>,
            nulls: u64,
            is_deprecated: bool,
        ) -> Self {
            Statistics::$stat(ValueStatistics::new(
                min,
                max,
                distinct,
                nulls,
                is_deprecated,
            ))
        }
    };
}

// Macro to generate getter functions for Statistics.
macro_rules! statistics_enum_func {
    ($self:ident, $func:ident) => {{
        match *$self {
            Statistics::Boolean(ref typed) => typed.$func(),
            Statistics::Int32(ref typed) => typed.$func(),
            Statistics::Int64(ref typed) => typed.$func(),
            Statistics::Int96(ref typed) => typed.$func(),
            Statistics::Float(ref typed) => typed.$func(),
            Statistics::Double(ref typed) => typed.$func(),
            Statistics::ByteArray(ref typed) => typed.$func(),
            Statistics::FixedLenByteArray(ref typed) => typed.$func(),
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
                Type::BOOLEAN => Statistics::boolean(
                    min.map(|data| data[0] != 0),
                    max.map(|data| data[0] != 0),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT32 => Statistics::int32(
                    min.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT64 => Statistics::int64(
                    min.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
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
                    Statistics::int96(min, max, distinct_count, null_count, old_format)
                }
                Type::FLOAT => Statistics::float(
                    min.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::DOUBLE => Statistics::double(
                    min.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::BYTE_ARRAY => Statistics::ByteArray(
                    ValueStatistics::new(
                        min.map(ByteArray::from),
                        max.map(ByteArray::from),
                        distinct_count,
                        null_count,
                        old_format,
                    )
                    .with_max_is_exact(stats.is_max_value_exact.unwrap_or(false))
                    .with_min_is_exact(stats.is_min_value_exact.unwrap_or(false)),
                ),
                Type::FIXED_LEN_BYTE_ARRAY => Statistics::FixedLenByteArray(
                    ValueStatistics::new(
                        min.map(ByteArray::from).map(FixedLenByteArray::from),
                        max.map(ByteArray::from).map(FixedLenByteArray::from),
                        distinct_count,
                        null_count,
                        old_format,
                    )
                    .with_max_is_exact(stats.is_max_value_exact.unwrap_or(false))
                    .with_min_is_exact(stats.is_min_value_exact.unwrap_or(false)),
                ),
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
    let (min, max, min_exact, max_exact) = if stats.has_min_max_set() {
        (
            Some(stats.min_bytes().to_vec()),
            Some(stats.max_bytes().to_vec()),
            Some(stats.min_is_exact()),
            Some(stats.max_is_exact()),
        )
    } else {
        (None, None, None, None)
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
    ByteArray(ValueStatistics<ByteArray>),
    FixedLenByteArray(ValueStatistics<FixedLenByteArray>),
}

impl<T: ParquetValueType> From<ValueStatistics<T>> for Statistics {
    fn from(t: ValueStatistics<T>) -> Self {
        T::make_statistics(t)
    }
}

impl Statistics {
    pub fn new<T: ParquetValueType>(
        min: Option<T>,
        max: Option<T>,
        distinct_count: Option<u64>,
        null_count: u64,
        is_deprecated: bool,
    ) -> Self {
        Self::from(ValueStatistics::new(
            min,
            max,
            distinct_count,
            null_count,
            is_deprecated,
        ))
    }

    statistics_new_func![boolean, Option<bool>, Boolean];

    statistics_new_func![int32, Option<i32>, Int32];

    statistics_new_func![int64, Option<i64>, Int64];

    statistics_new_func![int96, Option<Int96>, Int96];

    statistics_new_func![float, Option<f32>, Float];

    statistics_new_func![double, Option<f64>, Double];

    statistics_new_func![byte_array, Option<ByteArray>, ByteArray];

    statistics_new_func![
        fixed_len_byte_array,
        Option<FixedLenByteArray>,
        FixedLenByteArray
    ];

    /// Returns `true` if statistics have old `min` and `max` fields set.
    /// This means that the column order is likely to be undefined, which, for old files
    /// could mean a signed sort order of values.
    ///
    /// Refer to [`ColumnOrder`](crate::basic::ColumnOrder) and
    /// [`SortOrder`](crate::basic::SortOrder) for more information.
    pub fn is_min_max_deprecated(&self) -> bool {
        statistics_enum_func![self, is_min_max_deprecated]
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
        statistics_enum_func![self, is_min_max_backwards_compatible]
    }

    /// Returns optional value of number of distinct values occurring.
    /// When it is `None`, the value should be ignored.
    pub fn distinct_count(&self) -> Option<u64> {
        statistics_enum_func![self, distinct_count]
    }

    /// Returns number of null values for the column.
    /// Note that this includes all nulls when column is part of the complex type.
    pub fn null_count(&self) -> u64 {
        statistics_enum_func![self, null_count]
    }

    /// Returns `true` if statistics collected any null values, `false` otherwise.
    pub fn has_nulls(&self) -> bool {
        self.null_count() > 0
    }

    /// Returns `true` if min value and max value are set.
    /// Normally both min/max values will be set to `Some(value)` or `None`.
    pub fn has_min_max_set(&self) -> bool {
        statistics_enum_func![self, has_min_max_set]
    }

    /// Returns `true` if the min value is set, and is an exact min value.
    pub fn min_is_exact(&self) -> bool {
        statistics_enum_func![self, min_is_exact]
    }

    /// Returns `true` if the max value is set, and is an exact max value.
    pub fn max_is_exact(&self) -> bool {
        statistics_enum_func![self, max_is_exact]
    }

    /// Returns slice of bytes that represent min value.
    /// Panics if min value is not set.
    pub fn min_bytes(&self) -> &[u8] {
        statistics_enum_func![self, min_bytes]
    }

    /// Returns slice of bytes that represent max value.
    /// Panics if max value is not set.
    pub fn max_bytes(&self) -> &[u8] {
        statistics_enum_func![self, max_bytes]
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

/// Typed implementation for [`Statistics`].
pub type TypedStatistics<T> = ValueStatistics<<T as DataType>::T>;

/// Statistics for a particular `ParquetValueType`
#[derive(Clone, Eq, PartialEq)]
pub struct ValueStatistics<T> {
    min: Option<T>,
    max: Option<T>,
    // Distinct count could be omitted in some cases
    distinct_count: Option<u64>,
    null_count: u64,

    // Whether or not the min or max values are exact, or truncated.
    is_max_value_exact: bool,
    is_min_value_exact: bool,

    /// If `true` populate the deprecated `min` and `max` fields instead of
    /// `min_value` and `max_value`
    is_min_max_deprecated: bool,

    /// If `true` the statistics are compatible with the deprecated `min` and
    /// `max` fields. See [`ValueStatistics::is_min_max_backwards_compatible`]
    is_min_max_backwards_compatible: bool,
}

impl<T: ParquetValueType> ValueStatistics<T> {
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

    /// Set whether the stored `min` field represents the exact
    /// minimum, or just a bound on the minimum value.
    ///
    /// see [`Self::min_is_exact`]
    pub fn with_min_is_exact(self, is_min_value_exact: bool) -> Self {
        Self {
            is_min_value_exact,
            ..self
        }
    }

    /// Set whether the stored `max` field represents the exact
    /// maximum, or just a bound on the maximum value.
    ///
    /// see [`Self::max_is_exact`]
    pub fn with_max_is_exact(self, is_max_value_exact: bool) -> Self {
        Self {
            is_max_value_exact,
            ..self
        }
    }

    /// Set whether to write the deprecated `min` and `max` fields
    /// for compatibility with older parquet writers
    ///
    /// This should only be enabled if the field is signed,
    /// see [`Self::is_min_max_backwards_compatible`]
    pub fn with_backwards_compatible_min_max(self, backwards_compatible: bool) -> Self {
        Self {
            is_min_max_backwards_compatible: backwards_compatible,
            ..self
        }
    }

    /// Returns min value of the statistics.
    ///
    /// Panics if min value is not set, e.g. all values are `null`.
    /// Use `has_min_max_set` method to check that.
    pub fn min(&self) -> &T {
        self.min.as_ref().unwrap()
    }

    /// Returns max value of the statistics.
    ///
    /// Panics if max value is not set, e.g. all values are `null`.
    /// Use `has_min_max_set` method to check that.
    pub fn max(&self) -> &T {
        self.max.as_ref().unwrap()
    }

    /// Returns min value as bytes of the statistics.
    ///
    /// Panics if min value is not set, use `has_min_max_set` method to check
    /// if values are set.
    pub fn min_bytes(&self) -> &[u8] {
        self.min().as_bytes()
    }

    /// Returns max value as bytes of the statistics.
    ///
    /// Panics if max value is not set, use `has_min_max_set` method to check
    /// if values are set.
    pub fn max_bytes(&self) -> &[u8] {
        self.max().as_bytes()
    }

    /// Whether or not min and max values are set.
    /// Normally both min/max values will be set to `Some(value)` or `None`.
    pub fn has_min_max_set(&self) -> bool {
        self.min.is_some() && self.max.is_some()
    }

    /// Whether or not max value is set, and is an exact value.
    pub fn max_is_exact(&self) -> bool {
        self.max.is_some() && self.is_max_value_exact
    }

    /// Whether or not min value is set, and is an exact value.
    pub fn min_is_exact(&self) -> bool {
        self.min.is_some() && self.is_min_value_exact
    }

    /// Returns optional value of number of distinct values occurring.
    pub fn distinct_count(&self) -> Option<u64> {
        self.distinct_count
    }

    /// Returns null count.
    pub fn null_count(&self) -> u64 {
        self.null_count
    }

    /// Returns `true` if statistics were created using old min/max fields.
    fn is_min_max_deprecated(&self) -> bool {
        self.is_min_max_deprecated
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
        self.is_min_max_backwards_compatible
    }
}

impl<T: ParquetValueType> fmt::Display for ValueStatistics<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "min: ")?;
        match self.min {
            Some(ref value) => write!(f, "{value}")?,
            None => write!(f, "N/A")?,
        }
        write!(f, ", max: ")?;
        match self.max {
            Some(ref value) => write!(f, "{value}")?,
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

impl<T: ParquetValueType> fmt::Debug for ValueStatistics<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{{min: {:?}, max: {:?}, distinct_count: {:?}, null_count: {}, \
             min_max_deprecated: {}, min_max_backwards_compatible: {}, max_value_exact: {}, min_value_exact: {}}}",
            self.min,
            self.max,
            self.distinct_count,
            self.null_count,
            self.is_min_max_deprecated,
            self.is_min_max_backwards_compatible,
            self.is_max_value_exact,
            self.is_min_value_exact
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::AsBytes;

    #[test]
    fn test_statistics_min_max_bytes() {
        let stats = Statistics::int32(Some(-123), Some(234), None, 1, false);
        assert!(stats.has_min_max_set());
        assert_eq!(stats.min_bytes(), (-123).as_bytes());
        assert_eq!(stats.max_bytes(), 234.as_bytes());

        let stats = Statistics::byte_array(
            Some(ByteArray::from(vec![1, 2, 3])),
            Some(ByteArray::from(vec![3, 4, 5])),
            None,
            1,
            true,
        );
        assert!(stats.has_min_max_set());
        assert_eq!(stats.min_bytes(), &[1, 2, 3]);
        assert_eq!(stats.max_bytes(), &[3, 4, 5]);
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
    fn test_statistics_debug() {
        let stats = Statistics::int32(Some(1), Some(12), None, 12, true);
        assert_eq!(
            format!("{stats:?}"),
            "Int32({min: Some(1), max: Some(12), distinct_count: None, null_count: 12, \
             min_max_deprecated: true, min_max_backwards_compatible: true, max_value_exact: true, min_value_exact: true})"
        );

        let stats = Statistics::int32(None, None, None, 7, false);
        assert_eq!(
            format!("{stats:?}"),
            "Int32({min: None, max: None, distinct_count: None, null_count: 7, \
             min_max_deprecated: false, min_max_backwards_compatible: false, max_value_exact: false, min_value_exact: false})"
        )
    }

    #[test]
    fn test_statistics_display() {
        let stats = Statistics::int32(Some(1), Some(12), None, 12, true);
        assert_eq!(
            format!("{stats}"),
            "{min: 1, max: 12, distinct_count: N/A, null_count: 12, min_max_deprecated: true, max_value_exact: true, min_value_exact: true}"
        );

        let stats = Statistics::int64(None, None, None, 7, false);
        assert_eq!(
            format!("{stats}"),
            "{min: N/A, max: N/A, distinct_count: N/A, null_count: 7, min_max_deprecated: \
             false, max_value_exact: false, min_value_exact: false}"
        );

        let stats = Statistics::int96(
            Some(Int96::from(vec![1, 0, 0])),
            Some(Int96::from(vec![2, 3, 4])),
            None,
            3,
            true,
        );
        assert_eq!(
            format!("{stats}"),
            "{min: [1, 0, 0], max: [2, 3, 4], distinct_count: N/A, null_count: 3, \
             min_max_deprecated: true, max_value_exact: true, min_value_exact: true}"
        );

        let stats = Statistics::ByteArray(
            ValueStatistics::new(
                Some(ByteArray::from(vec![1u8])),
                Some(ByteArray::from(vec![2u8])),
                Some(5),
                7,
                false,
            )
            .with_max_is_exact(false)
            .with_min_is_exact(false),
        );
        assert_eq!(
            format!("{stats}"),
            "{min: [1], max: [2], distinct_count: 5, null_count: 7, min_max_deprecated: false, max_value_exact: false, min_value_exact: false}"
        );
    }

    #[test]
    fn test_statistics_partial_eq() {
        let expected = Statistics::int32(Some(12), Some(45), None, 11, true);

        assert!(Statistics::int32(Some(12), Some(45), None, 11, true) == expected);
        assert!(Statistics::int32(Some(11), Some(45), None, 11, true) != expected);
        assert!(Statistics::int32(Some(12), Some(44), None, 11, true) != expected);
        assert!(Statistics::int32(Some(12), Some(45), None, 23, true) != expected);
        assert!(Statistics::int32(Some(12), Some(45), None, 11, false) != expected);

        assert!(
            Statistics::int32(Some(12), Some(45), None, 11, false)
                != Statistics::int64(Some(12), Some(45), None, 11, false)
        );

        assert!(
            Statistics::boolean(Some(false), Some(true), None, 0, true)
                != Statistics::double(Some(1.2), Some(4.5), None, 0, true)
        );

        assert!(
            Statistics::byte_array(
                Some(ByteArray::from(vec![1, 2, 3])),
                Some(ByteArray::from(vec![1, 2, 3])),
                None,
                0,
                true
            ) != Statistics::fixed_len_byte_array(
                Some(ByteArray::from(vec![1, 2, 3]).into()),
                Some(ByteArray::from(vec![1, 2, 3]).into()),
                None,
                0,
                true,
            )
        );

        assert!(
            Statistics::byte_array(
                Some(ByteArray::from(vec![1, 2, 3])),
                Some(ByteArray::from(vec![1, 2, 3])),
                None,
                0,
                true,
            ) != Statistics::ByteArray(
                ValueStatistics::new(
                    Some(ByteArray::from(vec![1, 2, 3])),
                    Some(ByteArray::from(vec![1, 2, 3])),
                    None,
                    0,
                    true,
                )
                .with_max_is_exact(false)
            )
        );

        assert!(
            Statistics::fixed_len_byte_array(
                Some(FixedLenByteArray::from(vec![1, 2, 3])),
                Some(FixedLenByteArray::from(vec![1, 2, 3])),
                None,
                0,
                true,
            ) != Statistics::FixedLenByteArray(
                ValueStatistics::new(
                    Some(FixedLenByteArray::from(vec![1, 2, 3])),
                    Some(FixedLenByteArray::from(vec![1, 2, 3])),
                    None,
                    0,
                    true,
                )
                .with_min_is_exact(false)
            )
        );
    }

    #[test]
    fn test_statistics_from_thrift() {
        // Helper method to check statistics conversion.
        fn check_stats(stats: Statistics) {
            let tpe = stats.physical_type();
            let thrift_stats = to_thrift(Some(&stats));
            assert_eq!(from_thrift(tpe, thrift_stats).unwrap(), Some(stats));
        }

        check_stats(Statistics::boolean(Some(false), Some(true), None, 7, true));
        check_stats(Statistics::boolean(Some(false), Some(true), None, 7, true));
        check_stats(Statistics::boolean(Some(false), Some(true), None, 0, false));
        check_stats(Statistics::boolean(Some(true), Some(true), None, 7, true));
        check_stats(Statistics::boolean(Some(false), Some(false), None, 7, true));
        check_stats(Statistics::boolean(None, None, None, 7, true));

        check_stats(Statistics::int32(Some(-100), Some(500), None, 7, true));
        check_stats(Statistics::int32(Some(-100), Some(500), None, 0, false));
        check_stats(Statistics::int32(None, None, None, 7, true));

        check_stats(Statistics::int64(Some(-100), Some(200), None, 7, true));
        check_stats(Statistics::int64(Some(-100), Some(200), None, 0, false));
        check_stats(Statistics::int64(None, None, None, 7, true));

        check_stats(Statistics::float(Some(1.2), Some(3.4), None, 7, true));
        check_stats(Statistics::float(Some(1.2), Some(3.4), None, 0, false));
        check_stats(Statistics::float(None, None, None, 7, true));

        check_stats(Statistics::double(Some(1.2), Some(3.4), None, 7, true));
        check_stats(Statistics::double(Some(1.2), Some(3.4), None, 0, false));
        check_stats(Statistics::double(None, None, None, 7, true));

        check_stats(Statistics::byte_array(
            Some(ByteArray::from(vec![1, 2, 3])),
            Some(ByteArray::from(vec![3, 4, 5])),
            None,
            7,
            true,
        ));
        check_stats(Statistics::byte_array(None, None, None, 7, true));

        check_stats(Statistics::fixed_len_byte_array(
            Some(ByteArray::from(vec![1, 2, 3]).into()),
            Some(ByteArray::from(vec![3, 4, 5]).into()),
            None,
            7,
            true,
        ));
        check_stats(Statistics::fixed_len_byte_array(None, None, None, 7, true));
    }
}
