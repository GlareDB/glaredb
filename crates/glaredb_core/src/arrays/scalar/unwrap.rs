use std::fmt::Debug;

use glaredb_error::{DbError, Result};
use half::f16;

use super::BorrowedScalarValue;
use super::interval::Interval;
use crate::arrays::array::physical_type::UntypedNull;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullableValue<'a, V: ?Sized> {
    Value(&'a V),
    Null,
}

/// Helper for unwrapping the inner value from a scalar value.
pub trait ScalarValueUnwrap: Debug + Sync + Send + Clone + Copy + 'static {
    /// The resulting type from the unwrap.
    type StorageType: Debug + Sync + Send + ?Sized;

    /// Try to unwrap the inner value from the scalar value.
    fn try_unwrap<'a>(
        scalar: &'a BorrowedScalarValue<'a>,
    ) -> Result<NullableValue<'a, Self::StorageType>>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UnwrapUntypedNull;

impl ScalarValueUnwrap for UnwrapUntypedNull {
    type StorageType = UntypedNull;

    fn try_unwrap<'a>(
        scalar: &'a BorrowedScalarValue<'a>,
    ) -> Result<NullableValue<'a, Self::StorageType>> {
        match scalar {
            BorrowedScalarValue::Null => Ok(NullableValue::Null),
            other => Err(DbError::new(format!(
                "Cannot unwrap '{other}' using {Self:?}",
            ))),
        }
    }
}

macro_rules! impl_single_variant {
    ($prim:ty, $name:ident, $variant:ident) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub struct $name;

        impl ScalarValueUnwrap for $name {
            type StorageType = $prim;

            fn try_unwrap<'a>(
                scalar: &'a BorrowedScalarValue<'a>,
            ) -> Result<NullableValue<'a, Self::StorageType>> {
                match scalar {
                    BorrowedScalarValue::Null => Ok(NullableValue::Null),
                    BorrowedScalarValue::$variant(v) => Ok(NullableValue::Value(v)),
                    other => Err(DbError::new(format!(
                        "Cannot unwrap '{other}' using {:?}",
                        Self,
                    ))),
                }
            }
        }
    };
}

impl_single_variant!(bool, UnwrapBool, Boolean);

impl_single_variant!(i8, UnwrapI8, Int8);
impl_single_variant!(i16, UnwrapI16, Int16);

impl_single_variant!(u8, UnwrapU8, UInt8);
impl_single_variant!(u16, UnwrapU16, UInt16);
impl_single_variant!(u32, UnwrapU32, UInt32);
impl_single_variant!(u64, UnwrapU64, UInt64);
impl_single_variant!(u128, UnwrapU128, UInt128);

impl_single_variant!(f16, UnwrapF16, Float16);
impl_single_variant!(f32, UnwrapF32, Float32);
impl_single_variant!(f64, UnwrapF64, Float64);

impl_single_variant!(Interval, UnwrapInterval, Interval);

impl_single_variant!(str, UnwrapUtf8, Utf8);
impl_single_variant!([u8], UnwrapBinary, Binary);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnwrapI32;

impl ScalarValueUnwrap for UnwrapI32 {
    type StorageType = i32;

    fn try_unwrap<'a>(
        scalar: &'a BorrowedScalarValue<'a>,
    ) -> Result<NullableValue<'a, Self::StorageType>> {
        match scalar {
            BorrowedScalarValue::Null => Ok(NullableValue::Null),
            BorrowedScalarValue::Int32(v) => Ok(NullableValue::Value(v)),
            BorrowedScalarValue::Date32(v) => Ok(NullableValue::Value(v)),
            other => Err(DbError::new(format!(
                "Cannot unwrap '{other}' using {Self:?}",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnwrapI64;

impl ScalarValueUnwrap for UnwrapI64 {
    type StorageType = i64;

    fn try_unwrap<'a>(
        scalar: &'a BorrowedScalarValue<'a>,
    ) -> Result<NullableValue<'a, Self::StorageType>> {
        match scalar {
            BorrowedScalarValue::Null => Ok(NullableValue::Null),
            BorrowedScalarValue::Int64(v) => Ok(NullableValue::Value(v)),
            BorrowedScalarValue::Date64(v) => Ok(NullableValue::Value(v)),
            BorrowedScalarValue::Decimal64(v) => Ok(NullableValue::Value(&v.value)),
            BorrowedScalarValue::Timestamp(v) => Ok(NullableValue::Value(&v.value)),
            other => Err(DbError::new(format!(
                "Cannot unwrap '{other}' using {Self:?}",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnwrapI128;

impl ScalarValueUnwrap for UnwrapI128 {
    type StorageType = i128;

    fn try_unwrap<'a>(
        scalar: &'a BorrowedScalarValue<'a>,
    ) -> Result<NullableValue<'a, Self::StorageType>> {
        match scalar {
            BorrowedScalarValue::Null => Ok(NullableValue::Null),
            BorrowedScalarValue::Int128(v) => Ok(NullableValue::Value(v)),
            BorrowedScalarValue::Decimal128(v) => Ok(NullableValue::Value(&v.value)),
            other => Err(DbError::new(format!(
                "Cannot unwrap '{other}' using {Self:?}",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::scalar::ScalarValue;
    use crate::arrays::scalar::decimal::Decimal64Scalar;

    #[test]
    fn unwrap_i64_null() {
        let val = ScalarValue::Null;

        let got = UnwrapI64::try_unwrap(&val).unwrap();
        assert_eq!(NullableValue::Null, got);
    }

    #[test]
    fn unwrap_i64_decimal() {
        let val = ScalarValue::Decimal64(Decimal64Scalar {
            precision: 4,
            scale: 2,
            value: 1400,
        });

        let got = UnwrapI64::try_unwrap(&val).unwrap();
        assert_eq!(NullableValue::Value(&1400), got);
    }
}
