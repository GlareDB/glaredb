use std::fmt::{Debug, Display};

use glaredb_error::{DbError, Result};
use num_traits::{AsPrimitive, FromPrimitive, PrimInt, Signed, Zero};
use serde::{Deserialize, Serialize};

use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalI64, PhysicalI128};
use crate::arrays::datatype::{DataType, DataTypeMeta, DecimalTypeMeta};

/// Trait describing the underlying primivite representing the decimal.
// TODO: These num_trait traits are kind of annoying to use. It might make sense
// to put some methods/consts directly on this trait.
pub trait DecimalPrimitive:
    PrimInt + FromPrimitive + AsPrimitive<Self> + Signed + Default + Debug + Display + Sync + Send
{
    const ZERO: Self;
    const TEN: Self;

    /// Returns the base 10 log of this number, rounded down.
    ///
    /// This is guaranteed to be called with a non-zero positive number.
    fn ilog10(self) -> u32;
}

impl DecimalPrimitive for i64 {
    const ZERO: Self = 0;
    const TEN: Self = 10;

    fn ilog10(self) -> u32 {
        i64::ilog10(self)
    }
}

impl DecimalPrimitive for i128 {
    const ZERO: Self = 0;
    const TEN: Self = 10;

    fn ilog10(self) -> u32 {
        i128::ilog10(self)
    }
}

/// Describes a decimal type.
pub trait DecimalType: Debug + Sync + Send + Copy + 'static {
    /// The underlying primitive type storing the decimal's value.
    type Primitive: DecimalPrimitive;
    /// Storage type to use for accessing the primitives in a decimal array.
    type Storage: MutableScalarStorage<StorageType = Self::Primitive>;

    /// Max precision for this decimal type.
    const MAX_PRECISION: u8;

    /// Default scale to use if none provided.
    const DEFAULT_SCALE: i8;

    /// Validates that the value is within the provided precision.
    fn validate_precision(value: Self::Primitive, precision: u8) -> Result<()> {
        if precision > Self::MAX_PRECISION {
            return Err(DbError::new(format!(
                "Precision {precision} is greater than max precision {}",
                Self::MAX_PRECISION
            )));
        }

        if value.is_zero() {
            return Ok(());
        }

        let digits = value.abs().ilog10() + 1;
        if digits > precision as u32 {
            return Err(DbError::new(format!(
                "{value} cannot be stored in decimal with a precision of {precision}"
            )));
        }

        Ok(())
    }

    /// Try to unwrap the decimal type metadata from a data type.
    ///
    /// Should return None if the datatype is not the correct type or size.
    fn decimal_meta_opt(datatype: &DataType) -> Option<DecimalTypeMeta>;

    /// Unwrap the decimal type metadata, returning an error if the datatype is
    /// not the correct type.
    fn decimal_meta(datatype: &DataType) -> Result<DecimalTypeMeta> {
        Self::decimal_meta_opt(datatype).ok_or_else(|| {
            DbError::new(format!(
                "Failed to unwrap decimal type meta, got: {datatype}"
            ))
        })
    }

    /// Create a new datatype using the provide precision and scale.
    fn datatype_from_decimal_meta(meta: DecimalTypeMeta) -> DataType;
}

/// 64-bit decimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal64Type;

impl DecimalType for Decimal64Type {
    type Primitive = i64;
    type Storage = PhysicalI64;
    const MAX_PRECISION: u8 = 18;
    // Note that changing this would require changing some of the date functions
    // since they assume this is 3.
    const DEFAULT_SCALE: i8 = 3;

    fn decimal_meta_opt(datatype: &DataType) -> Option<DecimalTypeMeta> {
        match &datatype.metadata {
            DataTypeMeta::Decimal(m) => Some(*m),
            _ => None,
        }
    }

    fn datatype_from_decimal_meta(meta: DecimalTypeMeta) -> DataType {
        DataType::decimal64(meta)
    }
}

/// 128-bit decimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128Type;

impl DecimalType for Decimal128Type {
    type Primitive = i128;
    type Storage = PhysicalI128;
    const MAX_PRECISION: u8 = 38;
    const DEFAULT_SCALE: i8 = 9;

    fn decimal_meta_opt(datatype: &DataType) -> Option<DecimalTypeMeta> {
        match &datatype.metadata {
            DataTypeMeta::Decimal(m) => Some(*m),
            _ => None,
        }
    }

    fn datatype_from_decimal_meta(meta: DecimalTypeMeta) -> DataType {
        DataType::decimal128(meta)
    }
}

/// Represents a single decimal value.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Hash)]
pub struct DecimalScalar<T: DecimalType> {
    pub precision: u8,
    pub scale: i8,
    pub value: T::Primitive,
}

pub type Decimal64Scalar = DecimalScalar<Decimal64Type>;
pub type Decimal128Scalar = DecimalScalar<Decimal128Type>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_precision() {
        // Do validate
        Decimal64Type::validate_precision(222, 3).unwrap();
        Decimal64Type::validate_precision(222, 4).unwrap();

        // Don't validate
        Decimal64Type::validate_precision(222, 2).unwrap_err();
        Decimal64Type::validate_precision(222, 19).unwrap_err();
    }
}
