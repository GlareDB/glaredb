use std::fmt::{Debug, Display};

use glaredb_error::{DbError, Result, ResultExt};
use glaredb_proto::ProtoConv;
use num_traits::{FromPrimitive, PrimInt, Signed, Zero};
use serde::{Deserialize, Serialize};

use crate::arrays::array::physical_type::{MutableScalarStorage, PhysicalI128, PhysicalI64};
use crate::arrays::datatype::{DataType, DecimalTypeMeta};

/// Trait describing the underlying primivite representing the decimal.
pub trait DecimalPrimitive:
    PrimInt + FromPrimitive + Signed + Default + Debug + Display + Sync + Send
{
    /// Returns the base 10 log of this number, rounded down.
    ///
    /// This is guaranteed to be called with a non-zero positive number.
    fn ilog10(self) -> u32;
}

impl DecimalPrimitive for i64 {
    fn ilog10(self) -> u32 {
        i64::ilog10(self)
    }
}

impl DecimalPrimitive for i128 {
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
        if value.is_zero() {
            return Ok(());
        }

        if precision > Self::MAX_PRECISION {
            return Err(DbError::new(format!(
                "Precision {precision} is greater than max precision {}",
                Self::MAX_PRECISION
            )));
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
        Self::decimal_meta_opt(datatype)
            .ok_or_else(|| DbError::new("Failed to unwrap decimal type meta"))
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
        match datatype {
            DataType::Decimal64(m) => Some(*m),
            _ => None,
        }
    }

    fn datatype_from_decimal_meta(meta: DecimalTypeMeta) -> DataType {
        DataType::Decimal64(meta)
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
        match datatype {
            DataType::Decimal128(m) => Some(*m),
            _ => None,
        }
    }

    fn datatype_from_decimal_meta(meta: DecimalTypeMeta) -> DataType {
        DataType::Decimal128(meta)
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

impl ProtoConv for Decimal64Scalar {
    type ProtoType = glaredb_proto::generated::expr::Decimal64Scalar;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            precision: self.precision as i32,
            scale: self.scale as i32,
            value: self.value,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            precision: proto
                .precision
                .try_into()
                .context("precision doens't fit")?,
            scale: proto.scale.try_into().context("scale doens't fit")?,
            value: proto.value,
        })
    }
}

impl ProtoConv for Decimal128Scalar {
    type ProtoType = glaredb_proto::generated::expr::Decimal128Scalar;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            precision: self.precision as i32,
            scale: self.scale as i32,
            value: self.value.to_le_bytes().to_vec(),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            precision: proto
                .precision
                .try_into()
                .context("precision doens't fit")?,
            scale: proto.scale.try_into().context("scale doens't fit")?,
            value: i128::from_le_bytes(
                proto
                    .value
                    .try_into()
                    .map_err(|_| DbError::new("byte buffer not 16 bytes"))?,
            ),
        })
    }
}

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
