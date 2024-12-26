use std::fmt::{Debug, Display};

use num::{FromPrimitive, PrimInt, Signed, Zero};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

// use crate::executor::physical_type::{PhysicalI128, PhysicalI64, PhysicalStorage};

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

pub trait DecimalType: Debug + Sync + Send + Copy + 'static {
    /// The underlying primitive type storing the decimal's value.
    type Primitive: DecimalPrimitive;

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
            return Err(RayexecError::new(format!(
                "Precision {precision} is greater than max precision {}",
                Self::MAX_PRECISION
            )));
        }

        let digits = value.abs().ilog10() + 1;
        if digits > precision as u32 {
            return Err(RayexecError::new(format!(
                "{value} cannot be stored in decimal with a precision of {precision}"
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal64Type;

impl DecimalType for Decimal64Type {
    type Primitive = i64;
    const MAX_PRECISION: u8 = 18;
    // Note that changing this would require changing some of the date functions
    // since they assume this is 3.
    const DEFAULT_SCALE: i8 = 3;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128Type;

impl DecimalType for Decimal128Type {
    type Primitive = i128;
    const MAX_PRECISION: u8 = 38;
    const DEFAULT_SCALE: i8 = 9;
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
    type ProtoType = rayexec_proto::generated::expr::Decimal64Scalar;

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
    type ProtoType = rayexec_proto::generated::expr::Decimal128Scalar;

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
                    .map_err(|_| RayexecError::new("byte buffer not 16 bytes"))?,
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
