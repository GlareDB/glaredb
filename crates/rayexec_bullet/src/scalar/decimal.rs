use num::{PrimInt, Signed};
use rayexec_error::{RayexecError, Result};
use std::fmt::{Debug, Display};

/// Default scale to use for decimals if one isn't provided.
pub const DECIMAL_DEFUALT_SCALE: i8 = 9;

pub trait DecimalPrimitive: PrimInt + Signed + Debug + Display {
    /// Returns the base 10 log of this number, rounded down.
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

pub trait DecimalType: Debug {
    /// The underlying primitive type storing the decimal's value.
    type Primitive: DecimalPrimitive;

    /// Max precision for this decimal type.
    const MAX_PRECISION: u8;

    /// Validates that the value is within the provided precision.
    fn validate_precision(value: Self::Primitive, precision: u8) -> Result<()> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal64Type;

impl DecimalType for Decimal64Type {
    type Primitive = i64;
    const MAX_PRECISION: u8 = 18;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal128Type;

impl DecimalType for Decimal128Type {
    type Primitive = i128;
    const MAX_PRECISION: u8 = 38;
}

/// Represents a single decimal value.
#[derive(Debug, Clone, Copy, PartialEq)]
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
