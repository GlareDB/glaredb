use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    ops::{AddAssign, DivAssign, MulAssign},
    str::FromStr,
};

use num_traits::{one, zero, CheckedMul, NumCast, PrimInt, Signed, Zero};
use regex::Regex;

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum DecimalError {
    #[error("Invalid scale: {0}, max allowed {1}")]
    InvalidScale(i8, u8),

    #[error("Parse error: Invalid string '{0}'")]
    ParseError(String),
}

pub type Result<T, E = DecimalError> = std::result::Result<T, E>;

pub trait DecimalType: Debug + Eq {
    /// Integer type to store the mantissa.
    type MantissaType: PrimInt
        + Signed
        + Zero
        + NumCast
        + Display
        + Debug
        + DivAssign
        + MulAssign
        + AddAssign
        + FromStr;
    /// Maximum value of `abc(scale)`.
    const MAX_SCALE: u8;
}

#[inline]
fn ten<T: DecimalType>() -> T::MantissaType {
    NumCast::from(10).unwrap()
}

#[derive(Debug, PartialEq, Eq)]
pub struct DecimalType128;

impl DecimalType for DecimalType128 {
    type MantissaType = i128;
    const MAX_SCALE: u8 = 38;
}

/// Compatible with arrow's `Decimal128`.
pub type Decimal128 = Decimal<DecimalType128>;

/// [`Decimal`] is a precision decimal type that intends to act as an interface between
/// arrow and .
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal<T: DecimalType> {
    mantissa: T::MantissaType,
    scale: i8,
}

impl<T: DecimalType> Decimal<T> {
    /// Creates a new [`Decimal`] using the [`DecimalType`].
    pub fn new(mantissa: T::MantissaType, scale: i8) -> Result<Self> {
        if scale.unsigned_abs() > T::MAX_SCALE {
            return Err(DecimalError::InvalidScale(scale, T::MAX_SCALE));
        }
        Ok(Self { mantissa, scale })
    }

    /// Returns the scale (exponential part) of the decimal.
    pub fn scale(&self) -> i8 {
        self.scale
    }

    /// Returns the mantissa (significant digits) of the decimal.
    pub fn mantissa(&self) -> T::MantissaType {
        self.mantissa
    }

    /// Updates the scale for the decimal trying to keep the value intact.
    ///
    /// If the `new_scale` is more than the max allowed scale, either the
    /// decimal will be converted to 0 or it will overflow.
    pub fn rescale(&mut self, new_scale: i8) {
        let n10 = ten::<T>();
        while self.scale > new_scale {
            self.scale -= 1;
            self.mantissa /= n10;
        }
        while self.scale < new_scale {
            self.scale += 1;
            self.mantissa *= n10;
        }
    }
}

impl<T: DecimalType> Display for Decimal<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.scale.cmp(&0) {
            Ordering::Equal => write!(f, "{}", self.mantissa)?,
            Ordering::Less => write!(
                f,
                "{}{:0<width$}",
                self.mantissa,
                "",
                width = self.scale.unsigned_abs() as usize
            )?,
            Ordering::Greater => {
                let z = zero();
                let n10 = ten::<T>();
                let mut m = self.mantissa;
                if m.is_negative() {
                    f.write_str("-")?;
                }
                // Number of digits can't exceed more than 256.
                let mut digits: [u8; 256] = [0; 256];
                let mut num_dig: usize = 0;
                while m != z {
                    let d: u8 = NumCast::from((m % n10).abs()).unwrap();
                    digits[num_dig] = d;
                    num_dig += 1;
                    m /= n10;
                }
                let scale = self.scale as usize;
                if scale >= num_dig {
                    write!(f, "0.{:0>width$}", self.mantissa, width = scale)?;
                } else {
                    for i in 0..num_dig {
                        let i = num_dig - i - 1;
                        let d = digits[i];
                        write!(f, "{d}")?;
                        if i == scale {
                            write!(f, ".")?;
                        }
                    }
                }
            }
        };
        Ok(())
    }
}

impl<T: DecimalType> FromStr for Decimal<T> {
    type Err = DecimalError;

    fn from_str(v: &str) -> std::result::Result<Self, Self::Err> {
        let re = Regex::new(r"^([+-]?\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$").unwrap();
        let captures = re
            .captures(v)
            .ok_or(DecimalError::ParseError(v.to_string()))?;

        let mantissa = captures
            .get(1)
            .expect("capture group 1 should exist")
            .as_str();

        let identity: T::MantissaType = match mantissa.get(0..1) {
            Some("-") => NumCast::from(-1).unwrap(),
            _ => one(),
        };

        let dec = captures.get(2);
        let exp = captures.get(3);

        let mut idx = mantissa.len();
        if dec.is_none() && exp.is_none() && mantissa.len() > T::MAX_SCALE as usize {
            // Try reducing the trailing zeroes.
            for (i, c) in mantissa.char_indices().rev() {
                if c != '0' || mantissa.len() - i > T::MAX_SCALE as usize {
                    idx = i + 1;
                    break;
                }
            }
        }
        let scale: i8 = (mantissa.len() - idx)
            .try_into()
            .map_err(|_e| DecimalError::ParseError(v.to_string()))?;
        // Scale will either be negative here or 0 (after removing trailing
        // zeroes).
        let mut scale = -scale;
        let mantissa = mantissa.get(0..idx).unwrap();

        let mut mantissa: T::MantissaType = mantissa
            .parse()
            .map_err(|_e| DecimalError::ParseError(v.to_string()))?;

        let n10 = ten::<T>();

        if let Some(dec) = dec {
            let dec = dec.as_str();
            let num_dig = dec.len();

            let mut dec: T::MantissaType = dec
                .parse()
                .map_err(|_e| DecimalError::ParseError(v.to_string()))?;

            dec *= identity;

            let ten_pow = n10.pow(num_dig as u32);
            mantissa = mantissa
                .checked_mul(&ten_pow)
                .ok_or(DecimalError::ParseError(v.to_string()))?;

            mantissa += dec;

            let num_dig: i8 = num_dig
                .try_into()
                .map_err(|_e| DecimalError::ParseError(v.to_string()))?;

            scale = num_dig;
        }

        if let Some(exp) = exp {
            let exp: i8 = exp
                .as_str()
                .parse()
                .map_err(|_e| DecimalError::ParseError(v.to_string()))?;

            scale = scale
                .checked_sub(exp)
                .ok_or(DecimalError::ParseError(v.to_string()))?;
        }

        if scale.unsigned_abs() > T::MAX_SCALE {
            return Err(DecimalError::InvalidScale(scale, T::MAX_SCALE));
        }

        Ok(Self { mantissa, scale })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let test_cases = vec![
            (Decimal128::new(12345, 0), "12345"),
            (Decimal128::new(12345, -3), "12345000"),
            (Decimal128::new(12345, 7), "0.0012345"),
            (Decimal128::new(12345, 5), "0.12345"),
            (Decimal128::new(12345, 3), "12.345"),
            (Decimal128::new(0, 5), "0.00000"),
            (Decimal128::new(1, -21), "1000000000000000000000"),
            (Decimal128::new(1, 21), "0.000000000000000000001"),
            (
                Decimal128::new(i128::MAX, 38),
                "1.70141183460469231731687303715884105727",
            ),
            (
                Decimal128::new(i128::MIN, 38),
                "-1.70141183460469231731687303715884105728",
            ),
        ];
        for case in test_cases {
            let d = case.0.unwrap();
            assert_eq!(&d.to_string(), case.1);
        }
    }

    #[test]
    fn test_parse() {
        let test_cases = vec![
            (Decimal128::new(12345, 0), "12345"),
            (Decimal128::new(12345000, 0), "12345000"),
            (Decimal128::new(12345, 7), "0.0012345"),
            (Decimal128::new(12345, 5), "0.12345"),
            (Decimal128::new(12345, 3), "12.345"),
            (Decimal128::new(0, 5), "-0.00000"),
            (
                Decimal128::new(1000000000000000000000, 0),
                "1000000000000000000000",
            ),
            (Decimal128::new(1, 21), "0.000000000000000000001"),
            (
                Decimal128::new(12345, -35),
                "1234500000000000000000000000000000000000",
            ),
            (
                Decimal128::new(1234500, -38),
                "123450000000000000000000000000000000000000000",
            ),
            (
                Decimal128::new(i128::MAX, 38),
                "+1.70141183460469231731687303715884105727",
            ),
            (
                Decimal128::new(i128::MIN, 38),
                "-1.70141183460469231731687303715884105728",
            ),
            (Decimal128::new(12, 4), "12e-4"),
            (Decimal128::new(12345, 0), "12.345E+3"),
            (Decimal128::new(-123456, 2), "-12.3456e2"),
        ];
        for case in test_cases {
            let d: Decimal128 = case.1.parse().unwrap();
            assert_eq!(d, case.0.unwrap());
        }
    }

    #[test]
    fn test_scale() {
        let test_cases = vec![
            (Decimal128::new(12345, 2), 2),
            (Decimal128::new(123, -2), -2),
        ];
        for case in test_cases {
            let d = case.0.unwrap();
            assert_eq!(d.scale(), case.1);
        }
    }

    #[test]
    fn test_mantissa() {
        let test_cases = vec![
            (Decimal128::new(12345, 2), 12345),
            (Decimal128::new(123, -2), 123),
        ];
        for case in test_cases {
            let d = case.0.unwrap();
            assert_eq!(d.mantissa(), case.1);
        }
    }

    #[test]
    fn test_rescale() {
        let test_cases = vec![
            (Decimal128::new(12345, 2), 3, Decimal128::new(123450, 3)),
            (Decimal128::new(123450, 3), 2, Decimal128::new(12345, 2)),
            (Decimal128::new(12345000, 1), -2, Decimal128::new(12345, -2)),
            (Decimal128::new(12345, -2), 1, Decimal128::new(12345000, 1)),
            (Decimal128::new(1234500, -1), -3, Decimal128::new(12345, -3)),
            (Decimal128::new(12345, -3), -1, Decimal128::new(1234500, -1)),
        ];
        for case in test_cases {
            let mut d = case.0.unwrap();
            d.rescale(case.1);
            let d1 = case.2.unwrap();
            assert_eq!(d, d1);
            assert_eq!(d.to_string(), d1.to_string());
        }
    }

    #[test]
    fn test_invalid_new() {
        let test_cases = vec![Decimal128::new(123, 45), Decimal128::new(123, -45)];
        for case in test_cases {
            case.expect_err("invalid decimal scale should error");
        }
    }

    #[test]
    fn test_invalid_parse() {
        let test_cases = vec![
            "123_45",
            "123e",
            "1234567890123456789012345678901234567890123456",
            "123456789012345678901234567890.123456789012345",
        ];
        for case in test_cases {
            Decimal128::from_str(case).expect_err("invalid decimal string should error");
        }
    }
}
