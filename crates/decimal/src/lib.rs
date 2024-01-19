use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    ops::{Add, AddAssign, DivAssign, MulAssign},
    str::FromStr,
};

use num_traits::{
    one, ops::overflowing::OverflowingMul, zero, CheckedMul, Float, NumCast, One, PrimInt, Signed,
    Zero,
};
use regex::Regex;

#[derive(Debug, thiserror::Error)]
pub enum DecimalError {
    #[error("Invalid scale: {0}, max allowed {1}")]
    InvalidScale(i8, u8),

    #[error("Parse error: Invalid string '{0}'")]
    ParseError(String),

    #[error("Overflow error: {0}")]
    OverflowError(String),

    #[error("Cannot cast {0} into {1}")]
    CastError(String, &'static str),

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

pub type Result<T, E = DecimalError> = std::result::Result<T, E>;

pub trait DecimalType: Debug + Eq {
    /// Integer type to store the mantissa.
    type MantissaType: PrimInt
        + Signed
        + Zero
        + One
        + NumCast
        + Display
        + Debug
        + DivAssign
        + MulAssign
        + AddAssign
        + OverflowingMul
        + FromStr;
    /// Maximum value of `abc(scale)`.
    const MAX_SCALE: u8;
    /// Name of mantissa type.
    const MANTISSA_TYPE_NAME: &'static str;
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
    const MANTISSA_TYPE_NAME: &'static str = "128 bit integer";
}

/// Compatible with arrow's `Decimal128`.
pub type Decimal128 = Decimal<DecimalType128>;

/// [`Decimal`] is a precision decimal type that intends to act as an interface between
/// arrow and .
#[derive(Debug, PartialEq, Eq)]
pub struct Decimal<T: DecimalType> {
    mantissa: T::MantissaType,
    scale: i8,
}

impl<T: DecimalType> Clone for Decimal<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: DecimalType> Copy for Decimal<T> {}

impl<T: DecimalType> Decimal<T> {
    /// Creates a new [`Decimal`] using the [`DecimalType`].
    pub fn new(mantissa: T::MantissaType, scale: i8) -> Result<Self> {
        if scale.unsigned_abs() > T::MAX_SCALE {
            return Err(DecimalError::InvalidScale(scale, T::MAX_SCALE));
        }
        Ok(Self { mantissa, scale })
    }

    /// Creates a new [`Decimal`] from integers.
    pub fn try_from_int<I: PrimInt + Display>(i: I) -> Result<Self> {
        let mantissa: T::MantissaType = NumCast::from(i)
            .ok_or_else(|| DecimalError::CastError(i.to_string(), T::MANTISSA_TYPE_NAME))?;
        Ok(Self { mantissa, scale: 0 })
    }

    /// Creates a new [`Decimal`] from floats.
    pub fn try_from_float<F: Float + Display>(f: F) -> Result<Self> {
        use std::io::Write;

        let mut buf = [0_u8; 38];
        write!(&mut buf[..], "{f:+}")?;

        let mut buf = buf.into_iter();
        let mut mantissa: T::MantissaType = zero();
        let mut scale: Option<i8> = None;

        let neg = match buf.next() {
            Some(b'-') => true,
            Some(b'+') => false,
            _ => {
                debug_assert!(false, "first letter of float must be a sign");
                false
            }
        };

        let n10 = ten::<T>();
        for v in buf {
            if v.is_ascii_digit() {
                mantissa *= n10;
                mantissa += NumCast::from(v - b'0')
                    .ok_or_else(|| DecimalError::CastError(v.to_string(), T::MANTISSA_TYPE_NAME))?;
                scale = scale.map(|s| s + 1);
            } else if v == b'.' {
                scale = Some(0);
            } else {
                break;
            }
        }

        let scale = scale.unwrap_or(0);
        if neg {
            mantissa *= NumCast::from(-1).unwrap();
        }
        let decimal = Self::new(mantissa, scale)?;
        Ok(decimal)
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
            (self.mantissa, _) = self.mantissa.overflowing_mul(&n10);
        }
    }

    fn rescale_to_cmp(&mut self, other: &mut Self) {
        if self.scale < other.scale {
            self.rescale(other.scale);
        } else {
            other.rescale(self.scale);
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

impl<T: DecimalType> PartialOrd for Decimal<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // We try to rescale the one with lower scale to higher one and
        // compare the mantissa.
        let mut this = *self;
        let mut other = *other;
        this.rescale_to_cmp(&mut other);
        this.mantissa.partial_cmp(&other.mantissa)
    }
}

impl<T: DecimalType> AddAssign for Decimal<T> {
    fn add_assign(&mut self, mut rhs: Self) {
        self.rescale_to_cmp(&mut rhs);
        self.mantissa += rhs.mantissa;
    }
}

impl<T: DecimalType> Add for Decimal<T> {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl<T: DecimalType> Zero for Decimal<T> {
    fn is_zero(&self) -> bool {
        self.mantissa.is_zero()
    }

    fn zero() -> Self {
        Self {
            mantissa: zero(),
            scale: 0,
        }
    }

    fn set_zero(&mut self) {
        self.mantissa.set_zero()
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

    #[test]
    fn test_partial_ord() {
        let test_cases = vec![
            (
                Decimal128::new(123, 1),
                Decimal128::new(456, 1),
                Ordering::Less,
            ),
            (
                Decimal128::new(456, 1),
                Decimal128::new(123, 1),
                Ordering::Greater,
            ),
            (
                Decimal128::new(123, 1),
                Decimal128::new(123, 1),
                Ordering::Equal,
            ),
            (
                Decimal128::new(12345, 3),
                Decimal128::new(456, 1),
                Ordering::Less,
            ),
            (
                Decimal128::new(456, 1),
                Decimal128::new(12345, 3),
                Ordering::Greater,
            ),
            (
                Decimal128::new(12345, 3),
                Decimal128::new(456, 1),
                Ordering::Less,
            ),
            (
                Decimal128::new(456, 1),
                Decimal128::new(12345, 3),
                Ordering::Greater,
            ),
            (
                Decimal128::new(12345, 3),
                Decimal128::new(-456, 1),
                Ordering::Greater,
            ),
            (
                Decimal128::new(-456, 1),
                Decimal128::new(12345, 3),
                Ordering::Less,
            ),
            (
                Decimal128::new(12300, 3),
                Decimal128::new(123, 1),
                Ordering::Equal,
            ),
            (
                Decimal128::new(123, 1),
                Decimal128::new(12300, 3),
                Ordering::Equal,
            ),
        ];

        for (a, b, res) in test_cases {
            let a = a.unwrap();
            let b = b.unwrap();
            assert_eq!(a.partial_cmp(&b).unwrap(), res);
        }
    }

    #[test]
    fn test_add() {
        let test_cases = vec![
            (
                Decimal128::new(123, 1),
                Decimal128::new(456, 1),
                Decimal::new(579, 1),
            ),
            (
                Decimal128::new(12300, 3),
                Decimal128::new(456, 1),
                Decimal::new(57900, 3),
            ),
            (
                Decimal128::new(12300, 3),
                Decimal128::new(-456, 5),
                Decimal::new(1229544, 5),
            ),
        ];

        for (a, b, res) in test_cases {
            let mut a = a.unwrap();
            let b = b.unwrap();
            let res = res.unwrap();

            assert_eq!(a + b, res);

            a += b;
            assert_eq!(a, res);
        }
    }

    #[test]
    fn test_is_zero() {
        let test_cases = vec![
            (Decimal128::new(123, 0), false),
            (Decimal128::new(0, 0), true),
            (Decimal128::new(0, 37), true),
        ];
        for case in test_cases {
            let d = case.0.unwrap();
            assert_eq!(d.is_zero(), case.1);
        }
    }

    #[test]
    fn test_zero() {
        let z: Decimal128 = zero();
        assert!(z.is_zero());
    }

    #[test]
    fn test_set_zero() {
        let mut z = Decimal128::new(12345, 4).unwrap();
        assert!(!z.is_zero());
        z.set_zero();
        assert!(z.is_zero());
    }

    #[test]
    fn test_from_int() {
        let test_cases: Vec<(_, i128)> = vec![
            (Decimal128::try_from_int(12_u8), 12),
            (Decimal128::try_from_int(123245_i64), 123245),
            (Decimal128::try_from_int(-123245_i64), -123245),
        ];

        for (d, m) in test_cases {
            let d = d.unwrap();
            assert_eq!(d.mantissa(), m);
            assert_eq!(d.scale(), 0);
        }
    }

    #[test]
    fn test_from_float() {
        let test_cases: Vec<(_, i128, i8)> = vec![
            (Decimal128::try_from_float(12.345_f32), 12345, 3),
            (Decimal128::try_from_float(-123.45_f32), -12345, 2),
            (Decimal128::try_from_float(102.00_f64), 102, 0),
            (Decimal128::try_from_float(-0.234_f64), -234, 3),
        ];

        for (d, m, s) in test_cases {
            let d = d.unwrap();
            assert_eq!(d.mantissa(), m);
            assert_eq!(d.scale(), s);
        }
    }

    #[test]
    fn test_copy_and_eq() {
        let mut x = Decimal128::new(123, 2).unwrap();
        let y = x;
        x.rescale(4);

        assert_ne!(x, y);
        assert_eq!(x, Decimal128::new(12300, 4).unwrap());
        assert_eq!(y, Decimal128::new(123, 2).unwrap());
    }
}
