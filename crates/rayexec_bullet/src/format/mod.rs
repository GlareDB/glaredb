pub mod pretty;
pub mod ugly;

use crate::{
    array::Array,
    scalar::ScalarValue,
};
use rayexec_error::Result;
use std::fmt;

/// Formatting options for arrays and scalars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatOptions<'a> {
    /// String to use for printing null values.
    pub null: &'a str,

    /// String to use when a string value is empty.
    pub empty_string: &'a str,
}

impl FormatOptions<'_> {
    pub const fn new() -> Self {
        FormatOptions {
            null: "NULL",
            empty_string: "",
        }
    }
}

impl Default for FormatOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct Formatter<'a> {
    options: FormatOptions<'a>,
}

impl<'a> Formatter<'a> {
    pub const fn new(options: FormatOptions<'a>) -> Self {
        Formatter { options }
    }

    /// Create a formatted scalar value directly from a scalar value.
    pub fn format_scalar_value<'b>(&self, scalar: ScalarValue<'b>) -> FormattedScalarValue<'_, 'b> {
        FormattedScalarValue {
            options: &self.options,
            scalar,
        }
    }

    /// Create a formatted scalar value by retrieving the scalar at `idx` from
    /// the array.
    ///
    /// Returns `None` if the idx is out of bounds.
    pub fn format_array_value<'b>(
        &self,
        array: &'b Array,
        idx: usize,
    ) -> Result<FormattedScalarValue<'_, 'b>> {
        let scalar = array.logical_value(idx)?;
        Ok(self.format_scalar_value(scalar))
    }
}

#[derive(Debug, Clone)]
pub struct FormattedScalarValue<'a, 'b> {
    options: &'a FormatOptions<'a>,
    scalar: ScalarValue<'b>,
}

impl fmt::Display for FormattedScalarValue<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.scalar {
            ScalarValue::Null => write!(f, "{}", self.options.null),
            ScalarValue::Utf8(v) => {
                if v.is_empty() {
                    write!(f, "{}", self.options.empty_string)
                } else {
                    write!(f, "{v}")
                }
            }
            other => write!(f, "{other}"), // Use the scalar value's default display impl.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_scalar() {
        // Using the default options.

        // (input, formatted output)
        let test_cases = [
            (ScalarValue::Null, "NULL"),
            (ScalarValue::Int64(8), "8"),
            (ScalarValue::Utf8("hello".into()), "hello"),
            (
                ScalarValue::Binary([245, 255, 18].as_slice().into()),
                "[F5, FF, 12]",
            ),
        ];

        for (scalar, expected) in test_cases {
            let out = Formatter::new(FormatOptions::new())
                .format_scalar_value(scalar)
                .to_string();
            assert_eq!(expected, out);
        }
    }

    #[test]
    fn null_formatting() {
        let opts = FormatOptions {
            null: "NuLl",
            ..Default::default()
        };

        let out = Formatter::new(opts)
            .format_scalar_value(ScalarValue::Null)
            .to_string();

        assert_eq!("NuLl", out);
    }

    #[test]
    fn empty_string() {
        let opts = FormatOptions {
            empty_string: "(empty)",
            ..FormatOptions::new()
        };

        let formatter = Formatter::new(opts);

        let out = formatter
            .format_scalar_value(ScalarValue::Utf8("".into()))
            .to_string();
        assert_eq!("(empty)", out);
    }
}
