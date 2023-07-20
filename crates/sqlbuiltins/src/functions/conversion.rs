use std::fmt;

use datafusion::scalar::ScalarValue;

use super::*;
/// Value from a function parameter.
#[derive(Debug, Clone)]
pub enum FuncParamValue {
    /// Normalized value from an ident.
    Ident(String),
    /// Scalar value.
    Scalar(ScalarValue),
    Array(Vec<FuncParamValue>),
}

impl fmt::Display for FuncParamValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ident(s) => write!(f, "{s}"),
            Self::Scalar(s) => write!(f, "{s}"),
            Self::Array(vals) => write!(f, "{}", FuncParamValue::multiple_to_string(vals)),
        }
    }
}

impl FuncParamValue {
    /// Print multiple function parameter values.
    pub fn multiple_to_string<T: AsRef<[Self]>>(vals: T) -> String {
        let mut s = String::new();
        write!(&mut s, "(").unwrap();
        let mut sep = "";
        for val in vals.as_ref() {
            write!(&mut s, "{sep}{val}").unwrap();
            sep = ", ";
        }
        write!(&mut s, ")").unwrap();
        s
    }

    /// Wrapper over `FromFuncParamValue::from_param`.
    pub(super) fn param_into<T>(self) -> Result<T>
    where
        T: FromFuncParamValue,
    {
        T::from_param(self)
    }
}

pub(super) trait FromFuncParamValue: Sized {
    /// Get the value from parameter.
    fn from_param(value: FuncParamValue) -> Result<Self>;

    /// Check if the value is valid (able to convert).
    ///
    /// If `is_param_valid` returns true, `from_param` should be safely
    /// `unwrap`able (i.e., not panic).
    fn is_param_valid(value: &FuncParamValue) -> bool;
}

impl FromFuncParamValue for String {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s),
            other => Err(BuiltinError::UnexpectedArg {
                param: other,
                expected: DataType::Utf8,
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(value, FuncParamValue::Scalar(ScalarValue::Utf8(Some(_))))
    }
}

impl<T> FromFuncParamValue for Vec<T>
where
    T: FromFuncParamValue,
{
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Array(arr) => {
                let mut res = Vec::with_capacity(arr.len());
                for val in arr {
                    res.push(T::from_param(val)?);
                }
                Ok(res)
            }
            other => Err(BuiltinError::UnexpectedArg {
                param: other,
                expected: DataType::List(Arc::new(Field::new("list", DataType::Null, false))),
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        if let FuncParamValue::Array(arr) = value {
            arr.iter().all(|v| T::is_param_valid(v))
        } else {
            false
        }
    }
}

pub(super) struct IdentValue(String);

impl IdentValue {
    pub(super) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for IdentValue {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<IdentValue> for String {
    fn from(value: IdentValue) -> Self {
        value.0
    }
}

impl FromFuncParamValue for IdentValue {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Ident(s) => Ok(Self(s)),
            _other => Err(BuiltinError::Static("expected an identifier")),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(value, FuncParamValue::Ident(_))
    }
}

impl FromFuncParamValue for i64 {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(s) => match s {
                ScalarValue::Int8(Some(v)) => Ok(v as i64),
                ScalarValue::Int16(Some(v)) => Ok(v as i64),
                ScalarValue::Int32(Some(v)) => Ok(v as i64),
                ScalarValue::Int64(Some(v)) => Ok(v),
                ScalarValue::UInt8(Some(v)) => Ok(v as i64),
                ScalarValue::UInt16(Some(v)) => Ok(v as i64),
                ScalarValue::UInt32(Some(v)) => Ok(v as i64),
                ScalarValue::UInt64(Some(v)) => Ok(v as i64), // TODO: Handle overflow?
                other => Err(BuiltinError::UnexpectedArg {
                    param: other.into(),
                    expected: DataType::Int64,
                }),
            },
            other => Err(BuiltinError::UnexpectedArg {
                param: other,
                expected: DataType::Int64,
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Scalar(ScalarValue::Int8(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int16(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int64(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt8(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt16(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt64(Some(_)))
        )
    }
}

impl FromFuncParamValue for f64 {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(s) => match s {
                ScalarValue::Int8(Some(v)) => Ok(v as f64),
                ScalarValue::Int16(Some(v)) => Ok(v as f64),
                ScalarValue::Int32(Some(v)) => Ok(v as f64),
                ScalarValue::Int64(Some(v)) => Ok(v as f64),
                ScalarValue::UInt8(Some(v)) => Ok(v as f64),
                ScalarValue::UInt16(Some(v)) => Ok(v as f64),
                ScalarValue::UInt32(Some(v)) => Ok(v as f64),
                ScalarValue::UInt64(Some(v)) => Ok(v as f64),
                ScalarValue::Float32(Some(v)) => Ok(v as f64),
                ScalarValue::Float64(Some(v)) => Ok(v),
                other => Err(BuiltinError::UnexpectedArg {
                    param: other.into(),
                    expected: DataType::Float64,
                }),
            },
            other => Err(BuiltinError::UnexpectedArg {
                param: other,
                expected: DataType::Float64,
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Scalar(ScalarValue::Float32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Float64(Some(_)))
        ) || i64::is_param_valid(value)
    }
}

impl From<String> for FuncParamValue {
    fn from(value: String) -> Self {
        Self::Ident(value)
    }
}

impl From<ScalarValue> for FuncParamValue {
    fn from(value: ScalarValue) -> Self {
        Self::Scalar(value)
    }
}
