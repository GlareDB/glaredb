use std::fmt;

use datafusion::{
    scalar::ScalarValue,
    sql::sqlparser::ast::{Ident, UnaryOperator},
};

use super::*;

pub trait ExtractInto<T> {
    fn extract(&self) -> Result<T>;
    fn extract_named(&self, _name: &'static str) -> Result<T> {
        Err(BuiltinError::Unimplemented("named parameters"))
    }
}
pub trait ExtractNamedInto<T> {
    fn extract_named(&self, name: &'static str) -> Result<T>;
}

impl ExtractInto<String> for SqlFunctionArg {
    fn extract(&self) -> Result<String> {
        match self {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(
                SqlValue::SingleQuotedString(s),
            ))) => Ok(s.clone()),

            other => Err(BuiltinError::UnexpectedFunctionArg {
                param: other.clone(),
                expected: DataType::Utf8,
            }),
        }
    }
}

impl ExtractInto<Ident> for SqlFunctionArg {
    fn extract(&self) -> Result<Ident> {
        match self {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(ident))) => {
                Ok(ident.clone())
            }

            other => Err(BuiltinError::UnexpectedFunctionArg {
                param: other.clone(),
                expected: DataType::Utf8,
            }),
        }
    }

    fn extract_named(&self, name: &'static str) -> Result<Ident> {
        match self {
            FunctionArg::Named {
                name: n,
                arg: FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            } if n.value == name => Ok(ident.clone()),
            _ => Err(BuiltinError::UnexpectedFunctionArgs {
                params: vec![self.clone()],
                expected: format!("named parameter {}", name),
            }),
        }
    }
}

impl ExtractInto<Vec<String>> for SqlFunctionArg {
    fn extract(&self) -> Result<Vec<String>> {
        match self {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
                SqlExpr::Array(arr) => arr
                    .elem
                    .iter()
                    .map(|e| match e {
                        SqlExpr::Value(SqlValue::SingleQuotedString(s)) => Ok(s.clone()),

                        _ => Err(BuiltinError::UnexpectedFunctionArg {
                            param: self.clone(),
                            expected: DataType::Utf8,
                        }),
                    })
                    .collect(),
                _ => Err(BuiltinError::UnexpectedFunctionArg {
                    param: self.clone(),
                    expected: DataType::Utf8,
                }),
            },

            _ => Err(BuiltinError::UnexpectedFunctionArg {
                param: self.clone(),
                expected: DataType::Utf8,
            }),
        }
    }
}

impl ExtractInto<f64> for SqlFunctionArg {
    fn extract(&self) -> Result<f64> {
        match self {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(v))) => match v {
                SqlValue::Number(s, _) => Ok(s.parse::<f64>().unwrap()),
                _ => Err(BuiltinError::UnexpectedFunctionArg {
                    param: self.clone(),
                    expected: DataType::Float64,
                }),
            },
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: v,
            })) => {
                if let SqlExpr::Value(v) = v.as_ref() {
                    match v {
                        SqlValue::Number(s, _) => {
                            let v = s.parse::<f64>().unwrap();
                            Ok(-v)
                        }
                        _ => Err(BuiltinError::UnexpectedFunctionArg {
                            param: self.clone(),
                            expected: DataType::Int64,
                        }),
                    }
                } else {
                    Err(BuiltinError::UnexpectedFunctionArg {
                        param: self.clone(),
                        expected: DataType::Int64,
                    })
                }
            }
            _ => Err(BuiltinError::UnexpectedFunctionArg {
                param: self.clone(),
                expected: DataType::Float64,
            }),
        }
    }
}

impl ExtractInto<i64> for SqlFunctionArg {
    fn extract(&self) -> Result<i64> {
        match self {
            FunctionArg::Named { .. } => todo!(),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(v))) => {
                match v {
                    SqlValue::Number(s, _) => {
                        // Check for existence of decimal separator dot
                        if !s.contains('.') {
                            Ok(s.parse::<i64>().unwrap())
                        } else {
                            Err(BuiltinError::UnexpectedFunctionArg {
                                param: self.clone(),
                                expected: DataType::Int64,
                            })
                        }
                    }
                    _ => Err(BuiltinError::UnexpectedFunctionArg {
                        param: self.clone(),
                        expected: DataType::Int64,
                    }),
                }
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: v,
            })) => {
                if let SqlExpr::Value(v) = v.as_ref() {
                    match v {
                        SqlValue::Number(s, _) => {
                            // Check for existence of decimal separator dot
                            if !s.contains('.') {
                                let v = s.parse::<i64>().unwrap();
                                Ok(-v)
                            } else {
                                Err(BuiltinError::UnexpectedFunctionArg {
                                    param: self.clone(),
                                    expected: DataType::Int64,
                                })
                            }
                        }
                        _ => Err(BuiltinError::UnexpectedFunctionArg {
                            param: self.clone(),
                            expected: DataType::Int64,
                        }),
                    }
                } else {
                    todo!()
                    // Err(BuiltinError::UnexpectedFunctionArg {
                    //     param: self.clone(),
                    //     expected: DataType::Int64,
                    // })
                }
            }

            _ => Err(BuiltinError::UnexpectedFunctionArg {
                param: self.clone(),
                expected: DataType::Int64,
            }),
        }
    }
}

impl ExtractInto<String> for [SqlFunctionArg] {
    fn extract(&self) -> Result<String> {
        for arg in self {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(s),
                ))) => return Ok(s.clone()),
                _ => (),
            }
        }
        Err(BuiltinError::UnexpectedFunctionArgs {
            params: self.to_vec(),
            expected: "list of strings".to_string(),
        })
    }
    fn extract_named(&self, name: &'static str) -> Result<String> {
        for arg in self {
            match arg {
                FunctionArg::Named { name: n, .. } if n.value == name => return arg.extract(),
                _ => (),
            }
        }
        Err(BuiltinError::UnexpectedFunctionArgs {
            params: self.to_vec(),
            expected: "list of strings".to_string(),
        })
    }
}

/// Value from a function parameter.
#[derive(Debug, Clone)]
pub enum FuncParamValue {
    /// Normalized value from an ident.
    Ident(String),
    /// Scalar value.
    Scalar(ScalarValue),
}

impl fmt::Display for FuncParamValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ident(s) => write!(f, "{s}"),
            Self::Scalar(s) => write!(f, "{s}"),
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
    fn is_param_valid(value: &FuncParamValue) -> bool;
}

impl FromFuncParamValue for String {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s),
            // other => Err(BuiltinError::UnexpectedArg {
            //     param: other,
            //     expected: DataType::Utf8,
            // }),
            _ => todo!(),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(value, FuncParamValue::Scalar(ScalarValue::Utf8(Some(_))))
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
                // other => Err(BuiltinError::UnexpectedArg {
                //     param: other.into(),
                //     expected: DataType::Int64,
                // }),
                _ => todo!(),
            },
            // other => Err(BuiltinError::UnexpectedArg {
            //     param: other,
            //     expected: DataType::Int64,
            // }),
            _ => todo!(),
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
                // other => Err(BuiltinError::UnexpectedArg {
                //     param: other.into(),
                //     expected: DataType::Float64,
                // }),
                _ => todo!(),
            },
            // other => Err(BuiltinError::UnexpectedArg {
            //     param: other,
            //     expected: DataType::Float64,
            // }),
            _ => todo!(),
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
