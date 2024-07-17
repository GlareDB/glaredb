//! Conversion functions for converting between SQL AST types and DataFusion types
//! These are needed to insulate us from datafusion's reexport of sqlparser::ast.
//! Most of the types are the exact same as the ones in datafusion, but on a different version.
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion::scalar::ScalarValue;
use parser::options::{CompressionTypeVariant, FileType};
use parser::sqlparser::ast;

#[repr(transparent)]
/// transparent wrapper around AST types to implement conversions to datafusion types
///
/// This allows us to be fully decoupled from datafusion's AST types _(reexported from sqlparser::ast)_
pub struct AstWrap<T>(pub(crate) T);

/// Convert a wrapped type to the target type
/// NOTE: `impl From<T>` can't be used directly because we can't implement `From` for types we don't own.
/// The input types are from the `parser` crate, and most of the target types are from the `datafusion` crate.
/// So this just provides a utility function to convert between the two.
/// It is the equivalent of `AstWrap(value).into()`, but provides a more readable API.
pub fn convert<F, I>(value: F) -> I
where
    AstWrap<F>: Into<I>,
{
    AstWrap(value).into()
}

/// Try to convert a wrapped type to the target type
/// NOTE: `impl TryInto<T>` can't be used directly because we can't implement `TryInto` for types we don't own.
/// The input types are from the `parser` crate, and most of the target types are from the `datafusion` crate.
/// So this just provides a utility function to convert between the two.
/// It is the equivalent of `AstWrap(value).try_into()`, but provides a more readable API.
pub fn try_convert<F, I>(value: F) -> std::result::Result<I, <AstWrap<F> as TryInto<I>>::Error>
where
    AstWrap<F>: TryInto<I>,
{
    AstWrap(value).try_into()
}

impl From<AstWrap<ast::Ident>> for datafusion::sql::sqlparser::ast::Ident {
    fn from(value: AstWrap<ast::Ident>) -> Self {
        datafusion::sql::sqlparser::ast::Ident {
            value: value.0.value,
            quote_style: value.0.quote_style,
        }
    }
}

impl<T> From<AstWrap<Option<T>>> for Option<T>
where
    T: From<AstWrap<T>>,
{
    fn from(value: AstWrap<Option<T>>) -> Self {
        value.0
    }
}

impl From<AstWrap<ast::IdentWithAlias>> for datafusion::sql::sqlparser::ast::IdentWithAlias {
    fn from(value: AstWrap<ast::IdentWithAlias>) -> Self {
        datafusion::sql::sqlparser::ast::IdentWithAlias {
            ident: convert(value.0.ident),
            alias: convert(value.0.alias),
        }
    }
}
impl From<AstWrap<Vec<ast::Ident>>> for Vec<datafusion::sql::sqlparser::ast::Ident> {
    fn from(value: AstWrap<Vec<ast::Ident>>) -> Self {
        value.0.into_iter().map(convert).collect()
    }
}
impl From<AstWrap<ast::ObjectName>> for datafusion::sql::sqlparser::ast::ObjectName {
    fn from(value: AstWrap<ast::ObjectName>) -> Self {
        datafusion::sql::sqlparser::ast::ObjectName(value.0 .0.into_iter().map(convert).collect())
    }
}
impl From<AstWrap<ast::WindowFrameUnits>> for WindowFrameUnits {
    fn from(value: AstWrap<ast::WindowFrameUnits>) -> Self {
        match value.0 {
            ast::WindowFrameUnits::Range => Self::Range,
            ast::WindowFrameUnits::Groups => Self::Groups,
            ast::WindowFrameUnits::Rows => Self::Rows,
        }
    }
}

impl TryFrom<AstWrap<ast::WindowFrameBound>> for WindowFrameBound {
    type Error = DataFusionError;
    fn try_from(value: AstWrap<ast::WindowFrameBound>) -> Result<Self> {
        Ok(match value.0 {
            ast::WindowFrameBound::Preceding(Some(v)) => {
                Self::Preceding(convert_frame_bound_to_scalar_value(*v)?)
            }
            ast::WindowFrameBound::Preceding(None) => Self::Preceding(ScalarValue::Null),
            ast::WindowFrameBound::Following(Some(v)) => {
                Self::Following(convert_frame_bound_to_scalar_value(*v)?)
            }
            ast::WindowFrameBound::Following(None) => Self::Following(ScalarValue::Null),
            ast::WindowFrameBound::CurrentRow => Self::CurrentRow,
        })
    }
}

impl TryFrom<AstWrap<ast::WindowFrame>> for WindowFrame {
    type Error = DataFusionError;

    fn try_from(value: AstWrap<ast::WindowFrame>) -> Result<Self> {
        let start_bound = AstWrap(value.0.start_bound).try_into()?;
        let end_bound = match value.0.end_bound {
            Some(value) => AstWrap(value).try_into()?,
            None => WindowFrameBound::CurrentRow,
        };

        if let WindowFrameBound::Following(val) = &start_bound {
            if val.is_null() {
                plan_err!("Invalid window frame: start bound cannot be UNBOUNDED FOLLOWING")?
            }
        } else if let WindowFrameBound::Preceding(val) = &end_bound {
            if val.is_null() {
                plan_err!("Invalid window frame: end bound cannot be UNBOUNDED PRECEDING")?
            }
        };
        let units = AstWrap(value.0.units).into();
        Ok(Self::new_bounds(units, start_bound, end_bound))
    }
}

pub fn convert_frame_bound_to_scalar_value(v: ast::Expr) -> Result<ScalarValue> {
    Ok(ScalarValue::Utf8(Some(match v {
        ast::Expr::Value(ast::Value::Number(value, false))
        | ast::Expr::Value(ast::Value::SingleQuotedString(value)) => value,
        ast::Expr::Interval(ast::Interval {
            value,
            leading_field,
            ..
        }) => {
            let result = match *value {
                ast::Expr::Value(ast::Value::SingleQuotedString(item)) => item,
                e => {
                    return Err(datafusion::common::sql_datafusion_err!(
                        datafusion::sql::sqlparser::parser::ParserError::ParserError(format!(
                            "INTERVAL expression cannot be {e:?}"
                        ))
                    ));
                }
            };
            if let Some(leading_field) = leading_field {
                format!("{result} {leading_field}")
            } else {
                result
            }
        }
        _ => plan_err!("Invalid window frame: frame offsets must be non negative integers")?,
    })))
}

impl From<AstWrap<FileType>> for datafusion::common::FileType {
    fn from(value: AstWrap<FileType>) -> Self {
        match value.0 {
            FileType::ARROW => Self::ARROW,
            FileType::AVRO => Self::AVRO,
            FileType::PARQUET => Self::PARQUET,
            FileType::CSV => Self::CSV,
            FileType::JSON => Self::JSON,
        }
    }
}

impl From<AstWrap<CompressionTypeVariant>> for datafusion::common::parsers::CompressionTypeVariant {
    fn from(value: AstWrap<CompressionTypeVariant>) -> Self {
        match value.0 {
            CompressionTypeVariant::GZIP => Self::GZIP,
            CompressionTypeVariant::BZIP2 => Self::BZIP2,
            CompressionTypeVariant::XZ => Self::XZ,
            CompressionTypeVariant::ZSTD => Self::ZSTD,
            CompressionTypeVariant::UNCOMPRESSED => Self::UNCOMPRESSED,
        }
    }
}
