use glaredb_error::{DbError, Result};
use serde::{Deserialize, Serialize};

use super::{AstParseable, Expr};
use crate::keywords::Keyword;
use crate::parser::Parser;
use crate::tokens::Token;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// VARCHAR, VARCHAR(10), TEXT, STRING
    Varchar(Option<u64>),
    /// BINARY, BINARY(10), BLOB
    Binary(Option<u64>),
    /// TINYINT, INT1
    TinyInt,
    /// SMALLINT, INT2
    SmallInt,
    /// INTEGER, INT, INT4
    Integer,
    /// BIGINT, INT8
    BigInt,
    /// UTINYINT, UINT1
    UnsignedTinyInt,
    /// USMALLINT, UINT2
    UnsignedSmallInt,
    /// UINT, UINT4
    UnsignedInt,
    /// UBIGINT, UINT8
    UnsignedBigInt,
    /// HALF, FLOAT2,
    Half,
    /// REAL, FLOAT, FLOAT4
    Real,
    /// DOUBLE, FLOAT8
    Double,
    /// DECIMAL, DECIMAL(<prec>, <scale>), NUMERIC
    Decimal(Option<i64>, Option<i64>),
    /// BOOL, BOOLEAN
    Bool,
    /// DATE
    Date,
    /// TIMESTAMP
    Timestamp,
    /// INTERVAL
    Interval,
}

impl AstParseable for DataType {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let kw = match parser.next() {
            Some(tok) => match tok.keyword() {
                Some(kw) => kw,
                None => return Err(DbError::new(format!("Expected keyword, got: {tok:?}"))),
            },
            None => return Err(DbError::new("Unexpected end of query")),
        };

        Ok(match kw {
            Keyword::VARCHAR => DataType::Varchar(None), // TODO: With length.
            Keyword::TEXT | Keyword::STRING => DataType::Varchar(None),
            Keyword::BINARY | Keyword::BLOB => DataType::Binary(None), // TODO: With length.
            Keyword::TINYINT | Keyword::INT1 => DataType::TinyInt,
            Keyword::SMALLINT | Keyword::INT2 => DataType::SmallInt,
            Keyword::INT | Keyword::INTEGER | Keyword::INT4 => DataType::Integer,
            Keyword::BIGINT | Keyword::INT8 => DataType::BigInt,
            Keyword::UTINYINT | Keyword::UINT1 => DataType::UnsignedTinyInt,
            Keyword::USMALLINT | Keyword::UINT2 => DataType::UnsignedSmallInt,
            Keyword::UINT | Keyword::UINT4 => DataType::UnsignedInt,
            Keyword::UBIGINT | Keyword::UINT8 => DataType::UnsignedBigInt,
            Keyword::HALF | Keyword::FLOAT2 => DataType::Half,
            Keyword::REAL | Keyword::FLOAT | Keyword::FLOAT4 => DataType::Real,
            Keyword::DOUBLE | Keyword::FLOAT8 => DataType::Double,
            Keyword::DECIMAL | Keyword::NUMERIC => {
                let (prec, scale) = Self::parse_precision_scale(parser)?;
                DataType::Decimal(prec, scale)
            }
            Keyword::BOOL | Keyword::BOOLEAN => DataType::Bool,
            Keyword::DATE => DataType::Date,
            Keyword::TIMESTAMP => DataType::Timestamp,
            Keyword::INTERVAL => DataType::Interval,
            other => {
                return Err(DbError::new(format!(
                    "Unexpected keyword for data type: {other:?}",
                )));
            }
        })
    }
}

impl DataType {
    fn parse_precision_scale(parser: &mut Parser) -> Result<(Option<i64>, Option<i64>)> {
        let (mut prec, mut scale) = (None, None);
        if parser.consume_token(&Token::LeftParen) {
            prec = Some(Expr::parse_i64_literal(parser)?);
            if parser.consume_token(&Token::Comma) {
                scale = Some(Expr::parse_i64_literal(parser)?);
            }
            parser.expect_token(&Token::RightParen)?;
        }
        Ok((prec, scale))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::testutil::parse_ast;

    fn assert_ast_eq(expected: DataType, s: &str) {
        assert_eq!(expected, parse_ast(s).unwrap())
    }

    #[test]
    fn basic() {
        assert_ast_eq(DataType::Varchar(None), "varchar");
        assert_ast_eq(DataType::Varchar(None), "VARCHAR");
        assert_ast_eq(DataType::Varchar(None), "Varchar");
        assert_ast_eq(DataType::Varchar(None), "text");

        assert_ast_eq(DataType::Binary(None), "BINARY");
        assert_ast_eq(DataType::Binary(None), "binary");
        assert_ast_eq(DataType::Binary(None), "blob");

        assert_ast_eq(DataType::TinyInt, "tinyint");
        assert_ast_eq(DataType::TinyInt, "int1");

        assert_ast_eq(DataType::SmallInt, "smallint");
        assert_ast_eq(DataType::SmallInt, "int2");

        assert_ast_eq(DataType::Integer, "int");
        assert_ast_eq(DataType::Integer, "integer");
        assert_ast_eq(DataType::Integer, "int4");

        assert_ast_eq(DataType::BigInt, "bigint");
        assert_ast_eq(DataType::BigInt, "int8");

        assert_ast_eq(DataType::UnsignedTinyInt, "utinyint");
        assert_ast_eq(DataType::UnsignedTinyInt, "uint1");

        assert_ast_eq(DataType::UnsignedSmallInt, "usmallint");
        assert_ast_eq(DataType::UnsignedSmallInt, "uint2");

        assert_ast_eq(DataType::UnsignedInt, "uint");
        assert_ast_eq(DataType::UnsignedInt, "uint4");

        assert_ast_eq(DataType::UnsignedBigInt, "ubigint");
        assert_ast_eq(DataType::UnsignedBigInt, "uint8");

        assert_ast_eq(DataType::Half, "half");
        assert_ast_eq(DataType::Half, "float2");

        assert_ast_eq(DataType::Real, "real");
        assert_ast_eq(DataType::Real, "float");
        assert_ast_eq(DataType::Real, "float4");

        assert_ast_eq(DataType::Double, "double");
        assert_ast_eq(DataType::Double, "float8");

        assert_ast_eq(DataType::Bool, "bool");
        assert_ast_eq(DataType::Bool, "boolean");

        assert_ast_eq(DataType::Date, "date");

        assert_ast_eq(DataType::Timestamp, "TIMESTAMP");

        assert_ast_eq(DataType::Interval, "INTERVAL");
    }

    #[test]
    fn decimal() {
        assert_ast_eq(DataType::Decimal(None, None), "decimal");
        assert_ast_eq(DataType::Decimal(Some(4), None), "decimal(4)");
        assert_ast_eq(DataType::Decimal(Some(4), Some(1)), "decimal(4, 1)");
        assert_ast_eq(DataType::Decimal(Some(4), Some(-1)), "decimal(4, -1)");

        assert_ast_eq(DataType::Decimal(Some(4), Some(1)), "numeric(4, 1)");
    }
}
