use super::AstParseable;
use crate::keywords::Keyword;
use crate::parser::Parser;
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    /// VARCHAR, VARCHAR(10), TEXT, STRING
    Varchar(Option<u64>),
    /// SMALLINT, INT2
    SmallInt,
    /// INTEGER, INT, INT4
    Integer,
    /// BIGINT, INT8
    BigInt,
    /// REAL, FLOAT, FLOAT4
    Real,
    /// DOUBLE, FLOAT8
    Double,
    /// BOOL, BOOLEAN
    Bool,
}

impl AstParseable for DataType {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let kw = match parser.next() {
            Some(tok) => match tok.keyword() {
                Some(kw) => kw,
                None => return Err(RayexecError::new(format!("Expected keyword, got: {tok:?}"))),
            },
            None => return Err(RayexecError::new("Unexpected end of query")),
        };

        Ok(match kw {
            Keyword::VARCHAR => DataType::Varchar(None), // TODO: With length.
            Keyword::TEXT | Keyword::STRING => DataType::Varchar(None),
            Keyword::SMALLINT | Keyword::INT2 => DataType::SmallInt,
            Keyword::INT | Keyword::INTEGER | Keyword::INT4 => DataType::Integer,
            Keyword::BIGINT | Keyword::INT8 => DataType::BigInt,
            Keyword::REAL | Keyword::FLOAT | Keyword::FLOAT4 => DataType::Real,
            Keyword::DOUBLE | Keyword::FLOAT8 => DataType::Double,
            Keyword::BOOL | Keyword::BOOLEAN => DataType::Bool,
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected keyword for data type: {other:?}",
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::testutil::parse_ast;

    use super::*;

    fn assert_ast_eq(expected: DataType, s: &str) {
        assert_eq!(expected, parse_ast(s).unwrap())
    }

    #[test]
    fn basic() {
        assert_ast_eq(DataType::Varchar(None), "varchar");
        assert_ast_eq(DataType::Varchar(None), "VARCHAR");
        assert_ast_eq(DataType::Varchar(None), "Varchar");

        assert_ast_eq(DataType::SmallInt, "smallint");
        assert_ast_eq(DataType::SmallInt, "int2");

        assert_ast_eq(DataType::Integer, "int");
        assert_ast_eq(DataType::Integer, "integer");
        assert_ast_eq(DataType::Integer, "int4");

        assert_ast_eq(DataType::BigInt, "bigint");
        assert_ast_eq(DataType::BigInt, "int8");

        assert_ast_eq(DataType::Real, "real");
        assert_ast_eq(DataType::Real, "float");
        assert_ast_eq(DataType::Real, "float4");

        assert_ast_eq(DataType::Double, "double");
        assert_ast_eq(DataType::Double, "float8");

        assert_ast_eq(DataType::Bool, "bool");
        assert_ast_eq(DataType::Bool, "boolean");
    }
}
