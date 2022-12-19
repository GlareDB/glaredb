use crate::errors::Result;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::dialect::{Dialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::parser::{Parser, ParserError};

#[derive(Debug)]
pub struct SqlParser;

impl SqlParser {
    pub fn parse(sql: &str) -> Result<Vec<ast::Statement>> {
        let statements = Parser::parse_sql(&GlareSqlDialect {}, sql)?;
        Ok(statements)
    }
}

#[derive(Debug)]
pub struct GlareSqlDialect {}

impl Dialect for GlareSqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        PostgreSqlDialect {}.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        PostgreSqlDialect {}.is_identifier_part(ch)
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<ast::Statement, ParserError>> {
        PostgreSqlDialect {}.parse_statement(parser)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        PostgreSqlDialect {}.supports_filter_during_aggregation()
    }
}
