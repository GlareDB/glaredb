use crate::errors::Result;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;

/// Wrapper around our custom parse for parsing a sql statement.
pub fn parse_sql(sql: &str) -> Result<VecDeque<StatementWithExtensions>> {
    let stmts = CustomParser::parse_sql(sql)?;
    Ok(stmts)
}

/// DDL extension for GlareDB's external tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalTableStmt {
    /// Name of the table.
    pub name: String,
    /// Optionally don't error if table exists.
    pub if_not_exists: bool,
    /// The data source for the table.
    pub datasource: String,
    /// Datasource specific options.
    pub options: HashMap<String, String>,
}

impl fmt::Display for CreateExternalTableStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE EXTERNAL TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS")?;
        }
        write!(f, "{} FROM {} ", self.name, self.datasource)?;

        let opts = self
            .options
            .iter()
            .map(|(k, v)| format!("{} = '{}'", k, v))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "OPTIONS ({})", opts)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateConnectionStmt {
    /// Name of the connection.
    pub name: String,
    /// Optionatlly don't error if the connection already exists.
    pub if_not_exists: bool,
    /// The data source the connection is for.
    pub datasource: String,
    /// Options for the connection.
    pub options: HashMap<String, String>,
}

impl fmt::Display for CreateConnectionStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE CONNECTION ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS")?;
        }
        write!(f, "{} FOR {} ", self.name, self.datasource)?;

        let opts = self
            .options
            .iter()
            .map(|(k, v)| format!("{} = '{}'", k, v))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "OPTIONS ({})", opts)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementWithExtensions {
    /// Statement parsed by `sqlparser`.
    Statement(ast::Statement),
    /// Create external table extension.
    CreateExternalTable(CreateExternalTableStmt),
    /// Create connection extension.
    CreateConnection(CreateConnectionStmt),
}

impl fmt::Display for StatementWithExtensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementWithExtensions::Statement(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::CreateExternalTable(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::CreateConnection(stmt) => write!(f, "{}", stmt),
        }
    }
}

/// Parser with our extensions.
pub struct CustomParser<'a> {
    parser: Parser<'a>,
}

impl<'a> CustomParser<'a> {
    pub fn parse_sql(sql: &str) -> Result<VecDeque<StatementWithExtensions>, ParserError> {
        let dialect = PostgreSqlDialect {};
        let tokens = Tokenizer::new(&dialect, sql).tokenize()?;
        let mut parser = CustomParser {
            parser: Parser::new(tokens, &dialect),
        };

        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }

        Ok(stmts)
    }

    fn parse_statement(&mut self) -> Result<StatementWithExtensions, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => match w.keyword {
                Keyword::CREATE => {
                    self.parser.next_token();
                    self.parse_create()
                }
                _ => Ok(StatementWithExtensions::Statement(
                    self.parser.parse_statement()?,
                )),
            },
            _ => Ok(StatementWithExtensions::Statement(
                self.parser.parse_statement()?,
            )),
        }
    }

    /// Parse a SQL CREATE statement
    fn parse_create(&mut self) -> Result<StatementWithExtensions, ParserError> {
        if self.parser.parse_keyword(Keyword::EXTERNAL) {
            // CREATE EXTERNAL TABLE ...
            self.parse_create_external_table()
        } else if self.parser.parse_keyword(Keyword::CONNECTION) {
            // CREATE CONNECTION ...
            self.parse_create_connection()
        } else {
            // Fall back to underlying parser.
            Ok(StatementWithExtensions::Statement(
                self.parser.parse_create()?,
            ))
        }
    }

    /// Report unexpected token.
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        Err(ParserError::ParserError(format!(
            "Expected {}, found: {}",
            expected, found
        )))
    }

    fn parse_create_connection(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_object_name()?;

        // FOR datasource
        self.parser.expect_keyword(Keyword::FOR)?;

        let datasource = self.parse_datasource()?;

        // OPTIONS (..)
        let options = self.parse_datasource_options()?;

        Ok(StatementWithExtensions::CreateConnection(
            CreateConnectionStmt {
                name: name.to_string(),
                if_not_exists,
                datasource,
                options,
            },
        ))
    }

    fn parse_create_external_table(&mut self) -> Result<StatementWithExtensions, ParserError> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_object_name()?;

        // FROM datasource
        self.parser.expect_keyword(Keyword::FROM)?;
        let datasource = self.parse_datasource()?;

        // OPTIONS (..)
        let options = self.parse_datasource_options()?;

        Ok(StatementWithExtensions::CreateExternalTable(
            CreateExternalTableStmt {
                name: name.to_string(),
                if_not_exists,
                datasource,
                options,
            },
        ))
    }

    fn parse_datasource(&mut self) -> Result<String, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => Ok(w.value),
            other => self.expected("datasource", other),
        }
    }

    /// Parse options for a datasource.
    fn parse_datasource_options(&mut self) -> Result<HashMap<String, String>, ParserError> {
        let mut options = HashMap::new();

        // No options provided.
        if !self.consume_token(&Token::make_keyword("OPTIONS")) {
            return Ok(options);
        }

        self.parser.expect_token(&Token::LParen)?;

        loop {
            let key = self.parser.parse_identifier()?.to_string();
            self.parser.expect_token(&Token::Eq)?;
            let value = self.parser.parse_literal_string()?;

            options.insert(key.to_string(), value.to_string());
            let comma = self.parser.consume_token(&Token::Comma);

            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after option definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok(options)
    }

    /// Consume a token return, returning whether or not it was consumed.
    fn consume_token(&mut self, expected: &Token) -> bool {
        let token = self.parser.peek_token().to_string().to_uppercase();
        let token = Token::make_keyword(&token);
        if token == *expected {
            let _ = self.parser.next_token();
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: Hash maps make the order of iteration for the options
    // non-deterministic. Can cause test failures.

    #[test]
    fn external_table_display() {
        let mut options = HashMap::new();
        options.insert(
            "postgres_conn".to_string(),
            "host=localhost user=postgres".to_string(),
        );
        let stmt = CreateExternalTableStmt {
            name: "test".to_string(),
            if_not_exists: false,
            datasource: "postgres".to_string(),
            options,
        };

        let out = stmt.to_string();
        assert_eq!("CREATE EXTERNAL TABLE test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres')", out);
    }

    #[test]
    fn external_table_parse() {
        let sql = "CREATE EXTERNAL TABLE test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema='public')";
        let mut stmts = CustomParser::parse_sql(sql).unwrap();

        let stmt = stmts.pop_front().unwrap();
        let mut options = HashMap::new();
        options.insert(
            "postgres_conn".to_string(),
            "host=localhost user=postgres".to_string(),
        );
        options.insert("schema".to_string(), "public".to_string());

        assert_eq!(
            StatementWithExtensions::CreateExternalTable(CreateExternalTableStmt {
                name: "test".to_string(),
                if_not_exists: false,
                datasource: "postgres".to_string(),
                options,
            }),
            stmt
        );
    }

    #[test]
    fn connection_roundtrip() {
        // Display the create string...

        let mut options = HashMap::new();
        for (k, v) in [("host", "localhost"), ("user", "postgres")] {
            options.insert(k.to_string(), v.to_string());
        }
        let conn_stmt = CreateConnectionStmt {
            name: "my_conn".to_string(),
            if_not_exists: false,
            datasource: "postgres".to_string(),
            options,
        };

        let out = conn_stmt.to_string();
        assert_eq!("CREATE CONNECTION my_conn FOR postgres OPTIONS (host = 'localhost', user = 'postgres')", out);

        // Parse the string we go back.
        let mut stmts = CustomParser::parse_sql(&out).unwrap();
        assert_eq!(1, stmts.len());
        let parsed = stmts.pop_front().unwrap();
        assert_eq!(StatementWithExtensions::CreateConnection(conn_stmt), parsed);
    }
}
