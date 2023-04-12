use crate::errors::Result;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;

/// Wrapper around our custom parse for parsing a sql statement.
pub fn parse_sql(sql: &str) -> Result<VecDeque<StatementWithExtensions>> {
    let stmts = CustomParser::parse_sql(sql)?;
    Ok(stmts)
}

/// Contains the value parsed from Options(...).
///
/// `CREATE ... OPTIONS ( abc = 'def' )` will return the value `Literal("def")`
/// for option "abc" where as `OPTIONS ( abc = SECRET def )` will return
/// `Secret(def)` for key "abc".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionValue {
    Literal(String),
    Secret(String),
}

impl fmt::Display for OptionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(s) => write!(f, "'{s}'"),
            Self::Secret(s) => write!(f, "SECRET {s}"),
        }
    }
}

/// DDL extension for GlareDB's external tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalTableStmt {
    /// Name of the table.
    pub name: String,
    /// Optionally don't error if table exists.
    pub if_not_exists: bool,
    /// Data source type.
    pub datasource: String,
    /// Datasource specific options.
    pub options: BTreeMap<String, OptionValue>,
}

impl fmt::Display for CreateExternalTableStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE EXTERNAL TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} FROM {} ", self.name, self.datasource)?;

        let opts = self
            .options
            .iter()
            .map(|(k, v)| format!("{} = {}", k, v))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "OPTIONS ({})", opts)?;
        Ok(())
    }
}

/// DDL for external databases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalDatabaseStmt {
    /// Name of the database as it exists in GlareDB.
    pub name: String,
    /// Optionally don't error if database exists.
    pub if_not_exists: bool,
    /// The data source type the connection is for.
    pub datasource: String,
    /// Datasource specific options.
    pub options: BTreeMap<String, OptionValue>,
}

impl fmt::Display for CreateExternalDatabaseStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE EXTERNAL DATABASE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} FROM {} ", self.name, self.datasource)?;

        let opts = self
            .options
            .iter()
            .map(|(k, v)| format!("{} = {}", k, v))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "OPTIONS ({})", opts)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDatabaseStmt {
    pub name: String,
    pub if_exists: bool,
}

impl fmt::Display for DropDatabaseStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP DATABASE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }

        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabaseRenameStmt {
    pub name: String,
    pub new_name: String,
}

impl fmt::Display for AlterDatabaseRenameStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER DATABASE ")?;
        write!(f, "{}", self.name)?;
        write!(f, "RENAME TO ")?;
        write!(f, "{}", self.new_name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementWithExtensions {
    /// Statement parsed by `sqlparser`.
    Statement(ast::Statement),
    /// Create external table extension.
    CreateExternalTable(CreateExternalTableStmt),
    /// Create external database extension.
    CreateExternalDatabase(CreateExternalDatabaseStmt),
    /// Drop database extension.
    DropDatabase(DropDatabaseStmt),
    AlterDatabaseRename(AlterDatabaseRenameStmt),
}

impl fmt::Display for StatementWithExtensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementWithExtensions::Statement(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::CreateExternalTable(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::CreateExternalDatabase(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::DropDatabase(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::AlterDatabaseRename(stmt) => write!(f, "{}", stmt),
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
            parser: Parser::new(&dialect).with_tokens(tokens),
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
                return parser.expected("end of statement", parser.parser.peek_token().token);
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }

        Ok(stmts)
    }

    fn parse_statement(&mut self) -> Result<StatementWithExtensions, ParserError> {
        match self.parser.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::CREATE => {
                    self.parser.next_token();
                    self.parse_create()
                }
                Keyword::DROP => {
                    self.parser.next_token();
                    self.parse_drop()
                }
                Keyword::ALTER => {
                    self.parser.next_token();
                    self.parse_alter()
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
            if self.parser.parse_keyword(Keyword::TABLE) {
                self.parse_create_external_table()
            } else if self.parser.parse_keyword(Keyword::DATABASE) {
                self.parse_create_external_database()
            } else {
                let next = self.parser.peek_token().token;
                Err(ParserError::ParserError(format!(
                    "Expected 'CREATE EXTERNAL DATABASE' or 'CREATE EXTERNAL TABLE', found 'CREATE EXTERNAL {}'",
                    next
                )))
            }
        } else {
            // Fall back to underlying parser.
            Ok(StatementWithExtensions::Statement(
                self.parser.parse_create()?,
            ))
        }
    }

    /// Parse a SQL ALTER statement
    fn parse_alter(&mut self) -> Result<StatementWithExtensions, ParserError> {
        if self.parser.parse_keyword(Keyword::DATABASE) {
            self.parse_alter_database()
        } else {
            // Fall back to underlying parser.
            Ok(StatementWithExtensions::Statement(
                self.parser.parse_alter()?,
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

    fn parse_create_external_table(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_object_name()?;
        validate_object_name(&name)?;

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

    fn parse_create_external_database(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_object_name()?;
        validate_object_name(&name)?;

        // FROM datasource
        self.parser.expect_keyword(Keyword::FROM)?;
        let datasource = self.parse_datasource()?;

        // OPTIONS (..)
        let options = self.parse_datasource_options()?;

        Ok(StatementWithExtensions::CreateExternalDatabase(
            CreateExternalDatabaseStmt {
                name: name.to_string(),
                if_not_exists,
                datasource,
                options,
            },
        ))
    }

    fn parse_datasource(&mut self) -> Result<String, ParserError> {
        match self.parser.next_token().token {
            Token::Word(w) => Ok(w.value),
            other => self.expected("datasource", other),
        }
    }

    /// Parse options for a datasource.
    fn parse_datasource_options(&mut self) -> Result<BTreeMap<String, OptionValue>, ParserError> {
        let mut options = BTreeMap::new();

        // No options provided.
        if !self.consume_token(&Token::make_keyword("OPTIONS")) {
            return Ok(options);
        }

        self.parser.expect_token(&Token::LParen)?;

        loop {
            let key = self.parser.parse_identifier()?.value;
            self.parser.expect_token(&Token::Eq)?;

            // Check if we have a secret value.
            let is_secret = self.consume_token(&Token::make_keyword("SECRET"));
            let value = if is_secret {
                OptionValue::Secret(self.parser.parse_identifier()?.value)
            } else {
                OptionValue::Literal(self.parser.parse_literal_string()?)
            };

            options.insert(key, value);
            let comma = self.parser.consume_token(&Token::Comma);

            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after option definition",
                    self.parser.peek_token().token,
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

    /// Parse a SQL DROP statement
    fn parse_drop(&mut self) -> Result<StatementWithExtensions, ParserError> {
        if self.parser.parse_keyword(Keyword::DATABASE) {
            // DROP DATABASE ...
            self.parse_drop_database()
        } else {
            // Fall back to underlying parser.
            Ok(StatementWithExtensions::Statement(
                self.parser.parse_drop()?,
            ))
        }
    }

    fn parse_drop_database(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parser.parse_identifier()?;
        Ok(StatementWithExtensions::DropDatabase(DropDatabaseStmt {
            name: name.value,
            if_exists,
        }))
    }

    fn parse_alter_database(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let name = self.parser.parse_identifier()?;
        if !self.parser.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            return self.expected("RENAME TO", self.parser.peek_token().token);
        }
        let new_name = self.parser.parse_identifier()?;
        Ok(StatementWithExtensions::AlterDatabaseRename(
            AlterDatabaseRenameStmt {
                name: name.value,
                new_name: new_name.value,
            },
        ))
    }
}

/// Validate idents as per [postgres identifier
/// syntax](https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS)
pub fn validate_ident(ident: &ast::Ident) -> Result<(), ParserError> {
    const POSTGRES_IDENT_MAX_LENGTH: usize = 64;
    if ident.value.len() >= POSTGRES_IDENT_MAX_LENGTH {
        return Err(ParserError::ParserError(format!(
            "Ident {ident} is greater than 63 bytes in length"
        )));
    }
    Ok(())
}

/// Validate object names a `Vec<ast::Idents>`
pub fn validate_object_name(name: &ast::ObjectName) -> Result<(), ParserError> {
    for ident in name.0.iter() {
        validate_ident(ident)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_table_display() {
        let mut options = BTreeMap::new();
        options.insert(
            "postgres_conn".to_string(),
            OptionValue::Literal("host=localhost user=postgres".to_string()),
        );
        options.insert(
            "schema".to_string(),
            OptionValue::Literal("public".to_string()),
        );
        options.insert(
            "table".to_string(),
            OptionValue::Secret("pg_table".to_string()),
        );
        let stmt = CreateExternalTableStmt {
            name: "test".to_string(),
            if_not_exists: false,
            datasource: "postgres".to_string(),
            options,
        };

        let out = stmt.to_string();
        assert_eq!("CREATE EXTERNAL TABLE test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public', table = SECRET pg_table)", out);
    }

    #[test]
    fn external_table_parse() {
        let sql = "CREATE EXTERNAL TABLE test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema='public', table=secret pg_table)";
        let mut stmts = CustomParser::parse_sql(sql).unwrap();

        let stmt = stmts.pop_front().unwrap();
        let mut options = BTreeMap::new();
        options.insert(
            "postgres_conn".to_string(),
            OptionValue::Literal("host=localhost user=postgres".to_string()),
        );
        options.insert(
            "schema".to_string(),
            OptionValue::Literal("public".to_string()),
        );
        options.insert(
            "table".to_string(),
            OptionValue::Secret("pg_table".to_string()),
        );

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
    fn create_external_table_roundtrips() {
        let test_cases = [
            "CREATE EXTERNAL TABLE test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public')",
            "CREATE EXTERNAL TABLE IF NOT EXISTS test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public')",
        ];

        for test_case in test_cases {
            let stmt = CustomParser::parse_sql(test_case)
                .unwrap()
                .pop_front()
                .unwrap();
            assert_eq!(test_case, stmt.to_string().as_str());
        }
    }

    #[test]
    fn create_external_database_roundtrips() {
        let test_cases = [
            "CREATE EXTERNAL DATABASE qa FROM postgres OPTIONS (host = 'localhost', user = 'user')",
            "CREATE EXTERNAL DATABASE IF NOT EXISTS qa FROM postgres OPTIONS (host = 'localhost', user = 'user')",
        ];

        for test_case in test_cases {
            let stmt = CustomParser::parse_sql(test_case)
                .unwrap()
                .pop_front()
                .unwrap();
            assert_eq!(test_case, stmt.to_string().as_str());
        }
    }

    #[test]
    fn drop_database_roundtrips() {
        let test_cases = ["DROP DATABASE my_db", "DROP DATABASE IF EXISTS my_db"];

        for test_case in test_cases {
            let stmt = CustomParser::parse_sql(test_case)
                .unwrap()
                .pop_front()
                .unwrap();
            assert_eq!(test_case, stmt.to_string().as_str());
        }
    }
}
