use crate::errors::Result;
use datafusion::sql::sqlparser::ast::{self, Ident, ObjectName};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
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
    QuotedLiteral(String),
    UnquotedKeyword(String),
    Boolean(bool),
    Number(String),
    Secret(String),
}

impl fmt::Display for OptionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QuotedLiteral(s) => write!(f, "'{s}'"),
            Self::UnquotedKeyword(s) => write!(f, "{s}"),
            Self::Boolean(b) => {
                if *b {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Self::Number(n) => write!(f, "{n}"),
            Self::Secret(s) => write!(f, "SECRET {s}"),
        }
    }
}

/// DDL extension for GlareDB's external tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalTableStmt {
    /// Name of the table.
    pub name: ObjectName,
    /// Optionally don't error if table exists.
    pub if_not_exists: bool,
    /// Data source type.
    pub datasource: Ident,
    /// Optional tunnel to use for connection.
    pub tunnel: Option<Ident>,
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

        if let Some(tunnel) = &self.tunnel {
            write!(f, "TUNNEL {tunnel} ")?;
        }

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
    pub name: Ident,
    /// Optionally don't error if database exists.
    pub if_not_exists: bool,
    /// The data source type the connection is for.
    pub datasource: Ident,
    /// Optional tunnel to use for connection.
    pub tunnel: Option<Ident>,
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

        if let Some(tunnel) = &self.tunnel {
            write!(f, "TUNNEL {tunnel} ")?;
        }

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
    pub names: Vec<Ident>,
    pub if_exists: bool,
}

impl fmt::Display for DropDatabaseStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP DATABASE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        let mut sep = "";
        for name in self.names.iter() {
            write!(f, "{sep}{name}")?;
            sep = ", ";
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabaseRenameStmt {
    pub name: Ident,
    pub new_name: Ident,
}

impl fmt::Display for AlterDatabaseRenameStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER DATABASE ")?;
        write!(f, "{}", self.name)?;
        write!(f, " RENAME TO ")?;
        write!(f, "{}", self.new_name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTunnelStmt {
    /// Name of the tunnel as it exists in GlareDB.
    pub name: Ident,
    /// Optionally don't error if tunnel exists.
    pub if_not_exists: bool,
    /// The tunnel type the connection is for.
    pub tunnel: Ident,
    /// Tunnel specific options.
    pub options: BTreeMap<String, OptionValue>,
}

impl fmt::Display for CreateTunnelStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TUNNEL ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} FROM {} ", self.name, self.tunnel)?;

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
pub struct DropTunnelStmt {
    pub names: Vec<Ident>,
    pub if_exists: bool,
}

impl fmt::Display for DropTunnelStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP TUNNEL ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        let mut sep = "";
        for name in self.names.iter() {
            write!(f, "{sep}{name}")?;
            sep = ", ";
        }
        Ok(())
    }
}

/// A source for a COPY TO statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyToSource {
    Table(ObjectName),
    Query(ast::Query),
}

impl fmt::Display for CopyToSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyToSource::Table(table) => write!(f, "{table}"),
            CopyToSource::Query(query) => write!(f, "({query})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyToStmt {
    pub source: CopyToSource,
    pub dest: String,
    pub options: BTreeMap<String, OptionValue>,
}

impl fmt::Display for CopyToStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let opts = self
            .options
            .iter()
            .map(|(k, v)| format!("{} {}", k, v))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "COPY {0} TO '{1}'", self.source, self.dest)?;
        if !opts.is_empty() {
            write!(f, " ({opts})")?;
        }
        write!(f, ";")?;

        Ok(())
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
    // Alter database extension (rename).
    AlterDatabaseRename(AlterDatabaseRenameStmt),
    /// Create tunnel extension.
    CreateTunnel(CreateTunnelStmt),
    /// Drop tunnel extension.
    DropTunnel(DropTunnelStmt),
    /// Copy to extension.
    CopyTo(CopyToStmt),
}

impl fmt::Display for StatementWithExtensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementWithExtensions::Statement(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::CreateExternalTable(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::CreateExternalDatabase(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::DropDatabase(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::AlterDatabaseRename(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::CreateTunnel(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::DropTunnel(stmt) => write!(f, "{}", stmt),
            StatementWithExtensions::CopyTo(stmt) => write!(f, "{}", stmt),
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
                Keyword::COPY => {
                    self.parser.next_token();
                    self.parse_copy()
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

    fn parse_copy(&mut self) -> Result<StatementWithExtensions, ParserError> {
        // Parse table or query source.
        // COPY table ...
        // or
        // COPY (select ...) ...
        let source = if self.parser.consume_token(&Token::LParen) {
            let query = self.parser.parse_query()?;
            self.parser.expect_token(&Token::RParen)?;
            CopyToSource::Query(query)
        } else {
            let table_name = self.parser.parse_object_name()?;
            CopyToSource::Table(table_name)
        };

        // TO 'source'
        self.parser.expect_keyword(Keyword::TO)?;
        let dest = self.parser.parse_literal_string()?;

        // (<options>)

        // TODO: Do we want to unify options here and on data sources?
        let mut options = BTreeMap::new();

        if self.parser.consume_token(&Token::LParen) {
            loop {
                let key = self.parser.parse_identifier()?.value;

                // Check if we have a secret value.
                let is_secret = self.consume_token(&Token::make_keyword("SECRET"));
                let value = if is_secret {
                    OptionValue::Secret(self.parser.parse_identifier()?.value)
                } else {
                    self.parse_option_value()?
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
        }

        Ok(StatementWithExtensions::CopyTo(CopyToStmt {
            source,
            dest,
            options,
        }))
    }

    /// Parse a SQL CREATE statement
    fn parse_create(&mut self) -> Result<StatementWithExtensions, ParserError> {
        if self.parser.parse_keyword(Keyword::EXTERNAL) {
            // CREATE EXTERNAL ...
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
        } else if self.consume_token(&Token::make_keyword("TUNNEL")) {
            // CREATE TUNNEL ...
            self.parse_create_tunnel()
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
        let datasource = self.parse_object_type("datasource")?;

        // [TUNNEL ...]
        let tunnel = self.parse_connection_tunnel()?;

        // OPTIONS (..)
        let options = self.parse_options()?;

        Ok(StatementWithExtensions::CreateExternalTable(
            CreateExternalTableStmt {
                name,
                if_not_exists,
                datasource,
                tunnel,
                options,
            },
        ))
    }

    fn parse_create_external_database(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = self.parser.parse_identifier()?;
        validate_ident(&name)?;

        // FROM datasource
        self.parser.expect_keyword(Keyword::FROM)?;
        let datasource = self.parse_object_type("datasource")?;

        // [TUNNEL ...]
        let tunnel = self.parse_connection_tunnel()?;

        // OPTIONS (..)
        let options = self.parse_options()?;

        Ok(StatementWithExtensions::CreateExternalDatabase(
            CreateExternalDatabaseStmt {
                name,
                if_not_exists,
                datasource,
                tunnel,
                options,
            },
        ))
    }

    fn parse_create_tunnel(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = self.parser.parse_identifier()?;
        validate_ident(&name)?;

        // FROM tunnel
        self.parser.expect_keyword(Keyword::FROM)?;
        let tunnel = self.parse_object_type("tunnel")?;

        // OPTIONS (..)
        let options = self.parse_options()?;

        Ok(StatementWithExtensions::CreateTunnel(CreateTunnelStmt {
            name,
            if_not_exists,
            tunnel,
            options,
        }))
    }

    fn parse_object_type(&mut self, object_type: &str) -> Result<Ident, ParserError> {
        match self.parser.next_token().token {
            Token::Word(w) => Ok(w.to_ident()),
            other => self.expected(object_type, other),
        }
    }

    fn parse_connection_tunnel(&mut self) -> Result<Option<Ident>, ParserError> {
        let tunnel = if self.consume_token(&Token::make_keyword("TUNNEL")) {
            let tunnel = self.parser.parse_identifier()?;
            validate_ident(&tunnel)?;
            Some(tunnel)
        } else {
            None
        };
        Ok(tunnel)
    }

    /// Parse options for a datasource.
    fn parse_options(&mut self) -> Result<BTreeMap<String, OptionValue>, ParserError> {
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
                OptionValue::QuotedLiteral(self.parser.parse_literal_string()?)
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

    fn parse_option_value(&mut self) -> Result<OptionValue, ParserError> {
        let tok = self.parser.next_token();
        match tok.token {
            Token::Word(Word {
                keyword: Keyword::TRUE,
                ..
            }) => Ok(OptionValue::Boolean(true)),
            Token::Word(Word {
                keyword: Keyword::FALSE,
                ..
            }) => Ok(OptionValue::Boolean(false)),
            Token::Word(Word { value, .. }) => Ok(OptionValue::UnquotedKeyword(value)),
            Token::SingleQuotedString(s) => Ok(OptionValue::QuotedLiteral(s)),
            Token::Number(n, _) => Ok(OptionValue::Number(n)),
            _ => Err(ParserError::ParserError(format!(
                "Expected string, boolean, or number, found: {}",
                tok,
            ))),
        }
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
        } else if self.consume_token(&Token::make_keyword("TUNNEL")) {
            // DROP TUNNEL ...
            self.parse_drop_tunnel()
        } else {
            // Fall back to underlying parser.
            Ok(StatementWithExtensions::Statement(
                self.parser.parse_drop()?,
            ))
        }
    }

    fn parse_drop_database(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

        let names = self
            .parser
            .parse_comma_separated(Parser::parse_identifier)?;

        for name in names.iter() {
            validate_ident(name)?;
        }

        Ok(StatementWithExtensions::DropDatabase(DropDatabaseStmt {
            names,
            if_exists,
        }))
    }

    fn parse_drop_tunnel(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

        let names = self
            .parser
            .parse_comma_separated(Parser::parse_identifier)?;

        for name in names.iter() {
            validate_ident(name)?;
        }

        Ok(StatementWithExtensions::DropTunnel(DropTunnelStmt {
            names,
            if_exists,
        }))
    }

    fn parse_alter_database(&mut self) -> Result<StatementWithExtensions, ParserError> {
        let name = self.parser.parse_identifier()?;
        validate_ident(&name)?;

        if !self.parser.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            return self.expected("RENAME TO", self.parser.peek_token().token);
        }

        let new_name = self.parser.parse_identifier()?;
        validate_ident(&new_name)?;

        Ok(StatementWithExtensions::AlterDatabaseRename(
            AlterDatabaseRenameStmt { name, new_name },
        ))
    }
}

pub fn validate_ident(ident: &ast::Ident) -> Result<(), ParserError> {
    metastore::validation::validate_object_name(&ident.value)
        .map_err(|e| ParserError::ParserError(e.to_string()))
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
            OptionValue::QuotedLiteral("host=localhost user=postgres".to_string()),
        );
        options.insert(
            "schema".to_string(),
            OptionValue::QuotedLiteral("public".to_string()),
        );
        options.insert(
            "table".to_string(),
            OptionValue::Secret("pg_table".to_string()),
        );
        let mut stmt = CreateExternalTableStmt {
            name: ObjectName(vec![Ident::new("test")]),
            if_not_exists: false,
            datasource: Ident::new("postgres"),
            tunnel: None,
            options,
        };

        let out = stmt.to_string();
        assert_eq!("CREATE EXTERNAL TABLE test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public', table = SECRET pg_table)", out);

        stmt.tunnel = Some(Ident::new("ssh_tunnel"));
        let out = stmt.to_string();
        assert_eq!("CREATE EXTERNAL TABLE test FROM postgres TUNNEL ssh_tunnel OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public', table = SECRET pg_table)", out);
    }

    #[test]
    fn external_table_parse() {
        let sql = "CREATE EXTERNAL TABLE test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema='public', table=secret pg_table)";
        let mut stmts = CustomParser::parse_sql(sql).unwrap();

        let stmt = stmts.pop_front().unwrap();
        let mut options = BTreeMap::new();
        options.insert(
            "postgres_conn".to_string(),
            OptionValue::QuotedLiteral("host=localhost user=postgres".to_string()),
        );
        options.insert(
            "schema".to_string(),
            OptionValue::QuotedLiteral("public".to_string()),
        );
        options.insert(
            "table".to_string(),
            OptionValue::Secret("pg_table".to_string()),
        );

        let mut parsed_stmt = CreateExternalTableStmt {
            name: ObjectName(vec![Ident::new("test")]),
            if_not_exists: false,
            datasource: Ident::new("postgres"),
            tunnel: None,
            options,
        };

        assert_eq!(
            StatementWithExtensions::CreateExternalTable(parsed_stmt.clone()),
            stmt
        );

        let sql = "CREATE EXTERNAL TABLE test FROM postgres TUNNEL ssh_tunnel OPTIONS (postgres_conn = 'host=localhost user=postgres', schema='public', table=secret pg_table)";
        let mut stmts = CustomParser::parse_sql(sql).unwrap();

        let stmt = stmts.pop_front().unwrap();
        parsed_stmt.tunnel = Some(Ident::new("ssh_tunnel"));
        assert_eq!(
            StatementWithExtensions::CreateExternalTable(parsed_stmt),
            stmt
        );
    }

    #[test]
    fn create_external_table_roundtrips() {
        let test_cases = [
            "CREATE EXTERNAL TABLE test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public')",
            "CREATE EXTERNAL TABLE IF NOT EXISTS test FROM postgres OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public')",
            "CREATE EXTERNAL TABLE test FROM postgres TUNNEL my_ssh OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public')",
            "CREATE EXTERNAL TABLE IF NOT EXISTS test FROM postgres TUNNEL my_ssh OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public')",
            "CREATE EXTERNAL TABLE schema.test FROM postgres TUNNEL my_ssh OPTIONS (postgres_conn = 'host=localhost user=postgres', schema = 'public')",
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
            "CREATE EXTERNAL DATABASE qa FROM postgres TUNNEL my_ssh OPTIONS (host = 'localhost', user = 'user')",
            "CREATE EXTERNAL DATABASE IF NOT EXISTS qa FROM postgres TUNNEL my_ssh OPTIONS (host = 'localhost', user = 'user')",
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
    fn create_tunnel_roundtrips() {
        let test_cases = [
            "CREATE TUNNEL qa FROM postgres OPTIONS (host = 'localhost', user = 'user')",
            "CREATE TUNNEL IF NOT EXISTS qa FROM postgres OPTIONS (host = 'localhost', user = 'user')",
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

    #[test]
    fn drop_tunnel_roundtrips() {
        let test_cases = ["DROP TUNNEL my_tunnel", "DROP TUNNEL IF EXISTS my_tunnel"];

        for test_case in test_cases {
            let stmt = CustomParser::parse_sql(test_case)
                .unwrap()
                .pop_front()
                .unwrap();
            assert_eq!(test_case, stmt.to_string().as_str());
        }
    }

    #[test]
    fn alter_database_roundtrips() {
        let test_cases = ["ALTER DATABASE my_db RENAME TO your_db"];

        for test_case in test_cases {
            let stmt = CustomParser::parse_sql(test_case)
                .unwrap()
                .pop_front()
                .unwrap();
            assert_eq!(test_case, stmt.to_string().as_str());
        }
    }

    #[test]
    fn copy_to_roundtrip() {
        let test_cases = [
            "COPY table TO 's3://bucket';",
            "COPY table TO 's3://bucket' (option1 'true', option2 'hello');",
            "COPY (SELECT 1) TO 's3://bucket';",
            "COPY (SELECT 1) TO 's3://bucket' (option1 'true', option2 'hello');",
            "COPY table TO 's3://bucket' (bool_opt TRUE, num_opt 1);",
            "COPY table TO 's3://bucket' (FORMAT JSON);",
        ];

        for test_case in test_cases {
            let stmt = CustomParser::parse_sql(test_case)
                .unwrap()
                .pop_front()
                .unwrap();
            assert_eq!(test_case, stmt.to_string().as_str());
        }
    }
}
