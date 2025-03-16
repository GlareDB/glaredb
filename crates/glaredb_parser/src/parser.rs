use glaredb_error::{not_implemented, RayexecError, Result};
use tracing::trace;

use crate::ast::{
    AstParseable,
    Attach,
    CopyTo,
    CreateSchema,
    CreateTable,
    CreateView,
    Describe,
    Detach,
    DropStatement,
    ExplainNode,
    Ident,
    Insert,
    QueryNode,
    ResetVariable,
    SetVariable,
    Show,
};
use crate::keywords::{Keyword, RESERVED_FOR_COLUMN_ALIAS};
use crate::meta::Raw;
use crate::statement::{RawStatement, Statement};
use crate::tokens::{Token, TokenWithLocation, Tokenizer};

/// Parse a sql query into statements.
pub fn parse(sql: &str) -> Result<Vec<Statement<Raw>>> {
    trace!(%sql, "parsing sql statement");
    let mut toks = Vec::new();
    Tokenizer::new(sql).tokenize(&mut toks)?;
    Parser::with_tokens(toks, sql).parse_statements()
}

#[derive(Debug)]
pub struct Parser<'a> {
    toks: Vec<TokenWithLocation>,
    sql: &'a str,
    /// Index of token we should process next.
    pub(crate) idx: usize,
}

impl<'a> Parser<'a> {
    /// Create a parser with arbitrary tokens.
    pub fn with_tokens(toks: Vec<TokenWithLocation>, sql: &'a str) -> Self {
        Parser { toks, sql, idx: 0 }
    }

    /// Parse any number of statements, including zero statements.
    ///
    /// Statements are expected to be delineated with a semicolon.
    pub fn parse_statements(&mut self) -> Result<Vec<RawStatement>> {
        let mut stmts = Vec::new();
        let mut expect_delimiter = false;

        loop {
            while self.consume_token(&Token::SemiColon) {
                expect_delimiter = false;
            }

            if self.peek().is_none() {
                // We're done.
                break;
            }

            if expect_delimiter {
                let unparsed = match self.toks.get(self.idx) {
                    Some(tok) => self.sql.get(tok.start_idx..).unwrap_or_default(),
                    None => "",
                };

                return Err(RayexecError::new(format!(
                    "Expected semicolon between statements. Unparsed SQL: '{}'",
                    unparsed,
                )));
            }

            let stmt = self.parse_statement()?;
            stmts.push(stmt);

            expect_delimiter = true;
        }

        Ok(stmts)
    }

    /// Parse a single statement.
    pub fn parse_statement(&mut self) -> Result<RawStatement> {
        let tok = match self.peek() {
            Some(tok) => tok,
            None => return Err(RayexecError::new("Empty SQL statement")),
        };

        match &tok.token {
            Token::Word(word) => {
                let keyword = match word.keyword {
                    Some(k) => k,
                    None => {
                        return Err(RayexecError::new(format!(
                            "Expected a keyword, got {}",
                            word.value,
                        )))
                    }
                };

                match keyword {
                    Keyword::ATTACH => Ok(RawStatement::Attach(Attach::parse(self)?)),
                    Keyword::DETACH => Ok(RawStatement::Detach(Detach::parse(self)?)),
                    Keyword::COPY => Ok(RawStatement::CopyTo(CopyTo::parse(self)?)),
                    Keyword::CREATE => self.parse_create(),
                    Keyword::DROP => Ok(RawStatement::Drop(DropStatement::parse(self)?)),
                    Keyword::SET => Ok(RawStatement::SetVariable(SetVariable::parse(self)?)),
                    Keyword::RESET => Ok(RawStatement::ResetVariable(ResetVariable::parse(self)?)),
                    Keyword::SHOW => Ok(RawStatement::Show(Show::parse(self)?)),
                    Keyword::DESCRIBE => Ok(RawStatement::Describe(Describe::parse(self)?)),
                    Keyword::SELECT | Keyword::WITH | Keyword::VALUES => {
                        Ok(RawStatement::Query(QueryNode::parse(self)?))
                    }
                    Keyword::INSERT => Ok(RawStatement::Insert(Insert::parse(self)?)),
                    Keyword::EXPLAIN => Ok(RawStatement::Explain(ExplainNode::parse(self)?)),
                    other => Err(RayexecError::new(format!("Unexpected keyword: {other:?}",))),
                }
            }
            other => Err(RayexecError::new(format!(
                "Expected a SQL statement, got {other:?}"
            ))),
        }
    }

    /// Parse `CREATE ...`
    pub fn parse_create(&mut self) -> Result<RawStatement> {
        // Store the start index, we'll reset this when we call the actual thing
        // to parse.
        let start = self.idx;

        self.expect_keyword(Keyword::CREATE)?;

        // Skip these keywords to get the actual object being created. We reset
        // the parser index, so the actual object being parsed can get these
        // again.
        let _or_replace = self.parse_keyword_sequence(&[Keyword::OR, Keyword::REPLACE]);
        let _temp = self
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();

        if self.parse_keyword(Keyword::TABLE) {
            self.idx = start;
            Ok(RawStatement::CreateTable(CreateTable::parse(self)?))
        } else if self.parse_keyword(Keyword::SCHEMA) {
            self.idx = start;
            Ok(RawStatement::CreateSchema(CreateSchema::parse(self)?))
        } else if self.parse_keyword(Keyword::VIEW) {
            self.idx = start;
            Ok(RawStatement::CreateView(CreateView::parse(self)?))
        } else {
            not_implemented!("CREATE: {}", self.sql);
        }
    }

    /// Parse an optional alias.
    pub(crate) fn parse_alias(&mut self, reserved: &[Keyword]) -> Result<Option<Ident>> {
        let has_as = self.parse_keyword(Keyword::AS);
        let tok = match self.peek() {
            Some(tok) => &tok.token,
            None => return Ok(None),
        };

        let ident: Option<Ident> = match tok {
            // Allow any alias if `AS` was explicitly provided.
            Token::Word(w) if has_as => Some(w.clone().into()),

            // If `AS` wasn't provided, allow the next word to be used as the
            // alias if it's not a reserved word. Otherwise assume it's not an
            // alias.
            Token::Word(w) => match &w.keyword {
                Some(kw) if reserved.iter().any(|reserved| reserved == kw) => None,
                _ => Some(w.clone().into()),
            },

            // Allow any singly quoted string.
            Token::SingleQuotedString(s) => Some(Ident {
                value: s.clone(),
                quoted: false,
            }),

            _ => {
                if has_as {
                    return Err(RayexecError::new("Expected an identifier after AS"));
                }
                None
            }
        };

        // We've "consumed" the token if we've determined it's an alias.
        if ident.is_some() {
            self.next();
        }

        Ok(ident)
    }

    /// Parse a comma-separated list of one or more items.
    pub(crate) fn parse_comma_separated<T>(
        &mut self,
        mut f: impl FnMut(&mut Parser) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut values = Vec::new();
        loop {
            values.push(f(self)?);
            if !self.consume_token(&Token::Comma) {
                break;
            }

            let tok = match self.peek() {
                Some(tok) => &tok.token,
                None => break,
            };

            match tok {
                Token::RightParen | Token::SemiColon | Token::RightBrace | Token::RightBracket => {
                    break
                }
                Token::Word(w) => {
                    if let Some(kw) = &w.keyword {
                        if RESERVED_FOR_COLUMN_ALIAS
                            .iter()
                            .any(|reserved| reserved == kw)
                        {
                            break;
                        }
                    }
                }
                _ => (),
            }
        }

        Ok(values)
    }

    /// Parse a comma separated list of zero or more items surrounded by
    /// parentheses.
    pub(crate) fn parse_parenthesized_comma_separated<T>(
        &mut self,
        f: impl FnMut(&mut Parser) -> Result<T>,
    ) -> Result<Vec<T>> {
        self.expect_token(&Token::LeftParen)?;
        if self.consume_token(&Token::RightParen) {
            return Ok(Vec::new());
        }
        let vals = self.parse_comma_separated(f)?;
        self.expect_token(&Token::RightParen)?;
        Ok(vals)
    }

    /// Try to parse using the given function, reverting back to the previous
    /// state if not not successful.
    pub(crate) fn maybe_parse<T>(
        &mut self,
        mut f: impl FnMut(&mut Parser) -> Result<T>,
    ) -> Option<T> {
        let idx = self.idx;
        match f(self) {
            Ok(v) => Some(v),
            Err(_) => {
                self.idx = idx;
                None
            }
        }
    }

    /// Parse a single keyword.
    pub(crate) fn parse_keyword(&mut self, keyword: Keyword) -> bool {
        let idx = self.idx;
        if let Some(tok) = self.next() {
            if tok.is_keyword(keyword) {
                return true;
            }
        }

        // Keyword doesn't match. Reset index and return.
        self.idx = idx;
        false
    }

    /// Parse an exact sequence of keywords.
    ///
    /// If the sequence doesn't match, idx is not changed, and false is
    /// returned.
    pub(crate) fn parse_keyword_sequence(&mut self, keywords: &[Keyword]) -> bool {
        let idx = self.idx;
        for keyword in keywords {
            if let Some(tok) = self.next() {
                if tok.is_keyword(*keyword) {
                    continue;
                }
            }

            // Keyword doesn't match. Reset index and return.
            self.idx = idx;
            return false;
        }
        true
    }

    /// Parse any of the provided keywords, returning which keyword was parsed.
    pub(crate) fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> Option<Keyword> {
        let idx = self.idx;
        let tok = self.next()?;

        for &kw in keywords {
            match &tok.token {
                Token::Word(w) if w.keyword == Some(kw) => return Some(kw),
                _ => (),
            }
        }

        // No matches, reset index.
        self.idx = idx;
        None
    }

    /// Consume the current token if it matches expected, otherwise return an
    /// error.
    pub(crate) fn expect_token(&mut self, expected: &Token) -> Result<()> {
        if !self.consume_token(expected) {
            return Err(RayexecError::new(format!(
                "Expected {expected:?}, got {:?}",
                self.peek()
            )));
        }
        Ok(())
    }

    pub(crate) fn expect_one_of_tokens(&mut self, expected: &[&Token]) -> Result<()> {
        for tok in expected {
            if self.consume_token(tok) {
                return Ok(());
            }
        }
        Err(RayexecError::new(format!(
            "Expected one of {expected:?}, got {:?}",
            self.peek()
        )))
    }

    /// Consume the current keyword if it matches expected, otherwise return an
    /// error.
    pub(crate) fn expect_keyword(&mut self, expected: Keyword) -> Result<()> {
        if !self.parse_keyword(expected) {
            return Err(RayexecError::new(format!(
                "Expected {expected:?}, got {:?}",
                self.peek()
            )));
        }
        Ok(())
    }

    /// Consume the next token if it matches expected.
    ///
    /// Returns false with the state unchanged if the next token does not match
    /// expected.
    pub(crate) fn consume_token(&mut self, expected: &Token) -> bool {
        let tok = match self.peek() {
            Some(tok) => &tok.token,
            None => return false,
        };
        if tok == expected {
            let _ = self.next();
            return true;
        }
        false
    }

    /// Get the next keyword, erroring if the next token is not a keyword, or
    /// we've reach the end of a statement.
    ///
    /// This will consume the keyword.
    pub(crate) fn next_keyword(&mut self) -> Result<Keyword> {
        let tok = match self.peek() {
            Some(tok) => tok,
            None => return Err(RayexecError::new("Expected keyword, got end of statement")),
        };

        match &tok.token {
            Token::Word(word) => {
                let keyword = match word.keyword {
                    Some(k) => k,
                    None => {
                        return Err(RayexecError::new(format!(
                            "Expected a keyword, got {}",
                            word.value,
                        )))
                    }
                };

                let _ = self.next(); // Consume

                Ok(keyword)
            }
            other => Err(RayexecError::new(format!(
                "Expected a keyword: got {other:?}"
            ))),
        }
    }

    /// Get the next token.
    ///
    /// Ignores whitespace and comments.
    pub(crate) fn next(&mut self) -> Option<&TokenWithLocation> {
        loop {
            if self.idx >= self.toks.len() {
                return None;
            }

            let tok = &self.toks[self.idx];
            self.idx += 1;

            if matches!(&tok.token, Token::Whitespace | Token::Comment(_)) {
                continue;
            }

            return Some(tok);
        }
    }

    /// Get the next token without altering the current index.
    ///
    /// Ignores whitespace.
    pub(crate) fn peek(&self) -> Option<&TokenWithLocation> {
        self.peek_nth(0)
    }

    /// Get the next keyword without altering the current index.
    ///
    /// Returns None if the next token isn't a keyword or we're at the end of
    /// the token list.
    pub(crate) fn peek_keyword(&self) -> Option<Keyword> {
        self.peek().and_then(|tok| tok.keyword())
    }

    /// Get the nth next token without altering the current index.
    ///
    /// Ignores whitespace and comments.
    pub(crate) fn peek_nth(&self, mut n: usize) -> Option<&TokenWithLocation> {
        let mut idx = self.idx;
        loop {
            if idx >= self.toks.len() {
                return None;
            }

            let tok = &self.toks[idx];
            idx += 1;

            if matches!(&tok.token, Token::Whitespace | Token::Comment(_)) {
                continue;
            }

            if n == 0 {
                return Some(tok);
            }
            n -= 1;
        }
    }

    /// Returns a slice of the original sql string starting at some token to the
    /// current position of the parser.
    pub(crate) fn sql_slice_starting_at(&self, start: &TokenWithLocation) -> Result<&str> {
        match self.peek() {
            Some(end) => self.sql.get(start.start_idx..end.start_idx).ok_or_else(|| {
                RayexecError::new("Unable to get string slice for original sql string")
            }),
            None => self.sql.get(start.start_idx..).ok_or_else(|| {
                RayexecError::new("Unable to get string slice for original sql string")
            }),
        }
    }
}
