use crate::{
    ast::{AstParseable, Expr, Ident, Literal, ObjectReference, SelectModifer, SelectNode},
    keywords::{self, Keyword, RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_TABLE_ALIAS},
    statement::Statement,
    tokens::{Token, TokenWithLocation, Tokenizer, Word},
};
use rayexec_error::{RayexecError, Result};
use std::fmt;

/// Parse a sql query into statements.
pub fn parse(sql: &str) -> Result<Vec<Statement<'_>>> {
    let toks = Tokenizer::new(sql).tokenize()?;
    Parser::with_tokens(toks).parse_statements()
}

#[derive(Debug)]
pub struct Parser<'a> {
    toks: Vec<TokenWithLocation<'a>>,
    /// Index of token we should process next.
    pub(crate) idx: usize,
}

impl<'a> Parser<'a> {
    /// Create a parser with arbitrary tokens.
    pub fn with_tokens(toks: Vec<TokenWithLocation<'a>>) -> Self {
        Parser { toks, idx: 0 }
    }

    /// Parse any number of statements, including zero statements.
    ///
    /// Statements are expected to be delineated with a semicolon.
    pub fn parse_statements(&mut self) -> Result<Vec<Statement<'a>>> {
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
                return Err(RayexecError::new("Expected semicolon between statements"));
            }

            let stmt = self.parse_statement()?;
            stmts.push(stmt);

            expect_delimiter = true;
        }

        Ok(stmts)
    }

    /// Parse a single statement.
    pub fn parse_statement(&mut self) -> Result<Statement<'a>> {
        let tok = match self.next() {
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
                    Keyword::CREATE => self.parse_create(),
                    Keyword::SET => self.parse_set(),
                    other => {
                        return Err(RayexecError::new(format!("Unexpected keyword: {other:?}",)))
                    }
                }
            }
            other => {
                return Err(RayexecError::new(format!(
                    "Expected a SQL statement, got {other:?}"
                )))
            }
        }
    }

    /// Parse `CREATE ...`
    pub fn parse_create(&mut self) -> Result<Statement<'a>> {
        let or_replace = self.parse_keyword_sequence(&[Keyword::OR, Keyword::REPLACE]);
        let temp = self.parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY]);

        if self.parse_keyword(Keyword::TABLE) {
            // Table
            unimplemented!()
        } else if self.parse_keyword(Keyword::SCHEMA) {
            // Schema
            if or_replace {
                return Err(RayexecError::new(
                    "OR REPLACE not supported when creating a schema",
                ));
            }
            if temp {
                return Err(RayexecError::new(
                    "TEMPORARY not supported when creating a schema",
                ));
            }

            let if_not_exists =
                self.parse_keyword_sequence(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
            let reference = ObjectReference::parse(self)?;

            Ok(Statement::CreateSchema {
                reference,
                if_not_exists,
            })
        } else {
            unimplemented!()
        }
    }

    pub fn parse_set(&mut self) -> Result<Statement<'a>> {
        let name = ObjectReference::parse(self)?;
        if self.parse_keyword(Keyword::TO) || self.consume_token(&Token::Eq) {
            let expr = Expr::parse(self)?;
            return Ok(Statement::SetVariable {
                reference: name,
                value: expr,
            });
        }

        Err(RayexecError::new(format!(
            "Expected 'SET {name} TO <value>' or SET {name} = <value>'"
        )))
    }

    /// Parse a single `SELECT ...`
    pub(crate) fn parse_select_node(&mut self) -> Result<SelectNode<'a>> {
        self.expect_keyword(Keyword::SELECT)?;

        let all = self.parse_keyword(Keyword::ALL);
        let distinct = self.parse_keyword(Keyword::DISTINCT);
        if all && distinct {
            return Err(RayexecError::new("Cannot specifiy both ALL and DISTINCT"));
        }

        let modifier: Option<SelectModifer> = if self.parse_keyword(Keyword::DISTINCT) {
            unimplemented!()
        } else {
            None
        };

        unimplemented!()
    }

    /// Parse an optional alias.
    ///
    /// `reserved` must be sorted.
    pub(crate) fn parse_alias(&mut self, reserved: &[Keyword]) -> Result<Option<Ident<'a>>> {
        let has_as = self.parse_keyword(Keyword::AS);
        let tok = match self.peek() {
            Some(tok) => &tok.token,
            None => return Ok(None),
        };

        let ident: Option<Ident> = match tok {
            // Allow any alias if `AS` was explicitly provided.
            Token::Word(w) if has_as => Some(Ident { value: w.value }),

            // If `AS` wasn't provided, allow the next word to be used as the
            // alias if it's not a reserved word. Otherwise assume it's not an
            // alias.
            Token::Word(w) => match &w.keyword {
                Some(kw) if reserved.binary_search(kw).is_ok() => None,
                _ => Some(Ident { value: w.value }),
            },

            // Allow any singly quoted string.
            Token::SingleQuotedString(s) => Some(Ident { value: s }),

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
        mut f: impl FnMut(&mut Parser<'a>) -> Result<T>,
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

    /// Parse a comma separated list of one or more items surrounded by
    /// parentheses.
    pub(crate) fn parse_parenthesized_comma_separated<T>(
        &mut self,
        mut f: impl FnMut(&mut Parser<'a>) -> Result<T>,
    ) -> Result<Vec<T>> {
        self.expect_token(&Token::LeftParen)?;
        let vals = self.parse_comma_separated(f)?;
        self.expect_token(&Token::RightParen)?;
        Ok(vals)
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

    /// Parse any of the provided keywords.
    pub(crate) fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let idx = self.idx;
        let tok = match self.next() {
            Some(tok) => tok,
            None => return false,
        };

        if keywords.iter().any(|k| tok.is_keyword(*k)) {
            return true;
        }

        // No matches, reset index.
        self.idx = idx;
        false
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
        return Err(RayexecError::new(format!(
            "Expected one of {expected:?}, got {:?}",
            self.peek()
        )));
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

    /// Get the next token.
    ///
    /// Ignores whitespace.
    pub(crate) fn next(&mut self) -> Option<&TokenWithLocation<'a>> {
        loop {
            if self.idx >= self.toks.len() {
                return None;
            }

            let tok = &self.toks[self.idx];
            self.idx += 1;

            if matches!(&tok.token, Token::Whitespace) {
                continue;
            }

            return Some(tok);
        }
    }

    /// Get the next token without altering the current index.
    ///
    /// Ignores whitespace.
    pub(crate) fn peek(&mut self) -> Option<&TokenWithLocation<'a>> {
        self.peek_nth(0)
    }

    /// Get the nth next token without altering the current index.
    ///
    /// Ignores whitespace.
    pub(crate) fn peek_nth(&mut self, mut n: usize) -> Option<&TokenWithLocation<'a>> {
        let mut idx = self.idx;
        loop {
            if idx >= self.toks.len() {
                return None;
            }

            let tok = &self.toks[idx];
            idx += 1;

            if matches!(&tok.token, Token::Whitespace) {
                continue;
            }

            if n == 0 {
                return Some(tok);
            }
            n -= 1;
        }
    }
}
