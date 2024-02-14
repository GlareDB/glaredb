use crate::{keywords::Keyword, statement::Statement, tokens::Token};
use rayexec_error::{RayexecError, Result};
use std::fmt;

use crate::tokens::TokenWithLocation;

#[derive(Debug)]
pub struct Parser<'a> {
    toks: Vec<TokenWithLocation<'a>>,
    /// Index of token we should process next.
    idx: usize,
}

impl<'a> Parser<'a> {
    pub fn with_tokens(toks: Vec<TokenWithLocation<'a>>) -> Self {
        Parser { toks, idx: 0 }
    }

    pub fn parse_statement(&mut self) -> Result<Statement<'a>> {
        let tok = match self.next_token() {
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
            unimplemented!()
        } else {
            unimplemented!()
        }
    }

    /// Parse a single keyword.
    fn parse_keyword(&mut self, keyword: Keyword) -> bool {
        let idx = self.idx;
        if let Some(tok) = self.next_token() {
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
    fn parse_keyword_sequence(&mut self, keywords: &[Keyword]) -> bool {
        let idx = self.idx;
        for keyword in keywords {
            if let Some(tok) = self.next_token() {
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

    fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let idx = self.idx;
        let tok = match self.next_token() {
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

    /// Get the next non-whitespace token.
    fn next_token(&mut self) -> Option<&TokenWithLocation<'a>> {
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
}
