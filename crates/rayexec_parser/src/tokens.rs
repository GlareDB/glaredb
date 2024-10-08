use std::fmt;

use rayexec_error::{RayexecError, Result};

use crate::keywords::{keyword_from_str, Keyword};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Word(Word),
    SingleQuotedString(String),
    Number(String),
    Whitespace,
    /// '='
    Eq,
    /// '=='
    DoubleEq,
    /// '!=' or '<>'
    Neq,
    /// '<'
    Lt,
    /// '>'
    Gt,
    /// '<='
    LtEq,
    /// '>='
    GtEq,
    /// '+'
    Plus,
    /// '-'
    Minus,
    /// '*'
    Mul,
    /// '/'
    Div,
    /// '//'
    IntDiv,
    /// '%'
    Mod,
    /// '|'
    Pipe,
    /// '||'
    Concat,
    /// ','
    Comma,
    /// '('
    LeftParen,
    /// ')'
    RightParen,
    /// '.'
    Period,
    /// ':'
    Colon,
    /// '::'
    DoubleColon,
    /// ';'
    SemiColon,
    /// '{'
    LeftBrace,
    /// '}'
    RightBrace,
    /// '['
    LeftBracket,
    /// ']'
    RightBracket,
    /// '=>'
    RightArrow,
    /// '!'
    Exclamation,
    /// '^'
    Caret,
    /// '^@'
    CaretAt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenWithLocation {
    pub token: Token,
    /// Starting index of the token within the sql string.
    pub start_idx: usize,
    /// Line number for the token.
    pub line: usize,
    /// Column number for where the token starts.
    pub col: usize,
}

impl TokenWithLocation {
    pub fn is_keyword(&self, other: Keyword) -> bool {
        let word = match &self.token {
            Token::Word(w) => w,
            _ => return false,
        };
        let keyword = match word.keyword {
            Some(k) => k,
            None => return false,
        };
        keyword == other
    }

    /// Return the keyword for this token if available.
    pub fn keyword(&self) -> Option<Keyword> {
        match &self.token {
            Token::Word(w) => w.keyword,
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Word {
    pub value: String,
    pub quote: Option<char>,
    pub keyword: Option<Keyword>,
}

impl Word {
    pub fn new(value: impl Into<String>, quote: Option<char>) -> Self {
        let value = value.into();
        let keyword = if quote.is_some() {
            None
        } else {
            keyword_from_str(&value)
        };
        Word {
            value,
            quote,
            keyword,
        }
    }
}

impl fmt::Display for Word {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(quote) = &self.quote {
            write!(f, "{}{}{}", quote, self.value, quote)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

// TODO: Update line and column numbers.
#[derive(Debug)]
struct State<'a> {
    /// The entire sql query.
    query: &'a str,
    /// Index of what we're currently looking at.
    idx: usize,
    /// Current line number we're on.
    line: usize,
    /// Current column number we're on.
    col: usize,
}

impl<'a> State<'a> {
    fn peek(&mut self) -> Option<char> {
        let mut chars = self.query[self.idx..].chars();
        chars.next()
    }

    /// Get the next char from the query.
    fn next(&mut self) -> Option<char> {
        let mut chars = self.query[self.idx..].char_indices();
        match chars.next() {
            Some((_, next)) => {
                if next == '\n' {
                    self.line += 1;
                    self.col = 0;
                } else {
                    self.col += 1;
                }

                match chars.next() {
                    Some((next_idx, _)) => self.idx += next_idx,
                    None => self.idx = self.query.len(),
                }

                Some(next)
            }
            None => None,
        }
    }

    /// Take a string from the query using the given predicate.
    fn take_while(&mut self, mut predicate: impl FnMut(char) -> bool) -> &'a str {
        #[allow(unused_assignments)] // Clippy drunk
        let mut end_idx = self.idx;

        let mut chars = self.query[self.idx..].char_indices();

        loop {
            match chars.next() {
                Some((char_idx, c)) => {
                    end_idx = char_idx + self.idx;
                    if !predicate(c) {
                        break;
                    }
                }
                None => {
                    end_idx = self.query.len();
                    break;
                }
            }
        }

        let result = &self.query[self.idx..end_idx];
        self.idx = end_idx;
        result
    }
}

#[derive(Debug)]
pub struct Tokenizer<'a> {
    state: State<'a>,
}

impl<'a> Tokenizer<'a> {
    /// Create a tokenizer for the provided sql query.
    pub fn new(query: &'a str) -> Self {
        Tokenizer {
            state: State {
                query,
                idx: 0,
                line: 0,
                col: 0,
            },
        }
    }

    /// Generate tokens for the configured query.
    pub fn tokenize(&mut self) -> Result<Vec<TokenWithLocation>> {
        let mut tokens = Vec::new();
        let mut start_idx = self.state.idx;
        while let Some(token) = self.next_token()? {
            tokens.push(TokenWithLocation {
                token,
                start_idx,
                line: self.state.line,
                col: self.state.col,
            });

            start_idx = self.state.idx;
        }

        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Option<Token>> {
        let c = match self.state.peek() {
            Some(c) => c,
            None => return Ok(None),
        };

        Ok(Some(match c {
            ' ' | '\t' | '\n' | '\r' => {
                self.state.next();
                Token::Whitespace
            }
            ';' => {
                self.state.next();
                Token::SemiColon
            }
            '(' => {
                self.state.next();
                Token::LeftParen
            }
            ')' => {
                self.state.next();
                Token::RightParen
            }
            '[' => {
                self.state.next();
                Token::LeftBracket
            }
            ']' => {
                self.state.next();
                Token::RightBracket
            }
            ',' => {
                self.state.next();
                Token::Comma
            }
            '*' => {
                self.state.next();
                Token::Mul
            }
            '+' => {
                self.state.next();
                Token::Plus
            }
            '-' => {
                self.state.next();
                Token::Minus
            }
            '/' => {
                self.state.next();
                match self.state.peek() {
                    Some('/') => {
                        self.state.next();
                        Token::IntDiv
                    }
                    _ => Token::Div,
                }
            }
            '%' => {
                self.state.next();
                Token::Mod
            }
            '^' => {
                self.state.next();
                match self.state.peek() {
                    Some('@') => {
                        self.state.next();
                        Token::CaretAt
                    }
                    _ => Token::Caret,
                }
            }
            '>' => {
                self.state.next();
                match self.state.peek() {
                    Some('=') => {
                        self.state.next();
                        Token::GtEq
                    }
                    _ => Token::Gt,
                }
            }
            '<' => {
                self.state.next();
                match self.state.peek() {
                    Some('=') => {
                        self.state.next();
                        Token::LtEq
                    }
                    Some('>') => {
                        self.state.next();
                        Token::Neq
                    }
                    _ => Token::Lt,
                }
            }
            '!' => {
                self.state.next();
                match self.state.peek() {
                    Some('=') => {
                        self.state.next();
                        Token::Neq
                    }
                    _ => Token::Exclamation,
                }
            }
            '|' => {
                self.state.next();
                match self.state.peek() {
                    Some('|') => {
                        self.state.next();
                        Token::Concat
                    }
                    _ => Token::Pipe,
                }
            }
            ':' => {
                self.state.next();
                match self.state.peek() {
                    Some(':') => {
                        self.state.next();
                        Token::DoubleColon
                    }
                    _ => Token::Colon,
                }
            }
            // Strings
            '\'' => {
                self.state.next();
                let s = self.take_quoted_string('\'');
                Token::SingleQuotedString(s.to_string())
            }
            // Numbers
            '0'..='9' | '.' => {
                let mut period_found = false;
                let s = self.state.take_while(|c| {
                    if c.is_ascii_digit() {
                        return true;
                    }
                    if period_found {
                        return false;
                    }
                    if c == '.' {
                        period_found = true;
                        return true;
                    }
                    false
                });

                // Just a period, possibly for a compound identifier.
                if s == "." {
                    return Ok(Some(Token::Period));
                }

                Token::Number(s.to_string())
            }
            // Operators
            '=' => {
                self.state.next();
                match self.state.peek() {
                    Some('>') => {
                        self.state.next();
                        Token::RightArrow
                    }
                    Some('=') => {
                        self.state.next();
                        Token::DoubleEq
                    }
                    _ => Token::Eq,
                }
            }
            // Identifiers
            c if Self::is_identifier_start(c) => {
                let ident = self.take_identifier();
                Token::Word(ident)
            }
            c if Self::is_quoted_identifier_start(c) => {
                let ident = self.take_quoted_identifier();
                Token::Word(ident)
            }

            c => return Err(RayexecError::new(format!("Unhandled character: {c}"))),
        }))
    }

    fn take_quoted_string(&mut self, quote: char) -> &'a str {
        // TODO: End quote? Would need to track nested quotes.
        let s = self.state.take_while(|c| c != quote);
        // Consume the end quote.
        let _ = self.state.next();
        s
    }

    fn take_quoted_identifier(&mut self) -> Word {
        let _ = self.state.next(); // Take start quote
        let quoted = self.take_quoted_string('"');
        Word::new(quoted, Some('"'))
    }

    fn take_identifier(&mut self) -> Word {
        let s = self.state.take_while(|c| c.is_alphanumeric() || c == '_');
        Word::new(s, None)
    }

    fn is_identifier_start(c: char) -> bool {
        c.is_alphabetic()
    }

    fn is_quoted_identifier_start(c: char) -> bool {
        c == '"'
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_take_while() {
        let mut state = State {
            query: "abcdefghi",
            idx: 0,
            line: 0,
            col: 0,
        };

        let a = state.take_while(|c| c != 'd');
        assert_eq!("abc", a);

        let b = state.take_while(|_c| false);
        assert_eq!("", b);

        let c = state.take_while(|_c| true);
        assert_eq!("defghi", c);

        let d = state.take_while(|_c| true);
        assert_eq!("", d);
    }

    #[test]
    fn simple_token_start_idx() {
        let toks = Tokenizer::new("CREATE VIEW   hi AS SELECT 1 FROM my_table")
            .tokenize()
            .unwrap();

        // CREATE
        assert_eq!(toks[0].start_idx, 0);

        // <whitespace>
        assert_eq!(toks[1].start_idx, 6);

        // VIEW
        assert_eq!(toks[2].start_idx, 7);

        // <whitespace>
        assert_eq!(toks[3].start_idx, 11);
        assert_eq!(toks[4].start_idx, 12);
        assert_eq!(toks[5].start_idx, 13);

        // hi
        assert_eq!(toks[6].start_idx, 14);
    }
}
