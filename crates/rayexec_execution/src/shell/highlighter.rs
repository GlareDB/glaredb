use std::collections::VecDeque;
use std::io;

use rayexec_error::Result;
use rayexec_parser::tokens::{Token, TokenWithLocation, Tokenizer};

use super::vt100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    Reset,
    Keyword,
    Literal,
    Comment,
}

impl TokenType {
    pub fn vt100(&self) -> &str {
        match self {
            Self::Reset => vt100::MODES_OFF,
            Self::Keyword => vt100::COLOR_FG_GREEN,
            Self::Literal => vt100::COLOR_FG_BLUE,
            Self::Comment => vt100::COLOR_FG_BRIGHT_BLACK,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HighlightToken {
    pub token_type: TokenType,
    pub offset: usize,
}

#[derive(Debug)]
pub struct HighlightState {
    highlight_toks: VecDeque<HighlightToken>,
    toks: Vec<TokenWithLocation>,
}

impl HighlightState {
    pub fn new() -> Self {
        HighlightState {
            highlight_toks: VecDeque::new(),
            toks: Vec::new(),
        }
    }

    /// Tokenizes the query to prepare if for highlighting.
    ///
    /// This will initialize an internal queue of highlight tokens.
    /// `next_chunk_highlight` should be called with consecutive chunks of the
    /// same query to produce highlighted strings.
    pub fn tokenize(&mut self, query: &str) {
        self.highlight_toks.clear();
        self.toks.clear();

        if Tokenizer::new(query).tokenize(&mut self.toks).is_err() {
            // Ideally we don't error here, but if we do, just use a single
            // token that does no highlighting.
            self.highlight_toks.push_back(HighlightToken {
                token_type: TokenType::Reset,
                offset: 0,
            });
            return;
        }

        for (idx, tok) in self.toks.drain(..).enumerate() {
            if idx == 0 && tok.start_idx != 0 {
                self.highlight_toks.push_back(HighlightToken {
                    token_type: TokenType::Reset,
                    offset: 0,
                })
            }

            match tok.token {
                Token::Word(w) if w.keyword.is_some() => {
                    self.highlight_toks.push_back(HighlightToken {
                        token_type: TokenType::Keyword,
                        offset: tok.start_idx,
                    });
                }
                Token::Number(_) | Token::SingleQuotedString(_) => {
                    self.highlight_toks.push_back(HighlightToken {
                        token_type: TokenType::Literal,
                        offset: tok.start_idx,
                    });
                }
                Token::Comment(_) => {
                    self.highlight_toks.push_back(HighlightToken {
                        token_type: TokenType::Comment,
                        offset: tok.start_idx,
                    });
                }
                _ => {
                    // For everything else, do no highlighting.
                    match self.highlight_toks.back() {
                        Some(tok) if tok.token_type == TokenType::Reset => (), // We can just continue on from this token, do nothing.
                        _ => self.highlight_toks.push_back(HighlightToken {
                            token_type: TokenType::Reset,
                            offset: tok.start_idx,
                        }),
                    }
                }
            }
        }
    }

    /// Returns an iterator producing strings for highlighting.
    ///
    /// This accepts `amount_written` since we write queries across multiple
    /// lines with line-breaks/continuations in between.
    pub fn next_chunk_highlight<'a>(&'a mut self, s: &'a str) -> HighlightedStrIterMut<'a> {
        HighlightedStrIterMut {
            tokens: &mut self.highlight_toks,
            s,
        }
    }

    pub fn clear_highlight<W>(&self, w: &mut W) -> Result<()>
    where
        W: io::Write,
    {
        write!(w, "{}", vt100::MODES_OFF)?;
        Ok(())
    }
}

/// String with an associated token type to use for the highlight.
#[derive(Debug)]
pub struct HighlightedStr<'a> {
    pub token_type: TokenType,
    pub s: &'a str,
}

impl<'a> HighlightedStr<'a> {
    /// Write the the string to the writer using the correct term code for
    /// highlight.
    ///
    /// This will also take care of trimming newlines in the string.
    pub fn write_vt100_trim_nl<W>(&self, writer: &mut W) -> Result<()>
    where
        W: io::Write,
    {
        let s = self.s.trim_matches('\n');
        // debug::log(|| format!("highlight trimmed: '{s}', {:?}", self.token_type));
        write!(writer, "{}{}", self.token_type.vt100(), s)?;
        Ok(())
    }
}

/// Produces highlighted strings from a previously tokenized query string.
#[derive(Debug)]
pub struct HighlightedStrIterMut<'a> {
    /// Tokens we're using for highlighting.
    tokens: &'a mut VecDeque<HighlightToken>,
    /// The string we're highlighting.
    s: &'a str,
}

impl<'a> Iterator for HighlightedStrIterMut<'a> {
    type Item = HighlightedStr<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.s.is_empty() {
            return None;
        }

        let tok = match self.tokens.pop_front() {
            Some(tok) => tok,
            None => {
                // Shouldn't happen, but might as well take care of it.
                let highlight = HighlightedStr {
                    token_type: TokenType::Reset,
                    s: self.s,
                };
                self.s = "";

                return Some(highlight);
            }
        };

        // Peek the front to see if the token we just popped applies to more
        // than just the current string.
        match self.tokens.front() {
            Some(second) => {
                let tok_len = second.offset - tok.offset;
                if self.s.len() < tok_len {
                    // This token applies to this string and followup string.
                    // Use to highlight, and push an updated value in the queue
                    // with an adjusted offset.
                    self.tokens.push_front(HighlightToken {
                        token_type: tok.token_type,
                        offset: tok.offset + self.s.len(),
                    });

                    let highlight = HighlightedStr {
                        token_type: tok.token_type,
                        s: self.s,
                    };
                    self.s = "";
                    Some(highlight)
                } else if self.s.len() > tok_len {
                    // Token only highlights a portion of this string.
                    let (h_str, rem) = self.s.split_at(tok_len);
                    let highlight = HighlightedStr {
                        token_type: tok.token_type,
                        s: h_str,
                    };
                    self.s = rem;
                    Some(highlight)
                } else {
                    // Token highlights this string exactly.
                    let highlight = HighlightedStr {
                        token_type: tok.token_type,
                        s: self.s,
                    };
                    self.s = "";
                    Some(highlight)
                }
            }
            None => {
                // Our token is the last one, keep it in the queue while also
                // using it to highlight the current string.
                self.tokens.push_front(tok);
                let highlight = HighlightedStr {
                    token_type: tok.token_type,
                    s: self.s,
                };
                self.s = "";

                Some(highlight)
            }
        }
    }
}
