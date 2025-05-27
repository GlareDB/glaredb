use std::fmt;
use std::iter::Peekable;
use std::str::Lines;

use glaredb_error::{DbError, Result};

pub trait ParseableRecord<'a>: Sized {
    /// String representing the start of a record.
    const RECORD_START: &'static str;

    /// Parses the record from the provided state.
    ///
    /// The parser will be on the line indicated by `RECORD_START`.
    fn parse(parser: &mut Parser<'a>) -> Result<Self>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location<'a> {
    pub line: usize,
    pub file: &'a str,
}

impl<'a> Location<'a> {
    pub const fn new(file: &'a str, line: usize) -> Self {
        Location { line, file }
    }

    pub fn emit_error<T>(&self, msg: impl Into<String>) -> Result<T> {
        Err(self.format_error(msg))
    }

    pub fn format_error(&self, msg: impl Into<String>) -> DbError {
        DbError::new(msg).with_field("location", self.to_string())
    }
}

impl fmt::Display for Location<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.line)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TrimmedLine<'a> {
    /// Location within the file.
    pub loc: Location<'a>,
    /// The line itself, with a prefix trimmed.
    pub line: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimitedBy {
    Newline,
    Dashes,
    Eof,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DelimitedLines<'a> {
    pub lines: Vec<&'a str>,
    pub delimited_by: DelimitedBy,
}

#[derive(Debug)]
pub struct Parser<'a> {
    curr_loc: Location<'a>,
    lines: Peekable<Lines<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(file: &'a str, input: &'a str) -> Self {
        let lines = input.lines().peekable();
        Parser {
            curr_loc: Location { line: 1, file },
            lines,
        }
    }

    pub fn dispatch_parse<F, R>(&mut self, mut dispatch: F) -> Result<Option<R>>
    where
        F: FnMut(&mut Self, &str) -> Result<R>,
    {
        loop {
            let line = match self.lines.peek() {
                Some(line) => line,
                None => return Ok(None),
            };

            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("#") {
                self.lines.next().unwrap(); // Advance.
                self.curr_loc.line += 1;
                continue;
            }

            let prefix = trimmed.split(' ').next().unwrap();
            let out = dispatch(self, prefix)?;

            return Ok(Some(out));
        }
    }

    /// Takes all lines until a delimiter is reached.
    ///
    /// A delimiter can be:
    /// - Four dashes (----)
    /// - A new line
    /// - End of file
    pub fn take_lines_delimited(&mut self) -> Result<DelimitedLines<'a>> {
        let mut lines = Vec::new();
        for line in self.lines.by_ref() {
            self.curr_loc.line += 1;
            match line.trim() {
                "----" => {
                    return Ok(DelimitedLines {
                        lines,
                        delimited_by: DelimitedBy::Dashes,
                    });
                }
                "" => {
                    return Ok(DelimitedLines {
                        lines,
                        delimited_by: DelimitedBy::Newline,
                    });
                }
                _ => lines.push(line),
            }
        }

        Ok(DelimitedLines {
            lines,
            delimited_by: DelimitedBy::Eof,
        })
    }

    /// Takes the next line, trimming `trim` from the start of the line. The
    /// resulting line will then have whitespace trimmed from the start and end.
    pub fn take_next_and_trim_start(&mut self, trim: &str) -> Result<TrimmedLine<'a>> {
        let loc = self.curr_loc;
        let line = self
            .lines
            .next()
            .ok_or_else(|| DbError::new("Unexpected end of input"))?;
        self.curr_loc.line += 1;

        let trimmed = line.trim().trim_start_matches(trim).trim();

        Ok(TrimmedLine { loc, line: trimmed })
    }
}
