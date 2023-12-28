use std::io::{self};

use nu_ansi_term::{Color, Style};

use crate::local::is_client_cmd;
use reedline::{Highlighter, Hinter, SearchQuery, StyledText, ValidationResult, Validator};
use sqlbuiltins::functions::FUNCTION_REGISTRY;
use sqlexec::export::sqlparser::dialect::GenericDialect;
use sqlexec::export::sqlparser::keywords::Keyword;
use sqlexec::export::sqlparser::tokenizer::{Token, Tokenizer};

pub(crate) struct SQLHighlighter;
pub(crate) struct SQLValidator;

impl Validator for SQLValidator {
    fn validate(&self, line: &str) -> reedline::ValidationResult {
        if line.trim().is_empty() || is_client_cmd(line) {
            return ValidationResult::Complete;
        }

        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut last_char = '\0';

        for ch in line.chars() {
            match ch {
                '\'' if last_char != '\\' && !in_double_quote => in_single_quote = !in_single_quote,
                '"' if last_char != '\\' && !in_single_quote => in_double_quote = !in_double_quote,
                ';' if !in_single_quote && !in_double_quote => return ValidationResult::Complete,
                _ => {}
            }
            last_char = ch;
        }

        ValidationResult::Incomplete
    }
}

fn colorize_sql(query: &str, st: &mut StyledText, is_hint: bool) {
    let dialect = GenericDialect;

    let mut tokenizer = Tokenizer::new(&dialect, query);

    let tokens = tokenizer.tokenize().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Failed to tokenize SQL: {}", e),
        )
    });
    let new_style = || {
        if is_hint {
            Style::new().dimmed().italic()
        } else {
            Style::new()
        }
    };
    let colorize_function = || new_style().fg(Color::Cyan);

    // the tokenizer will error if the final character is an unescaped quote
    // such as `select * from read_csv("
    // in this case we will try to find the quote and colorize the rest of the query
    // otherwise we will return the error
    if tokens.is_err() {
        let pos = query.find(&['\'', '"'][..]);

        match pos {
            None => return,
            Some(pos) => {
                let (s1, s2) = query.split_at(pos);
                colorize_sql(s1, st, is_hint);

                st.push((new_style(), s2.to_string()));
                return;
            }
        }
    }
    let tokens = tokens.unwrap();

    for token in tokens {
        match token {
            // Symbols
            token @ (Token::LParen
            | Token::RParen
            | Token::Mul
            | Token::Comma
            | Token::Ampersand
            | Token::Pipe
            | Token::Plus
            | Token::Minus
            | Token::Div
            | Token::Mod
            | Token::Caret
            | Token::Lt
            | Token::LtEq
            | Token::Gt
            | Token::GtEq
            | Token::Eq
            | Token::Neq
            | Token::SemiColon) => st.push((new_style().fg(Color::Purple), format!("{token}"))),
            // Strings
            Token::SingleQuotedString(s) => {
                st.push((new_style().fg(Color::Yellow).italic(), format!("'{}'", s)))
            }
            Token::DoubleQuotedString(s) => {
                st.push((new_style().fg(Color::Yellow).italic(), format!("\"{}\"", s)))
            }
            Token::Word(w) => match w.keyword {
                // Keywords
                Keyword::SELECT
                | Keyword::FROM
                | Keyword::WHERE
                | Keyword::GROUP
                | Keyword::BY
                | Keyword::ORDER
                | Keyword::LIMIT
                | Keyword::OFFSET
                | Keyword::AND
                | Keyword::OR
                | Keyword::AS
                | Keyword::ON
                | Keyword::INNER
                | Keyword::LEFT
                | Keyword::RIGHT
                | Keyword::FULL
                | Keyword::OUTER
                | Keyword::JOIN
                | Keyword::CREATE
                | Keyword::EXTERNAL
                | Keyword::TABLE
                | Keyword::ASC
                | Keyword::DESC
                | Keyword::NULL
                | Keyword::NOT
                | Keyword::IN
                | Keyword::WITH
                | Keyword::INSERT
                | Keyword::INTO
                | Keyword::DATABASE
                | Keyword::DROP
                | Keyword::CREDENTIALS
                | Keyword::OPTIONS
                | Keyword::VALUES
                | Keyword::CASE
                | Keyword::WHEN
                | Keyword::THEN
                | Keyword::IF
                | Keyword::ELSE
                | Keyword::END
                | Keyword::UPDATE
                | Keyword::SET
                | Keyword::DELETE
                | Keyword::VIEW
                | Keyword::EXCEPT
                | Keyword::EXPLAIN
                | Keyword::ANALYZE
                | Keyword::DESCRIBE
                | Keyword::EXCLUDE
                | Keyword::TEMP
                | Keyword::TEMPORARY => {
                    st.push((new_style().fg(Color::LightGreen), format!("{w}")));
                }
                // Types
                Keyword::INT
                | Keyword::INT4
                | Keyword::INT8
                | Keyword::FLOAT
                | Keyword::FLOAT4
                | Keyword::FLOAT8
                | Keyword::DOUBLE
                | Keyword::BOOLEAN
                | Keyword::VARCHAR
                | Keyword::DATE
                | Keyword::TIME
                | Keyword::DATETIME
                | Keyword::ARRAY
                | Keyword::DECIMAL
                | Keyword::TEXT
                | Keyword::TIMESTAMP
                | Keyword::INTERVAL
                | Keyword::BIGINT
                | Keyword::SMALLINT
                | Keyword::TINYINT
                | Keyword::UNSIGNED => {
                    st.push((new_style().fg(Color::Purple), format!("{w}")));
                }
                // Custom Keywords
                Keyword::NoKeyword => match w.value.to_uppercase().as_str() {
                    "TUNNEL" | "PROVIDER" | "CREDENTIAL" => {
                        st.push((new_style().fg(Color::LightGreen), format!("{w}")))
                    }
                    // Functions
                    other if FUNCTION_REGISTRY.contains(other) => {
                        st.push((colorize_function(), format!("{w}")));
                    }
                    _ => {
                        st.push((new_style(), format!("{w}")));
                    }
                },
                // TODO: add more keywords
                _ => {
                    if FUNCTION_REGISTRY.contains(&w.value) {
                        st.push((colorize_function(), format!("{w}")));
                    } else {
                        st.push((new_style(), format!("{w}")))
                    }
                }
            },
            other => {
                st.push((new_style(), format!("{other}")));
            }
        }
    }
}

impl Highlighter for SQLHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> reedline::StyledText {
        let mut styled_text = StyledText::new();
        colorize_sql(line, &mut styled_text, false);
        styled_text
    }
}

pub struct SQLHinter {
    current_hint: String,
    min_chars: usize,
}

impl SQLHinter {
    pub fn new() -> Self {
        SQLHinter {
            current_hint: String::new(),
            min_chars: 1,
        }
    }
    fn paint(&self) -> String {
        let mut styled_text = StyledText::new();
        colorize_sql(&self.current_hint, &mut styled_text, true);
        styled_text.render_simple()
    }
}

impl Hinter for SQLHinter {
    fn handle(
        &mut self,
        line: &str,
        _pos: usize,
        history: &dyn reedline::History,
        use_ansi_coloring: bool,
    ) -> String {
        self.current_hint = if line.chars().count() >= self.min_chars {
            history
                .search(SearchQuery::last_with_prefix(
                    line.to_string(),
                    history.session(),
                ))
                .expect("todo: error handling")
                .get(0)
                .map_or_else(String::new, |entry| {
                    entry
                        .command_line
                        .get(line.len()..)
                        .unwrap_or_default()
                        .to_string()
                })
        } else {
            String::new()
        };

        if use_ansi_coloring && !self.current_hint.is_empty() {
            self.paint()
        } else {
            self.current_hint.clone()
        }
    }

    fn complete_hint(&self) -> String {
        self.current_hint.clone()
    }

    fn next_hint_token(&self) -> String {
        let mut reached_content = false;
        let result: String = self
            .current_hint
            .chars()
            .take_while(|c| match (c.is_whitespace(), reached_content) {
                (true, true) => false,
                (true, false) => true,
                (false, true) => true,
                (false, false) => {
                    reached_content = true;
                    true
                }
            })
            .collect();
        result
    }
}

#[cfg(test)]
mod tests {
    use reedline::{ValidationResult, Validator};

    #[test]
    fn test_is_line_complete() {
        use ValidationResult::*;
        let incompletes = vec![
            "';''",
            r#"'\'"#,
            r#"'\';'"#,
            r#"";"#,
            "select 'value;",                 // Single quote not closed
            "select \"value;",                // Double quote not closed
            "select 'value",                  // Missing semicolon and single quote not closed
            "select \"value",                 // Missing semicolon and double quote not closed
            "select 'val;ue",                 // Semicolon inside single quote
            "select \"val;ue",                // Semicolon inside double quote
            "select '",                       // Only single quote opened
            "select \"",                      // Only double quote opened
            "select 'value\\';",              // Escaped single quote
            "select \"value\\\";",            // Escaped double quote
            "select 'value\\\"",              // Mixed quote, single quote not closed
            "select \"value\\'",              // Mixed quote, double quote not closed
            "select 'value; \"inner\"'",      // Nested double quote inside single quote
            "select \"value; 'inner'\"",      // Nested single quote inside double quote
            "select 'value; \\\"incomplete",  // Escaped double quote inside single quote
            "select \"value; \\\'incomplete", // Escaped single quote inside double quote
        ];

        let completes = vec![
            "select 'value';",                // Correct single quote closed, semicolon outside
            "select 'value\\'; another';",    // Escaped single quote inside, semicolon outside
            "select ';value';",               // Semicolon inside single quotes, but also outside
            "select '';",                     // Empty single quotes
            "select \"value\";",              // Correct double quote closed, semicolon outside
            "select \"value\\\"; another\";", // Escaped double quote inside, semicolon outside
            "select \";value\";",             // Semicolon inside double quotes, but also outside
            "select \"\";",                   // Empty double quotes
            "select 'value; \"inner\"';", // Nested double quote inside single quote, semicolon outside
            "select \"value; 'inner'\";", // Nested single quote inside double quote, semicolon outside
            "select 'value; \\'another\\'';", // Escaped single quote inside single quotes
            "select \"value; \\\"another\\\"\";", // Escaped double quote inside double quotes
        ];

        let validator = super::SQLValidator;
        for case in incompletes {
            if !matches!(validator.validate(case), Incomplete) {
                panic!("Expected Incomplete for {:?}", case);
            }
        }
        for case in completes {
            if !matches!(validator.validate(case), Complete) {
                panic!("Expected Complete for {:?}", case);
            }
        }
    }
}
