use std::io::{self};

use nu_ansi_term::{Color, Style};

use reedline::{Highlighter, StyledText, Validator};
use sqlexec::export::sqlparser::dialect::GenericDialect;
use sqlexec::export::sqlparser::keywords::Keyword;
use sqlexec::export::sqlparser::tokenizer::{Token, Tokenizer};

use crate::local::is_client_cmd;

pub(crate) struct SQLHighlighter;
pub(crate) struct SQLValidator;
impl Validator for SQLValidator {
    fn validate(&self, line: &str) -> reedline::ValidationResult {
        if line.trim().is_empty() || line.trim_end().ends_with(';') || is_client_cmd(line) {
            reedline::ValidationResult::Complete
        } else {
            reedline::ValidationResult::Incomplete
        }
    }
}

fn colorize_sql(query: &str, st: &mut StyledText) -> std::io::Result<()> {
    let dialect = GenericDialect;

    let mut tokenizer = Tokenizer::new(&dialect, query);

    let tokens = tokenizer.tokenize().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Failed to tokenize SQL: {}", e),
        )
    });

    // the tokenizer will error if the final character is an unescaped quote
    // such as `select * from read_csv("
    // in this case we will try to find the quote and colorize the rest of the query
    // otherwise we will return the error
    if let Err(err) = tokens {
        let pos = query.find(&['\'', '"'][..]);
        match pos {
            None => return Err(err),
            Some(pos) => {
                let (s1, s2) = query.split_at(pos);
                colorize_sql(s1, st)?;

                st.push((Style::new(), s2.to_string()));
                return Ok(());
            }
        }
    }
    let tokens = tokens.unwrap();

    for token in tokens {
        match token {
            Token::Mul => st.push((Style::new().fg(Color::Purple), "*".to_string())),
            Token::LParen => st.push((Style::new().fg(Color::Purple), "(".to_string())),
            Token::RParen => st.push((Style::new().fg(Color::Purple), ")".to_string())),
            Token::Comma => st.push((Style::new().fg(Color::Purple), ",".to_string())),
            Token::SemiColon => st.push((Style::new().fg(Color::White).bold(), ";".to_string())),
            Token::SingleQuotedString(s) => {
                st.push((Style::new().fg(Color::Yellow).italic(), format!("'{}'", s)))
            }
            Token::DoubleQuotedString(s) => st.push((
                Style::new().fg(Color::Yellow).italic(),
                format!("\"{}\"", s),
            )),
            Token::Word(w) => match w.keyword {
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
                | Keyword::SHOW
                | Keyword::TABLES
                | Keyword::VARCHAR
                | Keyword::INT
                | Keyword::FLOAT
                | Keyword::DOUBLE
                | Keyword::BOOLEAN
                | Keyword::DATE
                | Keyword::TIME
                | Keyword::DATETIME
                | Keyword::ARRAY
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
                | Keyword::VALUES => {
                    st.push((Style::new().fg(Color::LightGreen), format!("{w}")));
                }
                Keyword::NoKeyword => match w.value.to_uppercase().as_str() {
                    "TUNNEL" | "PROVIDER" => {
                        st.push((Style::new().fg(Color::LightGreen), format!("{w}")))
                    }
                    _ => st.push((Style::new(), format!("{w}"))),
                },
                // TODO: add more keywords
                _ => st.push((Style::new(), format!("{w}"))),
            },
            other => {
                st.push((Style::new(), format!("{other}")));
            }
        }
    }
    Ok(())
}

impl Highlighter for SQLHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> reedline::StyledText {
        let mut styled_text = StyledText::new();
        colorize_sql(line, &mut styled_text).unwrap();
        styled_text
    }
}
