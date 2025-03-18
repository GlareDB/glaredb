use glaredb_error::{DbError, Result};
use serde::{Deserialize, Serialize};

use super::{AstParseable, Expr, Ident, OrderByNode};
use crate::keywords::Keyword;
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowSpec<T: AstMeta> {
    Named(Ident),
    Definition(WindowDefinition<T>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowDefinition<T: AstMeta> {
    pub existing: Option<Ident>,
    pub partition_by: Vec<Expr<T>>,
    pub order_by: Vec<OrderByNode<T>>,
    pub frame: Option<WindowFrame<T>>,
}

impl Default for WindowDefinition<Raw> {
    fn default() -> Self {
        WindowDefinition {
            existing: None,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        }
    }
}

impl AstParseable for WindowDefinition<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        // TODO: This will prevent window names from being a keyword.
        let existing = if parser.peek_keyword().is_none() {
            Some(Ident::parse(parser)?)
        } else {
            None
        };

        let partition_by = if parser.parse_keyword_sequence(&[Keyword::PARTITION, Keyword::BY]) {
            parser.parse_comma_separated(Expr::parse)?
        } else {
            Vec::new()
        };

        let order_by = if parser.parse_keyword_sequence(&[Keyword::ORDER, Keyword::BY]) {
            parser.parse_comma_separated(OrderByNode::parse)?
        } else {
            Vec::new()
        };

        let frame = parser.maybe_parse(WindowFrame::parse);

        Ok(WindowDefinition {
            existing,
            partition_by,
            order_by,
            frame,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum WindowFrameUnit {
    Rows,
    Range,
    Groups,
}

impl AstParseable for WindowFrameUnit {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Ok(
            match parser.parse_one_of_keywords(&[Keyword::ROWS, Keyword::RANGE, Keyword::GROUPS]) {
                Some(Keyword::ROWS) => WindowFrameUnit::Rows,
                Some(Keyword::RANGE) => WindowFrameUnit::Range,
                Some(Keyword::GROUPS) => WindowFrameUnit::Groups,
                _ => {
                    return Err(DbError::new(
                        "Expected ROWS, RANGE, or GROUPS for window frame unit",
                    ));
                }
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum WindowFrameExclusion {
    ExcludeCurrentRow,
    ExcludeGroup,
    ExcludeTies,
    ExcludeNoOthers,
}

impl AstParseable for WindowFrameExclusion {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Ok(
            if parser.parse_keyword_sequence(&[Keyword::EXCLUDE, Keyword::CURRENT, Keyword::ROW]) {
                WindowFrameExclusion::ExcludeCurrentRow
            } else if parser.parse_keyword_sequence(&[Keyword::EXCLUDE, Keyword::GROUP]) {
                WindowFrameExclusion::ExcludeGroup
            } else if parser.parse_keyword_sequence(&[Keyword::EXCLUDE, Keyword::TIES]) {
                WindowFrameExclusion::ExcludeTies
            } else if parser.parse_keyword_sequence(&[
                Keyword::EXCLUDE,
                Keyword::NO,
                Keyword::OTHERS,
            ]) {
                WindowFrameExclusion::ExcludeNoOthers
            } else {
                return Err(DbError::new(
                    "Expected EXLUDE CURRENT ROW, EXCLUDE GROUP, EXCLUDE TIES, EXCLUDE NO OTHERS for window frame exclusion",
                ));
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFrameBound<T: AstMeta> {
    CurrentRow,
    UnboundedPreceding,
    Preceding(Box<Expr<T>>),
    UnboundedFollowing,
    Following(Box<Expr<T>>),
}

impl AstParseable for WindowFrameBound<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Ok(
            if parser.parse_keyword_sequence(&[Keyword::UNBOUNDED, Keyword::PRECEDING]) {
                WindowFrameBound::UnboundedPreceding
            } else if parser.parse_keyword_sequence(&[Keyword::UNBOUNDED, Keyword::FOLLOWING]) {
                WindowFrameBound::UnboundedFollowing
            } else if parser.parse_keyword_sequence(&[Keyword::CURRENT, Keyword::ROW]) {
                WindowFrameBound::CurrentRow
            } else {
                let expr = Expr::parse(parser)?;
                match parser.parse_one_of_keywords(&[Keyword::PRECEDING, Keyword::FOLLOWING]) {
                    Some(Keyword::PRECEDING) => WindowFrameBound::Preceding(Box::new(expr)),
                    Some(Keyword::FOLLOWING) => WindowFrameBound::Following(Box::new(expr)),
                    _ => {
                        return Err(DbError::new(
                            "Expected <expr> PRECEDING, UNBOUNDED PRECEDING, CURRENT ROW, UNBOUNDED FOLLOWING, or <expr> FOLLOWING for window frame bound",
                        ));
                    }
                }
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFrame<T: AstMeta> {
    pub unit: WindowFrameUnit,
    pub start: WindowFrameBound<T>,
    pub end: Option<WindowFrameBound<T>>,
    pub exclusion: Option<WindowFrameExclusion>,
}

impl AstParseable for WindowFrame<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let unit = WindowFrameUnit::parse(parser)?;

        if parser.parse_keyword(Keyword::BETWEEN) {
            // BETWEEN <start> AND <end> [exclusion]
            let start = WindowFrameBound::parse(parser)?;
            parser.expect_keyword(Keyword::AND)?;
            let end = WindowFrameBound::parse(parser)?;
            let exclusion = parser.maybe_parse(WindowFrameExclusion::parse);

            Ok(WindowFrame {
                unit,
                start,
                end: Some(end),
                exclusion,
            })
        } else {
            // <start> [exclusion]
            let start = WindowFrameBound::parse(parser)?;
            let exclusion = parser.maybe_parse(WindowFrameExclusion::parse);

            Ok(WindowFrame {
                unit,
                start,
                end: None,
                exclusion,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::ast::Literal;
    use crate::ast::testutil::parse_ast;

    #[test]
    fn window_range_unbounded_preceding() {
        let frame: WindowFrame<_> = parse_ast("RANGE UNBOUNDED PRECEDING").unwrap();
        let expected = WindowFrame {
            unit: WindowFrameUnit::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: None,
            exclusion: None,
        };
        assert_eq!(expected, frame)
    }

    #[test]
    fn window_range_unbounded_preceding_exclude_current_row() {
        let frame: WindowFrame<_> =
            parse_ast("RANGE UNBOUNDED PRECEDING EXCLUDE CURRENT ROW").unwrap();
        let expected = WindowFrame {
            unit: WindowFrameUnit::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: None,
            exclusion: Some(WindowFrameExclusion::ExcludeCurrentRow),
        };
        assert_eq!(expected, frame)
    }

    #[test]
    fn window_rows_expr_following() {
        let frame: WindowFrame<_> = parse_ast("ROWS 4 FOLLOWING").unwrap();
        let expected = WindowFrame {
            unit: WindowFrameUnit::Rows,
            start: WindowFrameBound::Following(Box::new(Expr::Literal(Literal::Number(
                "4".to_string(),
            )))),
            end: None,
            exclusion: None,
        };
        assert_eq!(expected, frame)
    }
}
