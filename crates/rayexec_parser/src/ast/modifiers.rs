use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{AstParseable, Expr};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByModifier<T: AstMeta> {
    pub order_by_nodes: Vec<OrderByNode<T>>,
}

/// A single node in an ORDER BY clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByNode<T: AstMeta> {
    pub typ: Option<OrderByType>,
    pub nulls: Option<OrderByNulls>,
    pub expr: Expr<T>,
}

impl AstParseable for OrderByNode<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let expr = Expr::parse(parser)?;

        let typ = if parser.parse_keyword(Keyword::ASC) {
            Some(OrderByType::Asc)
        } else if parser.parse_keyword(Keyword::DESC) {
            Some(OrderByType::Desc)
        } else {
            None
        };

        let nulls = if parser.parse_keyword_sequence(&[Keyword::NULLS, Keyword::FIRST]) {
            Some(OrderByNulls::First)
        } else if parser.parse_keyword_sequence(&[Keyword::NULLS, Keyword::LAST]) {
            Some(OrderByNulls::Last)
        } else {
            None
        };

        Ok(OrderByNode { typ, nulls, expr })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderByType {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderByNulls {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitModifier<T: AstMeta> {
    pub limit: Option<Expr<T>>,
    pub offset: Option<Expr<T>>,
}

impl AstParseable for LimitModifier<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let mut limit = None;
        let mut offset = None;

        if parser.parse_keyword(Keyword::LIMIT) {
            limit = Some(Expr::parse(parser)?)
        }

        if parser.parse_keyword(Keyword::OFFSET) {
            offset = Some(Expr::parse(parser)?)
        }

        // Try limit again since LIMIT and OFFSET can be specified in any order.
        if parser.parse_keyword(Keyword::LIMIT) {
            limit = Some(Expr::parse(parser)?)
        }

        Ok(LimitModifier { limit, offset })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DistinctModifier<T: AstMeta> {
    On(Vec<Expr<T>>),
    All,
}
