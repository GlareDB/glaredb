use rayexec_parser::ast::{self, QueryNode};

use super::Bound;

// TODO: This might need some scoping information.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundCte {
    /// Normalized name for the CTE.
    pub name: String,
    /// Depth this CTE was found at.
    pub depth: usize,
    /// Column aliases taken directly from the ast.
    pub column_aliases: Option<Vec<ast::Ident>>,
    /// The bound query node.
    pub body: QueryNode<Bound>,
    /// If this CTE should be materialized.
    pub materialized: bool,
}

// TODO: Proto conv
