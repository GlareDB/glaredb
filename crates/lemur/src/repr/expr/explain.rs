use super::{MutateRelationExpr, RelationExpr};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A union between querying and mutating relation expressions. Useful for
/// explains.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExplainRelationExpr {
    Query(RelationExpr),
    Mutate(MutateRelationExpr),
}

impl From<RelationExpr> for ExplainRelationExpr {
    fn from(expr: RelationExpr) -> Self {
        ExplainRelationExpr::Query(expr)
    }
}

impl From<MutateRelationExpr> for ExplainRelationExpr {
    fn from(expr: MutateRelationExpr) -> Self {
        ExplainRelationExpr::Mutate(expr)
    }
}

impl fmt::Display for ExplainRelationExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Prettify.
        write!(f, "{:#?}", self)
    }
}
