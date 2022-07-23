use super::{MutateRelationExpr, RelationExpr};
use serde::{Deserialize, Serialize};

/// A union between querying and mutating relation expressions. Useful for
/// explains.
#[derive(Debug, Serialize, Deserialize)]
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
