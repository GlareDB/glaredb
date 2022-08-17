use super::{RelationExpr, RelationKey};
use crate::repr::df::Schema;

use serde::{Deserialize, Serialize};

/// Expressions that mutate underlying relations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutateRelationExpr {
    CreateTable(CreateTable),
    Insert(Insert),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTable {
    pub table: RelationKey,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Insert {
    pub table: RelationKey,
    pub input: RelationExpr,
}
