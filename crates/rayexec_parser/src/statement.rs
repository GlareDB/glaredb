use crate::ast::{Expr, ObjectReference, QueryNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Query(QueryNode),

    /// CREATE SCHEMA ...
    CreateSchema {
        reference: ObjectReference,
        if_not_exists: bool,
    },

    /// SET <variable> TO <value>
    SetVariable {
        reference: ObjectReference,
        value: Expr,
    },
}
