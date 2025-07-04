use serde::{Deserialize, Serialize};

use crate::ast::{
    Attach,
    CopyTo,
    CreateSchema,
    CreateTable,
    CreateView,
    Detach,
    DiscardStatement,
    DropStatement,
    ExplainNode,
    Insert,
    QueryNode,
    ResetVariable,
    SetVariable,
    Show,
};
use crate::meta::{AstMeta, Raw};

pub type RawStatement = Statement<Raw>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement<T: AstMeta> {
    Attach(Attach<T>),
    Detach(Detach<T>),

    Explain(ExplainNode<T>),

    /// COPY <table> TO <file>
    CopyTo(CopyTo<T>),

    /// SELECT/VALUES/DESCRIBE
    Query(QueryNode<T>),

    /// CREATE TABLE ...
    CreateTable(CreateTable<T>),

    /// CREATE SCHEMA ...
    CreateSchema(CreateSchema<T>),

    /// CREATE VIEW ...
    CreateView(CreateView<T>),

    /// DROP ...
    Drop(DropStatement<T>),

    /// INSERT INTO ...
    Insert(Insert<T>),

    /// SET <variable> TO <value>
    SetVariable(SetVariable<T>),

    /// SHOW <variable>
    /// SHOW DATABASES
    /// ...
    Show(Show<T>),

    /// RESET <variable>
    ResetVariable(ResetVariable<T>),

    /// DISCARD ...
    Discard(DiscardStatement),
}
