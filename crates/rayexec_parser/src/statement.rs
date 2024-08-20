use serde::{Deserialize, Serialize};

use crate::{
    ast::{
        Attach, CopyTo, CreateSchema, CreateTable, Describe, Detach, DropStatement, ExplainNode,
        Insert, QueryNode, ResetVariable, SetVariable, ShowVariable,
    },
    meta::{AstMeta, Raw},
};

pub type RawStatement = Statement<Raw>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement<T: AstMeta> {
    Attach(Attach<T>),
    Detach(Detach<T>),

    Explain(ExplainNode<T>),

    /// COPY <table> TO <file>
    CopyTo(CopyTo<T>),

    /// DESCRIBE <table>
    /// DESCRIBE <query>
    Describe(Describe<T>),

    /// SELECT/VALUES
    Query(QueryNode<T>),

    /// CREATE TABLE ...
    CreateTable(CreateTable<T>),

    /// DROP ...
    Drop(DropStatement<T>),

    /// INSERT INTO ...
    Insert(Insert<T>),

    /// CREATE SCHEMA ...
    CreateSchema(CreateSchema<T>),

    /// SET <variable> TO <value>
    SetVariable(SetVariable<T>),

    /// SHOW <variable>
    ShowVariable(ShowVariable<T>),

    /// RESET <variable>
    ResetVariable(ResetVariable<T>),
}