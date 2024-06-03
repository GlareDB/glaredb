use crate::ast::{
    CreateSchema, CreateTable, DropStatement, ExplainNode, Insert, QueryNode, ResetVariable,
    SetVariable, ShowVariable,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Explain(ExplainNode),

    /// SELECT/VALUES
    Query(QueryNode),

    /// CREATE TABLE ...
    CreateTable(CreateTable),

    /// DROP ...
    Drop(DropStatement),

    /// INSERT INTO ...
    Insert(Insert),

    /// CREATE SCHEMA ...
    CreateSchema(CreateSchema),

    /// SET <variable> TO <value>
    SetVariable(SetVariable),

    /// SHOW <variable>
    ShowVariable(ShowVariable),

    /// RESET <variable>
    ResetVariable(ResetVariable),
}
