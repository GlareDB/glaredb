use crate::catalog::TableSchema;

#[derive(Debug, PartialEq, Eq)]
pub enum DataDefinitionPlan {
    CreateTable(CreateTable),
}

#[derive(Debug, PartialEq, Eq)]
pub struct CreateTable {
    pub schema: TableSchema,
}

impl From<CreateTable> for TableSchema {
    fn from(create_table: CreateTable) -> Self {
        create_table.schema
    }
}
