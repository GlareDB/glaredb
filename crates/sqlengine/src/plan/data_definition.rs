use crate::catalog::TableSchema;

#[derive(Debug, PartialEq)]
pub enum DataDefinitionPlan {
    CreateTable(CreateTable),
}

#[derive(Debug, PartialEq)]
pub struct CreateTable {
    pub schema: TableSchema,
}

impl From<CreateTable> for TableSchema {
    fn from(create_table: CreateTable) -> Self {
        create_table.schema
    }
}
