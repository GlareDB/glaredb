use crate::errors::Result;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntry {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

impl From<&TableEntry> for ArrowSchema {
    fn from(ent: &TableEntry) -> Self {
        ArrowSchema::new(ent.columns.iter().map(|col| col.into()).collect())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
}

impl From<&Field> for ColumnDefinition {
    fn from(field: &Field) -> Self {
        ColumnDefinition {
            name: field.name().clone(),
            datatype: field.data_type().clone(),
            nullable: field.is_nullable(),
        }
    }
}

impl From<&ColumnDefinition> for Field {
    fn from(col: &ColumnDefinition) -> Self {
        Field::new(&col.name, col.datatype.clone(), col.nullable)
    }
}
