use crate::catalog::access::AccessMethod;
use crate::catalog::constants::INTERNAL_SCHEMA;
use crate::catalog::system::{columns, schemas, tables, views};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use dfutil::convert::from_df_schema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableEntry {
    pub schema: String,
    pub name: String,
    pub access: AccessMethod,
    pub columns: Vec<ColumnDefinition>,
}

impl TableEntry {
    pub fn generate_defaults() -> impl Iterator<Item = TableEntry> {
        [
            TableEntry {
                schema: INTERNAL_SCHEMA.to_string(),
                name: "views".to_string(),
                access: AccessMethod::InternalMemory,
                columns: ColumnDefinition::from_arrow_schema(&from_df_schema(
                    views::views_arrow_schema(),
                )),
            },
            TableEntry {
                schema: INTERNAL_SCHEMA.to_string(),
                name: "schemas".to_string(),
                access: AccessMethod::InternalMemory,
                columns: ColumnDefinition::from_arrow_schema(&from_df_schema(
                    schemas::schema_arrow_schema(),
                )),
            },
            TableEntry {
                schema: INTERNAL_SCHEMA.to_string(),
                name: "tables".to_string(),
                access: AccessMethod::InternalMemory,
                columns: ColumnDefinition::from_arrow_schema(&from_df_schema(
                    tables::tables_arrow_schema(),
                )),
            },
            TableEntry {
                schema: INTERNAL_SCHEMA.to_string(),
                name: "columns".to_string(),
                access: AccessMethod::InternalMemory,
                columns: ColumnDefinition::from_arrow_schema(&from_df_schema(
                    columns::columns_arrow_schema(),
                )),
            },
        ]
        .into_iter()
    }
}

impl From<&TableEntry> for ArrowSchema {
    fn from(ent: &TableEntry) -> Self {
        ArrowSchema::new(ent.columns.iter().map(|col| col.into()).collect())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
}

impl ColumnDefinition {
    fn from_arrow_schema(schema: &ArrowSchema) -> Vec<ColumnDefinition> {
        schema.fields().iter().map(|field| field.into()).collect()
    }
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
