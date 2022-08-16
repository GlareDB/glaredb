use lemur::repr::value::ValueType;

use crate::catalog::{Column, TableSchema, TableReference};

mod constants;

pub fn system_tables() -> Vec<Box<dyn SystemTable>> {
    vec![
        Box::new(ColumnsTable),
    ]
}

pub trait SystemTable {
    fn name(&self) -> &'static str;

    fn generate_columns(&self) -> Vec<Column>;

    fn generate_table_schema(&self) -> TableSchema;

    fn catalog(&self) -> &'static str {
        constants::SYSTEM_DATABASE
    }

    fn schema(&self) -> &'static str {
        constants::SYSTEM_SCHEMA
    }

    fn generate_table_reference(&self) -> TableReference {
        TableReference {
            catalog: constants::SYSTEM_DATABASE.to_string(),
            schema: constants::SYSTEM_SCHEMA.to_string(),
            table: self.name().to_string(),
        }
    }
}

pub struct ColumnsTable;

impl SystemTable for ColumnsTable {
    fn name(&self) -> &'static str {
        constants::COLUMNS
    }

    fn generate_columns(&self) -> Vec<Column> {
        vec![
            Column {
                name: "table_name".to_string(),
                ty: ValueType::Utf8,
                nullable: false,
            },
            Column {
                name: "table_schema".to_string(),
                ty: ValueType::Binary,
                nullable: false,
            },
        ]
    }

    fn generate_table_schema(&self) -> TableSchema {
        TableSchema {
            name: self.name().to_string(),
            columns: self.generate_columns(),
            pk_idxs: vec![0],
        }
    }
}
