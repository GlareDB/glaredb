//! System level catalog and table definitions.
use crate::catalog::{ResolvedTableReference, TableReference, TableSchema};
use coretypes::datatype::{DataType, NullableType, RelationSchema};

const SYSTEM_DATABASE: &str = "system";
const SYSTEM_SCHEMA: &str = "gl_internal";

pub fn system_tables() -> Vec<Box<dyn SystemTable>> {
    vec![Box::new(Attributes), Box::new(Dummy)]
}

pub trait SystemTable {
    fn name(&self) -> &'static str;

    fn generate_relation_schema(&self) -> RelationSchema;

    fn generate_columns(&self) -> Vec<String>;

    fn generate_table_schema(&self) -> TableSchema {
        TableSchema::new(
            self.resolved_reference(),
            self.generate_columns(),
            self.generate_relation_schema(),
        )
        .unwrap()
    }

    fn resolved_reference(&self) -> ResolvedTableReference {
        ResolvedTableReference {
            catalog: SYSTEM_DATABASE.to_string(),
            schema: SYSTEM_SCHEMA.to_string(),
            base: self.name().to_string(),
        }
    }
}

pub struct Attributes;

impl SystemTable for Attributes {
    fn name(&self) -> &'static str {
        "gl_attributes"
    }

    fn generate_relation_schema(&self) -> RelationSchema {
        let cols = vec![
            NullableType::new_nonnullable(DataType::Utf8),
            NullableType::new_nonnullable(DataType::Utf8),
            NullableType::new_nonnullable(DataType::Utf8),
            NullableType::new_nonnullable(DataType::Utf8),
            NullableType::new_nonnullable(DataType::Int8),
        ];
        RelationSchema::new(cols)
    }

    fn generate_columns(&self) -> Vec<String> {
        let cols = vec![
            "table_database",
            "table_schema",
            "table_name",
            "column_name",
            "builtin_type",
        ];
        cols.into_iter().map(|c| c.to_string()).collect()
    }
}

pub struct Dummy;

impl SystemTable for Dummy {
    fn name(&self) -> &'static str {
        "gl_dummy"
    }

    fn generate_relation_schema(&self) -> RelationSchema {
        let cols = vec![NullableType::new_nullable(DataType::Bool)];
        RelationSchema::new(cols)
    }

    fn generate_columns(&self) -> Vec<String> {
        vec!["dummy".to_string()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_schemas_no_panic() {
        let tables = system_tables();
        let _: Vec<_> = tables
            .iter()
            .map(|table| table.generate_table_schema())
            .collect();
    }
}
