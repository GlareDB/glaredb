use anyhow::Result;
use lemur::repr::value::ValueType;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

use super::{Column, CatalogReader, TableReference, TableSchema};

/// A dummy catalog that always returns the same schema.
///
/// This is useful for testing.
/// Two tables are defined:
/// t1: (a, b)
/// t2: (b, c)
pub struct DummyCatalog {
    tables: Arc<RwLock<BTreeMap<TableReference, TableSchema>>>,
}

impl DummyCatalog {
    pub fn new() -> DummyCatalog {
        let t1 = TableSchema {
            name: "t1".to_string(),
            columns: vec![
                Column {
                    name: "a".to_string(),
                    ty: ValueType::Int8,
                    nullable: false,
                },
                Column {
                    name: "b".to_string(),
                    ty: ValueType::Int8,
                    nullable: false,
                },
            ],
            pk_idxs: Vec::new(),
        };
        let t2 = TableSchema {
            name: "t2".to_string(),
            columns: vec![
                Column {
                    name: "b".to_string(),
                    ty: ValueType::Int8,
                    nullable: false,
                },
                Column {
                    name: "c".to_string(),
                    ty: ValueType::Int8,
                    nullable: false,
                },
            ],
            pk_idxs: Vec::new(),
        };
        let mut tables = BTreeMap::new();
        tables.insert(
            TableReference {
                catalog: "dummy_catalog".to_string(),
                schema: "schema".to_string(),
                table: "t1".to_string(),
            },
            t1,
        );
        tables.insert(
            TableReference {
                catalog: "dummy_catalog".to_string(),
                schema: "schema".to_string(),
                table: "t2".to_string(),
            },
            t2,
        );
        DummyCatalog {
            tables: Arc::new(RwLock::new(tables)),
        }
    }
}

impl Default for DummyCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogReader for DummyCatalog {
    fn get_table(&self, reference: &TableReference) -> Result<Option<TableSchema>> {
        let tables = self.tables.read();
        Ok(tables.get(reference).cloned())
    }

    fn get_table_by_name(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<(TableReference, TableSchema)>> {
        if catalog != "dummy_catalog" || schema != "schema" {
            return Ok(None);
        }
        let tables = self.tables.read();
        for (reference, table) in tables.iter() {
            if &table.name == name {
                return Ok(Some((reference.clone(), table.clone())));
            }
        }
        Ok(None)
    }

    fn current_catalog(&self) -> &str {
        "dummy_catalog"
    }

    fn current_schema(&self) -> &str {
        "schema"
    }
}
