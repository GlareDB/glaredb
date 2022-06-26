use coretypes::batch::Batch;
use anyhow::{Result, anyhow};
use coretypes::column::NullableColumnVec;
use coretypes::datatype::{RelationSchema, Row};
use std::collections::{btree_map::Entry, BTreeMap};

const DEFAULT_COLUMN_CAP: usize = 256;

/// A simple in-memory store.
#[derive(Debug)]
pub struct Store {
    tables: BTreeMap<String, Table>,
}

impl Store {
    pub fn new() -> Store {
        Store {
            tables: BTreeMap::new(),
        }
    }

    pub fn create_relation(&mut self, name: &str, schema: RelationSchema) -> Result<()> {
        match self.tables.entry(name.to_string()) {
            Entry::Occupied(_) => Err(anyhow!("duplicate relation name: {}", name)),
            Entry::Vacant(entry) => {
                let table = Table::new(schema);
                entry.insert(table);
                Ok(())
            }
        }
    }

    pub fn delete_relation(&mut self, name: &str) -> Result<()> {
        match self.tables.remove(name) {
            Some(_) => Ok(()),
            None => Err(anyhow!("missing relation: {}", name)),
        }
    }

    pub fn insert(&mut self, table: &str, row: &Row) -> Result<()> {
        match self.tables.get_mut(table) {
            Some(table) => table.insert(row),
            None => Err(anyhow!("missing relation: {}", table)),
        }
    }

    pub fn scan(&self, table: &str) -> Result<Batch> {
        match self.tables.get(table) {
            Some(table) => table.scan(),
            None => Err(anyhow!("missing relation: {}", table)),
        }
    }
}

#[derive(Debug)]
struct Table {
    schema: RelationSchema,
    columns: Vec<NullableColumnVec>,
}

impl Table {
    fn new(schema: RelationSchema) -> Table {
        let columns: Vec<_> = schema
            .columns
            .iter()
            .map(|typ| NullableColumnVec::with_capacity(DEFAULT_COLUMN_CAP, &typ.datatype))
            .collect();
        Table { schema, columns }
    }

    fn insert(&mut self, row: &Row) -> Result<()> {
        if !row.matches_schema(&self.schema) {
            return Err(anyhow!(
                "invalid row for schema, row: {:?}, schema: {:?}",
                row,
                self.schema
            ));
        }

        let iter = self.columns.iter_mut().zip(row.iter());
        for (col, val) in iter {
            col.push_value(val)?;
        }

        Ok(())
    }

    // TODO: Actually implement correctly.
    fn scan(&self) -> Result<Batch> {
        let columns = self.columns.clone();
        let batch = Batch::from_columns(columns)?;
        Ok(batch)
    }
}
