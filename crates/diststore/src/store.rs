use anyhow::{anyhow, Result};
use coretypes::batch::{Batch, BatchRepr, SelectivityBatch};
use coretypes::datatype::{RelationSchema, Row};
use coretypes::expr::ScalarExpr;
use coretypes::vec::ColumnVec;
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

    pub fn insert(&mut self, name: &str, row: &Row) -> Result<()> {
        match self.tables.get_mut(name) {
            Some(table) => table.insert(row),
            None => Err(anyhow!("missing relation: {}", name)),
        }
    }

    pub fn get_table_schema(&self, name: &str) -> Result<Option<RelationSchema>> {
        Ok(match self.tables.get(name) {
            Some(table) => Some(table.schema.clone()),
            None => None,
        })
    }

    pub fn scan(&self, table: &str, filter: Option<ScalarExpr>, limit: usize) -> Result<Batch> {
        match self.tables.get(table) {
            Some(table) => table.scan(filter, limit),
            None => Err(anyhow!("missing relation: {}", table)),
        }
    }
}

#[derive(Debug)]
struct Table {
    schema: RelationSchema,
    columns: Vec<ColumnVec>,
}

impl Table {
    fn new(schema: RelationSchema) -> Table {
        let columns: Vec<_> = schema
            .columns
            .iter()
            .map(|typ| ColumnVec::with_capacity_for_type(DEFAULT_COLUMN_CAP, &typ.datatype))
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
    fn scan(&self, filter: Option<ScalarExpr>, limit: usize) -> Result<Batch> {
        let columns = self.columns.clone();
        let batch: BatchRepr = Batch::from_columns(columns)?.into();

        match filter {
            Some(filter) => {
                let evaled = filter.evaluate(&batch)?;
                let selectivity = evaled
                    .try_get_bool_vec()
                    .ok_or(anyhow!("filter did not produce bool vec"))?;
                let batch = batch.into_shrunk_batch();
                let batch = SelectivityBatch::new_with_bool_vec(batch, selectivity)?;
                Ok(batch.shrink_to_selected())
            }
            None => Ok(batch.into_shrunk_batch()),
        }
    }
}
