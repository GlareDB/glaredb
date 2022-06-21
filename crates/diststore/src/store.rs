use crate::{Result, StoreError};
use coretypes::column::NullableColumnVec;
use coretypes::datatype::{RelationSchema, Row};
use std::collections::BTreeMap;

/// A simple in-memory store.
#[derive(Debug)]
pub struct Store {
    tables: BTreeMap<String, Table>,
}

#[derive(Debug)]
struct Table {
    schema: RelationSchema,
    columns: Vec<NullableColumnVec>,
}

impl Table {
    fn insert(&mut self, row: &Row) -> Result<()> {
        if !row.matches_schema(&self.schema) {
            return Err(StoreError::Internal(format!(
                "invalid row for schema, row: {:?}, schema: {:?}",
                row, self.schema
            )));
        }

        let iter = self.columns.iter_mut().zip(row.iter());
        for (col, val) in iter {
            col.push_value(val)?;
        }

        Ok(())
    }

    // TODO: Actually implement correctly.
    fn scan(&self) -> Vec<NullableColumnVec> {
        self.columns.clone()
    }
}
