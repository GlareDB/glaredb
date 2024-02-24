use crate::functions::table::{BoundTableFunction, TableFunction};
use arrow_schema::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;
use std::collections::HashMap;
use std::sync::Arc;

/// An index in the binding list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BindIdx(pub usize);

/// Represents a binding to some table.
#[derive(Debug, Clone)]
pub struct TableBinding {
    /// Index of the physical table in the bind context.
    pub idx: BindIdx,
    /// Schema of the table.
    pub schema: Arc<Schema>,
}

#[derive(Debug)]
pub struct BindContext {
    /// All tables in the query.
    tables: Vec<Box<dyn BoundTableFunction>>,

    /// Binding of aliases to tables.
    bindings: HashMap<String, TableBinding>,
}

impl BindContext {
    pub fn new() -> Self {
        unimplemented!()
    }

    /// Add a table to the context.
    pub fn add_table(
        &mut self,
        table: Box<dyn BoundTableFunction>,
        alias: String,
    ) -> Result<BindIdx> {
        unimplemented!()
    }

    /// Get a table by its bind index.
    pub fn get_table(&self, idx: BindIdx) -> Option<&dyn BoundTableFunction> {
        self.tables.get(idx.0).map(|t| t.as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct Scope {}
