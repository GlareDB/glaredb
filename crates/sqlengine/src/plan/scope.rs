use crate::catalog::TableSchema;

use anyhow::{anyhow, Result};
use std::collections::{hash_map::Entry, HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Scope {
    /// If true, the scope is constant and cannot contain any variables.
    constant: bool,
    /// Currently visible tables, by query name (i.e. alias or actual name).
    tables: HashMap<String, TableSchema>,
    /// Column labels, if any (qualified by table name when available)
    columns: Vec<(Option<String>, Option<String>)>,
    /// Qualified names to column indexes.
    qualified: HashMap<(String, String), usize>,
    /// Unqualified names to column indexes, if unique.
    unqualified: HashMap<String, usize>,
    /// Unqialified ambiguous names.
    ambiguous: HashSet<String>,
}

impl Scope {
    pub fn empty() -> Self {
        Self {
            constant: false,
            tables: HashMap::new(),
            columns: Vec::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ambiguous: HashSet::new(),
        }
    }

    pub fn add_table(&mut self, alias: Option<String>, table: TableSchema) -> Result<()> {
        let label = match alias {
            Some(alias) => alias,
            None => table.name.clone(),
        };
        if self.tables.contains_key(&label) {
            return Err(anyhow!("duplicate table name: {}", label));
        }
        for column in table.columns.iter() {
            self.add_column(Some(label.clone()), Some(column.name.clone()));
        }
        self.tables.insert(label, table);
        Ok(())
    }

    pub fn add_column(&mut self, table: Option<String>, label: Option<String>) {
        if let Some(l) = label.clone() {
            if let Some(t) = table.clone() {
                self.qualified.insert((t, l.clone()), self.columns.len());
            }
            if !self.ambiguous.contains(&l) {
                if let Entry::Vacant(e) = self.unqualified.entry(l.clone()) {
                    e.insert(self.columns.len());
                } else {
                    self.unqualified.remove(&l);
                    self.ambiguous.insert(l);
                }
            }
        }
        self.columns.push((table, label));
    }

    pub fn get_column(&self, idx: usize) -> Result<(Option<&String>, Option<&String>)> {
        self.columns
            .get(idx)
            .map(|(table, label)| (table.as_ref(), label.as_ref()))
            .ok_or_else(|| anyhow!("column not found for idx: {}", idx))
    }

    pub fn merge(&mut self, other: Scope) -> Result<()> {
        for (label, table) in other.tables {
            match self.tables.entry(label) {
                Entry::Occupied(entry) => {
                    return Err(anyhow!("duplicate table name from merge: {}", entry.key()))
                }
                Entry::Vacant(entry) => entry.insert(table),
            };
        }
        for (table, label) in other.columns {
            self.add_column(table, label);
        }
        Ok(())
    }

    pub fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        if let Some(table) = table {
            if !self.tables.contains_key(table) {
                return Err(anyhow!("missing table: {}", table));
            }
            self.qualified
                .get(&(table.into(), name.into()))
                .cloned()
                .ok_or_else(|| anyhow!("missing column: {}.{}", table, name))
        } else if self.ambiguous.contains(name) {
            Err(anyhow!("ambiguous column name: {}", name))
        } else {
            self.unqualified
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow!("missing column: {}", name))
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
}
