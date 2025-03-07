use std::fmt;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use crate::arrays::field::Field;
use crate::functions::copy::CopyToFunction;
use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet, TableFunctionSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogEntryType {
    Table,
    Schema,
    View,
    ScalarFunction,
    AggregateFunction,
    TableFunction,
    CopyToFunction,
}

impl fmt::Display for CatalogEntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::Schema => write!(f, "schema"),
            Self::View => write!(f, "view"),
            Self::ScalarFunction => write!(f, "scalar function"),
            Self::AggregateFunction => write!(f, "aggregate function"),
            Self::TableFunction => write!(f, "table function"),
            Self::CopyToFunction => write!(f, "copy to function"),
        }
    }
}

/// Describe a single entry in the catalog.
#[derive(Debug)]
pub struct CatalogEntry {
    pub oid: u32,
    pub name: String,
    pub entry: CatalogEntryInner,
    pub child: Option<Arc<CatalogEntry>>,
}

#[derive(Debug)]
pub enum CatalogEntryInner {
    Table(TableEntry),
    Schema(SchemaEntry),
    View(ViewEntry),
    ScalarFunction(ScalarFunctionEntry),
    AggregateFunction(AggregateFunctionEntry),
    TableFunction(TableFunctionEntry),
    CopyToFunction(CopyToFunctionEntry),
}

#[derive(Debug)]
pub struct ScalarFunctionEntry {
    pub function: ScalarFunctionSet,
}

#[derive(Debug)]
pub struct AggregateFunctionEntry {
    pub function: AggregateFunctionSet,
}

#[derive(Debug)]
pub struct TableFunctionEntry {
    /// The table function.
    pub function: TableFunctionSet,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CopyToFunctionEntry {
    /// COPY TO function implemenation.
    pub function: Box<dyn CopyToFunction>,
    /// The format this COPY TO is for.
    ///
    /// For example, this should be 'parquet' for the parquet COPY TO. This is
    /// looked at when the user includes a `(FORMAT <format>)` option in the
    /// statement.
    pub format: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEntry {
    pub columns: Vec<Field>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewEntry {
    pub column_aliases: Option<Vec<String>>,
    pub query_sql: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SchemaEntry {}

impl CatalogEntry {
    pub fn entry_type(&self) -> CatalogEntryType {
        match &self.entry {
            CatalogEntryInner::Table(_) => CatalogEntryType::Table,
            CatalogEntryInner::Schema(_) => CatalogEntryType::Schema,
            CatalogEntryInner::View(_) => CatalogEntryType::View,
            CatalogEntryInner::ScalarFunction(_) => CatalogEntryType::ScalarFunction,
            CatalogEntryInner::AggregateFunction(_) => CatalogEntryType::AggregateFunction,
            CatalogEntryInner::TableFunction(_) => CatalogEntryType::TableFunction,
            CatalogEntryInner::CopyToFunction(_) => CatalogEntryType::CopyToFunction,
        }
    }

    pub fn try_as_table_entry(&self) -> Result<&TableEntry> {
        match &self.entry {
            CatalogEntryInner::Table(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a table")),
        }
    }

    pub fn try_as_schema_entry(&self) -> Result<&SchemaEntry> {
        match &self.entry {
            CatalogEntryInner::Schema(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a schema")),
        }
    }

    pub fn try_as_scalar_function_entry(&self) -> Result<&ScalarFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::ScalarFunction(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a scalar function")),
        }
    }

    pub fn try_as_aggregate_function_entry(&self) -> Result<&AggregateFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::AggregateFunction(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not an aggregate function")),
        }
    }

    pub fn try_as_table_function_entry(&self) -> Result<&TableFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::TableFunction(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a table function")),
        }
    }

    pub fn try_as_copy_to_function_entry(&self) -> Result<&CopyToFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::CopyToFunction(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a copy to function")),
        }
    }
}
