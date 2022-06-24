use coretypes::datatype::RelationSchema;
use sqlparser::ast;
use std::convert::AsRef;
use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("invalid table reference: {0}")]
    InvalidTableReference(String),
    #[error("missing table: {0}")]
    MissingTable(String),
    #[error("invalid schema: columns: {columns:?}, schema: {schema:?}")]
    InvalidColumnsForSchema {
        columns: Vec<String>,
        schema: RelationSchema,
    },
}

/// A fully resolved table reference.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct ResolvedTableReference {
    pub catalog: String,
    pub schema: String,
    pub base: String,
}

impl fmt::Display for ResolvedTableReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.base)
    }
}

/// Reference to a table.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum TableReference {
    /// Only the base name of the table.
    ///
    /// "table"
    Unqualified { base: String },
    /// A qualified table name with the schema.
    ///
    /// "schema.table"
    Qualified { schema: String, base: String },
    /// A fully resolved table name with the catalog, schema, and base name.
    ///
    /// "catalog.schema.table"
    Full {
        catalog: String,
        schema: String,
        base: String,
    },
}

impl TableReference {
    pub fn new_unqualified(base: String) -> Self {
        TableReference::Unqualified { base }
    }

    pub fn resolve_with_defaults(self, catalog: &str, schema: &str) -> ResolvedTableReference {
        match self {
            Self::Unqualified { base } => ResolvedTableReference {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                base,
            },
            Self::Qualified { schema, base } => ResolvedTableReference {
                catalog: catalog.to_string(),
                schema,
                base,
            },
            Self::Full {
                catalog,
                schema,
                base,
            } => ResolvedTableReference {
                catalog,
                schema,
                base,
            },
        }
    }
}

impl fmt::Display for TableReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unqualified { base } => write!(f, "{}", base),
            Self::Qualified { schema, base } => write!(f, "{}.{}", schema, base),
            Self::Full {
                catalog,
                schema,
                base,
            } => write!(f, "{}.{}.{}", catalog, schema, base),
        }
    }
}

impl TryFrom<ast::ObjectName> for TableReference {
    type Error = CatalogError;

    fn try_from(value: ast::ObjectName) -> Result<Self, Self::Error> {
        if value.0.len() == 0 || value.0.len() > 3 {
            return Err(CatalogError::InvalidTableReference(value.to_string()));
        }
        let mut iter = value.0.into_iter();
        let len = iter.len();
        let mut next = || iter.next().unwrap().value;
        match len {
            1 => Ok(TableReference::Unqualified { base: next() }),
            2 => Ok(TableReference::Qualified {
                schema: next(),
                base: next(),
            }),
            3 => Ok(TableReference::Full {
                catalog: next(),
                schema: next(),
                base: next(),
            }),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    reference: ResolvedTableReference,
    columns: Vec<String>,
    schema: RelationSchema,
}

impl TableSchema {
    /// Create a new table schema, validating that the number of column names
    /// lines up with the number of columns in the relation schema.
    pub fn new(
        reference: ResolvedTableReference,
        columns: Vec<String>,
        schema: RelationSchema,
    ) -> Result<Self, CatalogError> {
        if columns.len() != schema.columns.len() {
            return Err(CatalogError::InvalidColumnsForSchema { columns, schema });
        }
        Ok(TableSchema {
            reference,
            columns,
            schema,
        })
    }

    pub fn get_reference(&self) -> &ResolvedTableReference {
        &self.reference
    }

    pub fn get_columns(&self) -> &[String] {
        &self.columns
    }

    pub fn get_schema(&self) -> &RelationSchema {
        &self.schema
    }
}

pub trait Catalog {
    /// Return the schema for the specified table.
    fn table_schema(&self, tbl: &TableReference) -> Result<TableSchema, CatalogError>;
}
