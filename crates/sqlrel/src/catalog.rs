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
#[derive(Debug, Hash, PartialEq, Eq)]
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

impl From<ResolvedTableReference> for TableReference {
    fn from(value: ResolvedTableReference) -> Self {
        TableReference::Full {
            catalog: value.catalog,
            schema: value.schema,
            base: value.base,
        }
    }
}

pub trait Catalog {
    /// Resolve a table reference to its fully qualified name.
    fn resolve_table(&self, tbl: TableReference) -> Result<ResolvedTableReference, CatalogError>;

    /// Return the schema for the specified table.
    fn table_schema(&self, tbl: &ResolvedTableReference) -> Result<RelationSchema, CatalogError>;
}
