use crate::catalog::{ResolvedTableReference, TableReference, TableSchema};
use anyhow::{anyhow, Result};
use coretypes::datatype::{DataType, DataValue, NullableType, RelationSchema};
use coretypes::expr::ScalarExpr;
use sqlparser::ast;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum TableAliasOrReference {
    /// A user provided alias for the table. Once a table is aliased, the table
    /// can only be reference by that alias.
    Alias(String),
    /// A table reference, e.g. "my_table" or "schema.my_table".
    Reference(TableReference),
}

impl TableAliasOrReference {
    /// Check if this table matches a compound identifier for a column.
    fn matches_col_compound_ident(&self, idents: &[ast::Ident]) -> bool {
        // Trivially matches any table.
        if idents.len() <= 1 {
            return true;
        }

        // Check "my_alias.column" and "base.column".
        if idents.len() == 2 {
            let matches = match self {
                Self::Alias(alias) => &idents[0].value == alias,
                Self::Reference(reference) => &idents[0].value == reference.base(),
            };
            if matches {
                return true;
            }
        }

        // Check "schema.base.column".
        if let Self::Reference(reference) = self {
            reference
                .schema()
                .map(|schema| schema == &idents[0].value)
                .unwrap_or(false)
                && reference.base() == &idents[1].value
        } else {
            false
        }
    }
}

impl fmt::Display for TableAliasOrReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableAliasOrReference::Alias(alias) => write!(f, "{} (alias)", alias),
            TableAliasOrReference::Reference(resolved) => write!(f, "{} (resolved)", resolved),
        }
    }
}

/// Track scope during SQL query planning.
#[derive(Debug, Clone)]
pub struct Scope {
    /// Maps table aliases to a schema.
    tables: HashMap<TableAliasOrReference, usize>,
    /// All table schemas within the scope.
    schemas: Vec<TableSchema>,
}

impl Scope {
    /// Create a new empty scope.
    pub fn new() -> Scope {
        Scope {
            tables: HashMap::new(),
            schemas: Vec::new(),
        }
    }

    /// Add a table with the given alias or reference to the scope.
    pub fn add_table(&mut self, alias: TableAliasOrReference, schema: TableSchema) -> Result<()> {
        match self.tables.entry(alias) {
            Entry::Occupied(ent) => Err(anyhow!("duplicate table reference: {}", ent.key())),
            Entry::Vacant(ent) => {
                let idx = self.schemas.len();
                self.schemas.push(schema);
                ent.insert(idx);
                Ok(())
            }
        }
    }

    /// Append another scope to the right of this one. Errors if there's any
    /// duplicate table references.
    pub fn append(&mut self, mut other: Scope) -> Result<()> {
        let diff = self.schemas.len();
        self.schemas.append(&mut other.schemas);
        for (alias, idx) in other.tables.into_iter() {
            match self.tables.entry(alias) {
                Entry::Occupied(ent) => {
                    return Err(anyhow!("duplicate table reference: {}", ent.key()))
                }
                Entry::Vacant(ent) => ent.insert(idx + diff),
            };
        }

        Ok(())
    }

    /// Resolve an unqualified column name.
    pub fn resolve_unqualified_column(&self, name: &str) -> Option<ScalarExpr> {
        // TODO: Resolve more efficiently. Also need to make sure that column isn't ambiguous.
        let (idx, _) = self
            .schemas
            .iter()
            .map(|tbl| tbl.columns.iter())
            .flatten()
            .enumerate()
            .find(|(_, col)| *col == name)?;

        Some(ScalarExpr::Column(idx))
    }

    pub fn resolve_qualified_column(&self, idents: &[ast::Ident]) -> Option<ScalarExpr> {
        if idents.len() == 0 {
            return None;
        }
        let (_, idx) = self
            .tables
            .iter()
            .find(|(tbl, _)| tbl.matches_col_compound_ident(idents))?;
        let last = &idents[idents.len() - 1].value;
        let (idx, _) = self
            .schemas
            .get(*idx)?
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| *col == last)?;

        Some(ScalarExpr::Column(idx))
    }

    pub fn resolve_wildcard(&self) -> Vec<ScalarExpr> {
        let num_cols = self
            .schemas
            .iter()
            .flat_map(|t| t.schema.columns.iter())
            .count();

        let mut exprs = Vec::with_capacity(num_cols);
        for idx in 0..num_cols {
            exprs.push(ScalarExpr::Column(idx));
        }

        exprs
    }

    /// Project the current schema.
    pub fn project_schema(&self) -> RelationSchema {
        self.schemas
            .iter()
            .map(|tbl| tbl.schema.columns.iter())
            .flatten()
            .cloned()
            .collect::<Vec<_>>()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableReference;

    fn create_table(name: &str, cols: Vec<(&str, DataType)>) -> TableSchema {
        let resolved = ResolvedTableReference {
            catalog: "db".to_string(),
            schema: "test".to_string(),
            base: name.to_string(),
        };

        let mut names = Vec::new();
        let mut types = Vec::new();
        for col in cols.into_iter() {
            names.push(col.0.to_string());
            types.push(NullableType::new_nullable(col.1));
        }

        let schema = RelationSchema::new(types);
        TableSchema::new(resolved, names, schema).unwrap()
    }

    #[test]
    fn resolve_unqualified_column() {
        let tables = vec![
            create_table(
                "t1",
                vec![
                    ("a", DataType::Int8),
                    ("b", DataType::Int8),
                    ("c", DataType::Int8),
                ],
            ),
            create_table(
                "t2",
                vec![
                    ("d", DataType::Int8),
                    ("e", DataType::Int8),
                    ("f", DataType::Int8),
                ],
            ),
        ];

        let mut scope = Scope::new();
        for table in tables.into_iter() {
            let alias = TableAliasOrReference::Reference(TableReference::new_unqualified(
                table.reference.base.clone(),
            ));
            scope.add_table(alias, table).unwrap();
        }

        let expr = scope.resolve_unqualified_column("b").unwrap();
        assert_eq!(ScalarExpr::Column(1), expr);
        let expr = scope.resolve_unqualified_column("f").unwrap();
        assert_eq!(ScalarExpr::Column(5), expr);
    }

    #[test]
    fn match_table_references() {
        #[derive(Debug)]
        struct TestCase {
            table: TableAliasOrReference,
            ident: &'static [&'static str],
            matches: bool,
        }

        let tests = vec![
            TestCase {
                table: TableAliasOrReference::Reference(TableReference::new_unqualified(
                    "table".to_string(),
                )),
                ident: &["table", "col"],
                matches: true,
            },
            TestCase {
                table: TableAliasOrReference::Reference(TableReference::new_unqualified(
                    "table".to_string(),
                )),
                ident: &["nope", "col"],
                matches: false,
            },
            TestCase {
                table: TableAliasOrReference::Reference(TableReference::new_unqualified(
                    "table".to_string(),
                )),
                ident: &["trivial"],
                matches: true,
            },
            TestCase {
                table: TableAliasOrReference::Alias("my_alias".to_string()),
                ident: &["my_alias", "col"],
                matches: true,
            },
            TestCase {
                table: TableAliasOrReference::Alias("my_alias".to_string()),
                ident: &["nope", "col"],
                matches: false,
            },
        ];

        for test in tests.into_iter() {
            let compound: Vec<_> = test
                .ident
                .iter()
                .map(|ident| ast::Ident::new(ident.to_string()))
                .collect();

            let out = test.table.matches_col_compound_ident(&compound);
            assert_eq!(test.matches, out, "test case: {:?}", test);
        }
    }
}
