use futures::future::BoxFuture;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;

use crate::database::catalog::{Catalog, CatalogTx};
use crate::database::ddl::CatalogModifier;
use crate::database::entry::{CatalogEntry, FunctionEntry, FunctionImpl, TableEntry};
use crate::database::table::DataTable;
use crate::datasource::DataSourceRegistry;
use crate::functions::aggregate::{AggregateFunction, BUILTIN_AGGREGATE_FUNCTIONS};
use crate::functions::scalar::{ScalarFunction, BUILTIN_SCALAR_FUNCTIONS};
use crate::functions::table::{TableFunction, BUILTIN_TABLE_FUNCTIONS};

/// Read-only system catalog that cannot be modified once constructed.
#[derive(Debug)]
pub struct SystemCatalog {
    // TODO: Wrap these two things in a reusable type.
    entries: Vec<CatalogEntry>,
    schemas: HashMap<&'static str, HashMap<&'static str, usize>>,
}

impl SystemCatalog {
    pub fn new(registry: &DataSourceRegistry) -> Self {
        let mut entries = Vec::new();
        let mut glare_catalog = HashMap::new();

        // Add builtin scalars.
        for func in BUILTIN_SCALAR_FUNCTIONS.iter() {
            let ent = CatalogEntry::Function(FunctionEntry {
                name: func.name().to_string(),
                implementation: FunctionImpl::Scalar(func.clone()),
            });
            let idx = entries.len();
            entries.push(ent);

            glare_catalog.insert(func.name(), idx);

            for alias in func.aliases() {
                glare_catalog.insert(alias, idx);
            }
        }

        // Add builtin aggregates.
        for func in BUILTIN_AGGREGATE_FUNCTIONS.iter() {
            let ent = CatalogEntry::Function(FunctionEntry {
                name: func.name().to_string(),
                implementation: FunctionImpl::Aggregate(func.clone()),
            });
            let idx = entries.len();
            entries.push(ent);

            glare_catalog.insert(func.name(), idx);

            for alias in func.aliases() {
                glare_catalog.insert(alias, idx);
            }
        }

        // Add builtin table functions.
        for func in BUILTIN_TABLE_FUNCTIONS.iter() {
            let ent = CatalogEntry::Function(FunctionEntry {
                name: func.name().to_string(),
                implementation: FunctionImpl::Table(func.clone()),
            });
            let idx = entries.len();
            entries.push(ent);

            glare_catalog.insert(func.name(), idx);

            for alias in func.aliases() {
                glare_catalog.insert(alias, idx);
            }
        }

        // Add table functions from registered data sources.
        for datasource in registry.iter() {
            let funcs = datasource.initialize_table_functions();
            for func in funcs {
                let ent = CatalogEntry::Function(FunctionEntry {
                    name: func.name().to_string(),
                    implementation: FunctionImpl::Table(func.clone()),
                });
                let idx = entries.len();
                entries.push(ent);

                glare_catalog.insert(func.name(), idx);

                for alias in func.aliases() {
                    glare_catalog.insert(alias, idx);
                }
            }
        }

        let schemas: HashMap<_, _> = [
            ("glare_catalog", glare_catalog),
            ("pg_catalog", HashMap::new()),
            ("information_schema", HashMap::new()),
        ]
        .into_iter()
        .collect();

        SystemCatalog { entries, schemas }
    }

    fn get_scalar_fn_inner(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn ScalarFunction>>> {
        let schema = self
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;
        let idx = match schema.get(name) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        match self.entries.get(*idx) {
            Some(CatalogEntry::Function(ent)) => match &ent.implementation {
                FunctionImpl::Scalar(scalar) => Ok(Some(scalar.clone())),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    fn get_aggregate_fn_inner(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn AggregateFunction>>> {
        let schema = self
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;
        let idx = match schema.get(name) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        match self.entries.get(*idx) {
            Some(CatalogEntry::Function(ent)) => match &ent.implementation {
                FunctionImpl::Aggregate(agg) => Ok(Some(agg.clone())),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    fn get_table_fn_inner(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn TableFunction>>> {
        let schema = self
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;
        let idx = match schema.get(name) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        match self.entries.get(*idx) {
            Some(CatalogEntry::Function(ent)) => match &ent.implementation {
                FunctionImpl::Table(table) => Ok(Some(table.clone())),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

impl Catalog for SystemCatalog {
    fn get_table_entry(
        &self,
        _tx: &CatalogTx,
        _schema: &str,
        _name: &str,
    ) -> BoxFuture<Result<Option<TableEntry>>> {
        // TODO: It will at some point (and views).
        Box::pin(async {
            Err(RayexecError::new(
                "System catalog contains no table entries",
            ))
        })
    }

    fn get_scalar_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn ScalarFunction>>> {
        self.get_scalar_fn_inner(tx, schema, name)
    }

    fn get_aggregate_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn AggregateFunction>>> {
        self.get_aggregate_fn_inner(tx, schema, name)
    }

    fn get_table_fn(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn TableFunction>>> {
        self.get_table_fn_inner(tx, schema, name)
    }

    fn data_table(
        &self,
        _tx: &CatalogTx,
        _schema: &str,
        _ent: &TableEntry,
    ) -> Result<Box<dyn DataTable>> {
        Err(RayexecError::new(
            "System catalog contains no table entries",
        ))
    }

    fn catalog_modifier(&self, _tx: &CatalogTx) -> Result<Box<dyn CatalogModifier>> {
        Err(RayexecError::new("Cannot modify the system catalog"))
    }

    fn entries(&self) -> Option<Vec<CatalogEntry>> {
        Some(self.entries.clone())
    }
}
