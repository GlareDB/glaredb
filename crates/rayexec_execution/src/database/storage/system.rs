use once_cell::sync::Lazy;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;

use crate::database::catalog::{Catalog, CatalogTx};
use crate::database::ddl::CatalogModifier;
use crate::database::entry::{CatalogEntry, FunctionEntry, FunctionImpl, TableEntry};
use crate::database::table::DataTable;
use crate::functions::aggregate::{GenericAggregateFunction, BUILTIN_AGGREGATE_FUNCTIONS};
use crate::functions::scalar::{GenericScalarFunction, BUILTIN_SCALAR_FUNCTIONS};

pub static GLOBAL_SYSTEM_CATALOG: Lazy<SystemCatalog> = Lazy::new(SystemCatalog::new);

/// Read-only system catalog that cannot be modified once constructed.
#[derive(Debug)]
pub struct SystemCatalog {
    schemas: HashMap<&'static str, HashMap<&'static str, CatalogEntry>>,
}

impl SystemCatalog {
    pub fn new() -> Self {
        let mut glare_catalog = HashMap::new();

        // Add builtin scalars.
        for func in BUILTIN_SCALAR_FUNCTIONS.iter() {
            let ent = CatalogEntry::Function(FunctionEntry {
                name: func.name().to_string(),
                implementation: FunctionImpl::Scalar(func.clone()),
            });
            glare_catalog.insert(func.name(), ent);

            for alias in func.aliases() {
                let ent = CatalogEntry::Function(FunctionEntry {
                    name: alias.to_string(),
                    implementation: FunctionImpl::Scalar(func.clone()),
                });
                glare_catalog.insert(alias, ent);
            }
        }

        // Add builtin aggregates.
        for func in BUILTIN_AGGREGATE_FUNCTIONS.iter() {
            let ent = CatalogEntry::Function(FunctionEntry {
                name: func.name().to_string(),
                implementation: FunctionImpl::Aggregate(func.clone()),
            });
            glare_catalog.insert(func.name(), ent);

            for alias in func.aliases() {
                let ent = CatalogEntry::Function(FunctionEntry {
                    name: alias.to_string(),
                    implementation: FunctionImpl::Aggregate(func.clone()),
                });
                glare_catalog.insert(alias, ent);
            }
        }

        let schemas: HashMap<_, _> = [
            ("glare_catalog", glare_catalog),
            ("pg_catalog", HashMap::new()),
            ("information_schema", HashMap::new()),
        ]
        .into_iter()
        .collect();

        SystemCatalog { schemas }
    }
}

impl Catalog for SystemCatalog {
    fn get_table_entry(
        &self,
        _tx: &CatalogTx,
        _schema: &str,
        _name: &str,
    ) -> Result<Option<TableEntry>> {
        // TODO: It will at some point (and views).
        Err(RayexecError::new(
            "System catalog contains no table entries",
        ))
    }

    fn get_scalar_fn(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn GenericScalarFunction>>> {
        let schema = self
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;
        match schema.get(name) {
            Some(CatalogEntry::Function(ent)) => match &ent.implementation {
                FunctionImpl::Scalar(scalar) => Ok(Some(scalar.clone())),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    fn get_aggregate_fn(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<Box<dyn GenericAggregateFunction>>> {
        let schema = self
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;
        match schema.get(name) {
            Some(CatalogEntry::Function(ent)) => match &ent.implementation {
                FunctionImpl::Aggregate(agg) => Ok(Some(agg.clone())),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
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
}

impl Default for SystemCatalog {
    fn default() -> Self {
        Self::new()
    }
}
