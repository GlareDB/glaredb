// allow deprecated items
// TODO: fix the deprecation warnings with scalarUDF.
#![allow(deprecated)]

//! Builtin sql objects.
//!
//! This crate provides the implementation of various builtin sql objects
//! (particularly functions). These live outside the `sqlexec` crate to allow
//! it to be imported into both `sqlexec` and `metastore`.

use std::collections::HashMap;
use std::sync::Arc;

use datasources::debug::DebugDatasource;
use datasources::lance::LanceDatasource;


pub mod builtins;
pub mod errors;
pub mod functions;
pub mod validation;

pub static DATASOURCE_REGISTRY: once_cell::sync::Lazy<DatasourceRegistry> =
    once_cell::sync::Lazy::new(|| DatasourceRegistry::new());

pub struct DatasourceRegistry {
    pub(crate) datasources: HashMap<&'static str, Arc<dyn datasources::Datasource>>,
}

impl DatasourceRegistry {
    pub fn new() -> Self {
        let datasources: Vec<Arc<dyn datasources::Datasource>> =
            vec![Arc::new(DebugDatasource), Arc::new(LanceDatasource)];

        let datasources = datasources.into_iter().map(|ds| (ds.name(), ds)).collect();
        DatasourceRegistry { datasources }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn datasources::Datasource>> {
        self.datasources.get(name).cloned()
    }
}
