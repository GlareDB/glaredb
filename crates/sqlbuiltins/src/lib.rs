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
use datasources::Datasource;
use once_cell::sync::Lazy;


pub mod builtins;
pub mod errors;
pub mod functions;
pub mod validation;


/// `DEFAULT_DATASOURCES` provides all implementations of [`Datasource`]
/// These are datasources that are globally available to all sessions.
/// TODO: Eventually, we will want to make this runtime configurable.
/// For now, we just hardcode the datasources.
pub static DEFAULT_DATASOURCES: Lazy<DatasourceRegistry> = Lazy::new(DatasourceRegistry::new);

pub struct DatasourceRegistry {
    datasources: HashMap<&'static str, Arc<dyn Datasource>>,
}

impl DatasourceRegistry {
    pub fn new() -> Self {
        let datasources: Vec<Arc<dyn Datasource>> =
            vec![Arc::new(DebugDatasource), Arc::new(LanceDatasource)];

        let datasources = datasources.into_iter().map(|ds| (ds.name(), ds)).collect();
        DatasourceRegistry { datasources }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Datasource>> {
        self.datasources.get(name).cloned()
    }
}
