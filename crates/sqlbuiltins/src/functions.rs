//! Builtin table returning functions.
//! mod bigquery;

mod bigquery;
mod delta;
mod generate_series;
mod mongo;
mod mysql;
mod object_store;
mod postgres;
mod snowflake;
mod virtual_listing;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_ext::functions::TableFunc;
use once_cell::sync::Lazy;

use self::bigquery::ReadBigQuery;
use self::delta::DeltaScan;
use self::generate_series::GenerateSeries;
use self::mongo::ReadMongoDb;
use self::mysql::ReadMysql;
use self::object_store::{CSV_SCAN, JSON_SCAN, PARQUET_SCAN};
use self::postgres::ReadPostgres;
use self::snowflake::ReadSnowflake;
use self::virtual_listing::{ListSchemas, ListTables};

/// Builtin table returning functions available for all sessions.
pub static BUILTIN_TABLE_FUNCS: Lazy<BuiltinTableFuncs> = Lazy::new(BuiltinTableFuncs::new);

/// All builtin table functions.
pub struct BuiltinTableFuncs {
    funcs: HashMap<String, Arc<dyn TableFunc>>,
}

impl BuiltinTableFuncs {
    pub fn new() -> BuiltinTableFuncs {
        let funcs: Vec<Arc<dyn TableFunc>> = vec![
            // Read from table sources
            Arc::new(ReadPostgres),
            Arc::new(ReadBigQuery),
            Arc::new(ReadMongoDb),
            Arc::new(ReadMysql),
            Arc::new(ReadSnowflake),
            Arc::new(PARQUET_SCAN),
            Arc::new(CSV_SCAN),
            Arc::new(JSON_SCAN),
            Arc::new(DeltaScan),
            // Listing
            Arc::new(ListSchemas),
            Arc::new(ListTables),
            // Series generating
            Arc::new(GenerateSeries),
        ];
        let funcs: HashMap<String, Arc<dyn TableFunc>> = funcs
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect();

        BuiltinTableFuncs { funcs }
    }

    pub fn find_function(&self, name: &str) -> Option<Arc<dyn TableFunc>> {
        self.funcs.get(name).cloned()
    }

    pub fn iter_funcs(&self) -> impl Iterator<Item = &Arc<dyn TableFunc>> {
        self.funcs.values()
    }
}

impl Default for BuiltinTableFuncs {
    fn default() -> Self {
        Self::new()
    }
}
