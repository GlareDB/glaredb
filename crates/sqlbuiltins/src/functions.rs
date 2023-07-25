//! Builtin table returning functions.
//! mod bigquery;
mod bigquery;
mod delta;
mod generate_series;
mod list_schemas;
mod list_tables;
mod mongo;
mod mysql;
mod object_store;
mod postgres;
mod snowflake;
mod utils;

use self::bigquery::*;
use self::delta::*;
use self::generate_series::*;
use self::list_schemas::*;
use self::list_tables::*;
use self::mongo::*;
use self::mysql::*;
use self::object_store::*;
use self::postgres::*;
use self::snowflake::*;
pub(self) use self::utils::*;
pub(self) use datafusion_ext::functions::*;

use crate::errors::{BuiltinError, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::OwnedTableReference;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_ext::functions::TableFunc;

use datafusion::{arrow::datatypes::DataType, datasource::TableProvider};
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasources::common::listing::VirtualLister;
use datasources::common::url::{DatasourceUrl, DatasourceUrlScheme};
use datasources::debug::DebugVirtualLister;
use datasources::mongodb::{MongoAccessor, MongoTableAccessInfo};
use datasources::mysql::{MysqlAccessor, MysqlTableAccess};
use datasources::object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasources::object_store::http::HttpAccessor;
use datasources::object_store::local::{LocalAccessor, LocalTableAccess};
use datasources::object_store::s3::{S3Accessor, S3TableAccess};
use datasources::object_store::{FileType, TableAccessor};
use datasources::postgres::{PostgresAccessor, PostgresTableAccess};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use futures::Stream;
use metastore_client::types::options::{
    CredentialsOptions, DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsMongo,
    DatabaseOptionsMysql, DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
};
use num_traits::Zero;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::ops::{Add, AddAssign};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
