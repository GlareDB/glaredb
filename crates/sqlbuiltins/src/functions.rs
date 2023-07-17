//! Builtin table returning functions.
//! mod bigquery;
mod bigquery;
mod conversion;
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
use self::generate_series::*;
use self::list_schemas::*;
use self::list_tables::*;
use self::mongo::*;
use self::mysql::*;
use self::object_store::*;
use self::postgres::*;
use self::snowflake::*;
pub(self) use self::utils::*;

pub use self::conversion::*;

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
use datafusion::sql::sqlparser::ast::UnaryOperator;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg as SqlFunctionArg, Value as SqlValue,
};
use datafusion::sql::sqlparser::ast::{FunctionArg, FunctionArgExpr};
use datafusion::{arrow::datatypes::DataType, datasource::TableProvider, scalar::ScalarValue};
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
use metastore_client::types::catalog::{CredentialsEntry, DatabaseEntry};
use metastore_client::types::options::{
    CredentialsOptions, DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsMongo,
    DatabaseOptionsMysql, DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
};
use num_traits::Zero;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::ops::{Add, AddAssign};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Builtin table returning functions available for all sessions.
pub static BUILTIN_TABLE_FUNCS: Lazy<BuiltinTableFuncs> = Lazy::new(BuiltinTableFuncs::new);

#[async_trait]
pub trait TableFunc: Sync + Send {
    /// The name for this table function. This name will be used when looking up
    /// function implementations.
    fn name(&self) -> &str;

    /// A list of function parameters.
    ///
    /// Note that returns a slice to allow for functions that take a variable
    /// number of arguments. For example, a function implementation might allow
    /// 2 or 3 parameters. The same implementation would be able to handle both
    /// of these calls:
    ///
    /// my_func(arg1, arg2)
    /// my_func(arg1, arg2, arg3)
    fn parameters(&self) -> &[TableFuncParameters];

    /// Return a table provider using the provided args.
    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FunctionArg>,
    ) -> Result<Arc<dyn TableProvider>>;

    /// Return a logical plan using the provided args.
    async fn create_logical_plan(
        &self,
        table_ref: OwnedTableReference,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FunctionArg>,
    ) -> Result<LogicalPlan> {
        let provider = self.create_provider(ctx, args).await?;
        let source = Arc::new(DefaultTableSource::new(provider));

        let plan_builder = LogicalPlanBuilder::scan(table_ref, source, None)?;
        let plan = plan_builder.build()?;
        Ok(plan)
    }
}

/// A single parameter for a table function.
#[derive(Debug, Clone)]
pub struct TableFuncParameter {
    pub name: &'static str,
    pub typ: DataType,
}

/// A set of parameters for a table function.
#[derive(Debug, Clone)]
pub struct TableFuncParameters {
    pub params: &'static [TableFuncParameter],
}

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

pub trait TableFuncContextProvider: Sync + Send {
    fn get_database_entry(&self, name: &str) -> Option<&DatabaseEntry>;
    fn get_credentials_entry(&self, name: &str) -> Option<&CredentialsEntry>;
}

fn is_scalar_int(val: &SqlFunctionArg) -> bool {
    match val {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(SqlValue::Number(s, _)))) => {
            !s.contains('.')
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: v,
        })) => {
            if let SqlExpr::Value(SqlValue::Number(s, _)) = v.as_ref() {
                !s.contains('.')
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_scalar_float(val: &SqlFunctionArg) -> bool {
    match val {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(SqlValue::Number(_, _)))) => true,
        FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: v,
        })) => {
            if let SqlExpr::Value(v) = v.as_ref() {
                match v {
                    SqlValue::Number(_, _) => true,
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_scalar_array(val: &FunctionArg) -> bool {
    matches!(
        val,
        FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Array(_)))
    )
}

fn is_ident(val: &FunctionArg) -> bool {
    matches!(
        val,
        FunctionArg::Named { .. }
            | FunctionArg::Unnamed(FunctionArgExpr::Expr(
                SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)
            ))
    )
}
