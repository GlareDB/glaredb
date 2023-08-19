use crate::background_jobs::storage::{BackgroundJobDeleteTable, BackgroundJobStorageTracker};
use crate::background_jobs::{BgJob, JobRunner};
use crate::environment::EnvironmentReader;
use crate::errors::{internal, ExecError, Result};
use crate::extension_codec::GlareDBExtensionCodec;
use crate::metastore::catalog::SessionCatalog;
use crate::metrics::SessionMetrics;
use crate::parser::{CustomParser, StatementWithExtensions};
use crate::planner::errors::PlanError;
use crate::planner::logical_plan::*;
use crate::planner::session_planner::SessionPlanner;
use crate::remote::client::RemoteSessionClient;
use crate::remote::planner::RemoteLogicalPlanner;
use crate::remote::staged_stream::StagedClientStreams;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column as DfColumn, SchemaReference};
use datafusion::config::{CatalogOptions, ConfigOptions, Extensions, OptimizerOptions};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::context::{
    SessionConfig, SessionContext as DfSessionContext, SessionState, TaskContext,
};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::{Expr as DfExpr, LogicalPlanBuilder as DfLogicalPlanBuilder};
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, execute_stream, ExecutionPlan,
};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use datasources::object_store::init_session_registry;
use futures::{future::BoxFuture, StreamExt};
use pgrepr::format::Format;
use pgrepr::types::arrow_to_pg_type;
use protogen::metastore::types::catalog::{CatalogEntry, EntryType};
use protogen::metastore::types::options::TableOptions;
use protogen::metastore::types::service::{self, Mutation};
use sqlbuiltins::builtins::{CURRENT_SESSION_SCHEMA, DEFAULT_CATALOG};
use std::collections::HashMap;
use std::path::PathBuf;
use std::slice;
use std::sync::Arc;
use tokio_postgres::types::Type as PgType;
use tracing::info;

/// A remote context configured exclusively to execute physical plans.
pub struct RemoteSessionContext {
    /// Native tables.
    tables: NativeTableStorage,
    /// Datafusion session context used for planning and execution.
    df_ctx: DfSessionContext,
    /// Job runner for background jobs.
    background_jobs: JobRunner,
}
impl RemoteSessionContext {}
