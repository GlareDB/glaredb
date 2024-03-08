//! Various table dispatchers.
pub mod external;
pub mod system;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use catalog::session_catalog::SessionCatalog;
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{Column, Expr, SessionContext as DfSessionContext};
use datafusion_ext::functions::{DefaultTableContextProvider, FuncParamValue};
use datasources::native::access::NativeTableStorage;
use parser::CustomParser;
use protogen::metastore::types::catalog::{DatabaseEntry, FunctionEntry, TableEntry, ViewEntry};
use sqlbuiltins::functions::FunctionRegistry;

use self::external::ExternalDispatcher;
use crate::context::local::LocalSessionContext;
use crate::dispatch::system::SystemTableDispatcher;
use crate::planner::errors::PlanError;
use crate::planner::session_planner::SessionPlanner;

type Result<T, E = DispatchError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("Missing database: {database}")]
    MissingDatabase { database: String },

    #[error("Missing catalog entry; schema: {schema}, name: {name}")]
    MissingEntry { schema: String, name: String },

    #[error("Missing builtin table; schema: {schema}, name: {name}")]
    MissingBuiltinTable { schema: String, name: String },

    #[error("Missing temp table: {name}")]
    MissingTempTable { name: String },

    #[error("Missing object with oid: {0}")]
    MissingObjectWithOid(u32),

    #[error("Missing tunnel connection: {0}")]
    MissingTunnel(u32),

    #[error("failed to plan view: {0}")]
    ViewPlanning(Box<crate::planner::errors::PlanError>),

    #[error("Invalid dispatch: {0}")]
    InvalidDispatch(&'static str),

    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    DebugDatasource(#[from] datasources::debug::errors::DebugError),
    #[error(transparent)]
    PostgresDatasource(#[from] datasources::postgres::errors::PostgresError),
    #[error(transparent)]
    BigQueryDatasource(#[from] datasources::bigquery::errors::BigQueryError),
    #[error(transparent)]
    MysqlDatasource(#[from] datasources::mysql::errors::MysqlError),
    #[error(transparent)]
    ObjectStoreDatasource(#[from] datasources::object_store::errors::ObjectStoreSourceError),
    #[error(transparent)]
    MongoDatasource(#[from] datasources::mongodb::errors::MongoDbError),
    #[error(transparent)]
    SnowflakeDatasource(#[from] datasources::snowflake::errors::DatasourceSnowflakeError),
    #[error(transparent)]
    DeltaDatasource(#[from] datasources::lake::delta::errors::DeltaError),
    #[error(transparent)]
    IcebergDatasource(#[from] datasources::lake::iceberg::errors::IcebergError),
    #[error(transparent)]
    SqlServerError(#[from] datasources::sqlserver::errors::SqlServerError),
    #[error(transparent)]
    BsonDatasource(#[from] datasources::bson::errors::BsonError),
    #[error(transparent)]
    ClickhouseDatasource(#[from] datasources::clickhouse::errors::ClickhouseError),
    #[error(transparent)]
    NativeDatasource(#[from] datasources::native::errors::NativeError),
    #[error(transparent)]
    CommonDatasource(#[from] datasources::common::errors::DatasourceCommonError),
    #[error(transparent)]
    SshKey(#[from] datasources::common::ssh::key::SshKeyError),
    #[error(transparent)]
    ExtensionError(#[from] datafusion_ext::errors::ExtensionError),
    #[error(transparent)]
    CassandraDatasource(#[from] datasources::cassandra::CassandraError),
    #[error(transparent)]
    SqliteDatasource(#[from] datasources::sqlite::errors::SqliteError),
    #[error(transparent)]
    ExcelDatasource(#[from] datasources::excel::errors::ExcelError),
    #[error(transparent)]
    LakeStorageOptions(#[from] datasources::lake::LakeStorageOptionsError),

    #[error("{0}")]
    String(String),

    #[error(transparent)]
    Builtin(#[from] sqlbuiltins::errors::BuiltinError),
    #[error(transparent)]
    Datasource(#[from] Box<dyn std::error::Error + Send + Sync>),
}


/// Trait for planning views.
///
/// Currently views aren't that sophisticated as we're only storing the SQL
/// statement and column aliases. We don't track view dependencies.
#[async_trait]
pub trait ViewPlanner: Send + Sync {
    /// Plan a view from SQL, producing a logical plan with the provided
    /// aliases.
    ///
    /// If no column aliases are provided, then columns should be returned
    /// as-is.
    async fn plan_view(&self, sql: &str, col_aliases: &[String]) -> Result<LogicalPlan, PlanError>;
}

#[async_trait]
impl ViewPlanner for LocalSessionContext {
    async fn plan_view(&self, sql: &str, col_aliases: &[String]) -> Result<LogicalPlan, PlanError> {
        // TODO: Instead of doing late planning, we should instead try to insert
        // the contents of the view into the parent query prior to any planning.
        let mut statements = CustomParser::parse_sql(sql)?;
        if statements.len() != 1 {
            return Err(PlanError::ExpectedExactlyOneStatement(
                statements.into_iter().collect(),
            ));
        }

        let planner = SessionPlanner::new(self);

        let plan = planner.plan_ast(statements.pop_front().unwrap()).await?;
        let mut df_plan = plan.try_into_datafusion_plan()?;

        // Wrap logical plan in projection if the view was defined with
        // column aliases.
        if !col_aliases.is_empty() {
            let fields = df_plan.schema().fields().clone();
            df_plan = LogicalPlanBuilder::from(df_plan)
                .project(fields.iter().zip(col_aliases.iter()).map(|(field, alias)| {
                    Expr::Column(Column::new_unqualified(field.name())).alias(alias)
                }))?
                .build()?;
        }
        Ok(df_plan)
    }
}

/// Dispatch to table providers.
pub struct Dispatcher<'a> {
    catalog: &'a SessionCatalog,
    tables: &'a NativeTableStorage,
    view_planner: &'a dyn ViewPlanner,
    // TODO: Remove need for this.
    df_ctx: &'a DfSessionContext,
    /// Whether or not local file system access should be disabled.
    disable_local_fs_access: bool,
    function_registry: &'a FunctionRegistry,
}

impl<'a> Dispatcher<'a> {
    pub fn new(
        catalog: &'a SessionCatalog,
        tables: &'a NativeTableStorage,
        view_planner: &'a dyn ViewPlanner,
        df_ctx: &'a DfSessionContext,
        disable_local_fs_access: bool,
        function_registry: &'a FunctionRegistry,
    ) -> Self {
        Dispatcher {
            catalog,
            tables,
            view_planner,
            df_ctx,
            disable_local_fs_access,
            function_registry,
        }
    }

    /// Dispatch a table.
    pub async fn dispatch_table(&self, tbl: &TableEntry) -> Result<Arc<dyn TableProvider>> {
        // Temp tables
        if tbl.meta.is_temp {
            return Ok(self
                .catalog
                .get_temp_catalog()
                .get_temp_table_provider(&tbl.meta.name)
                .ok_or_else(|| DispatchError::MissingTempTable {
                    name: tbl.meta.name.to_string(),
                })?);
        }

        // Builtin tables
        if tbl.meta.builtin {
            return SystemTableDispatcher::new(self.catalog, self.tables, self.function_registry)
                .dispatch(tbl)
                .await;
        }

        // External tables
        if tbl.meta.external {
            return ExternalDispatcher::new(
                self.catalog,
                self.df_ctx,
                self.function_registry,
                self.disable_local_fs_access,
            )
            .dispatch_external_table(tbl)
            .await;
        }

        // Native (user) tables
        let table = self.tables.load_table(tbl).await?;
        Ok(table.into_table_provider())
    }

    /// Dispatch a view.
    pub async fn dispatch_view(&self, view: &ViewEntry) -> Result<Arc<dyn TableProvider>> {
        let plan = self
            .view_planner
            .plan_view(&view.sql, &view.columns)
            .await
            .map_err(|e| DispatchError::ViewPlanning(Box::new(e)))?;
        Ok(Arc::new(ViewTable::try_new(plan, None)?))
    }

    /// Dispatch to an external system.
    pub async fn dispatch_external(
        &self,
        db_ent: &DatabaseEntry,
        schema: impl AsRef<str>,
        name: impl AsRef<str>,
    ) -> Result<Arc<dyn TableProvider>> {
        let schema = schema.as_ref();
        let name = name.as_ref();
        ExternalDispatcher::new(
            self.catalog,
            self.df_ctx,
            self.function_registry,
            self.disable_local_fs_access,
        )
        .dispatch_external(&db_ent.meta.name, schema, name)
        .await
    }

    pub async fn dispatch_table_function(
        &self,
        func: &FunctionEntry,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let func = match self.function_registry.get_table_func(&func.meta.name) {
            Some(func) => func,
            None => {
                return Err(DispatchError::String(format!(
                    "'{}' is not a table function",
                    func.meta.name
                )))
            }
        };

        let prov = func
            .create_provider(
                &DefaultTableContextProvider::new(self.catalog, self.df_ctx),
                args,
                opts,
            )
            .await?;
        Ok(prov)
    }
}
