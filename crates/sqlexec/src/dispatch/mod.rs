//! Various table dispatchers.
pub mod external;
pub mod listing;
pub mod system;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext as DfSessionContext;
use datafusion::prelude::{Column, Expr};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider, VirtualLister};
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use protogen::metastore::types::catalog::{
    CatalogEntry, CredentialsEntry, DatabaseEntry, EntryMeta, EntryType, FunctionEntry, ViewEntry,
};
use sqlbuiltins::functions::BUILTIN_TABLE_FUNCS;

use crate::context::local::LocalSessionContext;
use crate::dispatch::system::SystemTableDispatcher;
use crate::parser::CustomParser;
use crate::planner::errors::PlanError;
use crate::planner::session_planner::SessionPlanner;
use catalog::session_catalog::{SessionCatalog, TempCatalog};

use self::external::ExternalDispatcher;
use self::listing::CatalogLister;

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

    #[error("Invalid entry for table dispatch: {0}")]
    InvalidEntryTypeForDispatch(EntryType),

    #[error("Unhandled entry for table dispatch: {0:?}")]
    UnhandledEntry(EntryMeta),

    #[error("failed to plan view: {0}")]
    ViewPlanning(Box<crate::planner::errors::PlanError>),

    #[error("Invalid dispatch: {0}")]
    InvalidDispatch(&'static str),

    #[error(transparent)]
    RemoteDispatch(Box<dyn std::error::Error + Send + Sync>),

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
    MongoDatasource(#[from] datasources::mongodb::errors::MongoError),
    #[error(transparent)]
    SnowflakeDatasource(#[from] datasources::snowflake::errors::DatasourceSnowflakeError),
    #[error(transparent)]
    DeltaDatasource(#[from] datasources::lake::delta::errors::DeltaError),
    #[error(transparent)]
    IcebergDatasource(#[from] datasources::lake::iceberg::errors::IcebergError),
    #[error(transparent)]
    SqlServerError(#[from] datasources::sqlserver::errors::SqlServerError),
    #[error(transparent)]
    NativeDatasource(#[from] datasources::native::errors::NativeError),
    #[error(transparent)]
    CommonDatasource(#[from] datasources::common::errors::DatasourceCommonError),
    #[error(transparent)]
    SshKey(#[from] datasources::common::ssh::key::SshKeyError),
    #[error(transparent)]
    ExtensionError(#[from] datafusion_ext::errors::ExtensionError),
}

impl DispatchError {
    /// Whether or not this error should indicate to the planner to try looking
    /// in a different schema for the requested object.
    ///
    /// For example, if a user's search path is '[public, other]', and 'table_a'
    /// exists in 'other', then the dispatch will fail the first time with
    /// `MissingEntry` since it will look for 'public.table_a' first. In such
    /// cases, we should try the next schema.
    pub fn should_try_next_schema(&self) -> bool {
        matches!(
            self,
            DispatchError::MissingEntry { .. } | DispatchError::InvalidEntryTypeForDispatch(_)
        )
    }
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
    temp_objects: &'a TempCatalog,
    view_planner: &'a dyn ViewPlanner,
    // TODO: Remove need for this.
    df_ctx: &'a DfSessionContext,
    /// Whether or not local file system access should be disabled.
    disable_local_fs_access: bool,
}

impl<'a> Dispatcher<'a> {
    pub fn new(
        catalog: &'a SessionCatalog,
        tables: &'a NativeTableStorage,
        temp_objects: &'a TempCatalog,
        view_planner: &'a dyn ViewPlanner,
        df_ctx: &'a DfSessionContext,
        disable_local_fs_access: bool,
    ) -> Self {
        Dispatcher {
            catalog,
            tables,
            temp_objects,
            view_planner,
            df_ctx,
            disable_local_fs_access,
        }
    }

    /// Dispatch to a table provider.
    pub async fn dispatch(&self, ent: CatalogEntry) -> Result<Arc<dyn TableProvider>> {
        // Only allow dispatching to types we can actually convert to a table
        // provider.
        if !matches!(ent.entry_type(), EntryType::View | EntryType::Table) {
            return Err(DispatchError::InvalidEntryTypeForDispatch(ent.entry_type()));
        }

        match ent {
            CatalogEntry::View(view) => self.dispatch_view(&view).await,
            // Temp tables
            CatalogEntry::Table(tbl) if tbl.meta.is_temp => {
                let provider = self
                    .temp_objects
                    .get_temp_table_provider(&tbl.meta.name)
                    .ok_or_else(|| DispatchError::MissingTempTable {
                        name: tbl.meta.name.to_string(),
                    })?;
                Ok(provider)
            }
            // Dispatch to builtin tables.
            CatalogEntry::Table(tbl) if tbl.meta.builtin => {
                SystemTableDispatcher::new(self.catalog, self.temp_objects).dispatch(&tbl)
            }
            // Dispatch to external tables.
            CatalogEntry::Table(tbl) if tbl.meta.external => {
                ExternalDispatcher::new(self.catalog, self.df_ctx, self.disable_local_fs_access)
                    .dispatch_external_table(&tbl)
                    .await
            }
            // Dispatch to native tables.
            CatalogEntry::Table(tbl) => {
                let table = self.tables.load_table(&tbl).await?;
                Ok(table.into_table_provider())
            }
            other => Err(DispatchError::UnhandledEntry(other.get_meta().clone())),
        }
    }

    /// Dispatch to an external system.
    pub async fn dispatch_external(
        &self,
        db_ent: &DatabaseEntry,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        ExternalDispatcher::new(self.catalog, self.df_ctx, self.disable_local_fs_access)
            .dispatch_external(&db_ent.meta.name, schema, name)
            .await
    }

    async fn dispatch_view(&self, view: &ViewEntry) -> Result<Arc<dyn TableProvider>> {
        let plan = self
            .view_planner
            .plan_view(&view.sql, &view.columns)
            .await
            .map_err(|e| DispatchError::ViewPlanning(Box::new(e)))?;
        Ok(Arc::new(ViewTable::try_new(plan, None)?))
    }

    pub async fn dispatch_function(
        &self,
        func: &FunctionEntry,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let resolve_func = if func.meta.builtin {
            BUILTIN_TABLE_FUNCS.find_function(&func.meta.name)
        } else {
            // We only have builtin functions right now.
            None
        };
        let prov = resolve_func
            .unwrap()
            .create_provider(self, args, opts)
            .await?;
        Ok(prov)
    }
}

impl<'a> TableFuncContextProvider for Dispatcher<'a> {
    fn get_database_entry(&self, name: &str) -> Option<&DatabaseEntry> {
        self.catalog.resolve_database(name)
    }

    fn get_credentials_entry(&self, name: &str) -> Option<&CredentialsEntry> {
        self.catalog.resolve_credentials(name)
    }

    fn get_session_vars(&self) -> SessionVars {
        let cfg = self.df_ctx.copied_config();
        let vars = cfg.options().extensions.get::<SessionVars>().unwrap();
        vars.clone()
    }

    fn get_session_state(&self) -> SessionState {
        self.df_ctx.state()
    }

    fn get_catalog_lister(&self) -> Box<dyn VirtualLister> {
        Box::new(CatalogLister::new(self.catalog.clone(), false))
    }
}
