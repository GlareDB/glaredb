//! Various table dispatchers.
pub mod external;
pub mod system;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext as DfSessionContext;
use datafusion::prelude::{Column, Expr};
use datasources::native::access::NativeTableStorage;
use protogen::metastore::types::catalog::{CatalogEntry, EntryMeta, EntryType, ViewEntry};
use sqlbuiltins::builtins::{CURRENT_SESSION_SCHEMA, DEFAULT_CATALOG};

use crate::context::local::SessionContext;
use crate::parser::CustomParser;
use crate::planner::errors::PlanError;
use crate::planner::session_planner::SessionPlanner;
use crate::{
    dispatch::system::SystemTableDispatcher,
    metastore::catalog::{SessionCatalog, TempObjects},
    metrics::SessionMetrics,
};

use self::external::ExternalDispatcher;

type Result<T, E = DispatchError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("Missing database: {database}")]
    MissingDatabase { database: String },

    #[error("Missing catalog entry; schema: {schema}, name: {name}")]
    MissingEntry { schema: String, name: String },

    #[error("Missing builtin table; schema: {schema}, name: {name}")]
    MissingBuiltinTable { schema: String, name: String },

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
    NativeDatasource(#[from] datasources::native::errors::NativeError),
    #[error(transparent)]
    CommonDatasource(#[from] datasources::common::errors::DatasourceCommonError),
    #[error(transparent)]
    SshKey(#[from] datasources::common::ssh::key::SshKeyError),
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
impl ViewPlanner for SessionContext {
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
    metrics: &'a SessionMetrics,
    temp_objects: &'a TempObjects,
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
        metrics: &'a SessionMetrics,
        temp_objects: &'a TempObjects,
        view_planner: &'a dyn ViewPlanner,
        df_ctx: &'a DfSessionContext,
        disable_local_fs_access: bool,
    ) -> Self {
        Dispatcher {
            catalog,
            tables,
            metrics,
            temp_objects,
            view_planner,
            df_ctx,
            disable_local_fs_access,
        }
    }

    /// Dispatch to a table provider.
    pub async fn dispatch(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        // "External" database.
        if database != DEFAULT_CATALOG {
            let db = match self.catalog.resolve_database(database) {
                Some(db) => db,
                None => {
                    return Err(DispatchError::MissingDatabase {
                        database: database.to_string(),
                    })
                }
            };
            return ExternalDispatcher::new(
                self.catalog,
                self.df_ctx,
                self.disable_local_fs_access,
            )
            .dispatch_external_database(db, schema, name)
            .await;
        }

        if schema == CURRENT_SESSION_SCHEMA {
            return match self.temp_objects.resolve_temp_table(name) {
                Some(table) => Ok(table),
                None => Err(DispatchError::MissingEntry {
                    schema: schema.to_owned(),
                    name: name.to_owned(),
                }),
            };
        }

        let ent = self
            .catalog
            .resolve_entry(database, schema, name)
            .ok_or_else(|| DispatchError::MissingEntry {
                schema: schema.to_string(),
                name: name.to_string(),
            })?;

        // Only allow dispatching to types we can actually convert to a table
        // provider.
        if !matches!(ent.entry_type(), EntryType::View | EntryType::Table) {
            return Err(DispatchError::InvalidEntryTypeForDispatch(ent.entry_type()));
        }

        match ent {
            CatalogEntry::View(view) => self.dispatch_view(view).await,
            // Dispatch to builtin tables.
            CatalogEntry::Table(tbl) if tbl.meta.builtin => {
                SystemTableDispatcher::new(self.catalog, self.metrics, self.temp_objects)
                    .dispatch(schema, name)
            }
            // Dispatch to external tables.
            CatalogEntry::Table(tbl) if tbl.meta.external => {
                ExternalDispatcher::new(self.catalog, self.df_ctx, self.disable_local_fs_access)
                    .dispatch_external_table(tbl)
                    .await
            }
            // Dispatch to native tables.
            CatalogEntry::Table(tbl) => {
                let table = self.tables.load_table(tbl).await?;
                Ok(table.into_table_provider())
            }
            other => Err(DispatchError::UnhandledEntry(other.get_meta().clone())),
        }
    }

    async fn dispatch_view(&self, view: &ViewEntry) -> Result<Arc<dyn TableProvider>> {
        let plan = self
            .view_planner
            .plan_view(&view.sql, &view.columns)
            .await
            .map_err(|e| DispatchError::ViewPlanning(Box::new(e)))?;
        Ok(Arc::new(ViewTable::try_new(plan, None)?))
    }
}
