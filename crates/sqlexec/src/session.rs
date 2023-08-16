use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use crate::extension_codec::GlareDBExtensionCodec;
use crate::metastore::catalog::SessionCatalog;
use crate::planner::context_builder::PartialContextProvider;
use crate::planner::extension::{ExtensionNode, ExtensionType};
use crate::remote::client::RemoteSessionClient;
use crate::remote::staged_stream::StagedClientStreams;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::OwnedTableReference;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Extension, LogicalPlan as DfLogicalPlan};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::{
    execute_stream, memory::MemoryStream, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use datafusion_ext::vars::{SessionVars, VarSetter};
use datasources::common::sink::csv::{CsvSink, CsvSinkOpts};
use datasources::common::sink::json::{JsonSink, JsonSinkOpts};
use datasources::common::sink::parquet::{ParquetSink, ParquetSinkOpts};
use datasources::native::access::NativeTableStorage;
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::ObjStoreAccess;
use pgrepr::format::Format;
use protogen::metastore::types::options::{CopyToDestinationOptions, CopyToFormatOptions};
use telemetry::Tracker;

use crate::background_jobs::JobRunner;
use crate::context::{Portal, PreparedStatement, SessionContext};
use crate::environment::EnvironmentReader;
use crate::errors::{internal, Result};
use crate::metrics::{BatchStreamWithMetricSender, ExecutionStatus, QueryMetrics, SessionMetrics};
use crate::parser::StatementWithExtensions;
use crate::planner::logical_plan::*;

/// Results from a sql statement execution.
pub enum ExecutionResult {
    /// The stream for the output of a query.
    Query {
        stream: SendableRecordBatchStream,
        /// Plan used to create the stream. Used for getting metrics after the
        /// stream completes.
        ///
        /// TODO: I would like to remove this. Putting the plan on the result
        /// was the easiest way of providing everything needed to construct a
        /// `BatchStreamWithMetricSender` without a bit more refactoring. This
        /// stream requires physical plan and a starting set of execution
        /// metrics, which are created separately.
        plan: Arc<dyn ExecutionPlan>,
    },
    /// Showing a variable.
    // TODO: We don't need to make a stream for this.
    ShowVariable { stream: SendableRecordBatchStream },
    /// No statement provided.
    EmptyQuery,
    /// Transaction started.
    Begin,
    /// Transaction committed,
    Commit,
    /// Transaction rolled abck.
    Rollback,
    /// Data successfully deleted.
    DeleteSuccess { deleted_rows: usize },
    /// Data successfully updated.
    UpdateSuccess { updated_rows: usize },
    /// Data successfully written.
    WriteSuccess,
    /// Data successfully copied.
    CopySuccess,
    /// Table created.
    CreateTable,
    /// Database created.
    CreateDatabase,
    /// Tunnel created.
    CreateTunnel,
    /// Credentials created.
    CreateCredentials,
    /// Schema created.
    CreateSchema,
    /// A view was created.
    CreateView,
    /// A table was renamed.
    AlterTableRename,
    /// A database was renamed.
    AlterDatabaseRename,
    /// A tunnel was altered.
    AlterTunnelRotateKeys,
    /// A client local variable was set.
    SetLocal,
    /// Tables dropped.
    DropTables,
    /// Views dropped.
    DropViews,
    /// Schemas dropped.
    DropSchemas,
    /// Database dropped.
    DropDatabase,
    /// Tunnel is dropped.
    DropTunnel,
    /// Credentials are dropped.
    DropCredentials,
}

impl ExecutionResult {
    const fn result_type_str(&self) -> &'static str {
        match self {
            ExecutionResult::Query { .. } => "query",
            ExecutionResult::ShowVariable { .. } => "show_variable",
            ExecutionResult::EmptyQuery => "empty_query",
            ExecutionResult::Begin => "begin",
            ExecutionResult::Commit => "commit",
            ExecutionResult::Rollback => "rollback",
            ExecutionResult::WriteSuccess => "write_success",
            ExecutionResult::CopySuccess => "copy_success",
            ExecutionResult::DeleteSuccess { .. } => "delete_success",
            ExecutionResult::UpdateSuccess { .. } => "update success",
            ExecutionResult::CreateTable => "create_table",
            ExecutionResult::CreateDatabase => "create_database",
            ExecutionResult::CreateTunnel => "create_tunnel",
            ExecutionResult::CreateCredentials => "create_credentials",
            ExecutionResult::CreateSchema => "create_schema",
            ExecutionResult::CreateView => "create_view",
            ExecutionResult::AlterTableRename => "alter_table_rename",
            ExecutionResult::AlterDatabaseRename => "alter_database_rename",
            ExecutionResult::AlterTunnelRotateKeys => "alter_tunnel_rotate_keys",
            ExecutionResult::SetLocal => "set_local",
            ExecutionResult::DropTables => "drop_tables",
            ExecutionResult::DropViews => "drop_views",
            ExecutionResult::DropSchemas => "drop_schemas",
            ExecutionResult::DropDatabase => "drop_database",
            ExecutionResult::DropTunnel => "drop_tunnel",
            ExecutionResult::DropCredentials => "drop_credentials",
        }
    }
}

impl fmt::Display for ExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionResult::Query { stream, .. } => {
                let fields: Vec<_> = stream
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                write!(f, "Query ({})", fields.join(", "))
            }
            ExecutionResult::ShowVariable { .. } => {
                write!(f, "Show")
            }
            ExecutionResult::EmptyQuery => write!(f, "No results"),
            ExecutionResult::Begin => write!(f, "Begin"),
            ExecutionResult::Commit => write!(f, "Commit"),
            ExecutionResult::Rollback => write!(f, "Rollback"),
            ExecutionResult::WriteSuccess => write!(f, "Write success"),
            ExecutionResult::CopySuccess => write!(f, "Copy success"),
            ExecutionResult::DeleteSuccess { deleted_rows } => {
                if *deleted_rows == 1 {
                    write!(f, "Deleted 1 row")
                } else {
                    write!(f, "Deleted {} rows", deleted_rows)
                }
            }
            ExecutionResult::UpdateSuccess { updated_rows } => {
                if *updated_rows == 1 {
                    write!(f, "Updated 1 row")
                } else {
                    write!(f, "Updated {} rows", updated_rows)
                }
            }
            ExecutionResult::CreateTable => write!(f, "Table created"),
            ExecutionResult::CreateDatabase => write!(f, "Database created"),
            ExecutionResult::CreateTunnel => write!(f, "Tunnel created"),
            ExecutionResult::CreateCredentials => write!(f, "Credentials created"),
            ExecutionResult::CreateSchema => write!(f, "Schema create"),
            ExecutionResult::CreateView => write!(f, "View created"),
            ExecutionResult::AlterTableRename => write!(f, "Table renamed"),
            ExecutionResult::AlterDatabaseRename => write!(f, "Database renamed"),
            ExecutionResult::AlterTunnelRotateKeys => write!(f, "Keys rotated"),
            ExecutionResult::SetLocal => write!(f, "Local variable set"),
            ExecutionResult::DropTables => write!(f, "Table(s) dropped"),
            ExecutionResult::DropViews => write!(f, "View(s) dropped"),
            ExecutionResult::DropSchemas => write!(f, "Schema(s) dropped"),
            ExecutionResult::DropDatabase => write!(f, "Database(s) dropped"),
            ExecutionResult::DropTunnel => write!(f, "Tunnel(s) dropped"),
            ExecutionResult::DropCredentials => write!(f, "Credentials dropped"),
        }
    }
}

/// A per-client user session.
///
/// This is a thin wrapper around a session context. Having a layer between
/// pgsrv and actual execution against the catalog allows for easy extensibility
/// in the future (e.g. consensus).
pub struct Session {
    pub(crate) ctx: SessionContext,
}

impl Session {
    /// Create a new session.
    ///
    /// All system schemas (including `information_schema`) should already be in
    /// the provided catalog.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vars: SessionVars,
        catalog: SessionCatalog,
        native_tables: NativeTableStorage,
        tracker: Arc<Tracker>,
        spill_path: Option<PathBuf>,
        background_jobs: JobRunner,
        exec_client: Option<RemoteSessionClient>,
        remote_ctx: bool,
    ) -> Result<Session> {
        let metrics = SessionMetrics::new(
            *vars.user_id.value(),
            *vars.database_id.value(),
            *vars.connection_id.value(),
            tracker,
        );

        let ctx = SessionContext::new(
            vars,
            catalog,
            native_tables,
            metrics,
            spill_path,
            background_jobs,
            exec_client,
            remote_ctx,
        )?;

        Ok(Session { ctx })
    }

    pub fn get_session_catalog(&self) -> &SessionCatalog {
        self.ctx.get_session_catalog()
    }

    pub fn register_env_reader(&mut self, env_reader: Box<dyn EnvironmentReader>) {
        self.ctx.register_env_reader(env_reader);
    }

    /// Return the DF session context.
    pub fn df_ctx(&self) -> &datafusion::prelude::SessionContext {
        self.ctx.df_ctx()
    }

    /// Create a physical plan for a given datafusion logical plan.
    pub async fn create_physical_plan(
        &mut self,
        plan: DfLogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = self.ctx.df_ctx().state();

        // TODO!: these should be pushed down to physical execs,
        // currently we need to early exit on extension nodes as we don't have a physical exec for them.
        match plan {
            DfLogicalPlan::Extension(ref extension) if self.is_main_instance() => {
                let _exec_res = self.execute_extension(extension).await?;
                Ok(Arc::new(EmptyExec::new(false, Schema::empty().into())))
            }
            plan => {
                let plan = state.create_physical_plan(&plan).await?;
                Ok(plan)
            }
        }
    }

    pub fn staged_streams(&self) -> Result<Arc<StagedClientStreams>> {
        self.ctx.staged_streams()
    }

    pub async fn execute_extension(&mut self, extension: &Extension) -> Result<ExecutionResult> {
        let node = extension.node.name().parse::<ExtensionType>()?;

        match node {
            ExtensionType::CreateTable => {
                let create_table = CreateTable::try_decode_extension(extension)?;
                self.create_table(create_table).await?;
                Ok(ExecutionResult::CreateTable)
            }
            ExtensionType::CreateExternalTable => {
                let create_table = CreateExternalTable::try_decode_extension(extension)?;
                self.create_external_table(create_table).await?;
                Ok(ExecutionResult::CreateTable)
            }
            ExtensionType::CreateSchema => {
                let create_schema = CreateSchema::try_decode_extension(extension)?;
                self.create_schema(create_schema).await?;
                Ok(ExecutionResult::CreateSchema)
            }
            ExtensionType::DropTables => {
                let drop_tables = DropTables::try_decode_extension(extension)?;
                self.drop_tables(drop_tables).await?;
                Ok(ExecutionResult::DropTables)
            }
            ExtensionType::AlterTableRename => {
                let alter_table_rename = AlterTableRename::try_decode_extension(extension)?;
                self.alter_table_rename(alter_table_rename).await?;
                Ok(ExecutionResult::AlterTableRename)
            }
            ExtensionType::AlterDatabaseRename => {
                let alter_database_rename = AlterDatabaseRename::try_decode_extension(extension)?;
                self.alter_database_rename(alter_database_rename).await?;
                Ok(ExecutionResult::AlterDatabaseRename)
            }
            ExtensionType::AlterTunnelRotateKeys => {
                let alter_tunnel_rotate_keys =
                    AlterTunnelRotateKeys::try_decode_extension(extension)?;
                self.alter_tunnel_rotate_keys(alter_tunnel_rotate_keys)
                    .await?;
                Ok(ExecutionResult::AlterTunnelRotateKeys)
            }
            ExtensionType::CreateCredentials => {
                let create_credentials = CreateCredentials::try_decode_extension(extension)?;
                self.create_credentials(create_credentials).await?;
                Ok(ExecutionResult::CreateCredentials)
            }
            ExtensionType::CreateExternalDatabase => {
                let create_database = CreateExternalDatabase::try_decode_extension(extension)?;
                self.create_database(create_database).await?;
                Ok(ExecutionResult::CreateDatabase)
            }
            ExtensionType::CreateTunnel => {
                let create_tunnel = CreateTunnel::try_decode_extension(extension)?;
                self.create_tunnel(create_tunnel).await?;
                Ok(ExecutionResult::CreateTunnel)
            }
            ExtensionType::CreateTempTable => {
                let create_temp_table = CreateTempTable::try_decode_extension(extension)?;
                self.create_temp_table(create_temp_table).await?;
                Ok(ExecutionResult::CreateTable)
            }
            ExtensionType::CreateView => {
                let create_view = CreateView::try_decode_extension(extension)?;
                self.create_view(create_view).await?;
                Ok(ExecutionResult::CreateView)
            }
            ExtensionType::DropCredentials => {
                let drop_credentials = DropCredentials::try_decode_extension(extension)?;
                self.drop_credentials(drop_credentials).await?;
                Ok(ExecutionResult::DropCredentials)
            }
            ExtensionType::DropDatabase => {
                let drop_database = DropDatabase::try_decode_extension(extension)?;
                self.drop_database(drop_database).await?;
                Ok(ExecutionResult::DropDatabase)
            }
            ExtensionType::DropSchemas => {
                let drop_schemas = DropSchemas::try_decode_extension(extension)?;
                self.drop_schemas(drop_schemas).await?;
                Ok(ExecutionResult::DropSchemas)
            }
            ExtensionType::DropTunnel => {
                let drop_tunnel = DropTunnel::try_decode_extension(extension)?;
                self.drop_tunnel(drop_tunnel).await?;
                Ok(ExecutionResult::DropTunnel)
            }
            ExtensionType::DropViews => {
                let drop_views = DropViews::try_decode_extension(extension)?;
                self.drop_views(drop_views).await?;
                Ok(ExecutionResult::DropViews)
            }
        }
    }

    /// Execute a datafusion physical plan.
    pub fn execute_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let context = self.ctx.task_context();
        let stream = execute_stream(plan, context)?;
        Ok(stream)
    }

    /// Get a table provider from session.
    pub fn get_table_provider(&self, provider_id: &uuid::Uuid) -> Result<Arc<dyn TableProvider>> {
        self.ctx.get_table_provider(provider_id)
    }

    /// Add a table provider to the session. Returns the ID of the provider.
    pub fn add_table_provider(&mut self, provider: Arc<dyn TableProvider>) -> Result<uuid::Uuid> {
        self.ctx.add_table_provider(provider)
    }

    /// Get a physical plan from session.
    pub fn get_physical_plan(&self, exec_id: &uuid::Uuid) -> Result<Arc<dyn ExecutionPlan>> {
        self.ctx.get_physical_plan(exec_id)
    }

    /// Add a physical plan to the session. Returns the ID of the plan.
    pub fn add_physical_plan(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<uuid::Uuid> {
        self.ctx.add_physical_plan(plan)
    }

    /// Returns the extension codec used for serializing and deserializing data
    /// over RPCs.
    pub fn extension_codec(&self) -> Result<GlareDBExtensionCodec<'_>> {
        self.ctx.extension_codec()
    }

    /// Get the session dispatcher.
    pub async fn dispatch_access(
        &self,
        table_ref: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = self.ctx.df_ctx().state();
        let mut ctx_provider = PartialContextProvider::new(&self.ctx, &state)?;
        Ok(ctx_provider.table_provider(table_ref).await?)
    }

    pub(crate) async fn create_table(
        &mut self,
        plan: CreateTable,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = plan.schema.as_ref().clone();
        self.ctx.create_table(plan).await?;
        Ok(Arc::new(EmptyExec::new(false, schema.into())))
    }

    pub(crate) async fn create_temp_table(&mut self, plan: CreateTempTable) -> Result<()> {
        self.ctx.create_temp_table(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_external_table(&mut self, plan: CreateExternalTable) -> Result<()> {
        self.ctx.create_external_table(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_schema(&mut self, plan: CreateSchema) -> Result<()> {
        self.ctx.create_schema(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_database(&mut self, plan: CreateExternalDatabase) -> Result<()> {
        self.ctx.create_external_database(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_tunnel(&mut self, plan: CreateTunnel) -> Result<()> {
        self.ctx.create_tunnel(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_credentials(&mut self, plan: CreateCredentials) -> Result<()> {
        self.ctx.create_credentials(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_view(&mut self, plan: CreateView) -> Result<()> {
        self.ctx.create_view(plan).await?;
        Ok(())
    }

    pub(crate) async fn alter_table_rename(&mut self, plan: AlterTableRename) -> Result<()> {
        self.ctx.alter_table_rename(plan).await?;
        Ok(())
    }

    pub(crate) async fn alter_database_rename(&mut self, plan: AlterDatabaseRename) -> Result<()> {
        self.ctx.alter_database_rename(plan).await?;
        Ok(())
    }

    pub(crate) async fn alter_tunnel_rotate_keys(
        &mut self,
        plan: AlterTunnelRotateKeys,
    ) -> Result<()> {
        self.ctx.alter_tunnel_rotate_keys(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_tables(&mut self, plan: DropTables) -> Result<()> {
        self.ctx.drop_tables(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_views(&mut self, plan: DropViews) -> Result<()> {
        self.ctx.drop_views(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_schemas(&mut self, plan: DropSchemas) -> Result<()> {
        self.ctx.drop_schemas(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_database(&mut self, plan: DropDatabase) -> Result<()> {
        self.ctx.drop_database(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_tunnel(&mut self, plan: DropTunnel) -> Result<()> {
        self.ctx.drop_tunnel(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_credentials(&mut self, plan: DropCredentials) -> Result<()> {
        self.ctx.drop_credentials(plan).await?;
        Ok(())
    }

    pub(crate) fn set_variable(&mut self, plan: SetVariable) -> Result<()> {
        self.ctx.get_session_vars_mut().set(
            &plan.variable,
            plan.try_value_into_string()?.as_str(),
            VarSetter::User,
        )?;
        Ok(())
    }

    pub(crate) async fn insert_into(&mut self, plan: Insert) -> Result<()> {
        self.ctx.insert(plan).await?;
        Ok(())
    }

    pub(crate) async fn plan_copy_to(&mut self, plan: CopyTo) -> Result<()> {
        fn get_sink_for_obj(
            format: CopyToFormatOptions,
            access: &dyn ObjStoreAccess,
            location: &str,
        ) -> Result<Box<dyn DataSink>> {
            let store = access.create_store()?;
            let path = access.path(location)?;

            let sink: Box<dyn DataSink> = match format {
                CopyToFormatOptions::Csv(csv_opts) => Box::new(CsvSink::from_obj_store(
                    store,
                    path,
                    CsvSinkOpts {
                        delim: csv_opts.delim,
                        header: csv_opts.header,
                    },
                )),
                CopyToFormatOptions::Parquet(parquet_opts) => {
                    Box::new(ParquetSink::from_obj_store(
                        store,
                        path,
                        ParquetSinkOpts {
                            row_group_size: parquet_opts.row_group_size,
                        },
                    ))
                }
                CopyToFormatOptions::Json(json_opts) => Box::new(JsonSink::from_obj_store(
                    store,
                    path,
                    JsonSinkOpts {
                        array: json_opts.array,
                    },
                )),
            };
            Ok(sink)
        }

        let sink = match (plan.dest, plan.format) {
            (CopyToDestinationOptions::Local(local_options), format) => {
                {
                    // Create the path if it doesn't exist (for local).
                    let _ = tokio::fs::File::create(&local_options.location).await?;
                }
                let access = LocalStoreAccess;
                get_sink_for_obj(format, &access, &local_options.location)?
            }
            (CopyToDestinationOptions::Gcs(gcs_options), format) => {
                let access = GcsStoreAccess {
                    bucket: gcs_options.bucket,
                    service_account_key: gcs_options.service_account_key,
                };
                get_sink_for_obj(format, &access, &gcs_options.location)?
            }
            (CopyToDestinationOptions::S3(s3_options), format) => {
                let access = S3StoreAccess {
                    region: s3_options.region,
                    bucket: s3_options.bucket,
                    access_key_id: s3_options.access_key_id,
                    secret_access_key: s3_options.secret_access_key,
                };
                get_sink_for_obj(format, &access, &s3_options.location)?
            }
        };

        let physical = self.create_physical_plan(plan.source).await?;
        let stream = self.execute_physical(physical)?;

        sink.write_all(stream, &self.ctx.task_context()).await?;
        Ok(())
    }

    pub(crate) async fn delete(&mut self, plan: Delete) -> Result<usize> {
        self.ctx.delete(plan).await
    }

    pub(crate) async fn update(&mut self, plan: Update) -> Result<usize> {
        self.ctx.update(plan).await
    }

    pub(crate) fn show_variable(&self, plan: ShowVariable) -> Result<SendableRecordBatchStream> {
        let var = self.ctx.get_session_vars().get(&plan.variable)?;
        let batch = var.record_batch();
        let schema = batch.schema();
        // Creating this stream should never error.
        let stream = MemoryStream::try_new(vec![batch], schema, None).unwrap();
        Ok(Box::pin(stream))
    }

    pub fn get_session_vars(&self) -> &SessionVars {
        self.ctx.get_session_vars()
    }

    pub fn get_session_vars_mut(&mut self) -> &mut SessionVars {
        self.ctx.get_session_vars_mut()
    }

    /// Prepare a parsed statement for future execution.
    pub async fn prepare_statement(
        &mut self,
        name: String,
        stmt: Option<StatementWithExtensions>,
        params: Vec<i32>, // OIDs
    ) -> Result<()> {
        // Flush any completed metrics prior to planning. This is mostly
        // beneficial when planning successive calls to the
        // `session_query_history` table since the mem table is created during
        // planning.
        //
        // In all other cases, it's correct to only need to flush immediately
        // prior to execute (which we also do).
        self.ctx.get_metrics_mut().flush_completed();

        self.ctx.prepare_statement(name, stmt, params).await
    }

    pub fn get_prepared_statement(&self, name: &str) -> Result<&PreparedStatement> {
        self.ctx.get_prepared_statement(name)
    }

    pub fn get_portal(&self, name: &str) -> Result<&Portal> {
        self.ctx.get_portal(name)
    }

    pub fn remove_prepared_statement(&mut self, name: &str) {
        self.ctx.remove_prepared_statement(name);
    }

    pub fn remove_portal(&mut self, name: &str) {
        self.ctx.remove_portal(name);
    }

    /// Bind the parameters of a prepared statement to the given values.
    ///
    /// If successful, the bound statement will create a portal which can be
    /// used to execute the statement.
    pub fn bind_statement(
        &mut self,
        portal_name: String,
        stmt_name: &str,
        params: Vec<ScalarValue>,
        result_formats: Vec<Format>,
    ) -> Result<()> {
        self.ctx
            .bind_statement(portal_name, stmt_name, params, result_formats)
    }
    pub fn is_main_instance(&self) -> bool {
        self.ctx
            .get_session_vars()
            .remote_session_id
            .value()
            .is_none()
    }

    pub async fn execute_inner(&mut self, plan: LogicalPlan) -> Result<ExecutionResult> {
        // Note that transaction support is fake, in that we don't currently do
        // anything and do not provide any transactional semantics.
        //
        // We stub out transaction commands since many tools (even BI ones) will
        // try to open a transaction for some queries.
        let result = match plan {
            LogicalPlan::Transaction(plan) => match plan {
                TransactionPlan::Begin => ExecutionResult::Begin,
                TransactionPlan::Commit => ExecutionResult::Commit,
                TransactionPlan::Abort => ExecutionResult::Rollback,
            },

            LogicalPlan::Write(WritePlan::Insert(plan)) => {
                self.insert_into(plan).await?;
                ExecutionResult::WriteSuccess
            }
            LogicalPlan::Write(WritePlan::CopyTo(plan)) => {
                self.plan_copy_to(plan).await?;
                ExecutionResult::CopySuccess
            }
            LogicalPlan::Write(WritePlan::Delete(plan)) => {
                let deleted_rows = self.delete(plan).await?;
                ExecutionResult::DeleteSuccess { deleted_rows }
            }
            LogicalPlan::Write(WritePlan::Update(plan)) => {
                let updated_rows = self.update(plan).await?;
                ExecutionResult::UpdateSuccess { updated_rows }
            }
            LogicalPlan::Datafusion(plan) => {
                let physical = self.create_physical_plan(plan).await?;
                let stream = self.execute_physical(physical.clone())?;
                ExecutionResult::Query {
                    stream,
                    plan: physical,
                }
            }
            LogicalPlan::Variable(VariablePlan::SetVariable(plan)) => {
                self.set_variable(plan)?;
                ExecutionResult::SetLocal
            }
            LogicalPlan::Variable(VariablePlan::ShowVariable(plan)) => {
                let stream = self.show_variable(plan)?;
                ExecutionResult::ShowVariable { stream }
            }
        };

        Ok(result)
    }

    /// Execute a portal.
    ///
    /// This will handle metrics tracking for query executions.
    // TODO: Handle max rows.
    pub async fn execute_portal(
        &mut self,
        portal_name: &str,
        _max_rows: i32,
    ) -> Result<ExecutionResult> {
        // Flush any completed metrics.
        self.ctx.get_metrics_mut().flush_completed();

        let portal = self.ctx.get_portal(portal_name)?;
        let plan = match &portal.stmt.plan {
            Some(plan) => plan.clone(),
            None => return Ok(ExecutionResult::EmptyQuery),
        };

        // Create "base" metrics.
        let mut metrics = QueryMetrics::new_for_portal(portal);

        let result = self.execute_inner(plan).await;
        let result = match result {
            Ok(result) => {
                metrics.execution_status = ExecutionStatus::Success;
                metrics.result_type = result.result_type_str();
                result
            }
            Err(e) => {
                metrics.execution_status = ExecutionStatus::Fail;
                metrics.error_message = Some(e.to_string());

                // Ensure we push the metrics for this failed query even though
                // we're returning an error. This allows for querying for and
                // reporting failed executions.
                self.ctx.get_metrics_mut().push_metric(metrics);
                return Err(e);
            }
        };

        // If we're running a query, then swap out the batch stream with one
        // that will send metrics at the completions of the stream.
        match result {
            ExecutionResult::Query { stream, plan } => {
                let sender = self.ctx.get_metrics().get_sender();
                Ok(ExecutionResult::Query {
                    stream: Box::pin(BatchStreamWithMetricSender::new(
                        stream,
                        plan.clone(),
                        metrics,
                        sender,
                    )),
                    plan,
                })
            }
            result => {
                // Query not async (already completed), we're good to go ahead
                // and push this metric.
                self.ctx.get_metrics_mut().push_metric(metrics);
                Ok(result)
            }
        }
    }

    pub async fn sql_to_lp(&mut self, query: &str) -> Result<LogicalPlan> {
        const UNNAMED: String = String::new();

        let mut statements = crate::parser::parse_sql(query)?;
        match statements.len() {
            0 => todo!(),
            1 => {
                let stmt = statements.pop_front().unwrap();
                self.prepare_statement(UNNAMED, Some(stmt), Vec::new())
                    .await?;
                let prepared = self.get_prepared_statement(&UNNAMED)?;
                let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
                self.bind_statement(
                    UNNAMED,
                    &UNNAMED,
                    Vec::new(),
                    vec![Format::Text; num_fields],
                )?;
                let portal = self.ctx.get_portal(&UNNAMED)?.clone();
                Ok(portal.stmt.plan.unwrap())
            }
            _ => todo!(),
        }
    }
}
