use crate::context::SessionContext;
use crate::errors::ExecError;
use crate::parser::{
    validate_ident, validate_object_name, AlterDatabaseRenameStmt, CopyToSource, CopyToStmt,
    CreateExternalDatabaseStmt, CreateExternalTableStmt, CreateTunnelStmt, DropDatabaseStmt,
    DropTunnelStmt, OptionValue, StatementWithExtensions,
};
use crate::planner::context_builder::PlanContextBuilder;
use crate::planner::errors::{internal, PlanError, Result};
use crate::planner::logical_plan::*;
use crate::planner::preprocess::{preprocess, CastRegclassReplacer, EscapedStringToDoubleQuoted};
use datafusion::arrow::datatypes::{
    DataType, Field, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion::common::OwnedTableReference;
use datafusion::logical_expr::{
    LogicalPlan as DfLogicalPlan, LogicalPlanBuilder as DfLogicalPlanBuilder,
};
use datafusion::sql::planner::{object_name_to_table_reference, IdentNormalizer, SqlToRel};
use datafusion::sql::sqlparser::ast::AlterTableOperation;
use datafusion::sql::sqlparser::ast::{self, Ident, ObjectType};
use datafusion::sql::TableReference;
use datasource_bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasource_common::ssh::{SshConnection, SshConnectionParameters, SshKey};
use datasource_debug::DebugTableType;
use datasource_mongodb::{MongoAccessor, MongoDbConnection, MongoProtocol};
use datasource_mysql::{MysqlAccessor, MysqlDbConnection, MysqlTableAccess};
use datasource_object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasource_object_store::local::{LocalAccessor, LocalTableAccess};
use datasource_object_store::s3::{S3Accessor, S3TableAccess};
use datasource_object_store::sink::csv::CsvSinkOpts;
use datasource_object_store::sink::json::JsonSinkOpts;
use datasource_object_store::sink::parquet::ParquetSinkOpts;
use datasource_object_store::url::{ObjectStoreAuth, ObjectStoreProvider, ObjectStoreSourceUrl};
use datasource_postgres::{PostgresAccessor, PostgresDbConnection, PostgresTableAccess};
use datasource_snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use metastore::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsDebug, DatabaseOptionsMongo,
    DatabaseOptionsMysql, DatabaseOptionsPostgres, DatabaseOptionsSnowflake, TableOptions,
    TableOptionsBigQuery, TableOptionsDebug, TableOptionsGcs, TableOptionsLocal, TableOptionsMongo,
    TableOptionsMysql, TableOptionsPostgres, TableOptionsS3, TableOptionsSnowflake, TunnelOptions,
    TunnelOptionsDebug, TunnelOptionsInternal, TunnelOptionsSsh,
};
use metastore::validation::{validate_database_tunnel_support, validate_table_tunnel_support};
use sqlparser::ast::ObjectName;
use std::collections::BTreeMap;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use tracing::debug;

/// Plan SQL statements for a session.
pub struct SessionPlanner<'a> {
    ctx: &'a SessionContext,
}

impl<'a> SessionPlanner<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        SessionPlanner { ctx }
    }

    pub async fn plan_ast(&self, mut statement: StatementWithExtensions) -> Result<LogicalPlan> {
        debug!(%statement, "planning sql statement");

        // Run replacers as needed.
        if let StatementWithExtensions::Statement(inner) = &mut statement {
            preprocess(inner, &mut CastRegclassReplacer { ctx: self.ctx })?;
            preprocess(inner, &mut EscapedStringToDoubleQuoted)?;
        }

        match statement {
            StatementWithExtensions::Statement(stmt) => self.plan_statement(stmt).await,
            StatementWithExtensions::CreateExternalTable(stmt) => {
                self.plan_create_external_table(stmt).await
            }
            StatementWithExtensions::CreateExternalDatabase(stmt) => {
                self.plan_create_external_database(stmt).await
            }
            StatementWithExtensions::DropDatabase(stmt) => self.plan_drop_database(stmt),
            StatementWithExtensions::AlterDatabaseRename(stmt) => {
                self.plan_alter_database_rename(stmt)
            }
            StatementWithExtensions::CreateTunnel(stmt) => self.plan_create_tunnel(stmt).await,
            StatementWithExtensions::DropTunnel(stmt) => self.plan_drop_tunnel(stmt),
            StatementWithExtensions::CopyTo(stmt) => self.plan_copy_to(stmt).await,
        }
    }

    async fn plan_create_external_database(
        &self,
        mut stmt: CreateExternalDatabaseStmt,
    ) -> Result<LogicalPlan> {
        let tunnel = stmt.tunnel.map(normalize_ident);
        let tunnel_options = self.get_tunnel_opts(&tunnel)?;
        let datasource = normalize_ident(stmt.datasource);
        if let Some(tunnel_options) = &tunnel_options {
            // Validate if the tunnel type is supported by the datasource
            validate_database_tunnel_support(&datasource, tunnel_options.as_str()).map_err(
                |e| PlanError::InvalidExternalDatabase {
                    source: Box::new(e),
                },
            )?;
        }

        let m = &mut stmt.options;

        let db_options = match datasource.as_str() {
            DatabaseOptions::POSTGRES => {
                let connection_string = get_pg_conn_str(m)?;
                PostgresAccessor::validate_external_database(&connection_string, tunnel_options)
                    .await
                    .map_err(|e| PlanError::InvalidExternalDatabase {
                        source: Box::new(e),
                    })?;
                DatabaseOptions::Postgres(DatabaseOptionsPostgres { connection_string })
            }
            DatabaseOptions::BIGQUERY => {
                let service_account_key = remove_required_opt(m, "service_account_key")?;
                let project_id = remove_required_opt(m, "project_id")?;
                BigQueryAccessor::validate_external_database(&service_account_key, &project_id)
                    .await
                    .map_err(|e| PlanError::InvalidExternalDatabase {
                        source: Box::new(e),
                    })?;
                DatabaseOptions::BigQuery(DatabaseOptionsBigQuery {
                    service_account_key,
                    project_id,
                })
            }
            DatabaseOptions::MYSQL => {
                let connection_string = get_mysql_conn_str(m)?;
                MysqlAccessor::validate_external_database(&connection_string, tunnel_options)
                    .await
                    .map_err(|e| PlanError::InvalidExternalDatabase {
                        source: Box::new(e),
                    })?;
                DatabaseOptions::Mysql(DatabaseOptionsMysql { connection_string })
            }
            DatabaseOptions::MONGO => {
                let connection_string = get_mongo_conn_str(m)?;
                // Validate the accessor
                MongoAccessor::validate_external_database(connection_string.as_str())
                    .await
                    .map_err(|e| PlanError::InvalidExternalDatabase {
                        source: Box::new(e),
                    })?;
                DatabaseOptions::Mongo(DatabaseOptionsMongo { connection_string })
            }
            DatabaseOptions::SNOWFLAKE => {
                let account_name = remove_required_opt(m, "account")?;
                let login_name = remove_required_opt(m, "username")?;
                let password = remove_required_opt(m, "password")?;
                let database_name = remove_required_opt(m, "database")?;
                let warehouse = remove_required_opt(m, "warehouse")?;
                let role_name = remove_optional_opt(m, "role")?;
                SnowflakeAccessor::validate_external_database(SnowflakeDbConnection {
                    account_name: account_name.clone(),
                    login_name: login_name.clone(),
                    password: password.clone(),
                    database_name: database_name.clone(),
                    warehouse: warehouse.clone(),
                    role_name: role_name.clone(),
                })
                .await
                .map_err(|e| PlanError::InvalidExternalDatabase {
                    source: Box::new(e),
                })?;
                DatabaseOptions::Snowflake(DatabaseOptionsSnowflake {
                    account_name,
                    login_name,
                    password,
                    database_name,
                    warehouse,
                    role_name: role_name.unwrap_or_default(),
                })
            }
            DatabaseOptions::DEBUG => {
                datasource_debug::validate_tunnel_connections(tunnel_options.as_ref())?;
                DatabaseOptions::Debug(DatabaseOptionsDebug {})
            }
            other => return Err(internal!("unsupported datasource: {}", other)),
        };

        let database_name = normalize_ident(stmt.name);

        let plan = CreateExternalDatabase {
            database_name,
            if_not_exists: stmt.if_not_exists,
            options: db_options,
            tunnel,
        };

        Ok(LogicalPlan::Ddl(DdlPlan::CreateExternalDatabase(plan)))
    }

    async fn plan_create_external_table(
        &self,
        mut stmt: CreateExternalTableStmt,
    ) -> Result<LogicalPlan> {
        let tunnel = stmt.tunnel.map(normalize_ident);
        let tunnel_options = self.get_tunnel_opts(&tunnel)?;
        let datasource = normalize_ident(stmt.datasource);
        if let Some(tunnel_options) = &tunnel_options {
            // Validate if the tunnel type is supported by the datasource
            validate_table_tunnel_support(&datasource, tunnel_options.as_str()).map_err(|e| {
                PlanError::InvalidExternalTable {
                    source: Box::new(e),
                }
            })?;
        }

        let m = &mut stmt.options;

        let external_table_options = match datasource.as_str() {
            TableOptions::POSTGRES => {
                let connection_string = get_pg_conn_str(m)?;
                let schema = remove_required_opt(m, "schema")?;
                let table = remove_required_opt(m, "table")?;

                let access = PostgresTableAccess {
                    schema,
                    name: table,
                };

                let _ = PostgresAccessor::validate_table_access(
                    &connection_string,
                    &access,
                    tunnel_options,
                )
                .await
                .map_err(|e| PlanError::InvalidExternalTable {
                    source: Box::new(e),
                })?;

                TableOptions::Postgres(TableOptionsPostgres {
                    connection_string,
                    schema: access.schema,
                    table: access.name,
                })
            }
            TableOptions::BIGQUERY => {
                let service_account_key = remove_required_opt(m, "service_account_key")?;
                let project_id = remove_required_opt(m, "project_id")?;
                let dataset_id = remove_required_opt(m, "dataset_id")?;
                let table_id = remove_required_opt(m, "table_id")?;

                let access = BigQueryTableAccess {
                    dataset_id,
                    table_id,
                };

                BigQueryAccessor::validate_table_access(&service_account_key, &project_id, &access)
                    .await
                    .map_err(|e| PlanError::InvalidExternalTable {
                        source: Box::new(e),
                    })?;

                TableOptions::BigQuery(TableOptionsBigQuery {
                    service_account_key,
                    project_id,
                    dataset_id: access.dataset_id,
                    table_id: access.table_id,
                })
            }
            TableOptions::MYSQL => {
                let connection_string = get_mysql_conn_str(m)?;
                let schema = remove_required_opt(m, "schema")?;
                let table = remove_required_opt(m, "table")?;

                let access = MysqlTableAccess {
                    schema,
                    name: table,
                };

                MysqlAccessor::validate_table_access(&connection_string, &access, tunnel_options)
                    .await
                    .map_err(|e| PlanError::InvalidExternalTable {
                        source: Box::new(e),
                    })?;

                TableOptions::Mysql(TableOptionsMysql {
                    connection_string,
                    schema: access.schema,
                    table: access.name,
                })
            }
            TableOptions::MONGO => {
                let connection_string = get_mongo_conn_str(m)?;
                let database = remove_required_opt(m, "database")?;
                let collection = remove_required_opt(m, "collection")?;

                TableOptions::Mongo(TableOptionsMongo {
                    connection_string,
                    database,
                    collection,
                })
            }
            TableOptions::SNOWFLAKE => {
                let account_name = remove_required_opt(m, "account")?;
                let login_name = remove_required_opt(m, "username")?;
                let password = remove_required_opt(m, "password")?;
                let database_name = remove_required_opt(m, "database")?;
                let warehouse = remove_required_opt(m, "warehouse")?;
                let role_name = remove_optional_opt(m, "role")?;
                let schema_name = remove_required_opt(m, "schema")?;
                let table_name = remove_required_opt(m, "table")?;

                let conn_params = SnowflakeDbConnection {
                    account_name: account_name.clone(),
                    login_name: login_name.clone(),
                    password: password.clone(),
                    database_name: database_name.clone(),
                    warehouse: warehouse.clone(),
                    role_name: role_name.clone(),
                };

                let access_info = SnowflakeTableAccess {
                    schema_name,
                    table_name,
                };

                let _ = SnowflakeAccessor::validate_table_access(conn_params, &access_info)
                    .await
                    .map_err(|e| PlanError::InvalidExternalTable {
                        source: Box::new(e),
                    })?;

                TableOptions::Snowflake(TableOptionsSnowflake {
                    account_name,
                    login_name,
                    password,
                    database_name,
                    warehouse,
                    role_name: role_name.unwrap_or_default(),
                    schema_name: access_info.schema_name,
                    table_name: access_info.table_name,
                })
            }
            TableOptions::LOCAL => {
                let location = remove_required_opt(m, "location")?;

                let access = LocalTableAccess {
                    location: location.clone(),
                    file_type: None,
                };

                LocalAccessor::validate_table_access(access)
                    .await
                    .map_err(|e| PlanError::InvalidExternalTable {
                        source: Box::new(e),
                    })?;

                TableOptions::Local(TableOptionsLocal { location })
            }
            TableOptions::GCS => {
                let service_account_key = remove_required_opt(m, "service_account_key")?;
                let bucket = remove_required_opt(m, "bucket")?;
                let location = remove_required_opt(m, "location")?;

                let access = GcsTableAccess {
                    bucket_name: bucket,
                    service_acccount_key_json: service_account_key,
                    location,
                    file_type: None,
                };

                GcsAccessor::validate_table_access(access.clone())
                    .await
                    .map_err(|e| PlanError::InvalidExternalTable {
                        source: Box::new(e),
                    })?;

                TableOptions::Gcs(TableOptionsGcs {
                    service_account_key: access.service_acccount_key_json,
                    bucket: access.bucket_name,
                    location: access.location,
                })
            }
            TableOptions::S3_STORAGE => {
                let access_key_id = remove_required_opt(m, "access_key_id")?;
                let secret_access_key = remove_required_opt(m, "secret_access_key")?;
                let region = remove_required_opt(m, "region")?;
                let bucket = remove_required_opt(m, "bucket")?;
                let location = remove_required_opt(m, "location")?;

                let access = S3TableAccess {
                    region,
                    bucket_name: bucket,
                    access_key_id,
                    secret_access_key,
                    location,
                    file_type: None,
                };

                S3Accessor::validate_table_access(access.clone())
                    .await
                    .map_err(|e| PlanError::InvalidExternalTable {
                        source: Box::new(e),
                    })?;

                TableOptions::S3(TableOptionsS3 {
                    access_key_id: access.access_key_id,
                    secret_access_key: access.secret_access_key,
                    region: access.region,
                    bucket: access.bucket_name,
                    location: access.location,
                })
            }
            TableOptions::DEBUG => {
                datasource_debug::validate_tunnel_connections(tunnel_options.as_ref())?;

                let typ = remove_required_opt(m, "table_type")?;
                let typ = DebugTableType::from_str(&typ)?;

                TableOptions::Debug(TableOptionsDebug {
                    table_type: typ.to_string(),
                })
            }
            other => return Err(internal!("unsupported datasource: {}", other)),
        };

        let table_name = object_name_to_table_ref(stmt.name)?;

        let plan = CreateExternalTable {
            table_name,
            if_not_exists: stmt.if_not_exists,
            table_options: external_table_options,
            tunnel,
        };

        Ok(DdlPlan::CreateExternalTable(plan).into())
    }

    async fn plan_create_tunnel(&self, mut stmt: CreateTunnelStmt) -> Result<LogicalPlan> {
        let m = &mut stmt.options;

        let tunnel_type = normalize_ident(stmt.tunnel);

        let options = match tunnel_type.as_str() {
            TunnelOptions::INTERNAL => TunnelOptions::Internal(TunnelOptionsInternal {}),
            TunnelOptions::DEBUG => TunnelOptions::Debug(TunnelOptionsDebug {}),
            TunnelOptions::SSH => {
                let connection_string = get_ssh_conn_str(m)?;
                let ssh_key = SshKey::generate_random()?;

                TunnelOptions::Ssh(TunnelOptionsSsh {
                    connection_string,
                    ssh_key: ssh_key.to_bytes()?,
                })
            }
            other => return Err(internal!("unsupported tunnel: {other}")),
        };

        let name = normalize_ident(stmt.name);

        let plan = CreateTunnel {
            name,
            options,
            if_not_exists: stmt.if_not_exists,
        };

        Ok(DdlPlan::CreateTunnel(plan).into())
    }

    async fn plan_copy_to(&self, stmt: CopyToStmt) -> Result<LogicalPlan> {
        let source = match stmt.source {
            CopyToSource::Table(table) => unimplemented!(),
            CopyToSource::Query(query) => {
                let statement = ast::Statement::Query(Box::new(query));
                let builder = PlanContextBuilder::new(self.ctx);
                let context_provider = builder.build_plan_context(&statement).await?;
                let planner = SqlToRel::new(&context_provider);

                planner.sql_statement_to_plan(statement)?
            }
        };

        let dest = ObjectStoreSourceUrl::parse(&stmt.dest)?;
        let format: CopyFormat = match stmt.options.get("format") {
            Some(OptionValue::QuotedLiteral(s) | OptionValue::UnquotedKeyword(s)) => s.parse()?,
            Some(v) => return Err(PlanError::UnsupportedOptionValue(v.clone())),
            None => match dest.extension() {
                Some(s) => s.parse()?,
                None => return Err(PlanError::UnableToInferCopyFormat),
            },
        };

        let format_opts = match format {
            CopyFormat::Parquet => {
                let mut opts = ParquetSinkOpts::default();
                match stmt.options.get("row_group_size") {
                    Some(OptionValue::Number(n)) => {
                        let n = n.parse::<usize>()?;
                        opts.row_group_size = n;
                    }
                    Some(v) => return Err(PlanError::UnsupportedOptionValue(v.clone())),
                    None => (),
                }
                CopyFormatOpts::Parquet(opts)
            }
            CopyFormat::Csv => {
                let mut opts = CsvSinkOpts::default();
                if let Some(delim) = stmt.options.get("delim") {
                    match delim {
                        OptionValue::QuotedLiteral(s) => {
                            let bs = s.as_bytes();
                            if bs.len() != 1 {
                                return Err(PlanError::UnsupportedOptionValue(delim.clone()));
                            }
                            opts.delim = bs[0];
                        }
                        other => return Err(PlanError::UnsupportedOptionValue(other.clone())),
                    }
                }

                CopyFormatOpts::Csv(opts)
            }
            CopyFormat::Json => {
                let mut opts = JsonSinkOpts::default();
                if let Some(array) = stmt.options.get("array") {
                    match array {
                        OptionValue::Boolean(b) => opts.array = *b,
                        other => return Err(PlanError::UnsupportedOptionValue(other.clone())),
                    }
                }
                CopyFormatOpts::Json(opts)
            }
        };

        let mut auth_opts = object_store_auth_from_opts(&stmt.options, dest.get_provider())?;
        // Fall back to auth values set for the session.
        match (&auth_opts, dest.get_provider()) {
            (ObjectStoreAuth::None, ObjectStoreProvider::Gcs) => {
                let fallback = self.ctx.get_session_vars().gcs_service_account_key.value();
                if fallback != "" {
                    auth_opts = ObjectStoreAuth::Gcs {
                        service_account_key: fallback.to_string(),
                    }
                }
            }
            _ => (),
        }

        Ok(LogicalPlan::Write(WritePlan::CopyTo(CopyTo {
            source,
            dest,
            format_opts,
            auth_opts,
            partition_by: Vec::new(),
        })))
    }

    async fn plan_statement(&self, statement: ast::Statement) -> Result<LogicalPlan> {
        let builder = PlanContextBuilder::new(self.ctx);
        let context_provider = builder.build_plan_context(&statement).await?;

        let planner = SqlToRel::new(&context_provider);
        match statement {
            ast::Statement::StartTransaction { .. } => Ok(TransactionPlan::Begin.into()),
            ast::Statement::Commit { .. } => Ok(TransactionPlan::Commit.into()),
            ast::Statement::Rollback { .. } => Ok(TransactionPlan::Abort.into()),

            stmt @ ast::Statement::Query(_) => {
                let plan = planner.sql_statement_to_plan(stmt)?;
                Ok(LogicalPlan::Query(plan))
            }

            stmt @ ast::Statement::Explain { .. } => {
                let plan = planner.sql_statement_to_plan(stmt)?;
                Ok(LogicalPlan::Query(plan))
            }

            ast::Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                // TODO: Schema Authorization
                let schema_name = match schema_name {
                    ast::SchemaName::Simple(name) => {
                        validate_object_name(&name)?;
                        object_name_to_schema_ref(name)?
                    }
                    ast::SchemaName::UnnamedAuthorization(ident) => {
                        validate_ident(&ident)?;
                        SchemaReference::Bare {
                            schema: normalize_ident(ident),
                        }
                    }
                    ast::SchemaName::NamedAuthorization(name, ident) => {
                        validate_object_name(&name)?;
                        validate_ident(&ident)?;
                        object_name_to_schema_ref(name)?
                    }
                };

                Ok(DdlPlan::CreateSchema(CreateSchema {
                    schema_name,
                    if_not_exists,
                })
                .into())
            }

            // Normal tables.
            ast::Statement::CreateTable {
                external: false,
                if_not_exists,
                engine: None,
                name,
                columns,
                query: None,
                ..
            } => {
                validate_object_name(&name)?;
                let table_name = object_name_to_table_ref(name)?;

                let mut arrow_cols = Vec::with_capacity(columns.len());
                for column in columns.into_iter() {
                    let dt = convert_data_type(&column.data_type)?;
                    let field = Field::new(&column.name.value, dt, false);
                    arrow_cols.push(field);
                }

                Ok(DdlPlan::CreateTable(CreateTable {
                    table_name,
                    columns: arrow_cols,
                    if_not_exists,
                })
                .into())
            }

            // Tables generated from a source query.
            //
            // CREATE TABLE table2 AS (SELECT * FROM table1);
            ast::Statement::CreateTable {
                external: false,
                name,
                query: Some(query),
                ..
            } => {
                validate_object_name(&name)?;
                let table_name = object_name_to_table_ref(name)?;

                let source = planner.sql_statement_to_plan(ast::Statement::Query(query))?;
                Ok(DdlPlan::CreateTableAs(CreateTableAs { table_name, source }).into())
            }

            // Views
            ast::Statement::CreateView {
                or_replace,
                materialized: false,
                name,
                columns,
                query,
                with_options,
                ..
            } => {
                validate_object_name(&name)?;
                let name = object_name_to_table_ref(name)?;

                if !with_options.is_empty() {
                    return Err(PlanError::UnsupportedFeature("view options"));
                }

                match query.body.as_ref() {
                    ast::SetExpr::Select(select) => select.projection.len(),
                    ast::SetExpr::Values(values) => {
                        values.rows.first().map(|first| first.len()).unwrap_or(0)
                    }
                    _ => {
                        return Err(PlanError::InvalidViewStatement {
                            msg: "view body must either be a SELECT or VALUES statement",
                        })
                    }
                };

                // Check that this is a valid body.
                // TODO: Avoid cloning.
                let input = planner.sql_statement_to_plan(ast::Statement::Query(query.clone()))?;

                let columns: Vec<_> = columns.into_iter().map(normalize_ident).collect();
                // Only validate number of aliases equals number of fields in
                // the ouput if aliases were actually provided.
                if !columns.is_empty() && input.schema().fields().len() != columns.len() {
                    return Err(PlanError::InvalidNumberOfAliasesForView {
                        sql: query.to_string(),
                        aliases: columns,
                    });
                }

                Ok(DdlPlan::CreateView(CreateView {
                    view_name: name,
                    sql: query.to_string(),
                    columns,
                    or_replace,
                })
                .into())
            }

            stmt @ ast::Statement::Insert { .. } => {
                Err(PlanError::UnsupportedSQLStatement(stmt.to_string()))
            }

            ast::Statement::AlterTable {
                name,
                operation: AlterTableOperation::RenameTable { table_name },
            } => {
                validate_object_name(&name)?;
                let name = object_name_to_table_ref(name)?;

                validate_object_name(&table_name)?;
                let new_name = object_name_to_table_ref(table_name)?;

                Ok(DdlPlan::AlterTableRaname(AlterTableRename { name, new_name }).into())
            }

            // Drop tables
            ast::Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                let mut refs = Vec::with_capacity(names.len());
                for name in names.into_iter() {
                    validate_object_name(&name)?;
                    let r = object_name_to_table_ref(name)?;
                    refs.push(r);
                }
                Ok(DdlPlan::DropTables(DropTables {
                    if_exists,
                    names: refs,
                })
                .into())
            }

            // Drop views
            ast::Statement::Drop {
                object_type: ObjectType::View,
                if_exists,
                names,
                ..
            } => {
                let mut refs = Vec::with_capacity(names.len());
                for name in names.into_iter() {
                    validate_object_name(&name)?;
                    let r = object_name_to_table_ref(name)?;
                    refs.push(r);
                }
                Ok(DdlPlan::DropViews(DropViews {
                    if_exists,
                    names: refs,
                })
                .into())
            }

            // Drop schemas
            ast::Statement::Drop {
                object_type: ObjectType::Schema,
                if_exists,
                cascade,
                names,
                ..
            } => {
                let mut refs = Vec::with_capacity(names.len());
                for name in names.into_iter() {
                    validate_object_name(&name)?;
                    let r = object_name_to_schema_ref(name)?;
                    refs.push(r);
                }
                Ok(DdlPlan::DropSchemas(DropSchemas {
                    if_exists,
                    names: refs,
                    cascade,
                })
                .into())
            }

            // "SET ...".
            //
            // NOTE: Only session local variables are supported. Transaction
            // local variables behave the same as session local (they're not
            // reset on transaction abort/commit).
            ast::Statement::SetVariable {
                local: false,
                hivevar: false,
                variable,
                value,
                ..
            } => Ok(VariablePlan::SetVariable(SetVariable {
                variable: variable.to_string(),
                values: value,
            })
            .into()),

            // "SHOW ..."
            //
            // Show the value of a variable.
            ast::Statement::ShowVariable { variable } => {
                // Normalize variables
                let mut variable: Vec<_> = variable.into_iter().map(normalize_ident).collect();

                let variable = if is_show_transaction_isolation_level(&variable) {
                    // SHOW TRANSACTION ISOLATION LEVEL
                    // Alias of "SHOW transaction_isolation".
                    "transaction_isolation".to_string()
                } else if variable.len() != 1 {
                    return Err(internal!(
                        "expecting only one variable to show, found: {variable:?}"
                    ));
                } else {
                    variable.pop().unwrap()
                };

                Ok(VariablePlan::ShowVariable(ShowVariable { variable }).into())
            }

            stmt => Err(PlanError::UnsupportedSQLStatement(stmt.to_string())),
        }
    }

    fn plan_drop_database(&self, stmt: DropDatabaseStmt) -> Result<LogicalPlan> {
        let mut names = Vec::with_capacity(stmt.names.len());
        for name in stmt.names.into_iter() {
            validate_ident(&name)?;
            let name = normalize_ident(name);
            names.push(name);
        }

        Ok(DdlPlan::DropDatabase(DropDatabase {
            names,
            if_exists: stmt.if_exists,
        })
        .into())
    }

    fn plan_drop_tunnel(&self, stmt: DropTunnelStmt) -> Result<LogicalPlan> {
        let mut names = Vec::with_capacity(stmt.names.len());
        for name in stmt.names.into_iter() {
            validate_ident(&name)?;
            let name = normalize_ident(name);
            names.push(name);
        }

        Ok(DdlPlan::DropTunnel(DropTunnel {
            names,
            if_exists: stmt.if_exists,
        })
        .into())
    }

    fn plan_alter_database_rename(&self, stmt: AlterDatabaseRenameStmt) -> Result<LogicalPlan> {
        validate_ident(&stmt.name)?;
        let name = normalize_ident(stmt.name);

        validate_ident(&stmt.new_name)?;
        let new_name = normalize_ident(stmt.new_name);

        Ok(DdlPlan::AlterDatabaseRename(AlterDatabaseRename { name, new_name }).into())
    }

    fn get_tunnel_opts(&self, tunnel: &Option<String>) -> Result<Option<TunnelOptions>> {
        // Check if the tunnel exists, get tunnel options and pass them on for
        // connection validation.
        let tunnel_options = if let Some(tunnel) = &tunnel {
            let ent = self
                .ctx
                .get_session_catalog()
                .resolve_tunnel(tunnel)
                .ok_or(PlanError::InvalidTunnel {
                    tunnel: tunnel.to_owned(),
                    reason: "does not exist".to_string(),
                })?;
            Some(ent.options.clone())
        } else {
            None
        };
        Ok(tunnel_options)
    }
}

/// Resolves an ident (unquoted -> lowercase else case sensitive).
fn normalize_ident(ident: Ident) -> String {
    let normalizer = IdentNormalizer::new(/* normalize = */ true);
    normalizer.normalize(ident)
}

fn object_name_to_table_ref(name: ObjectName) -> Result<OwnedTableReference> {
    let r = object_name_to_table_reference(name, /* enable_normalization = */ true)?;
    Ok(r)
}

fn object_name_to_schema_ref(name: ObjectName) -> Result<SchemaReference> {
    let r = match object_name_to_table_ref(name)? {
        // Table becomes the schema and schema becomes the catalog.
        TableReference::Bare { table } => SchemaReference::Bare {
            schema: table.into_owned(),
        },
        TableReference::Partial { schema, table } => SchemaReference::Full {
            schema: table.into_owned(),
            catalog: schema.into_owned(),
        },
        tr => return Err(internal!("invalid schema object: {tr}")),
    };
    Ok(r)
}

/// Convert a ast data type to an arrow data type.
///
/// NOTE: This and `convert_simple_data_type` were both taken from datafusion's
/// sql planner. These functions were made internal in version 15.0. Light
/// modifications were made to fit our use case.
fn convert_data_type(sql_type: &ast::DataType) -> Result<DataType> {
    match sql_type {
        ast::DataType::Array(Some(inner_sql_type)) => {
            let data_type = convert_simple_data_type(inner_sql_type)?;

            Ok(DataType::List(Arc::new(Field::new(
                "field", data_type, true,
            ))))
        }
        ast::DataType::Array(None) => {
            Err(internal!("Arrays with unspecified type is not supported",))
        }
        other => convert_simple_data_type(other),
    }
}

fn convert_simple_data_type(sql_type: &ast::DataType) -> Result<DataType> {
    match sql_type {
            ast::DataType::Boolean => Ok(DataType::Boolean),
            ast::DataType::TinyInt(_) => Ok(DataType::Int8),
            ast::DataType::SmallInt(_) => Ok(DataType::Int16),
            ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(DataType::Int32),
            ast::DataType::BigInt(_) => Ok(DataType::Int64),
            ast::DataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
            ast::DataType::UnsignedSmallInt(_) => Ok(DataType::UInt16),
            ast::DataType::UnsignedInt(_) | ast::DataType::UnsignedInteger(_) => {
                Ok(DataType::UInt32)
            }
            ast::DataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
            ast::DataType::Float(_) => Ok(DataType::Float32),
            ast::DataType::Real => Ok(DataType::Float32),
            ast::DataType::Double | ast::DataType::DoublePrecision => Ok(DataType::Float64),
            ast::DataType::Char(_)
            | ast::DataType::Varchar(_)
            | ast::DataType::Text
            | ast::DataType::String => Ok(DataType::Utf8),
            ast::DataType::Timestamp(None, tz_info) => {
                let tz = if matches!(tz_info, ast::TimezoneInfo::Tz)
                    || matches!(tz_info, ast::TimezoneInfo::WithTimeZone)
                {
                    // Timestamp With Time Zone
                    // INPUT : [ast::DataType]   TimestampTz + [RuntimeConfig] Time Zone
                    // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Some(Time Zone)>
                    return Err(internal!("setting timezone unsupported"))
                } else {
                    // Timestamp Without Time zone
                    None
                };
                Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz))
            }
            ast::DataType::Date => Ok(DataType::Date32),
            ast::DataType::Time(None, tz_info) => {
                if matches!(tz_info, ast::TimezoneInfo::None)
                    || matches!(tz_info, ast::TimezoneInfo::WithoutTimeZone)
                {
                    Ok(DataType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We dont support TIMETZ and TIME WITH TIME ZONE for now
                    Err(internal!(
                        "Unsupported SQL type {:?}",
                        sql_type
                    ))
                }
            }
            ast::DataType::Numeric(exact_number_info)
            |ast::DataType::Decimal(exact_number_info) => {
                let (precision, scale) = match *exact_number_info {
                    ast::ExactNumberInfo::None => (None, None),
                    ast::ExactNumberInfo::Precision(precision) => (Some(precision), None),
                    ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                        (Some(precision), Some(scale))
                    }
                };
                make_decimal_type(precision, scale)
            }
            ast::DataType::Bytea => Ok(DataType::Binary),
            // Explicitly list all other types so that if sqlparser
            // adds/changes the `ast::DataType` the compiler will tell us on upgrade
            // and avoid bugs like https://github.com/apache/arrow-datafusion/issues/3059
            ast::DataType::Nvarchar(_)
            | ast::DataType::JSON
            | ast::DataType::Uuid
            | ast::DataType::Binary(_)
            | ast::DataType::Varbinary(_)
            | ast::DataType::Blob(_)
            | ast::DataType::Datetime(_)
            | ast::DataType::Interval
            | ast::DataType::Regclass
            | ast::DataType::Custom(_, _)
            | ast::DataType::Array(_)
            | ast::DataType::Enum(_)
            | ast::DataType::Set(_)
            | ast::DataType::MediumInt(_)
            | ast::DataType::UnsignedMediumInt(_)
            | ast::DataType::Character(_)
            | ast::DataType::CharacterVarying(_)
            | ast::DataType::CharVarying(_)
            | ast::DataType::CharacterLargeObject(_)
            | ast::DataType::CharLargeObject(_)
            // precision is not supported
            | ast::DataType::Timestamp(Some(_), _)
            // precision is not supported
            | ast::DataType::Time(Some(_), _)
            | ast::DataType::Dec(_)
            | ast::DataType::BigNumeric(_)
            | ast::DataType::BigDecimal(_)
            | ast::DataType::Clob(_) => Err(internal!(
                "Unsupported SQL type {:?}",
                sql_type
            )),
        }
}

fn get_pg_conn_str(m: &mut BTreeMap<String, OptionValue>) -> Result<String> {
    let conn = match remove_optional_opt(m, "connection_string")? {
        Some(conn_str) => PostgresDbConnection::ConnectionString(conn_str),
        None => {
            let host = remove_required_opt(m, "host")?;
            let port = match remove_optional_opt(m, "port")? {
                Some(p) => {
                    let p: u16 = p.parse()?;
                    Some(p)
                }
                None => None,
            };
            let user = remove_required_opt(m, "user")?;
            let password = remove_optional_opt(m, "password")?;
            let database = remove_required_opt(m, "database")?;
            PostgresDbConnection::Parameters {
                host,
                port,
                user,
                password,
                database,
            }
        }
    };

    Ok(conn.connection_string())
}

fn get_mysql_conn_str(m: &mut BTreeMap<String, OptionValue>) -> Result<String> {
    let conn = match remove_optional_opt(m, "connection_string")? {
        Some(conn_str) => MysqlDbConnection::ConnectionString(conn_str),
        None => {
            let host = remove_required_opt(m, "host")?;
            let port = match remove_optional_opt(m, "port")? {
                Some(p) => {
                    let p: u16 = p.parse()?;
                    Some(p)
                }
                None => None,
            };
            let user = remove_required_opt(m, "user")?;
            let password = remove_optional_opt(m, "password")?;
            let database = remove_required_opt(m, "database")?;
            MysqlDbConnection::Parameters {
                host,
                port,
                user,
                password,
                database,
            }
        }
    };

    Ok(conn.connection_string())
}

fn get_mongo_conn_str(m: &mut BTreeMap<String, OptionValue>) -> Result<String> {
    let conn = match remove_optional_opt(m, "connection_string")? {
        Some(conn_str) => MongoDbConnection::ConnectionString(conn_str),
        None => {
            let protocol: MongoProtocol = match remove_optional_opt(m, "protocol")? {
                Some(p) => p
                    .parse()
                    .map_err(|e| internal!("Cannot parse mongo protocol: {e}"))?,
                None => Default::default(),
            };
            let host = remove_required_opt(m, "host")?;
            let port = match remove_optional_opt(m, "port")? {
                Some(p) => {
                    let p: u16 = p.parse()?;
                    Some(p)
                }
                None => None,
            };
            let user = remove_required_opt(m, "user")?;
            let password = remove_optional_opt(m, "password")?;
            MongoDbConnection::Parameters {
                protocol,
                host,
                port,
                user,
                password,
            }
        }
    };

    Ok(conn.connection_string())
}

fn get_ssh_conn_str(m: &mut BTreeMap<String, OptionValue>) -> Result<String> {
    let conn = match remove_optional_opt(m, "connection_string")? {
        Some(conn_str) => SshConnection::ConnectionString(conn_str),
        None => {
            let host = remove_required_opt(m, "host")?;
            let port = match remove_optional_opt(m, "port")? {
                Some(p) => {
                    let p: u16 = p.parse()?;
                    Some(p)
                }
                None => None,
            };
            let user = remove_required_opt(m, "user")?;
            SshConnection::Parameters(SshConnectionParameters { host, port, user })
        }
    };
    Ok(conn.connection_string())
}

/// Returns a validated `DataType` for the specified precision and
/// scale
fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => {
            return Err(internal!("Cannot specify only scale for decimal data type",))
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Arrow decimal is i128 meaning 38 maximum decimal digits
    if precision == 0 || precision > DECIMAL128_MAX_PRECISION || scale.unsigned_abs() > precision {
        Err(internal!(
            "Decimal(precision = {}, scale = {}) should satisfy `0 < precision <= 38`, and `scale <= precision`.",
            precision, scale
        ))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

/// If the "SHOW ..." statement equivalent to "SHOW TRANSACTION ISOLATION
/// LEVEL", return the variable for which to show the value.
fn is_show_transaction_isolation_level(variable: &[String]) -> bool {
    const TRANSACTION_ISOLATION_LEVEL_STMT: [&str; 3] = ["transaction", "isolation", "level"];
    variable.iter().eq(TRANSACTION_ISOLATION_LEVEL_STMT.iter())
}

/// Get authentication options depending on the object store provider.
fn object_store_auth_from_opts(
    m: &BTreeMap<String, OptionValue>,
    prov: ObjectStoreProvider,
) -> Result<ObjectStoreAuth> {
    let auth = match prov {
        ObjectStoreProvider::Gcs => match m.get("service_account_key") {
            Some(v) => ObjectStoreAuth::Gcs {
                service_account_key: v.to_string(),
            },
            None => ObjectStoreAuth::None,
        },
        ObjectStoreProvider::S3 => match (m.get("access_key_id"), m.get("secret_access_key")) {
            (Some(id), Some(secret)) => ObjectStoreAuth::S3 {
                access_key_id: id.to_string(),
                secret_access_key: secret.to_string(),
            },
            (Some(_), None) => {
                return Err(PlanError::MissingRequiredOption(
                    "secret_access_key".to_string(),
                ))
            }
            (None, Some(_)) => {
                return Err(PlanError::MissingRequiredOption(
                    "access_key_id".to_string(),
                ))
            }
            _ => ObjectStoreAuth::None,
        },
        ObjectStoreProvider::File => ObjectStoreAuth::None,
    };

    Ok(auth)
}

fn remove_required_opt(m: &mut BTreeMap<String, OptionValue>, k: &str) -> Result<String> {
    let opt = remove_optional_opt(m, k)?;
    opt.ok_or_else(|| PlanError::MissingRequiredOption(k.to_string()))
}

fn remove_optional_opt(m: &mut BTreeMap<String, OptionValue>, k: &str) -> Result<Option<String>> {
    let val = match m.remove(k) {
        Some(v) => v,
        None => return Ok(None),
    };

    fn get_env(k: &str, upper: bool) -> Result<String> {
        let key = format!("glaredb_secret_{k}");
        let key = if upper {
            key.to_uppercase()
        } else {
            key.to_lowercase()
        };
        env::var(key).map_err(|_e| internal!("invalid secret '{k}'"))
    }

    let opt = match val {
        OptionValue::QuotedLiteral(l) => l,
        OptionValue::Secret(s) => {
            if let Ok(opt) = get_env(&s, /* uppercase: */ true) {
                opt
            } else {
                get_env(&s, /* uppercase: */ false)?
            }
        }
        other => return Err(PlanError::UnsupportedOptionValue(other)),
    };

    Ok(Some(opt))
}
