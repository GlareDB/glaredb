use crate::context::SessionContext;
use crate::parser::options::StmtOptions;
use crate::parser::{
    validate_ident, validate_object_name, AlterDatabaseRenameStmt, AlterTunnelAction,
    AlterTunnelStmt, CopyToSource, CopyToStmt, CreateCredentialsStmt, CreateExternalDatabaseStmt,
    CreateExternalTableStmt, CreateTunnelStmt, DropCredentialsStmt, DropDatabaseStmt,
    DropTunnelStmt, StatementWithExtensions,
};
use crate::planner::errors::{internal, PlanError, Result};
use crate::planner::logical_plan::*;
use crate::planner::preprocess::{preprocess, CastRegclassReplacer, EscapedStringToDoubleQuoted};
use datafusion::arrow::datatypes::{
    DataType, Field, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion::common::OwnedTableReference;
use datafusion::sql::planner::{object_name_to_table_reference, IdentNormalizer};
use datafusion::sql::sqlparser::ast::AlterTableOperation;
use datafusion::sql::sqlparser::ast::{self, Ident, ObjectName, ObjectType};
use datafusion::sql::TableReference;
use datafusion_planner::planner::SqlQueryPlanner;
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasources::common::ssh::{key::SshKey, SshConnection, SshConnectionParameters};
use datasources::debug::DebugTableType;
use datasources::delta::access::DeltaLakeAccessor;
use datasources::mongodb::{MongoAccessor, MongoDbConnection};
use datasources::mysql::{MysqlAccessor, MysqlDbConnection, MysqlTableAccess};
use datasources::object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasources::object_store::local::{LocalAccessor, LocalTableAccess};
use datasources::object_store::s3::{S3Accessor, S3TableAccess};
use datasources::postgres::{PostgresAccessor, PostgresDbConnection, PostgresTableAccess};
use datasources::snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use metastore::types::{
    CopyToDestinationOptions, CopyToDestinationOptionsGcs, CopyToDestinationOptionsLocal,
    CopyToDestinationOptionsS3, CopyToFormatOptions, CopyToFormatOptionsCsv,
    CopyToFormatOptionsJson, CopyToFormatOptionsParquet,
};
use metastore::validation::{
    validate_copyto_dest_creds_support, validate_copyto_dest_format_support,
    validate_database_creds_support, validate_database_tunnel_support,
    validate_table_creds_support, validate_table_tunnel_support,
};
use metastoreproto::types::options::{
    CredentialsOptions, CredentialsOptionsAws, CredentialsOptionsDebug, CredentialsOptionsGcp,
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsDebug, DatabaseOptionsDeltaLake,
    DatabaseOptionsMongo, DatabaseOptionsMysql, DatabaseOptionsPostgres, DatabaseOptionsSnowflake,
    DeltaLakeCatalog, DeltaLakeUnityCatalog, InternalColumnDefinition, TableOptions,
    TableOptionsBigQuery, TableOptionsDebug, TableOptionsGcs, TableOptionsInternal,
    TableOptionsLocal, TableOptionsMongo, TableOptionsMysql, TableOptionsPostgres, TableOptionsS3,
    TableOptionsSnowflake, TunnelOptions, TunnelOptionsDebug, TunnelOptionsInternal,
    TunnelOptionsSsh,
};
use std::str::FromStr;
use std::sync::Arc;
use tracing::debug;

use super::context_builder::PartialContextProvider;

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
            StatementWithExtensions::CreateTunnel(stmt) => self.plan_create_tunnel(stmt),
            StatementWithExtensions::DropTunnel(stmt) => self.plan_drop_tunnel(stmt),
            StatementWithExtensions::AlterTunnel(stmt) => self.plan_alter_tunnel(stmt),
            StatementWithExtensions::CreateCredentials(stmt) => self.plan_create_credentials(stmt),
            StatementWithExtensions::DropCredentials(stmt) => self.plan_drop_credentials(stmt),
            StatementWithExtensions::CopyTo(stmt) => self.plan_copy_to(stmt).await,
        }
    }

    async fn plan_create_external_database(
        &self,
        mut stmt: CreateExternalDatabaseStmt,
    ) -> Result<LogicalPlan> {
        let datasource = normalize_ident(stmt.datasource);

        let tunnel = stmt.tunnel.map(normalize_ident);
        let tunnel_options = self.get_tunnel_opts(&tunnel)?;
        if let Some(tunnel_options) = &tunnel_options {
            // Validate if the tunnel type is supported by the datasource
            validate_database_tunnel_support(&datasource, tunnel_options.as_str()).map_err(
                |e| PlanError::InvalidExternalDatabase {
                    source: Box::new(e),
                },
            )?;
        }

        let creds = stmt.credentials.map(normalize_ident);
        let creds_options = self.get_credentials_opts(&creds)?;
        if let Some(creds_options) = &creds_options {
            validate_database_creds_support(&datasource, creds_options.as_str()).map_err(|e| {
                PlanError::InvalidExternalDatabase {
                    source: Box::new(e),
                }
            })?;
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
                let creds = creds_options.as_ref().map(|c| match c {
                    CredentialsOptions::Gcp(c) => c,
                    other => unreachable!("invalid credentials {other} for bigquery"),
                });

                let service_account_key =
                    m.remove_required_or_creds("service_account_key", creds, |c| {
                        c.service_account_key.clone()
                    })?;

                let project_id: String = m.remove_required("project_id")?;

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
                let account_name: String = m.remove_required("account")?;
                let login_name: String = m.remove_required("username")?;
                let password: String = m.remove_required("password")?;
                let database_name: String = m.remove_required("database")?;
                let warehouse: String = m.remove_required("warehouse")?;
                let role_name: Option<String> = m.remove_optional("role")?;
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
            DatabaseOptions::DELTA => {
                let access_key_id: String = m.remove_required("access_key_id")?;
                let secret_access_key: String = m.remove_required("secret_access_key")?;
                let region: String = m.remove_required("region")?;

                let catalog = match m.remove_required::<String>("catalog_type")?.as_str() {
                    "unity" => DeltaLakeCatalog::Unity(DeltaLakeUnityCatalog {
                        catalog_id: m.remove_required("catalog_id")?,
                        databricks_access_token: m.remove_required("access_token")?,
                        workspace_url: m.remove_required("workspace_url")?,
                    }),
                    other => return Err(internal!("Unknown catalog type: {}", other)),
                };

                // Try connecting to validate.
                DeltaLakeAccessor::connect(&catalog, &access_key_id, &secret_access_key, &region)
                    .await
                    .map_err(|e| PlanError::InvalidExternalDatabase {
                        source: Box::new(e),
                    })?;

                DatabaseOptions::Delta(DatabaseOptionsDeltaLake {
                    catalog,
                    access_key_id,
                    secret_access_key,
                    region,
                })
            }
            DatabaseOptions::DEBUG => {
                datasources::debug::validate_tunnel_connections(tunnel_options.as_ref())?;
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
        let datasource = normalize_ident(stmt.datasource);

        let tunnel = stmt.tunnel.map(normalize_ident);
        let tunnel_options = self.get_tunnel_opts(&tunnel)?;
        if let Some(tunnel_options) = &tunnel_options {
            // Validate if the tunnel type is supported by the datasource
            validate_table_tunnel_support(&datasource, tunnel_options.as_str()).map_err(|e| {
                PlanError::InvalidExternalTable {
                    source: Box::new(e),
                }
            })?;
        }

        let creds = stmt.credentials.map(normalize_ident);
        let creds_options = self.get_credentials_opts(&creds)?;
        if let Some(creds_options) = &creds_options {
            validate_table_creds_support(&datasource, creds_options.as_str()).map_err(|e| {
                PlanError::InvalidExternalTable {
                    source: Box::new(e),
                }
            })?;
        }

        let m = &mut stmt.options;

        let external_table_options = match datasource.as_str() {
            TableOptions::POSTGRES => {
                let connection_string = get_pg_conn_str(m)?;
                let schema = m.remove_required("schema")?;
                let table = m.remove_required("table")?;

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
                let creds = creds_options.as_ref().map(|c| match c {
                    CredentialsOptions::Gcp(c) => c,
                    other => unreachable!("invalid credentials {other} for bigquery"),
                });

                let service_account_key =
                    m.remove_required_or_creds("service_account_key", creds, |c| {
                        c.service_account_key.clone()
                    })?;

                let project_id: String = m.remove_required("project_id")?;
                let dataset_id = m.remove_required("dataset_id")?;
                let table_id = m.remove_required("table_id")?;

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
                let schema = m.remove_required("schema")?;
                let table = m.remove_required("table")?;

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
                let database = m.remove_required("database")?;
                let collection = m.remove_required("collection")?;

                TableOptions::Mongo(TableOptionsMongo {
                    connection_string,
                    database,
                    collection,
                })
            }
            TableOptions::SNOWFLAKE => {
                let account_name: String = m.remove_required("account")?;
                let login_name: String = m.remove_required("username")?;
                let password: String = m.remove_required("password")?;
                let database_name: String = m.remove_required("database")?;
                let warehouse: String = m.remove_required("warehouse")?;
                let role_name: Option<String> = m.remove_optional("role")?;
                let schema_name: String = m.remove_required("schema")?;
                let table_name: String = m.remove_required("table")?;

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
                let location: String = m.remove_required("location")?;

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
                let creds = creds_options.as_ref().map(|c| match c {
                    CredentialsOptions::Gcp(c) => c,
                    other => unreachable!("invalid credentials {other} for google cloud storage"),
                });

                let service_account_key =
                    m.remove_optional_or_creds("service_account_key", creds, |c| {
                        c.service_account_key.clone()
                    })?;

                let bucket = m.remove_required("bucket")?;
                let location = m.remove_required("location")?;

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
                let creds = creds_options.as_ref().map(|c| match c {
                    CredentialsOptions::Aws(c) => c,
                    other => unreachable!("invalid credentials {other} for aws s3"),
                });

                let access_key_id = m.remove_optional_or_creds("access_key_id", creds, |c| {
                    c.access_key_id.clone()
                })?;

                let secret_access_key =
                    m.remove_optional_or_creds("secret_access_key", creds, |c| {
                        c.secret_access_key.clone()
                    })?;

                let region = m.remove_required("region")?;
                let bucket = m.remove_required("bucket")?;
                let location = m.remove_required("location")?;

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
                datasources::debug::validate_tunnel_connections(tunnel_options.as_ref())?;

                let creds = creds_options.as_ref().map(|c| match c {
                    CredentialsOptions::Debug(c) => c,
                    other => unreachable!("invalid credentials {other} for debug datasource"),
                });

                let typ =
                    m.remove_required_or_creds("table_type", creds, |c| c.table_type.clone())?;
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

    fn plan_create_tunnel(&self, mut stmt: CreateTunnelStmt) -> Result<LogicalPlan> {
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

    fn plan_create_credentials(&self, mut stmt: CreateCredentialsStmt) -> Result<LogicalPlan> {
        let m = &mut stmt.options;

        let provider = normalize_ident(stmt.provider);

        let options = match provider.as_str() {
            CredentialsOptions::DEBUG => {
                let table_type: DebugTableType = m.remove_required("table_type")?;
                CredentialsOptions::Debug(CredentialsOptionsDebug {
                    table_type: table_type.to_string(),
                })
            }
            CredentialsOptions::GCP => {
                let service_account_key = m.remove_required("service_account_key")?;
                CredentialsOptions::Gcp(CredentialsOptionsGcp {
                    service_account_key,
                })
            }
            CredentialsOptions::AWS => {
                let access_key_id = m.remove_required("access_key_id")?;
                let secret_access_key = m.remove_required("secret_access_key")?;
                CredentialsOptions::Aws(CredentialsOptionsAws {
                    access_key_id,
                    secret_access_key,
                })
            }
            other => return Err(internal!("unsupported credentials provider: {other}")),
        };

        let name = normalize_ident(stmt.name);

        let plan = CreateCredentials {
            name,
            options,
            comment: stmt.comment,
        };

        Ok(DdlPlan::CreateCredentials(plan).into())
    }

    async fn plan_statement(&self, statement: ast::Statement) -> Result<LogicalPlan> {
        let mut context_provider = PartialContextProvider::new(self.ctx)?;
        let mut planner = SqlQueryPlanner::new(&mut context_provider);
        match statement {
            ast::Statement::StartTransaction { .. } => Ok(TransactionPlan::Begin.into()),
            ast::Statement::Commit { .. } => Ok(TransactionPlan::Commit.into()),
            ast::Statement::Rollback { .. } => Ok(TransactionPlan::Abort.into()),

            ast::Statement::Query(q) => {
                let plan = planner.query_to_plan(*q).await?;
                Ok(LogicalPlan::Query(plan))
            }

            ast::Statement::Explain {
                verbose,
                statement,
                analyze,
                ..
            } => {
                let plan = planner
                    .explain_statement_to_plan(verbose, analyze, *statement)
                    .await?;
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
                temporary,
                ..
            } => {
                validate_object_name(&name)?;
                let table_name = object_name_to_table_ref(name)?;

                let mut arrow_cols = Vec::with_capacity(columns.len());
                for column in columns.into_iter() {
                    let dt = convert_data_type(&column.data_type)?;
                    let field = Field::new(&column.name.value, dt, /* nullable = */ true);
                    arrow_cols.push(field);
                }

                if temporary {
                    let table_name = match table_name {
                        TableReference::Bare { table } => table.into_owned(),
                        _ => return Err(internal!("cannot specify schema with temporary tables")),
                    };
                    Ok(DdlPlan::CreateTempTable(CreateTempTable {
                        table_name,
                        columns: arrow_cols,
                        if_not_exists,
                    })
                    .into())
                } else {
                    let opts = TableOptionsInternal {
                        columns: InternalColumnDefinition::from_arrow_fields(arrow_cols),
                    };
                    Ok(DdlPlan::CreateTable(CreateTable {
                        table_name: table_name.to_owned_reference(),
                        table_options: opts,
                        if_not_exists,
                    })
                    .into())
                }
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

                let source = planner.query_to_plan(*query).await?;
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

                let query_string = query.to_string();

                // Check that this is a valid body.
                // TODO: Avoid cloning.
                let input = planner.query_to_plan(*query).await?;

                let columns: Vec<_> = columns.into_iter().map(normalize_ident).collect();
                // Only validate number of aliases equals number of fields in
                // the ouput if aliases were actually provided.
                if !columns.is_empty() && input.schema().fields().len() != columns.len() {
                    Err(PlanError::InvalidNumberOfAliasesForView {
                        sql: query_string,
                        aliases: columns,
                    })
                } else {
                    Ok(DdlPlan::CreateView(CreateView {
                        view_name: name,
                        sql: query_string,
                        columns,
                        or_replace,
                    })
                    .into())
                }
            }

            ast::Statement::Insert {
                or: None,
                into: _,
                table_name,
                columns,
                overwrite: false,
                source,
                partitioned: None,
                after_columns,
                table: false,
                on: None,
                returning: None,
            } if after_columns.is_empty() => {
                validate_object_name(&table_name)?;
                let table_name = object_name_to_table_ref(table_name)?;

                let columns = columns
                    .into_iter()
                    .map(|col| {
                        validate_ident(&col)?;
                        Ok(normalize_ident(col))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let source = planner
                    .insert_to_source_plan(&table_name, &columns, source)
                    .await?;

                // This provider should be available in the context provider
                // cache since we have successfully generated the insert plan.
                //
                // TODO: Get rid of context_provider when moving DF code into
                // sqlexec. Will get rid of a lot of weirdness.
                let table_provider = context_provider
                    .table_provider(table_name)
                    .ok_or(internal!("unable to get table provider to insert into"))?;

                Ok(WritePlan::Insert(Insert {
                    table_provider,
                    source,
                })
                .into())
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

    fn plan_drop_credentials(&self, stmt: DropCredentialsStmt) -> Result<LogicalPlan> {
        let mut names = Vec::with_capacity(stmt.names.len());
        for name in stmt.names.into_iter() {
            validate_ident(&name)?;
            let name = normalize_ident(name);
            names.push(name);
        }

        Ok(DdlPlan::DropCredentials(DropCredentials {
            names,
            if_exists: stmt.if_exists,
        })
        .into())
    }

    fn plan_alter_tunnel(&self, stmt: AlterTunnelStmt) -> Result<LogicalPlan> {
        validate_ident(&stmt.name)?;
        let name = normalize_ident(stmt.name);

        let plan = match stmt.action {
            AlterTunnelAction::RotateKeys => {
                let new_ssh_key = SshKey::generate_random()?;
                let new_ssh_key = new_ssh_key.to_bytes()?;
                DdlPlan::AlterTunnelRotateKeys(AlterTunnelRotateKeys {
                    name,
                    if_exists: stmt.if_exists,
                    new_ssh_key,
                })
            }
        };

        Ok(plan.into())
    }

    fn plan_alter_database_rename(&self, stmt: AlterDatabaseRenameStmt) -> Result<LogicalPlan> {
        validate_ident(&stmt.name)?;
        let name = normalize_ident(stmt.name);

        validate_ident(&stmt.new_name)?;
        let new_name = normalize_ident(stmt.new_name);

        Ok(DdlPlan::AlterDatabaseRename(AlterDatabaseRename { name, new_name }).into())
    }

    async fn plan_copy_to(&self, stmt: CopyToStmt) -> Result<LogicalPlan> {
        let query = match stmt.source {
            CopyToSource::Table(table) => {
                validate_object_name(&table)?;
                let _table_ref = object_name_to_table_ref(table)?;
                return Err(PlanError::UnsupportedFeature(
                    "COPY .. TO .. with table source",
                ));
            }
            CopyToSource::Query(query) => query,
        };

        let mut context_provider = PartialContextProvider::new(self.ctx)?;
        let mut planner = SqlQueryPlanner::new(&mut context_provider);
        let source = planner.query_to_plan(query).await?;

        let mut m = stmt.options;

        validate_ident(&stmt.dest)?;
        let dest = normalize_ident(stmt.dest);

        let creds = stmt.credentials.map(normalize_ident);
        let creds_options = self.get_credentials_opts(&creds)?;
        if let Some(creds_options) = &creds_options {
            validate_copyto_dest_creds_support(&dest, creds_options.as_str()).map_err(|e| {
                PlanError::InvalidExternalTable {
                    source: Box::new(e),
                }
            })?;
        }

        let dest = match dest.as_str() {
            CopyToDestinationOptions::LOCAL => {
                let location: String = m.remove_required("location")?;
                CopyToDestinationOptions::Local(CopyToDestinationOptionsLocal { location })
            }
            CopyToDestinationOptions::GCS => {
                let creds = creds_options.as_ref().map(|c| match c {
                    CredentialsOptions::Gcp(c) => c,
                    other => unreachable!("Invalid credentials {other} to copy into gcs"),
                });

                let service_account_key =
                    m.remove_optional_or_creds("service_account_key", creds, |c| {
                        c.service_account_key.clone()
                    })?;

                let bucket: String = m.remove_required("bucket")?;
                let location: String = m.remove_required("location")?;

                CopyToDestinationOptions::Gcs(CopyToDestinationOptionsGcs {
                    service_account_key,
                    bucket,
                    location,
                })
            }
            CopyToDestinationOptions::S3_STORAGE => {
                let creds = creds_options.as_ref().map(|c| match c {
                    CredentialsOptions::Aws(c) => c,
                    other => unreachable!("invalid credentials {other} for aws s3"),
                });

                let access_key_id = m.remove_optional_or_creds("access_key_id", creds, |c| {
                    c.access_key_id.clone()
                })?;

                let secret_access_key =
                    m.remove_optional_or_creds("secret_access_key", creds, |c| {
                        c.secret_access_key.clone()
                    })?;

                let region = m.remove_required("region")?;
                let bucket = m.remove_required("bucket")?;
                let location = m.remove_required("location")?;

                CopyToDestinationOptions::S3(CopyToDestinationOptionsS3 {
                    access_key_id,
                    secret_access_key,
                    region,
                    bucket,
                    location,
                })
            }
            // TODO: Parse "other" as a URL.
            other => return Err(internal!("unsupported datasource: {}", other)),
        };

        let format = match stmt.format.as_ref().map(|f| f.value.as_str()) {
            None => {
                // TODO: Choose the default based on "destination".
                CopyToFormatOptions::default()
            }
            Some(CopyToFormatOptions::CSV) => {
                let delim = m.remove_optional::<char>("delimeter")?.unwrap_or(',');
                let header = m.remove_optional::<bool>("header")?.unwrap_or(true);
                CopyToFormatOptions::Csv(CopyToFormatOptionsCsv {
                    delim: delim as u8,
                    header,
                })
            }
            Some(CopyToFormatOptions::PARQUET) => {
                let row_group_size = m
                    .remove_optional::<usize>("row_group_size")?
                    .unwrap_or(122880);
                CopyToFormatOptions::Parquet(CopyToFormatOptionsParquet { row_group_size })
            }
            Some(CopyToFormatOptions::JSON) => {
                let array = m.remove_optional::<bool>("array")?.unwrap_or(false);
                CopyToFormatOptions::Json(CopyToFormatOptionsJson { array })
            }
            Some(other) => return Err(internal!("unsupported output format: {other}")),
        };

        validate_copyto_dest_format_support(dest.as_str(), format.as_str()).map_err(|e| {
            PlanError::InvalidExternalTable {
                source: Box::new(e),
            }
        })?;

        Ok(WritePlan::CopyTo(CopyTo {
            source,
            dest,
            format,
        })
        .into())
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

    fn get_credentials_opts(
        &self,
        credentials: &Option<String>,
    ) -> Result<Option<CredentialsOptions>> {
        // Check if the credentials exists, get credentials options and pass
        // them on for connection validation.
        let credentials_options = if let Some(credentials) = &credentials {
            let ent = self
                .ctx
                .get_session_catalog()
                .resolve_credentials(credentials)
                .ok_or(PlanError::InvalidCredentials {
                    credentials: credentials.to_owned(),
                    reason: "does not exist".to_string(),
                })?;
            Some(ent.options.clone())
        } else {
            None
        };
        Ok(credentials_options)
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

fn get_pg_conn_str(m: &mut StmtOptions) -> Result<String> {
    let conn = match m.remove_optional("connection_string")? {
        Some(conn_str) => PostgresDbConnection::ConnectionString(conn_str),
        None => {
            let host = m.remove_required("host")?;
            let port = m.remove_optional("port")?;
            let user = m.remove_required("user")?;
            let password = m.remove_optional("password")?;
            let database = m.remove_required("database")?;
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

fn get_mysql_conn_str(m: &mut StmtOptions) -> Result<String> {
    let conn = match m.remove_optional("connection_string")? {
        Some(conn_str) => MysqlDbConnection::ConnectionString(conn_str),
        None => {
            let host = m.remove_required("host")?;
            let port = m.remove_optional("port")?;
            let user = m.remove_required("user")?;
            let password = m.remove_optional("password")?;
            let database = m.remove_required("database")?;
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

fn get_mongo_conn_str(m: &mut StmtOptions) -> Result<String> {
    let conn = match m.remove_optional("connection_string")? {
        Some(conn_str) => MongoDbConnection::ConnectionString(conn_str),
        None => {
            let protocol = m.remove_optional("protocol")?.unwrap_or_default();
            let host = m.remove_required("host")?;
            let port = m.remove_optional("port")?;
            let user = m.remove_required("user")?;
            let password = m.remove_optional("password")?;
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

fn get_ssh_conn_str(m: &mut StmtOptions) -> Result<String> {
    let conn = match m.remove_optional("connection_string")? {
        Some(conn_str) => SshConnection::ConnectionString(conn_str),
        None => {
            let host = m.remove_required("host")?;
            let port = m.remove_optional("port")?;
            let user = m.remove_required("user")?;
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
