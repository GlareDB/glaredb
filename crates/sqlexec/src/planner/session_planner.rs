use crate::context::SessionContext;
use crate::parser::{
    validate_ident, validate_object_name, AlterDatabaseRenameStmt, CreateExternalDatabaseStmt,
    CreateExternalTableStmt, DropDatabaseStmt, OptionValue, StatementWithExtensions,
};
use crate::planner::context_builder::PlanContextBuilder;
use crate::planner::errors::{internal, PlanError, Result};
use crate::planner::logical_plan::*;
use crate::planner::preprocess::{preprocess, CastRegclassReplacer, EscapedStringToDoubleQuoted};
use datafusion::arrow::datatypes::{
    DataType, Field, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{self, Ident, ObjectType};
use datasource_bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasource_debug::DebugTableType;
use datasource_mongodb::{MongoDbConnection, MongoProtocol};
use datasource_mysql::{MysqlAccessor, MysqlDbConnection, MysqlTableAccess};
use datasource_object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasource_object_store::local::{LocalAccessor, LocalTableAccess};
use datasource_object_store::s3::{S3Accessor, S3TableAccess};
use datasource_postgres::{PostgresAccessor, PostgresDbConnection, PostgresTableAccess};
use datasource_snowflake::{SnowflakeAccessor, SnowflakeDbConnection, SnowflakeTableAccess};
use metastore::types::options::{
    DatabaseOptions, DatabaseOptionsBigQuery, DatabaseOptionsDebug, DatabaseOptionsMongo,
    DatabaseOptionsMysql, DatabaseOptionsPostgres, DatabaseOptionsSnowflake, TableOptions,
    TableOptionsBigQuery, TableOptionsDebug, TableOptionsGcs, TableOptionsLocal, TableOptionsMongo,
    TableOptionsMysql, TableOptionsPostgres, TableOptionsS3, TableOptionsSnowflake,
};
use sqlparser::ast::AlterTableOperation;
use std::collections::BTreeMap;
use std::env;
use std::str::FromStr;
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
        }
    }

    async fn plan_create_external_database(
        &self,
        mut stmt: CreateExternalDatabaseStmt,
    ) -> Result<LogicalPlan> {
        let m = &mut stmt.options;

        let db_options = match stmt.datasource.to_lowercase().as_str() {
            DatabaseOptions::POSTGRES => {
                let connection_string = get_pg_conn_str(m)?;
                PostgresAccessor::validate_external_database(&connection_string, None)
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
                MysqlAccessor::validate_external_database(&connection_string, None)
                    .await
                    .map_err(|e| PlanError::InvalidExternalDatabase {
                        source: Box::new(e),
                    })?;
                DatabaseOptions::Mysql(DatabaseOptionsMysql { connection_string })
            }
            DatabaseOptions::MONGO => {
                let connection_string = get_mongo_conn_str(m)?;
                // TODO: Validate external db connection
                DatabaseOptions::Mongo(DatabaseOptionsMongo { connection_string })
            }
            DatabaseOptions::SNOWFLAKE => {
                let account_name = remove_required_opt(m, "account_name")?;
                let login_name = remove_required_opt(m, "login_name")?;
                let password = remove_required_opt(m, "password")?;
                let database_name = remove_required_opt(m, "database_name")?;
                let warehouse = remove_required_opt(m, "warehouse")?;
                let role_name = remove_optional_opt(m, "role_name")?.unwrap_or_default();
                DatabaseOptions::Snowflake(DatabaseOptionsSnowflake {
                    account_name,
                    login_name,
                    password,
                    database_name,
                    warehouse,
                    role_name,
                })
            }
            DatabaseOptions::DEBUG => DatabaseOptions::Debug(DatabaseOptionsDebug {}),
            other => return Err(internal!("unsupported datasource: {}", other)),
        };

        let plan = CreateExternalDatabase {
            database_name: stmt.name,
            if_not_exists: stmt.if_not_exists,
            options: db_options,
        };

        Ok(LogicalPlan::Ddl(DdlPlan::CreateExternalDatabase(plan)))
    }

    async fn plan_create_external_table(
        &self,
        mut stmt: CreateExternalTableStmt,
    ) -> Result<LogicalPlan> {
        let m = &mut stmt.options;
        let (external_table_options, external_table_columns) = match stmt
            .datasource
            .to_lowercase()
            .as_str()
        {
            TableOptions::POSTGRES => {
                let connection_string = get_pg_conn_str(m)?;
                let schema = remove_required_opt(m, "schema")?;
                let table = remove_required_opt(m, "table")?;

                let access = PostgresTableAccess {
                    schema,
                    name: table,
                };

                let arrow_schema =
                    PostgresAccessor::validate_table_access(&connection_string, &access, None)
                        .await
                        .map_err(|e| PlanError::InvalidExternalTable {
                            source: Box::new(e),
                        })?;

                (
                    TableOptions::Postgres(TableOptionsPostgres {
                        connection_string,
                        schema: access.schema,
                        table: access.name,
                    }),
                    arrow_schema.fields,
                )
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

                (
                    TableOptions::BigQuery(TableOptionsBigQuery {
                        service_account_key,
                        project_id,
                        dataset_id: access.dataset_id,
                        table_id: access.table_id,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            TableOptions::MYSQL => {
                let connection_string = get_mysql_conn_str(m)?;
                let schema = remove_required_opt(m, "schema")?;
                let table = remove_required_opt(m, "table")?;

                let access = MysqlTableAccess {
                    schema,
                    name: table,
                };

                MysqlAccessor::validate_table_access(&connection_string, &access, None)
                    .await
                    .map_err(|e| PlanError::InvalidExternalTable {
                        source: Box::new(e),
                    })?;

                (
                    TableOptions::Mysql(TableOptionsMysql {
                        connection_string,
                        schema: access.schema,
                        table: access.name,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            TableOptions::MONGO => {
                let connection_string = get_mongo_conn_str(m)?;
                let database = remove_required_opt(m, "database")?;
                let collection = remove_required_opt(m, "collection")?;

                (
                    TableOptions::Mongo(TableOptionsMongo {
                        connection_string,
                        database,
                        collection,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            TableOptions::SNOWFLAKE => {
                let account_name = remove_required_opt(m, "account_name")?;
                let login_name = remove_required_opt(m, "login_name")?;
                let password = remove_required_opt(m, "password")?;
                let database_name = remove_required_opt(m, "database_name")?;
                let warehouse = remove_required_opt(m, "warehouse")?;
                let role_name = remove_optional_opt(m, "role_name")?;
                let schema_name = remove_required_opt(m, "schema_name")?;
                let table_name = remove_required_opt(m, "table_name")?;

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

                let arrow_schema =
                    SnowflakeAccessor::validate_table_access(conn_params, &access_info)
                        .await
                        .map_err(|e| PlanError::InvalidExternalTable {
                            source: Box::new(e),
                        })?;

                (
                    TableOptions::Snowflake(TableOptionsSnowflake {
                        account_name,
                        login_name,
                        password,
                        database_name,
                        warehouse,
                        role_name: role_name.unwrap_or_default(),
                        schema_name: access_info.schema_name,
                        table_name: access_info.table_name,
                    }),
                    arrow_schema.fields,
                )
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

                (
                    TableOptions::Local(TableOptionsLocal { location }),
                    // TODO: return column info for this datasource
                    vec![],
                )
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

                (
                    TableOptions::Gcs(TableOptionsGcs {
                        service_account_key: access.service_acccount_key_json,
                        bucket: access.bucket_name,
                        location: access.location,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
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

                (
                    TableOptions::S3(TableOptionsS3 {
                        access_key_id: access.access_key_id,
                        secret_access_key: access.secret_access_key,
                        region: access.region,
                        bucket: access.bucket_name,
                        location: access.location,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            TableOptions::DEBUG => {
                let typ = remove_required_opt(m, "table_type")?;
                let typ = DebugTableType::from_str(&typ)?;
                let columns = typ.arrow_schema().fields;

                (
                    TableOptions::Debug(TableOptionsDebug {
                        table_type: typ.to_string(),
                    }),
                    columns,
                )
            }
            other => return Err(internal!("unsupported datasource: {}", other)),
        };

        let plan = CreateExternalTable {
            table_name: stmt.name,
            if_not_exists: stmt.if_not_exists,
            table_options: external_table_options,
            columns: external_table_columns,
        };

        Ok(DdlPlan::CreateExternalTable(plan).into())
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
                // Validate the schema name idents
                match &schema_name {
                    ast::SchemaName::Simple(name) => validate_object_name(name)?,
                    ast::SchemaName::UnnamedAuthorization(ident) => validate_ident(ident)?,
                    ast::SchemaName::NamedAuthorization(name, ident) => {
                        validate_object_name(name)?;
                        validate_ident(ident)?
                    }
                }

                Ok(DdlPlan::CreateSchema(CreateSchema {
                    schema_name: schema_name.to_string(),
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
                let mut arrow_cols = Vec::with_capacity(columns.len());
                for column in columns.into_iter() {
                    let dt = convert_data_type(&column.data_type)?;
                    let field = Field::new(&column.name.value, dt, false);
                    arrow_cols.push(field);
                }

                Ok(DdlPlan::CreateTable(CreateTable {
                    table_name: name.to_string(),
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
                let source = planner.sql_statement_to_plan(ast::Statement::Query(query))?;
                Ok(DdlPlan::CreateTableAs(CreateTableAs {
                    table_name: name.to_string(),
                    source,
                })
                .into())
            }

            // Views
            ast::Statement::CreateView {
                or_replace: false,
                materialized: false,
                name,
                columns,
                query,
                with_options,
                ..
            } => {
                validate_object_name(&name)?;
                if !columns.is_empty() {
                    return Err(PlanError::UnsupportedFeature("named columns in views"));
                }
                if !with_options.is_empty() {
                    return Err(PlanError::UnsupportedFeature("view options"));
                }

                // Also validates that the view body is either a SELECT or
                // VALUES.
                let num_columns = match query.body.as_ref() {
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

                Ok(DdlPlan::CreateView(CreateView {
                    view_name: name.to_string(),
                    num_columns,
                    sql: query.to_string(),
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
                validate_object_name(&table_name)?;
                Ok(DdlPlan::AlterTableRaname(AlterTableRename {
                    name: name.to_string(),
                    new_name: table_name.to_string(),
                })
                .into())
            }

            // Drop tables
            ast::Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                let names = names
                    .into_iter()
                    .map(|name| name.to_string())
                    .collect::<Vec<_>>();

                Ok(DdlPlan::DropTables(DropTables { if_exists, names }).into())
            }

            // Drop views
            ast::Statement::Drop {
                object_type: ObjectType::View,
                if_exists,
                names,
                ..
            } => {
                let names = names
                    .into_iter()
                    .map(|name| name.to_string())
                    .collect::<Vec<_>>();

                Ok(DdlPlan::DropViews(DropViews { if_exists, names }).into())
            }

            // Drop schemas
            ast::Statement::Drop {
                object_type: ObjectType::Schema,
                if_exists,
                names,
                ..
            } => {
                let names = names
                    .into_iter()
                    .map(|name| name.to_string())
                    .collect::<Vec<_>>();

                Ok(DdlPlan::DropSchemas(DropSchemas { if_exists, names }).into())
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
                variable,
                values: value,
            })
            .into()),

            // "SHOW ..."
            //
            // Show the value of a variable.
            ast::Statement::ShowVariable { mut variable } => {
                if variable.len() != 1 {
                    if is_show_transaction_isolation_level(&variable) {
                        // Consider the special case where the query is:
                        // "SHOW TRANSACTION ISOLATION LEVEL".
                        // This is an alias of "SHOW transaction_isolation".
                        return Ok(VariablePlan::ShowVariable(ShowVariable {
                            variable: "transaction_isolation".to_owned(),
                        })
                        .into());
                    }
                    return Err(internal!("invalid variable ident: {:?}", variable));
                }
                let variable = variable
                    .pop()
                    .ok_or_else(|| internal!("missing ident for variable name"))?
                    .value;

                Ok(VariablePlan::ShowVariable(ShowVariable { variable }).into())
            }

            stmt => Err(PlanError::UnsupportedSQLStatement(stmt.to_string())),
        }
    }

    fn plan_drop_database(&self, stmt: DropDatabaseStmt) -> Result<LogicalPlan> {
        Ok(DdlPlan::DropDatabase(DropDatabase {
            name: stmt.name,
            if_exists: stmt.if_exists,
        })
        .into())
    }

    fn plan_alter_database_rename(&self, stmt: AlterDatabaseRenameStmt) -> Result<LogicalPlan> {
        Ok(DdlPlan::AlterDatabaseRename(AlterDatabaseRename {
            name: stmt.name,
            new_name: stmt.new_name,
        })
        .into())
    }
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

            Ok(DataType::List(Box::new(Field::new(
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

/// Is the "SHOW ..." statement equivalent to "SHOW TRANSACTION ISOLATION LEVEL".
fn is_show_transaction_isolation_level(variable: &Vec<Ident>) -> bool {
    let statement = ["transaction", "isolation", "level"];
    if statement.len() != variable.len() {
        return false;
    }
    let transaction_isolation_level = variable.iter().map(|i| i.value.to_lowercase());
    statement.into_iter().eq(transaction_isolation_level)
}

fn remove_required_opt(m: &mut BTreeMap<String, OptionValue>, k: &str) -> Result<String> {
    let opt = remove_optional_opt(m, k)?;
    opt.ok_or_else(|| internal!("missing required option: {}", k))
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
        OptionValue::Literal(l) => l,
        OptionValue::Secret(s) => {
            if let Ok(opt) = get_env(&s, /* uppercase: */ true) {
                opt
            } else {
                get_env(&s, /* uppercase: */ false)?
            }
        }
    };

    Ok(Some(opt))
}
