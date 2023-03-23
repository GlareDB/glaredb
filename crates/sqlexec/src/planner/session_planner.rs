use crate::context::{ContextProviderAdapter, SessionContext};
use crate::errors::{internal, ExecError, Result};
use crate::logical_plan::*;
use crate::parser::{
    CreateConnectionStmt, CreateExternalTableStmt, DropConnectionStmt, StatementWithExtensions,
};
use crate::preprocess::{preprocess, CastRegclassReplacer, EscapedStringToDoubleQuoted};
use datafusion::arrow::datatypes::{
    DataType, Field, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{self, Ident, ObjectType};
use datasource_bigquery::{BigQueryAccessor, BigQueryTableAccess};
use datasource_common::ssh::SshKey;
use datasource_debug::DebugTableType;

use datasource_mysql::{MysqlAccessor, MysqlTableAccess};
use datasource_object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasource_object_store::local::{LocalAccessor, LocalTableAccess};
use datasource_object_store::s3::{S3Accessor, S3TableAccess};
use datasource_postgres::{PostgresAccessor, PostgresTableAccess};
use metastore::types::catalog::{
    ConnectionOptions, ConnectionOptionsBigQuery, ConnectionOptionsDebug, ConnectionOptionsGcs,
    ConnectionOptionsLocal, ConnectionOptionsMongo, ConnectionOptionsMysql,
    ConnectionOptionsPostgres, ConnectionOptionsS3, ConnectionOptionsSsh, TableOptions,
    TableOptionsBigQuery, TableOptionsDebug, TableOptionsGcs, TableOptionsLocal, TableOptionsMongo,
    TableOptionsMysql, TableOptionsPostgres, TableOptionsS3,
};
use std::collections::BTreeMap;
use std::str::FromStr;
use tokio::runtime::Handle;
use tokio::task;
use tracing::debug;

/// Plan SQL statements for a session.
pub struct SessionPlanner<'a> {
    ctx: &'a SessionContext,
}

impl<'a> SessionPlanner<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        SessionPlanner { ctx }
    }

    pub fn plan_ast(&self, mut statement: StatementWithExtensions) -> Result<LogicalPlan> {
        debug!(%statement, "planning sql statement");

        // Run replacers as needed.
        if let StatementWithExtensions::Statement(inner) = &mut statement {
            preprocess(inner, &mut CastRegclassReplacer { ctx: self.ctx })?;
            preprocess(inner, &mut EscapedStringToDoubleQuoted)?;
        }

        match statement {
            StatementWithExtensions::Statement(stmt) => self.plan_statement(stmt),
            StatementWithExtensions::CreateExternalTable(stmt) => {
                self.plan_create_external_table(stmt)
            }
            StatementWithExtensions::CreateConnection(stmt) => self.plan_create_connection(stmt),
            StatementWithExtensions::DropConnection(stmt) => self.plan_drop_connection(stmt),
        }
    }

    fn plan_create_connection(&self, mut stmt: CreateConnectionStmt) -> Result<LogicalPlan> {
        let m = &mut stmt.options;

        let connection_options = match stmt.datasource.to_lowercase().as_str() {
            ConnectionOptions::POSTGRES => {
                let connection_string = remove_required_opt(m, "postgres_conn")?;
                let ssh_tunnel = remove_optional_opt(m, "ssh_tunnel");

                let (tunn_id, access) = match ssh_tunnel {
                    Some(name) => Some(self.ctx.get_ssh_tunnel_access_by_name(&name)?).unzip(),
                    None => None.unzip(),
                };

                task::block_in_place(|| {
                    Handle::current().block_on(async {
                        PostgresAccessor::validate_connection(&connection_string, access)
                            .await
                            .map_err(|e| ExecError::InvalidConnection {
                                source: Box::new(e),
                            })
                    })
                })?;

                ConnectionOptions::Postgres(ConnectionOptionsPostgres {
                    connection_string,
                    ssh_tunnel: tunn_id,
                })
            }
            ConnectionOptions::BIGQUERY => {
                let service_account_key = remove_required_opt(m, "service_account_key")?;
                let project_id = remove_required_opt(m, "project_id")?;

                let options = ConnectionOptionsBigQuery {
                    service_account_key,
                    project_id,
                };

                task::block_in_place(|| {
                    Handle::current().block_on(async {
                        BigQueryAccessor::validate_connection(&options)
                            .await
                            .map_err(|e| ExecError::InvalidConnection {
                                source: Box::new(e),
                            })
                    })
                })?;

                ConnectionOptions::BigQuery(options)
            }
            ConnectionOptions::MYSQL => {
                let connection_string = remove_required_opt(m, "mysql_conn")?;
                let ssh_tunnel = remove_optional_opt(m, "ssh_tunnel");

                let (tunn_id, access) = match ssh_tunnel {
                    Some(name) => Some(self.ctx.get_ssh_tunnel_access_by_name(&name)?).unzip(),
                    None => None.unzip(),
                };

                task::block_in_place(|| {
                    Handle::current().block_on(async {
                        MysqlAccessor::validate_connection(&connection_string, access)
                            .await
                            .map_err(|e| ExecError::InvalidConnection {
                                source: Box::new(e),
                            })
                    })
                })?;

                ConnectionOptions::Mysql(ConnectionOptionsMysql {
                    connection_string,
                    ssh_tunnel: tunn_id,
                })
            }
            ConnectionOptions::LOCAL => ConnectionOptions::Local(ConnectionOptionsLocal {}),
            // TODO: create connection validation
            ConnectionOptions::GCS => {
                let service_account_key = remove_required_opt(m, "service_account_key")?;
                ConnectionOptions::Gcs(ConnectionOptionsGcs {
                    service_account_key,
                })
            }
            // TODO: create connection validation
            ConnectionOptions::S3_STORAGE => {
                // Require `access_key_id` and `secret_access_key` as per access key generated on
                // AWS IAM
                let access_key_id = remove_required_opt(m, "access_key_id")?;
                let secret_access_key = remove_required_opt(m, "secret_access_key")?;
                ConnectionOptions::S3(ConnectionOptionsS3 {
                    access_key_id,
                    secret_access_key,
                })
            }
            ConnectionOptions::SSH => {
                let host = remove_required_opt(m, "host")?;
                let user = remove_required_opt(m, "user")?;
                let port: u16 = remove_required_opt(m, "port")?.parse()?;

                // Generate random ssh keypair
                let keypair = SshKey::generate_random()?.to_bytes()?;

                ConnectionOptions::Ssh(ConnectionOptionsSsh {
                    host,
                    user,
                    port,
                    keypair,
                })
            }
            ConnectionOptions::MONGO => {
                let conn_str = remove_required_opt(m, "mongo_conn")?;

                // TODO: Validate

                ConnectionOptions::Mongo(ConnectionOptionsMongo {
                    connection_string: conn_str,
                })
            }
            ConnectionOptions::DEBUG
                if *self.ctx.get_session_vars().enable_debug_datasources.value() =>
            {
                ConnectionOptions::Debug(ConnectionOptionsDebug {})
            }
            other => return Err(internal!("unsupported datasource: {}", other)),
        };

        let plan = CreateConnection {
            connection_name: stmt.name,
            if_not_exists: stmt.if_not_exists,
            options: connection_options,
        };

        Ok(LogicalPlan::Ddl(DdlPlan::CreateConnection(plan)))
    }

    fn plan_create_external_table(&self, mut stmt: CreateExternalTableStmt) -> Result<LogicalPlan> {
        let m = &mut stmt.options;

        let conn = stmt.connection.to_lowercase();
        let conn = self.ctx.get_connection_by_name(&conn)?;

        let (external_table_options, external_table_columns) = match &conn.options {
            ConnectionOptions::Debug(_) => {
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
            ConnectionOptions::Postgres(options) => {
                let source_schema = remove_required_opt(m, "schema")?;
                let source_table = remove_required_opt(m, "table")?;

                let access = PostgresTableAccess {
                    schema: source_schema,
                    name: source_table,
                    connection_string: options.connection_string.to_owned(),
                };
                let tunn_access = options
                    .ssh_tunnel
                    .map(|oid| self.ctx.get_ssh_tunnel_access_by_oid(oid))
                    .transpose()?;

                let result = task::block_in_place(|| {
                    Handle::current().block_on(async {
                        PostgresAccessor::validate_table_access(&access, tunn_access.as_ref())
                            .await
                            .map_err(|e| ExecError::InvalidExternalTable {
                                source: Box::new(e),
                            })
                    })
                });
                let arrow_schema = result?;

                (
                    TableOptions::Postgres(TableOptionsPostgres {
                        schema: access.schema,
                        table: access.name,
                    }),
                    arrow_schema.fields,
                )
            }
            ConnectionOptions::BigQuery(options) => {
                let dataset_id = remove_required_opt(m, "dataset_id")?;
                let table_id = remove_required_opt(m, "table_id")?;

                let access = BigQueryTableAccess {
                    gcp_service_acccount_key_json: options.service_account_key.to_owned(),
                    gcp_project_id: options.project_id.to_owned(),
                    dataset_id,
                    table_id,
                };

                task::block_in_place(|| {
                    Handle::current().block_on(async {
                        BigQueryAccessor::validate_table_access(&access)
                            .await
                            .map_err(|e| ExecError::InvalidExternalTable {
                                source: Box::new(e),
                            })
                    })
                })?;

                (
                    TableOptions::BigQuery(TableOptionsBigQuery {
                        dataset_id: access.dataset_id,
                        table_id: access.table_id,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            ConnectionOptions::Mysql(options) => {
                let source_schema = remove_required_opt(m, "schema")?;
                let source_table = remove_required_opt(m, "table")?;

                let access = MysqlTableAccess {
                    schema: source_schema,
                    name: source_table,
                    connection_string: options.connection_string.clone(),
                };
                let tunn_access = options
                    .ssh_tunnel
                    .map(|oid| self.ctx.get_ssh_tunnel_access_by_oid(oid))
                    .transpose()?;

                task::block_in_place(|| {
                    Handle::current().block_on(async {
                        MysqlAccessor::validate_table_access(&access, tunn_access)
                            .await
                            .map_err(|e| ExecError::InvalidExternalTable {
                                source: Box::new(e),
                            })
                    })
                })?;

                (
                    TableOptions::Mysql(TableOptionsMysql {
                        schema: access.schema,
                        table: access.name,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            ConnectionOptions::Local(_) => {
                let location = remove_required_opt(m, "location")?;

                let access = LocalTableAccess {
                    location: location.clone(),
                    file_type: None,
                };

                task::block_in_place(|| {
                    Handle::current().block_on(async {
                        LocalAccessor::validate_table_access(access)
                            .await
                            .map_err(|e| ExecError::InvalidExternalTable {
                                source: Box::new(e),
                            })
                    })
                })?;

                (
                    TableOptions::Local(TableOptionsLocal { location }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            ConnectionOptions::Gcs(options) => {
                let bucket_name = remove_required_opt(m, "bucket_name")?;
                let location = remove_required_opt(m, "location")?;

                let access = GcsTableAccess {
                    bucket_name: bucket_name.clone(),
                    service_acccount_key_json: options.service_account_key.to_owned(),
                    location: location.clone(),
                    file_type: None,
                };

                task::block_in_place(|| {
                    Handle::current().block_on(async {
                        GcsAccessor::validate_table_access(access)
                            .await
                            .map_err(|e| ExecError::InvalidExternalTable {
                                source: Box::new(e),
                            })
                    })
                })?;

                (
                    TableOptions::Gcs(TableOptionsGcs {
                        bucket_name,
                        location,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            ConnectionOptions::S3(options) => {
                let region = remove_required_opt(m, "region")?;
                let bucket_name = remove_required_opt(m, "bucket_name")?;
                let location = remove_required_opt(m, "location")?;

                let access = S3TableAccess {
                    region: region.clone(),
                    bucket_name: bucket_name.clone(),
                    access_key_id: options.access_key_id.to_owned(),
                    secret_access_key: options.secret_access_key.to_owned(),
                    location: location.clone(),
                    file_type: None,
                };

                task::block_in_place(|| {
                    Handle::current().block_on(async {
                        S3Accessor::validate_table_access(access)
                            .await
                            .map_err(|e| ExecError::InvalidExternalTable {
                                source: Box::new(e),
                            })
                    })
                })?;

                (
                    TableOptions::S3(TableOptionsS3 {
                        region,
                        bucket_name,
                        location,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
            ConnectionOptions::Ssh(_) => {
                return Err(ExecError::ExternalTableWithSsh);
            }
            ConnectionOptions::Mongo(_options) => {
                let database = remove_required_opt(m, "database")?;
                let collection = remove_required_opt(m, "collection")?;

                // TODO: Validate.

                (
                    TableOptions::Mongo(TableOptionsMongo {
                        database,
                        collection,
                    }),
                    // TODO: return column info for this datasource
                    vec![],
                )
            }
        };

        let plan = CreateExternalTable {
            table_name: stmt.name,
            if_not_exists: stmt.if_not_exists,
            connection_id: conn.meta.id,
            table_options: external_table_options,
            columns: external_table_columns,
        };

        Ok(DdlPlan::CreateExternalTable(plan).into())
    }

    fn plan_statement(&self, statement: ast::Statement) -> Result<LogicalPlan> {
        let context = ContextProviderAdapter { context: self.ctx };
        let planner = SqlToRel::new(&context);
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
            } => Ok(DdlPlan::CreateSchema(CreateSchema {
                schema_name: schema_name.to_string(),
                if_not_exists,
            })
            .into()),

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
                if !columns.is_empty() {
                    return Err(ExecError::UnsupportedFeature("named columns in views"));
                }
                if !with_options.is_empty() {
                    return Err(ExecError::UnsupportedFeature("view options"));
                }

                // Also validates that the view body is either a SELECT or
                // VALUES.
                let num_columns = match query.body.as_ref() {
                    ast::SetExpr::Select(select) => select.projection.len(),
                    ast::SetExpr::Values(values) => {
                        values.rows.first().map(|first| first.len()).unwrap_or(0)
                    }
                    _ => {
                        return Err(ExecError::InvalidViewStatement {
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
                Err(ExecError::UnsupportedSQLStatement(stmt.to_string()))
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

            stmt => Err(ExecError::UnsupportedSQLStatement(stmt.to_string())),
        }
    }

    fn plan_drop_connection(&self, stmt: DropConnectionStmt) -> Result<LogicalPlan> {
        Ok(DdlPlan::DropConnections(DropConnections {
            if_exists: stmt.if_exists,
            names: stmt.names,
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

fn remove_required_opt(m: &mut BTreeMap<String, String>, k: &str) -> Result<String> {
    m.remove(k)
        .ok_or_else(|| internal!("missing required option: {}", k))
}

fn remove_optional_opt(m: &mut BTreeMap<String, String>, k: &str) -> Option<String> {
    m.remove(k)
}
