use crate::catalog::access::AccessMethod;
use crate::catalog::entry::{AccessOrConnection, ConnectionMethod, TableOptions};
use crate::context::{ContextProviderAdapter, SessionContext};
use crate::errors::{internal, ExecError, Result};
use crate::logical_plan::*;
use crate::parser::{CreateConnectionStmt, CreateExternalTableStmt, StatementWithExtensions};
use datafusion::arrow::datatypes::{
    DataType, Field, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{self, Ident, ObjectType};
use datasource_bigquery::BigQueryTableAccess;
use datasource_debug::DebugTableType;
use datasource_object_store::gcs::GcsTableAccess;
use datasource_object_store::local::LocalTableAccess;
use datasource_object_store::s3::S3TableAccess;
use datasource_postgres::PostgresTableAccess;
use std::collections::{BTreeMap, HashMap};
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

    pub fn plan_ast(&self, statement: StatementWithExtensions) -> Result<LogicalPlan> {
        debug!(%statement, "planning sql statement");
        match statement {
            StatementWithExtensions::Statement(stmt) => self.plan_statement(stmt),
            StatementWithExtensions::CreateExternalTable(stmt) => {
                self.plan_create_external_table(stmt)
            }
            StatementWithExtensions::CreateConnection(stmt) => self.plan_create_connection(stmt),
        }
    }

    fn plan_create_connection(&self, mut stmt: CreateConnectionStmt) -> Result<LogicalPlan> {
        let m = &mut stmt.options;
        let plan = match stmt.datasource.to_lowercase().as_str() {
            "postgres" => {
                let connection_string = remove_required_opt(m, "postgres_conn")?;
                CreateConnection {
                    connection_name: stmt.name,
                    method: ConnectionMethod::Postgres { connection_string },
                }
            }
            "bigquery" => {
                let service_account_key = remove_required_opt(m, "service_account_key")?;
                let project_id = remove_required_opt(m, "project_id")?;
                CreateConnection {
                    connection_name: stmt.name,
                    method: ConnectionMethod::BigQuery {
                        service_account_key,
                        project_id,
                    },
                }
            }
            "local" => CreateConnection {
                connection_name: stmt.name,
                method: ConnectionMethod::Local,
            },
            "gcs" => {
                let service_account_key = remove_required_opt(m, "service_account_key")?;
                CreateConnection {
                    connection_name: stmt.name,
                    method: ConnectionMethod::Gcs {
                        service_account_key,
                    },
                }
            }
            "debug" if *self.ctx.get_session_vars().enable_debug_datasources.value() => {
                CreateConnection {
                    connection_name: stmt.name,
                    method: ConnectionMethod::Debug,
                }
            }
            other => return Err(internal!("unsupported datasource: {}", other)),
        };

        Ok(LogicalPlan::Ddl(DdlPlan::CreateConnection(plan)))
    }

    fn plan_create_external_table(&self, mut stmt: CreateExternalTableStmt) -> Result<LogicalPlan> {
        let create_sql = stmt.to_string();

        let datasource_or_connection = stmt.datasource_or_connection.to_lowercase();
        let m = &mut stmt.options;

        // TODO: Remove jank. Just here until things get switched over to using
        // only connections.
        if !is_datasource(datasource_or_connection.as_str()) {
            let conn = self.ctx.get_connection(&datasource_or_connection)?;
            let plan = match conn.method {
                ConnectionMethod::Debug => {
                    let typ = remove_required_opt(m, "table_type")?;
                    let typ = DebugTableType::from_str(&typ)?;
                    CreateExternalTable {
                        create_sql,
                        table_name: stmt.name,
                        access: AccessOrConnection::Connection(conn.name.clone()),
                        table_options: TableOptions::Debug { typ },
                    }
                }
                ConnectionMethod::Postgres { .. } => {
                    let source_schema = remove_required_opt(m, "schema")?;
                    let source_table = remove_required_opt(m, "table")?;
                    CreateExternalTable {
                        create_sql,
                        table_name: stmt.name,
                        access: AccessOrConnection::Connection(conn.name.clone()),
                        table_options: TableOptions::Postgres {
                            schema: source_schema,
                            table: source_table,
                        },
                    }
                }
                ConnectionMethod::BigQuery { .. } => {
                    let dataset_id = remove_required_opt(m, "dataset_id")?;
                    let table_id = remove_required_opt(m, "table_id")?;
                    CreateExternalTable {
                        create_sql,
                        table_name: stmt.name,
                        access: AccessOrConnection::Connection(conn.name.clone()),
                        table_options: TableOptions::BigQuery {
                            dataset_id,
                            table_id,
                        },
                    }
                }
                ConnectionMethod::Local => {
                    let location = remove_required_opt(m, "location")?;
                    CreateExternalTable {
                        create_sql,
                        table_name: stmt.name,
                        access: AccessOrConnection::Connection(conn.name.clone()),
                        table_options: TableOptions::Local { location },
                    }
                }
                ConnectionMethod::Gcs { .. } => {
                    let bucket_name = remove_required_opt(m, "bucket_name")?;
                    let location = remove_required_opt(m, "location")?;
                    CreateExternalTable {
                        create_sql,
                        table_name: stmt.name,
                        access: AccessOrConnection::Connection(conn.name.clone()),
                        table_options: TableOptions::Gcs {
                            bucket_name,
                            location,
                        },
                    }
                }
            };

            return Ok(LogicalPlan::Ddl(DdlPlan::CreateExternalTable(plan)));
        }

        let plan = match stmt.datasource_or_connection.to_lowercase().as_str() {
            "postgres" => {
                let conn_str = remove_required_opt(m, "postgres_conn")?;
                let source_schema = remove_required_opt(m, "schema")?;
                let source_table = remove_required_opt(m, "table")?;
                CreateExternalTable {
                    create_sql,
                    table_name: stmt.name,
                    access: AccessMethod::Postgres(PostgresTableAccess {
                        connection_string: conn_str,
                        schema: source_schema,
                        name: source_table,
                    })
                    .into(),
                    table_options: TableOptions::None,
                }
            }
            "bigquery" => {
                let sa_key = remove_required_opt(m, "gcp_service_account_key")?;
                let project_id = remove_required_opt(m, "gcp_project_id")?;
                let dataset_id = remove_required_opt(m, "dataset_id")?;
                let table_id = remove_required_opt(m, "table_id")?;
                CreateExternalTable {
                    create_sql,
                    table_name: stmt.name,
                    access: AccessMethod::BigQuery(BigQueryTableAccess {
                        gcp_service_acccount_key_json: sa_key,
                        gcp_project_id: project_id,
                        dataset_id,
                        table_id,
                    })
                    .into(),
                    table_options: TableOptions::None,
                }
            }
            "local" => {
                let file = remove_required_opt(m, "location")?;
                CreateExternalTable {
                    create_sql,
                    table_name: stmt.name,
                    access: AccessMethod::Local(LocalTableAccess { location: file }).into(),
                    table_options: TableOptions::None,
                }
            }
            "gcs" => {
                let service_acccount_key_json = remove_required_opt(m, "gcp_service_account_key")?;
                let bucket_name = remove_required_opt(m, "bucket_name")?;
                let table_location = remove_required_opt(m, "location")?;
                CreateExternalTable {
                    create_sql,
                    table_name: stmt.name,
                    access: AccessMethod::Gcs(GcsTableAccess {
                        bucket_name,
                        service_acccount_key_json,
                        location: table_location,
                    })
                    .into(),
                    table_options: TableOptions::None,
                }
            }
            "s3" => {
                let access_key_id = remove_required_opt(m, "aws_access_key_id")?;
                let secret_access_key = remove_required_opt(m, "aws_secret_access_key")?;
                let region = remove_required_opt(m, "region")?;
                let bucket_name = remove_required_opt(m, "bucket_name")?;
                let table_location = remove_required_opt(m, "location")?;
                CreateExternalTable {
                    create_sql,
                    table_name: stmt.name,
                    access: AccessMethod::S3(S3TableAccess {
                        region,
                        bucket_name,
                        access_key_id,
                        secret_access_key,
                        location: table_location,
                    })
                    .into(),
                    table_options: TableOptions::None,
                }
            }
            "debug" if *self.ctx.get_session_vars().enable_debug_datasources.value() => {
                let typ = remove_required_opt(m, "table_type")?;
                let typ = DebugTableType::from_str(&typ)?;
                CreateExternalTable {
                    create_sql,
                    table_name: stmt.name,
                    access: AccessMethod::Debug(typ).into(),
                    table_options: TableOptions::None,
                }
            }
            other => return Err(internal!("unsupported datasource: {}", other)),
        };

        Ok(DdlPlan::CreateExternalTable(plan).into())
    }

    fn plan_statement(&self, statement: ast::Statement) -> Result<LogicalPlan> {
        let context = ContextProviderAdapter { context: self.ctx };
        let planner = SqlToRel::new(&context);
        let sql_string = statement.to_string();
        match statement {
            ast::Statement::StartTransaction { .. } => Ok(TransactionPlan::Begin.into()),
            ast::Statement::Commit { .. } => Ok(TransactionPlan::Commit.into()),
            ast::Statement::Rollback { .. } => Ok(TransactionPlan::Abort.into()),

            ast::Statement::Query(query) => {
                let plan = planner.query_to_plan(*query, &mut HashMap::new())?;
                Ok(LogicalPlan::Query(plan))
            }

            ast::Statement::Explain {
                analyze,
                verbose,
                statement,
                ..
            } => {
                let plan = planner.explain_statement_to_plan(verbose, analyze, *statement)?;
                Ok(LogicalPlan::Query(plan))
            }

            ast::Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => Ok(DdlPlan::CreateSchema(CreateSchema {
                create_sql: sql_string,
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
                    create_sql: sql_string,
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
                let source = planner.query_to_plan(*query, &mut HashMap::new())?;
                Ok(DdlPlan::CreateTableAs(CreateTableAs {
                    create_sql: sql_string,
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
                        values.0.first().map(|first| first.len()).unwrap_or(0)
                    }
                    _ => {
                        return Err(ExecError::InvalidViewStatement {
                            msg: "view body must either be a SELECT or VALUES statement",
                        })
                    }
                };

                Ok(DdlPlan::CreateView(CreateView {
                    create_sql: sql_string,
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

// TODO: Remove when everything uses connections.
fn is_datasource(s: &str) -> bool {
    matches!(
        s,
        "postgres" | "local" | "bigquery" | "gcs" | "s3" | "debug"
    )
}
