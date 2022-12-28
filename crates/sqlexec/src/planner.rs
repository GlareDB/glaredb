use crate::context::{ContextProviderAdapter, SessionContext};
use crate::errors::{internal, ExecError, Result};
use crate::logical_plan::*;
use crate::parser::{CreateExternalTableStmt, StatementWithExtensions};
use access::external::postgres::PostgresTableAccess;
use datafusion::arrow::datatypes::{
    DataType, Field, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{self, ObjectType};
use jsoncat::access::AccessMethod;
use std::collections::HashMap;
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
        }
    }

    fn plan_create_external_table(&self, mut stmt: CreateExternalTableStmt) -> Result<LogicalPlan> {
        let pop_required_opt = |m: &mut HashMap<String, String>, k: &str| {
            m.remove(k)
                .ok_or_else(|| internal!("missing required option: {}", k))
        };

        let m = &mut stmt.options;
        let plan = match stmt.datasource.to_lowercase().as_str() {
            "postgres" => {
                let conn_str = pop_required_opt(m, "postgres_conn")?;
                let source_schema = pop_required_opt(m, "schema")?;
                let source_table = pop_required_opt(m, "table")?;
                CreateExternalTable {
                    table_name: stmt.name,
                    access: AccessMethod::Postgres(PostgresTableAccess {
                        connection_string: conn_str,
                        schema: source_schema,
                        name: source_table,
                    }),
                }
            }
            other => return Err(internal!("unsupported datasource: {}", other)),
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
                let source = planner.query_to_plan(*query, &mut HashMap::new())?;
                Ok(DdlPlan::CreateTableAs(CreateTableAs {
                    table_name: name.to_string(),
                    source,
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
                variable, value, ..
            } => Ok(ConfigurationPlan::SetConfiguration(SetConfiguration {
                variable,
                values: value,
            })
            .into()),

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
