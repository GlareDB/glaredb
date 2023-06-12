// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL Query Planner (produces logical plan from SQL AST)
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::IntervalUnit;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::config::ConfigOptions;
use datafusion::common::field_not_found;
use datafusion::common::{unqualified_field_not_found, DFSchema, DataFusionError, Result};
use datafusion::common::{OwnedTableReference, TableReference};
use datafusion::logical_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion::logical_expr::utils::find_column_exprs;
use datafusion::logical_expr::TableSource;
use datafusion::logical_expr::{col, AggregateUDF, Expr, ScalarUDF, SubqueryAlias};
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::planner::ParserOptions;
use datafusion::sql::sqlparser::ast::ExactNumberInfo;
use datafusion::sql::sqlparser::ast::TimezoneInfo;
use datafusion::sql::sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use datafusion::sql::sqlparser::ast::{DataType as SQLDataType, Ident, ObjectName, TableAlias};
use sqlbuiltins::functions::TableFunc;
use sqlbuiltins::functions::TableFuncContextProvider;

use crate::utils::make_decimal_type;

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
#[async_trait]
pub trait AsyncContextProvider: Send + Sync {
    type TableFuncContextProvider: TableFuncContextProvider;

    /// Getter for a datasource
    async fn get_table_provider(
        &mut self,
        name: TableReference<'_>,
    ) -> Result<Arc<dyn TableSource>>;
    /// Getter for a UDF description
    async fn get_function_meta(&mut self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    async fn get_aggregate_meta(&mut self, name: &str) -> Option<Arc<AggregateUDF>>;
    /// Getter for system/user-defined variable type
    async fn get_variable_type(&mut self, variable_names: &[String]) -> Option<DataType>;

    /// Get a table returning function.
    ///
    /// Note that this accepts a table reference since these functions are
    /// namespaced similiarly to tables.
    fn get_table_func(&mut self, name: TableReference<'_>) -> Option<Arc<dyn TableFunc>>;

    /// Get the table func context provider.
    fn table_fn_ctx_provider(&self) -> Self::TableFuncContextProvider;

    /// Get configuration options.
    fn options(&self) -> &ConfigOptions;
}

/// SQL query planner
pub struct SqlQueryPlanner<'a, S: AsyncContextProvider> {
    pub(crate) schema_provider: &'a mut S,
    pub(crate) options: ParserOptions,
    pub(crate) normalizer: IdentNormalizer,
}

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    /// Create a new query planner
    pub fn new(schema_provider: &'a mut S) -> Self {
        Self::new_with_options(schema_provider, ParserOptions::default())
    }

    /// Create a new query planner
    pub fn new_with_options(schema_provider: &'a mut S, options: ParserOptions) -> Self {
        let normalize = options.enable_ident_normalization;
        SqlQueryPlanner {
            schema_provider,
            options,
            normalizer: IdentNormalizer::new(normalize),
        }
    }

    pub fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.convert_simple_data_type(&column.data_type)?;
            let not_nullable = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::NotNull);
            fields.push(Field::new(
                self.normalizer.normalize(column.name),
                data_type,
                !not_nullable,
            ));
        }

        Ok(Schema::new(fields))
    }

    /// Apply the given TableAlias to the top-level projection.
    pub(crate) fn apply_table_alias(
        &self,
        plan: LogicalPlan,
        alias: TableAlias,
    ) -> Result<LogicalPlan> {
        let apply_name_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            plan,
            self.normalizer.normalize(alias.name),
        )?);

        self.apply_expr_alias(apply_name_plan, alias.columns)
    }

    pub(crate) fn apply_expr_alias(
        &self,
        plan: LogicalPlan,
        idents: Vec<Ident>,
    ) -> Result<LogicalPlan> {
        if idents.is_empty() {
            Ok(plan)
        } else if idents.len() != plan.schema().fields().len() {
            Err(DataFusionError::Plan(format!(
                "Source table contains {} columns but only {} names given as column alias",
                plan.schema().fields().len(),
                idents.len(),
            )))
        } else {
            let fields = plan.schema().fields().clone();
            LogicalPlanBuilder::from(plan)
                .project(fields.iter().zip(idents.into_iter()).map(|(field, ident)| {
                    col(field.name()).alias(self.normalizer.normalize(ident))
                }))?
                .build()
        }
    }

    /// Validate the schema provides all of the columns referenced in the expressions.
    pub(crate) fn validate_schema_satisfies_exprs(
        &self,
        schema: &DFSchema,
        exprs: &[Expr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                Expr::Column(col) => match &col.relation {
                    Some(r) => {
                        schema.field_with_qualified_name(r, &col.name)?;
                        Ok(())
                    }
                    None => {
                        if !schema.fields_with_unqualified_name(&col.name).is_empty() {
                            Ok(())
                        } else {
                            Err(unqualified_field_not_found(col.name.as_str(), schema))
                        }
                    }
                }
                .map_err(|_: DataFusionError| {
                    field_not_found(col.relation.clone(), col.name.as_str(), schema)
                }),
                _ => Err(DataFusionError::Internal("Not a column".to_string())),
            })
    }

    pub(crate) fn convert_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Array(Some(inner_sql_type)) => {
                let data_type = self.convert_simple_data_type(inner_sql_type)?;

                Ok(DataType::List(Arc::new(Field::new(
                    "field", data_type, true,
                ))))
            }
            SQLDataType::Array(None) => Err(DataFusionError::NotImplemented(
                "Arrays with unspecified type is not supported".to_string(),
            )),
            other => self.convert_simple_data_type(other),
        }
    }

    fn convert_simple_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Boolean => Ok(DataType::Boolean),
            SQLDataType::TinyInt(_) => Ok(DataType::Int8),
            SQLDataType::SmallInt(_) => Ok(DataType::Int16),
            SQLDataType::Int(_) | SQLDataType::Integer(_) => Ok(DataType::Int32),
            SQLDataType::BigInt(_) => Ok(DataType::Int64),
            SQLDataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
            SQLDataType::UnsignedSmallInt(_) => Ok(DataType::UInt16),
            SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) => {
                Ok(DataType::UInt32)
            }
            SQLDataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real => Ok(DataType::Float32),
            SQLDataType::Double | SQLDataType::DoublePrecision => Ok(DataType::Float64),
            SQLDataType::Char(_)
            | SQLDataType::Varchar(_)
            | SQLDataType::Text
            | SQLDataType::String => Ok(DataType::Utf8),
            SQLDataType::Timestamp(None, tz_info) => {
                let tz = if matches!(tz_info, TimezoneInfo::Tz)
                    || matches!(tz_info, TimezoneInfo::WithTimeZone)
                {
                    // Timestamp With Time Zone
                    // INPUT : [SQLDataType]   TimestampTz + [RuntimeConfig] Time Zone
                    // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Some(Time Zone)>
                    self.schema_provider.options().execution.time_zone.clone()
                } else {
                    // Timestamp Without Time zone
                    None
                };
                Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz.map(Into::into)))
            }
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time(None, tz_info) => {
                if matches!(tz_info, TimezoneInfo::None)
                    || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
                {
                    Ok(DataType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We dont support TIMETZ and TIME WITH TIME ZONE for now
                    Err(DataFusionError::NotImplemented(format!(
                        "Unsupported SQL type {sql_type:?}"
                    )))
                }
            }
            SQLDataType::Numeric(exact_number_info)
            | SQLDataType::Decimal(exact_number_info) => {
                let (precision, scale) = match *exact_number_info {
                    ExactNumberInfo::None => (None, None),
                    ExactNumberInfo::Precision(precision) => (Some(precision), None),
                    ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                        (Some(precision), Some(scale))
                    }
                };
                make_decimal_type(precision, scale)
            }
            SQLDataType::Bytea => Ok(DataType::Binary),
            SQLDataType::Interval => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
            // Explicitly list all other types so that if sqlparser
            // adds/changes the `SQLDataType` the compiler will tell us on upgrade
            // and avoid bugs like https://github.com/apache/arrow-datafusion/issues/3059
            SQLDataType::Nvarchar(_)
            | SQLDataType::JSON
            | SQLDataType::Uuid
            | SQLDataType::Binary(_)
            | SQLDataType::Varbinary(_)
            | SQLDataType::Blob(_)
            | SQLDataType::Datetime(_)
            | SQLDataType::Regclass
            | SQLDataType::Custom(_, _)
            | SQLDataType::Array(_)
            | SQLDataType::Enum(_)
            | SQLDataType::Set(_)
            | SQLDataType::MediumInt(_)
            | SQLDataType::UnsignedMediumInt(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::CharacterLargeObject(_)
            | SQLDataType::CharLargeObject(_)
            // precision is not supported
            | SQLDataType::Timestamp(Some(_), _)
            // precision is not supported
            | SQLDataType::Time(Some(_), _)
            | SQLDataType::Dec(_)
            | SQLDataType::BigNumeric(_)
            | SQLDataType::BigDecimal(_)
            | SQLDataType::Clob(_) => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL type {sql_type:?}"
            ))),
        }
    }

    pub(crate) fn object_name_to_table_reference(
        &self,
        object_name: ObjectName,
    ) -> Result<OwnedTableReference> {
        object_name_to_table_reference(object_name, self.options.enable_ident_normalization)
    }
}
