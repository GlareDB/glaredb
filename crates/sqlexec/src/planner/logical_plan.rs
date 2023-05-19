use crate::errors::{internal, Result};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::OwnedTableReference;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast;
use datasource_object_store::url::ObjectStoreSourceUrl;
use metastore::types::options::{DatabaseOptions, TableOptions, TunnelOptions};
use std::collections::HashMap;
use std::str::FromStr;

use super::errors::PlanError;

#[derive(Clone, Debug)]
pub enum LogicalPlan {
    /// DDL plans.
    Ddl(DdlPlan),
    /// Write plans.
    Write(WritePlan),
    /// Plans related to querying the underlying data store. This will run
    /// through datafusion.
    Query(DfLogicalPlan),
    /// Plans related to transaction management.
    Transaction(TransactionPlan),
    /// Plans related to altering the state or runtime of the session.
    Variable(VariablePlan),
}

impl LogicalPlan {
    /// Try to get the data fusion logical plan from this logical plan.
    pub fn try_into_datafusion_plan(self) -> Result<DfLogicalPlan> {
        match self {
            LogicalPlan::Query(plan) => Ok(plan),
            other => Err(internal!("expected datafusion plan, got: {:?}", other)),
        }
    }

    /// Get the arrow schema of the output for the logical plan if it produces
    /// one.
    pub fn output_schema(&self) -> Option<ArrowSchema> {
        match self {
            LogicalPlan::Query(plan) => {
                let schema: ArrowSchema = plan.schema().as_ref().into();
                Some(schema)
            }
            LogicalPlan::Variable(VariablePlan::ShowVariable(plan)) => Some(ArrowSchema::new(
                vec![Field::new(&plan.variable, DataType::Utf8, false)],
            )),
            _ => None,
        }
    }

    /// Get parameter types for the logical plan.
    ///
    /// Note this will only try to get the parameters if the plan is a
    /// datafusion logical plan. Possible support for other plans may come
    /// later.
    pub fn get_parameter_types(&self) -> Result<HashMap<String, Option<DataType>>> {
        Ok(match self {
            LogicalPlan::Query(plan) => plan.get_parameter_types()?,
            _ => HashMap::new(),
        })
    }

    /// Replace placeholders in this plan with the provided scalars.
    ///
    /// Note this currently only replaces placeholders for datafusion plans.
    pub fn replace_placeholders(&mut self, scalars: Vec<ScalarValue>) -> Result<()> {
        if let LogicalPlan::Query(plan) = self {
            *plan = plan.replace_params_with_values(&scalars)?;
        }

        Ok(())
    }
}

impl From<DfLogicalPlan> for LogicalPlan {
    fn from(plan: DfLogicalPlan) -> Self {
        LogicalPlan::Query(plan)
    }
}

#[allow(dead_code)] // Inserts not constructed anywhere (yet)
#[derive(Clone, Debug)]
pub enum WritePlan {
    Insert(Insert),
    CopyTo(CopyTo),
}

impl From<WritePlan> for LogicalPlan {
    fn from(plan: WritePlan) -> Self {
        LogicalPlan::Write(plan)
    }
}

#[derive(Clone, Debug)]
pub struct Insert {
    pub table_name: String,
    pub columns: Vec<String>,
    pub source: DfLogicalPlan,
}

#[derive(Clone, Debug)]
pub enum CopyToAuthOptions {
    S3 {
        access_key_id: String,
        secret_access_key: String,
    },
    Gcs {
        service_account_key: String,
    },
    None,
}

#[derive(Clone, Debug)]
pub enum CopyFormat {
    Parquet,
}

impl FromStr for CopyFormat {
    type Err = PlanError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "parquet" => Ok(CopyFormat::Parquet),
            _ => Err(PlanError::UnknownCopyFormat(lower)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CopyFormatOpts {
    Parquet(datasource_object_store::sink::parquet::ParquetSinkOpts),
}

#[derive(Clone, Debug)]
pub struct CopyTo {
    pub source: DfLogicalPlan,
    pub dest: ObjectStoreSourceUrl,
    pub auth_options: CopyToAuthOptions,
    pub format: CopyFormat,
    pub format_opts: CopyFormatOpts,
    pub partition_by: Vec<String>,
}

/// Data defintion logical plans.
///
/// Note that while datafusion has some support for DDL, it's very much focused
/// on working with "external" data that won't be modified like parquet files.
#[derive(Clone, Debug)]
pub enum DdlPlan {
    CreateExternalDatabase(CreateExternalDatabase),
    CreateTunnel(CreateTunnel),
    CreateSchema(CreateSchema),
    CreateTable(CreateTable),
    CreateExternalTable(CreateExternalTable),
    CreateTableAs(CreateTableAs),
    CreateView(CreateView),
    AlterTableRaname(AlterTableRename),
    AlterDatabaseRename(AlterDatabaseRename),
    DropTables(DropTables),
    DropViews(DropViews),
    DropSchemas(DropSchemas),
    DropDatabase(DropDatabase),
    DropTunnel(DropTunnel),
}

impl From<DdlPlan> for LogicalPlan {
    fn from(plan: DdlPlan) -> Self {
        LogicalPlan::Ddl(plan)
    }
}

#[derive(Clone, Debug)]
pub struct CreateExternalDatabase {
    pub database_name: String,
    pub if_not_exists: bool,
    pub options: DatabaseOptions,
    pub tunnel: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CreateTunnel {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TunnelOptions,
}

#[derive(Clone, Debug)]
pub struct CreateSchema {
    pub schema_name: SchemaReference,
    pub if_not_exists: bool,
}

#[derive(Clone, Debug)]
pub struct CreateTable {
    pub table_name: OwnedTableReference,
    pub if_not_exists: bool,
    pub columns: Vec<Field>,
}

#[derive(Clone, Debug)]
pub struct CreateExternalTable {
    pub table_name: OwnedTableReference,
    pub if_not_exists: bool,
    pub table_options: TableOptions,
    pub tunnel: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CreateTableAs {
    pub table_name: OwnedTableReference,
    pub source: DfLogicalPlan,
}

#[derive(Clone, Debug)]
pub struct CreateView {
    pub view_name: OwnedTableReference,
    pub sql: String,
    pub columns: Vec<String>,
    pub or_replace: bool,
}

#[derive(Clone, Debug)]
pub struct AlterTableRename {
    pub name: OwnedTableReference,
    pub new_name: OwnedTableReference,
}

#[derive(Clone, Debug)]
pub struct DropTables {
    pub names: Vec<OwnedTableReference>,
    pub if_exists: bool,
}

#[derive(Clone, Debug)]
pub struct DropViews {
    pub names: Vec<OwnedTableReference>,
    pub if_exists: bool,
}

#[derive(Clone, Debug)]
pub struct DropSchemas {
    pub names: Vec<SchemaReference>,
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Clone, Debug)]
pub struct DropDatabase {
    pub names: Vec<String>,
    pub if_exists: bool,
}

#[derive(Clone, Debug)]
pub struct DropTunnel {
    pub names: Vec<String>,
    pub if_exists: bool,
}

#[derive(Clone, Debug)]
pub struct AlterDatabaseRename {
    pub name: String,
    pub new_name: String,
}

#[derive(Clone, Debug)]
pub enum TransactionPlan {
    Begin,
    Commit,
    Abort,
}

impl From<TransactionPlan> for LogicalPlan {
    fn from(plan: TransactionPlan) -> Self {
        LogicalPlan::Transaction(plan)
    }
}

#[derive(Clone, Debug)]
pub enum VariablePlan {
    SetVariable(SetVariable),
    ShowVariable(ShowVariable),
}

impl From<VariablePlan> for LogicalPlan {
    fn from(plan: VariablePlan) -> Self {
        LogicalPlan::Variable(plan)
    }
}

#[derive(Clone, Debug)]
pub struct SetVariable {
    pub variable: String,
    pub values: Vec<ast::Expr>,
}

impl SetVariable {
    /// Try to convert the value into a string.
    pub fn try_value_into_string(&self) -> Result<String> {
        let expr_to_string = |expr: &ast::Expr| {
            Ok(match expr {
                ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_) => expr.to_string(),
                ast::Expr::Value(ast::Value::SingleQuotedString(s)) => s.clone(),
                ast::Expr::Value(ast::Value::DoubleQuotedString(s)) => format!("\"{}\"", s),
                ast::Expr::Value(ast::Value::UnQuotedString(s)) => s.clone(),
                ast::Expr::Value(ast::Value::Number(s, _)) => s.clone(),
                ast::Expr::Value(v) => v.to_string(),
                other => return Err(internal!("invalid expression for SET var: {:}", other)),
            })
        };

        Ok(self
            .values
            .iter()
            .map(expr_to_string)
            .collect::<Result<Vec<_>>>()?
            .join(","))
    }
}

#[derive(Clone, Debug)]
pub struct ShowVariable {
    pub variable: String,
}

/// [`SchemaReference`] represents a multi-part schema that may require further
/// resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaReference {
    Bare { schema: String },
    Full { schema: String, catalog: String },
}
