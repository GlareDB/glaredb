use crate::errors::{internal, ExecError, Result};
use datafusion::arrow::datatypes::Field;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::sql::sqlparser::ast;
use jsoncat::access::AccessMethod;

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
    Configuration(ConfigurationPlan),
}

impl LogicalPlan {
    /// Try to get the data fusion logical plan from this logical plan.
    pub fn try_into_datafusion_plan(self) -> Result<DfLogicalPlan> {
        match self {
            LogicalPlan::Query(plan) => Ok(plan),
            other => Err(internal!("expected datafusion plan, got: {:?}", other)),
        }
    }
}

impl From<DfLogicalPlan> for LogicalPlan {
    fn from(plan: DfLogicalPlan) -> Self {
        LogicalPlan::Query(plan)
    }
}

#[derive(Clone, Debug)]
pub enum WritePlan {
    Insert(Insert),
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

/// Data defintion logical plans.
///
/// Note that while datafusion has some support for DDL, it's very much focused
/// on working with "external" data that won't be modified like parquet files.
#[derive(Clone, Debug)]
pub enum DdlPlan {
    CreateSchema(CreateSchema),
    CreateTable(CreateTable),
    CreateExternalTable(CreateExternalTable),
    CreateTableAs(CreateTableAs),
    DropTables(DropTables),
    DropSchemas(DropSchemas),
}

impl From<DdlPlan> for LogicalPlan {
    fn from(plan: DdlPlan) -> Self {
        LogicalPlan::Ddl(plan)
    }
}

#[derive(Clone, Debug)]
pub struct CreateSchema {
    pub schema_name: String,
    pub if_not_exists: bool,
}

#[derive(Clone, Debug)]
pub struct CreateTable {
    pub table_name: String,
    pub if_not_exists: bool,
    pub columns: Vec<Field>,
}

#[derive(Clone, Debug)]
pub enum FileType {
    Parquet,
}

impl TryFrom<ast::FileFormat> for FileType {
    type Error = ExecError;
    fn try_from(value: ast::FileFormat) -> Result<Self, Self::Error> {
        Ok(match value {
            ast::FileFormat::PARQUET => FileType::Parquet,
            other => return Err(internal!("unsupported file format: {:?}", other)),
        })
    }
}

#[derive(Clone, Debug)]
pub struct CreateExternalTable {
    pub table_name: String,
    pub access: AccessMethod,
}

#[derive(Clone, Debug)]
pub struct CreateTableAs {
    pub table_name: String,
    pub source: DfLogicalPlan,
}

#[derive(Clone, Debug)]
pub struct DropTables {
    pub names: Vec<String>,
    pub if_exists: bool,
}

#[derive(Clone, Debug)]
pub struct DropSchemas {
    pub names: Vec<String>,
    pub if_exists: bool,
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
pub enum ConfigurationPlan {
    SetVariable(SetVariable),
    ShowVariable(ShowVariable),
}

impl From<ConfigurationPlan> for LogicalPlan {
    fn from(plan: ConfigurationPlan) -> Self {
        LogicalPlan::Configuration(plan)
    }
}

#[derive(Clone, Debug)]
pub struct SetVariable {
    pub variable: ast::ObjectName,
    pub values: Vec<ast::Expr>,
}

impl SetVariable {
    /// Try to get a single string value.
    ///
    /// Errors if more than one value, or if the value is not a string.
    pub fn try_as_single_string(&self) -> Result<String> {
        if self.values.len() > 1 {
            return Err(internal!(
                "invalid number of values given: {:?}",
                self.values
            ));
        }

        let first = self.values.first().unwrap();
        match first {
            ast::Expr::Identifier(ident) => Ok(ident.value.clone()),
            ast::Expr::Value(ast::Value::SingleQuotedString(s)) => Ok(s.clone()),
            expr => Err(internal!("invalid expression for SET: {}", expr)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShowVariable {
    pub variable: String,
}
