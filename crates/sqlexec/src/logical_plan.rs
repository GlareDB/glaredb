use crate::catalog::access::AccessMethod;
use crate::catalog::entry::ConnectionMethod;
use crate::errors::{internal, Result};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::sql::sqlparser::ast;

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
    CreateConnection(CreateConnection),
    CreateView(CreateView),
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
    pub create_sql: String,
    pub schema_name: String,
    pub if_not_exists: bool,
}

#[derive(Clone, Debug)]
pub struct CreateTable {
    pub create_sql: String,
    pub table_name: String,
    pub if_not_exists: bool,
    pub columns: Vec<Field>,
}

#[derive(Clone, Debug)]
pub struct CreateExternalTable {
    pub create_sql: String,
    pub table_name: String,
    pub access: AccessMethod,
}

#[derive(Clone, Debug)]
pub struct CreateTableAs {
    pub create_sql: String,
    pub table_name: String,
    pub source: DfLogicalPlan,
}

#[derive(Clone, Debug)]
pub struct CreateConnection {
    pub connection_name: String,
    pub method: ConnectionMethod,
}

#[derive(Clone, Debug)]
pub struct CreateView {
    pub create_sql: String,
    pub view_name: String,
    pub num_columns: usize,
    pub sql: String,
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
    pub variable: ast::ObjectName,
    pub values: Vec<ast::Expr>,
}

impl SetVariable {
    /// Try to convert the value into a string.
    pub fn try_into_string(&self) -> Result<String> {
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
