use crate::errors::{internal, ExecError};
use datafusion::arrow::datatypes::Field;
use datafusion::logical_plan::LogicalPlan as DfLogicalPlan;
use datafusion::sql::sqlparser::ast::{self};

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
    // TODO: Actually implement this. This would correspond to "SET ..." and "SET SESSION ..." statements.
    Runtime,
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
    DropTable(DropTable),
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
    pub location: String,
    pub file_type: FileType,
}

#[derive(Clone, Debug)]
pub struct CreateTableAs {
    pub table_name: String,
    pub source: DfLogicalPlan,
}

#[derive(Clone, Debug)]
pub struct DropTable {
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
