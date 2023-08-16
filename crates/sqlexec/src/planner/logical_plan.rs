mod alter_database_rename;
mod alter_table_rename;
mod alter_tunnel_rotate_keys;
mod create_credentials;
mod create_external_database;
mod create_external_table;
mod create_schema;
mod create_table;
mod create_temp_table;
mod create_tunnel;
mod create_view;
mod drop_credentials;
mod drop_database;
mod drop_schemas;
mod drop_tables;
mod drop_tunnel;
mod drop_views;

use crate::errors::{internal, Result};
use crate::planner::extension::ExtensionNode;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::{DFSchema, DFSchemaRef, OwnedSchemaReference, OwnedTableReference};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Explain, Expr, LogicalPlan as DfLogicalPlan};
use datafusion::logical_expr::{Extension as LogicalPlanExtension, UserDefinedLogicalNodeCore};
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast;
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use datafusion_proto::protobuf::LogicalPlanNode;
use once_cell::sync::Lazy;
use protogen::export::prost::Message;
use protogen::metastore::types::options::{CopyToDestinationOptions, CopyToFormatOptions};
use protogen::metastore::types::options::{
    CredentialsOptions, DatabaseOptions, TableOptions, TunnelOptions,
};
use protogen::ProtoConvError;
use std::collections::HashMap;
use std::sync::Arc;

pub use alter_database_rename::*;
pub use alter_table_rename::*;
pub use alter_tunnel_rotate_keys::*;
pub use create_credentials::*;
pub use create_external_database::*;
pub use create_external_table::*;
pub use create_schema::*;
pub use create_table::*;
pub use create_temp_table::*;
pub use create_tunnel::*;
pub use create_view::*;
pub use drop_credentials::*;
pub use drop_database::*;
pub use drop_schemas::*;
pub use drop_tables::*;
pub use drop_tunnel::*;
pub use drop_views::*;

static EMPTY_SCHEMA: Lazy<Arc<DFSchema>> = Lazy::new(|| Arc::new(DFSchema::empty()));

#[derive(Clone, Debug)]
pub enum LogicalPlan {
    /// Write plans.
    Write(WritePlan),
    /// Plans related to querying the underlying data store. This will run
    /// through datafusion.
    Datafusion(DfLogicalPlan),
    /// Plans related to transaction management.
    Transaction(TransactionPlan),
    /// Plans related to altering the state or runtime of the session.
    Variable(VariablePlan),
}

impl LogicalPlan {
    /// Try to get the data fusion logical plan from this logical plan.
    pub fn try_into_datafusion_plan(self) -> Result<DfLogicalPlan> {
        match self {
            LogicalPlan::Datafusion(plan) => Ok(plan),
            other => Err(internal!("expected datafusion plan, got: {:?}", other)),
        }
    }

    /// Get the arrow schema of the output for the logical plan if it produces
    /// one.
    pub fn output_schema(&self) -> Option<ArrowSchema> {
        match self {
            LogicalPlan::Datafusion(plan) => {
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
            LogicalPlan::Datafusion(plan) => plan.get_parameter_types()?,
            _ => HashMap::new(),
        })
    }

    /// Replace placeholders in this plan with the provided scalars.
    ///
    /// Note this currently only replaces placeholders for datafusion plans.
    pub fn replace_placeholders(&mut self, scalars: Vec<ScalarValue>) -> Result<()> {
        if let LogicalPlan::Datafusion(plan) = self {
            // Replace placeholders in the inner plan if the wrapped in an
            // EXPLAIN.
            //
            // TODO: Make sure this is the correct behavior.
            if let DfLogicalPlan::Explain(explain) = plan {
                let mut inner = explain.plan.clone();
                let inner = Arc::make_mut(&mut inner);

                *plan = DfLogicalPlan::Explain(Explain {
                    verbose: explain.verbose,
                    plan: Arc::new(inner.replace_params_with_values(&scalars)?),
                    stringified_plans: explain.stringified_plans.clone(),
                    schema: explain.schema.clone(),
                    logical_optimization_succeeded: explain.logical_optimization_succeeded,
                });

                return Ok(());
            }

            *plan = plan.replace_params_with_values(&scalars)?;
        }

        Ok(())
    }
}

impl From<DfLogicalPlan> for LogicalPlan {
    fn from(plan: DfLogicalPlan) -> Self {
        LogicalPlan::Datafusion(plan)
    }
}

#[derive(Clone, Debug)]
pub enum WritePlan {
    Insert(Insert),
    CopyTo(CopyTo),
    Delete(Delete),
    Update(Update),
}

impl From<WritePlan> for LogicalPlan {
    fn from(plan: WritePlan) -> Self {
        LogicalPlan::Write(plan)
    }
}

#[derive(Clone)]
pub struct Insert {
    pub table_provider: Arc<dyn TableProvider>,
    pub source: DfLogicalPlan,
}

impl std::fmt::Debug for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Insert")
            .field("source", &self.source)
            .field("table_provider", &self.table_provider.schema())
            .finish()
    }
}

#[derive(Clone)]
pub struct CopyTo {
    pub source: DfLogicalPlan,
    pub dest: CopyToDestinationOptions,
    pub format: CopyToFormatOptions,
}

impl std::fmt::Debug for CopyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyTo")
            .field("source", &self.source.schema())
            .field("dest", &self.dest)
            .field("format", &self.format)
            .finish()
    }
}

#[derive(Clone)]
pub struct Delete {
    pub table_name: OwnedTableReference,
    pub where_expr: Option<Expr>,
}

impl std::fmt::Debug for Delete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delete")
            .field("table_name", &self.table_name.schema())
            .field("where_expr", &self.where_expr)
            .finish()
    }
}

#[derive(Clone)]
pub struct Update {
    pub table_name: OwnedTableReference,
    pub updates: Vec<(String, Expr)>,
    pub where_expr: Option<Expr>,
}

impl std::fmt::Debug for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Update")
            .field("table_name", &self.table_name.schema())
            .field(
                "updates",
                &self.updates.iter().map(|(k, v)| (k, v.to_string())),
            )
            .field("where_expr", &self.where_expr)
            .finish()
    }
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
