mod alter_database;
mod alter_table;
mod alter_tunnel_rotate_keys;
mod copy_to;
mod create_credentials;
mod create_external_database;
mod create_external_table;
mod create_schema;
mod create_table;
mod create_temp_table;
mod create_tunnel;
mod create_view;
mod delete;
mod describe_table;
mod drop_credentials;
mod drop_database;
mod drop_schemas;
mod drop_tables;
mod drop_tunnel;
mod drop_views;
mod insert;
mod set_variable;
mod show_variable;
mod update;

use crate::errors::{internal, Result};
use crate::planner::extension::ExtensionNode;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::{DFField, DFSchema, DFSchemaRef, ParamValues};
use datafusion::logical_expr::UserDefinedLogicalNodeCore;
use datafusion::logical_expr::{Explain, Expr, LogicalPlan as DfLogicalPlan};
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::TableReference;
use once_cell::sync::Lazy;
use protogen::metastore::types::options::{CopyToDestinationOptions, CopyToFormatOptions};
use protogen::metastore::types::options::{
    CredentialsOptions, DatabaseOptions, TableOptions, TunnelOptions,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub use alter_database::*;
pub use alter_table::*;
pub use alter_tunnel_rotate_keys::*;
pub use copy_to::*;
pub use create_credentials::*;
pub use create_external_database::*;
pub use create_external_table::*;
pub use create_schema::*;
pub use create_table::*;
pub use create_temp_table::*;
pub use create_tunnel::*;
pub use create_view::*;
pub use delete::*;
pub use describe_table::*;
pub use drop_credentials::*;
pub use drop_database::*;
pub use drop_schemas::*;
pub use drop_tables::*;
pub use drop_tunnel::*;
pub use drop_views::*;
pub use insert::*;
pub use set_variable::*;
pub use show_variable::*;
pub use update::*;

use super::physical_plan::{
    GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA, GENERIC_OPERATION_PHYSICAL_SCHEMA,
};

pub static GENERIC_OPERATION_LOGICAL_SCHEMA: Lazy<DFSchemaRef> = Lazy::new(|| {
    Arc::new(
        GENERIC_OPERATION_PHYSICAL_SCHEMA
            .as_ref()
            .clone()
            .try_into()
            .unwrap(),
    )
});

pub static GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA: Lazy<DFSchemaRef> = Lazy::new(|| {
    Arc::new(
        GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA
            .as_ref()
            .clone()
            .try_into()
            .unwrap(),
    )
});

#[derive(Clone, Debug, Default)]
pub struct OperationInfo {
    query: Option<String>,
}

impl OperationInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_query_text(mut self, query_text: impl Into<String>) -> Self {
        self.query = Some(query_text.into());
        self
    }

    pub fn query_text(&self) -> &str {
        self.query.as_deref().unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub enum LogicalPlan {
    /// Plans related to querying the underlying data store. This will run
    /// through datafusion.
    Datafusion(DfLogicalPlan),
    /// Plans related to transaction management.
    Transaction(TransactionPlan),
    Noop,
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
        let param_values = ParamValues::LIST(scalars);

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
                    plan: Arc::new(inner.replace_params_with_values(&param_values)?),
                    stringified_plans: explain.stringified_plans.clone(),
                    schema: explain.schema.clone(),
                    logical_optimization_succeeded: explain.logical_optimization_succeeded,
                });

                return Ok(());
            }

            *plan = plan.replace_params_with_values(&param_values)?;
        }

        Ok(())
    }
}

impl From<DfLogicalPlan> for LogicalPlan {
    fn from(plan: DfLogicalPlan) -> Self {
        LogicalPlan::Datafusion(plan)
    }
}

/// A fully qualified reference to a database object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FullObjectReference<'a> {
    pub database: Cow<'a, str>,
    pub schema: Cow<'a, str>,
    pub name: Cow<'a, str>,
}

impl<'a> From<FullObjectReference<'a>> for TableReference<'a> {
    fn from(value: FullObjectReference<'a>) -> Self {
        TableReference::Full {
            catalog: value.database,
            schema: value.schema,
            table: value.name,
        }
    }
}
pub type OwnedFullObjectReference = FullObjectReference<'static>;

impl<'a> fmt::Display for FullObjectReference<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.database, self.schema, self.name)
    }
}

impl<'a> From<protogen::sqlexec::common::FullObjectReference> for FullObjectReference<'a> {
    fn from(value: protogen::sqlexec::common::FullObjectReference) -> Self {
        FullObjectReference {
            database: value.database.into(),
            schema: value.schema.into(),
            name: value.name.into(),
        }
    }
}

impl<'a> From<FullObjectReference<'a>> for protogen::sqlexec::common::FullObjectReference {
    fn from(value: FullObjectReference) -> Self {
        Self {
            database: value.database.into(),
            schema: value.schema.into(),
            name: value.name.into(),
        }
    }
}

/// A fully qualified reference to a database schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FullSchemaReference<'a> {
    pub database: Cow<'a, str>,
    pub schema: Cow<'a, str>,
}

pub type OwnedFullSchemaReference = FullSchemaReference<'static>;

impl<'a> fmt::Display for FullSchemaReference<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.database, self.schema)
    }
}

impl<'a> From<protogen::sqlexec::common::FullSchemaReference> for FullSchemaReference<'a> {
    fn from(value: protogen::sqlexec::common::FullSchemaReference) -> Self {
        FullSchemaReference {
            database: value.database.into(),
            schema: value.schema.into(),
        }
    }
}

impl<'a> From<FullSchemaReference<'a>> for protogen::sqlexec::common::FullSchemaReference {
    fn from(value: FullSchemaReference) -> Self {
        Self {
            database: value.database.into(),
            schema: value.schema.into(),
        }
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
