//! Common data source utilities.

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::ToDFSchema;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::Expr;

pub mod errors;
pub mod query;
pub mod sink;
pub mod ssh;
pub mod url;
pub mod util;

pub(crate) fn exprs_to_phys_exprs(
    exprs: &[Expr],
    state: &SessionState,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    if let Some(expr) = conjunction(exprs.to_vec()) {
        let table_df_schema = schema.clone().to_dfschema()?;
        let filters = create_physical_expr(&expr, &table_df_schema, state.execution_props())?;
        Ok(Some(filters))
    } else {
        Ok(None)
    }
}
