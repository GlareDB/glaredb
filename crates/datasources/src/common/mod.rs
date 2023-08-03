//! Common data source utilities.

use std::sync::Arc;

use datafusion::common::ToDFSchema;
use datafusion::error::Result;
use datafusion::{
    arrow::datatypes::Schema, execution::context::SessionState, optimizer::utils::conjunction,
    physical_expr::create_physical_expr, physical_plan::PhysicalExpr, prelude::Expr,
};
pub mod errors;
pub mod listing;
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
        // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
        let table_df_schema = schema.clone().to_dfschema()?;
        let filters =
            create_physical_expr(&expr, &table_df_schema, &schema, state.execution_props())?;
        Ok(Some(filters))
    } else {
        Ok(None)
    }
}
