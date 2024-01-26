use std::sync::Arc;

use datafusion::datasource::TableProvider;

/// Read from the environment (e.g. Python dataframes).
pub trait EnvironmentReader: Send + Sync {
    /// Try to resolve a table from the environment.
    ///
    /// If no table with the name exists, return `Ok(None)`.
    fn resolve_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, Box<dyn std::error::Error + Send + Sync>>;
}
