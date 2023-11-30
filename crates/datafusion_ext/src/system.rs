use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use futures::Future;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

/// A system operation can execute an arbitrary operation.
///
/// This should be focused on operations that do not require user interactions
/// (e.g. background operations like optimizing tables or running caching jobs).
pub trait SystemOperation: fmt::Debug + Sync + Send {
    /// Name of the operation. Use for debugging as well as generating
    /// appropriate errors.
    fn name(&self) -> &'static str;

    /// Create a boxed future for executing the operation.
    fn create_future(
        &self,
        context: Arc<TaskContext>,
    ) -> Pin<Box<dyn Future<Output = DataFusionResult<()>> + Send>>;
}
