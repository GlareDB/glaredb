pub mod csv;
pub mod dummy;
pub mod empty;
pub mod generate_series;
pub mod read_parquet;

use futures::{Future, Stream};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::Result;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;

use crate::physical::plans::SourceOperator2;
use crate::planner::explainable::Explainable;

/// Statistics for a table function.
#[derive(Debug, Clone, Copy)]
pub struct Statistics {
    pub estimated_cardinality: Option<usize>,
    pub max_cardinality: Option<usize>,
}

/// Arguments to a table function.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableFunctionArgs {
    /// Unnamed arguments to the function.
    ///
    /// Order typically matters.
    pub unnamed: Vec<OwnedScalarValue>,

    /// Named arguments to the function.
    pub named: HashMap<String, OwnedScalarValue>,
}

#[derive(Debug, Default)]
pub struct Pushdown {}

/// A "generic" table function.
pub trait TableFunction: Debug {
    /// Name of the function.
    fn name(&self) -> &str;

    /// Specialize a table function using the provided arguments.
    fn specialize(&self, args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>>;
}

/// A "specialized" table function.
///
/// Specialized table functions allow for returning different implementations of
/// a table function based on provided arguments. For example, the function
/// 'read_csv' might have specializations for reading from a local file system,
/// object storeage, and http.
pub trait SpecializedTableFunction: Send + Debug {
    /// Convert the function into a task for creating a bound function.
    ///
    /// This task will be passed to the scheduler.
    fn into_bind_task(self: Box<Self>) -> BindTableFunctionTask;
}

/// Task for binding a table function.
pub enum BindTableFunctionTask {
    /// Binding can be done synchronously.
    Sync {
        bind_fn: Box<dyn Fn() -> Result<Box<dyn BoundTableFunction>>>,
    },

    /// Binding needs to happen async.
    ///
    /// This is particularly useful for non-local data sources.
    Async {
        bind_fn: Box<dyn Future<Output = Result<Box<dyn BoundTableFunction>>>>,
    },
}

impl BindTableFunctionTask {
    pub fn new_sync(
        bind_fn: impl Into<Box<dyn Fn() -> Result<Box<dyn BoundTableFunction>>>>,
    ) -> Self {
        BindTableFunctionTask::Sync {
            bind_fn: bind_fn.into(),
        }
    }

    pub fn new_async(
        bind_fn: impl Into<Box<dyn Future<Output = Result<Box<dyn BoundTableFunction>>>>>,
    ) -> Self {
        BindTableFunctionTask::Async {
            bind_fn: bind_fn.into(),
        }
    }
}

/// Async stream of batches from a table function.
pub type TableFunctionStream = Pin<Box<dyn Stream<Item = Result<Batch>>>>;

/// A table function that's been bound.
pub trait BoundTableFunction: Sync + Send + Debug + Explainable {
    /// Returns the full schema of the table.
    fn schema(&self) -> &Schema;

    /// Return statistics about the table.
    fn statistics(&self) -> Statistics;

    /// Return number of partitions.
    fn partitions(&self) -> usize;

    /// Get a stream for the provided partition.
    fn execute(&self, partition: usize) -> Result<TableFunctionStream>;
}

pub trait TableFunctionOld: Debug {
    /// Name of the function, used when aliasing a function call.
    fn name(&self) -> &str;

    /// Bind the table function using the provided arguments.
    ///
    /// A TableFunction may return different implementations of a
    /// BoundTableFunction depending on the arguments provided.
    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunctionOld>>;
}

/// An intermediate function type used during planning an optimization.
// TODO: Clone semantics
pub trait BoundTableFunctionOld: Send + Debug + Explainable {
    /// Return the schema.
    ///
    /// This should the full output schema without any sort of projections
    /// applied.
    fn schema(&self) -> &Schema;

    /// Returns statistics for the bound function.
    fn statistics(&self) -> Statistics;

    // /// Configure this function to execution with `n` output partitions.
    // ///
    // /// The true number of output partitions should be returned.
    // fn with_partitions(&mut self, n: usize) -> usize;

    // /// Configure this function operate using the provided predicates.
    // ///
    // /// Predicates which this function cannot push down should be returned.
    // fn with_predicates(&mut self, predicates: Vec<()>) -> Vec<()>;

    // /// Configure this function to only project the provided columns.
    // fn with_projection(&mut self, columns: Vec<usize>);

    /// Convert the bound function into an executable source.
    ///
    /// Note that this accepts a boxed Self to allow dynamically dispatching the
    /// table functions and ensuring that creating an operator takes complete
    /// ownership of the bound function.
    fn into_source(
        self: Box<Self>,
        projection: Vec<usize>,
        pushdown: Pushdown,
    ) -> Result<Box<dyn SourceOperator2>>;
}
