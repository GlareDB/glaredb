pub mod dummy;
pub mod empty;

use arrow_schema::Schema;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::expr::scalar::ScalarValue;
use crate::physical::plans::Source;
use crate::physical::PhysicalOperator;
use crate::planner::explainable::Explainable;
use crate::types::batch::NamedDataBatchSchema;

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
    pub unnamed: Vec<ScalarValue>,

    /// Named arguments to the function.
    pub named: HashMap<String, ScalarValue>,
}

#[derive(Debug, Default)]
pub struct Pushdown {}

pub trait TableFunction: Debug {
    /// Name of the function, used when aliasing a function call.
    fn name(&self) -> &str;

    /// Bind the table function using the provided arguments.
    ///
    /// A TableFunction may return different implementations of a
    /// BoundTableFunction depending on the arguments provided.
    fn bind(&self, args: TableFunctionArgs) -> Result<Box<dyn BoundTableFunction>>;
}

/// An intermediate function type used during planning an optimization.
// TODO: Clone semantics
pub trait BoundTableFunction: Send + Debug + Explainable {
    /// Return the schema.
    ///
    /// This should the full output schema without any sort of projections
    /// applied.
    fn schema(&self) -> NamedDataBatchSchema;

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
    fn create_operator(
        &self,
        projection: Vec<usize>,
        pushdown: Pushdown,
    ) -> Arc<dyn PhysicalOperator>;
}

pub trait TableFunctionSource: Source + Explainable {}
