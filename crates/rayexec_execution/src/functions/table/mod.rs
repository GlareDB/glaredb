pub mod series;

use dyn_clone::DynClone;
use futures::future::BoxFuture;
use futures::FutureExt;
use once_cell::sync::Lazy;
use rayexec_bullet::field::Schema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};
use series::GenerateSeries;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};

use crate::database::table::DataTable;
use crate::runtime::ExecutionRuntime;

pub static BUILTIN_TABLE_FUNCTIONS: Lazy<Vec<Box<dyn TableFunction>>> =
    Lazy::new(|| vec![Box::new(GenerateSeries)]);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableFunctionArgs {
    /// Named arguments to a table function.
    pub named: HashMap<String, OwnedScalarValue>,

    /// Positional arguments to a table function.
    pub positional: Vec<OwnedScalarValue>,
}

/// A generic table function provides a way to dispatch to a more specialized
/// table functions.
///
/// For example, the generic function 'read_csv' might have specialized versions
/// for reading a csv from the local file system, another for reading from
/// object store, etc.
///
/// The specialized variant should be determined by function argument inputs.
pub trait TableFunction: Debug + Sync + Send + DynClone {
    /// Name of the function.
    fn name(&self) -> &'static str;

    /// Optional aliases for this function.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// Plan the table function using the provide args, and do any necessary
    /// initialization.
    ///
    /// Intialization may include opening connections a remote database, and
    /// should be used determine the schema of the table we'll be returning. Any
    /// connections should remain open through execution.
    fn plan_and_initialize<'a>(
        &'a self,
        runtime: &'a Arc<dyn ExecutionRuntime>,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>>;

    /// Deserialize existing state into a planned table function.
    ///
    /// The planned functions `renitialize` method will be called prior to
    /// executing of the pipeline containing this function. `reinitialize` will
    /// not be called if the machine deserializing the function will not be
    /// executing the function.
    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>>;
}

impl Clone for Box<dyn TableFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn TableFunction> for Box<dyn TableFunction + '_> {
    fn eq(&self, other: &dyn TableFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn TableFunction + '_ {
    fn eq(&self, other: &dyn TableFunction) -> bool {
        self.name() == other.name()
    }
}

pub trait PlannedTableFunction: Debug + Sync + Send + DynClone {
    /// Reinitialize the table function, including re-opening any connections
    /// needed.
    ///
    /// This is called immediately after deserializing a planned function in
    /// order populate fields that cannot be serialized and moved across
    /// machines.
    ///
    /// The default implementation does nothing.
    fn reinitialize(&self, _runtime: &Arc<dyn ExecutionRuntime>) -> BoxFuture<Result<()>> {
        async move { Ok(()) }.boxed()
    }

    /// Return an reference to some serializable state.
    ///
    /// Typically this just returns `self` with Self implementing Serialize via
    /// a derive macro.
    fn serializable_state(&self) -> &dyn erased_serde::Serialize;

    /// Returns a reference to the table function that initialized this
    /// function.
    fn table_function(&self) -> &dyn TableFunction;

    /// Get the schema for the function output.
    ///
    /// Admittedly passing a runtime here feels a bit weird, but I don't think
    /// is a terrible solution. This might change as we implement more data
    /// sources.
    fn schema(&self) -> Schema;

    /// Return a data table representing the function output.
    ///
    /// An engine runtime is provided for table funcs that return truly async
    /// data tables.
    fn datatable(&self, runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>>;
}

impl PartialEq<dyn PlannedTableFunction> for Box<dyn PlannedTableFunction + '_> {
    fn eq(&self, other: &dyn PlannedTableFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn PlannedTableFunction + '_ {
    fn eq(&self, other: &dyn PlannedTableFunction) -> bool {
        self.table_function() == other.table_function() && self.schema() == other.schema()
    }
}

impl Clone for Box<dyn PlannedTableFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

pub fn check_named_args_is_empty(func: &dyn TableFunction, args: &TableFunctionArgs) -> Result<()> {
    if !args.named.is_empty() {
        return Err(RayexecError::new(format!(
            "'{}' does not take named arguments",
            func.name()
        )));
    }
    Ok(())
}
