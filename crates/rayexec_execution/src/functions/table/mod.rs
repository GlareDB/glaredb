pub mod series;

use dyn_clone::DynClone;
use futures::future::BoxFuture;
use once_cell::sync::Lazy;
use rayexec_bullet::field::Schema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use series::GenerateSeries;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};

use crate::database::table::DataTable;
use crate::runtime::ExecutionRuntime;

pub static BUILTIN_TABLE_FUNCTIONS: Lazy<Vec<Box<dyn GenericTableFunction>>> =
    Lazy::new(|| vec![Box::new(GenerateSeries)]);

#[derive(Debug, Clone, PartialEq)]
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
pub trait GenericTableFunction: Debug + Sync + Send + DynClone {
    /// Name of the function.
    fn name(&self) -> &'static str;

    /// Optional aliases for this function.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// Specializes this function with the given arguments.
    fn specialize(&self, args: TableFunctionArgs) -> Result<Box<dyn SpecializedTableFunction>>;
}

impl Clone for Box<dyn GenericTableFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn GenericTableFunction> for Box<dyn GenericTableFunction + '_> {
    fn eq(&self, other: &dyn GenericTableFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn GenericTableFunction + '_ {
    fn eq(&self, other: &dyn GenericTableFunction) -> bool {
        self.name() == other.name()
    }
}

pub trait SpecializedTableFunction: Debug + Sync + Send + DynClone {
    /// Name of the specialized function.
    fn name(&self) -> &'static str;

    /// Initializes the table function using the provided runtime.
    fn initialize(
        self: Box<Self>,
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>>;
}

impl PartialEq<dyn SpecializedTableFunction> for Box<dyn SpecializedTableFunction + '_> {
    fn eq(&self, other: &dyn SpecializedTableFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn SpecializedTableFunction + '_ {
    fn eq(&self, other: &dyn SpecializedTableFunction) -> bool {
        self.name() == other.name()
    }
}

pub trait InitializedTableFunction: Debug + Sync + Send + DynClone {
    /// Returns a reference to the specialized function that initialized this
    /// function.
    fn specialized(&self) -> &dyn SpecializedTableFunction;

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

impl PartialEq<dyn InitializedTableFunction> for Box<dyn InitializedTableFunction + '_> {
    fn eq(&self, other: &dyn InitializedTableFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn InitializedTableFunction + '_ {
    fn eq(&self, other: &dyn InitializedTableFunction) -> bool {
        self.specialized() == other.specialized() && self.schema() == other.schema()
    }
}

impl Clone for Box<dyn InitializedTableFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

pub fn check_named_args_is_empty(
    func: &dyn GenericTableFunction,
    args: &TableFunctionArgs,
) -> Result<()> {
    if !args.named.is_empty() {
        return Err(RayexecError::new(format!(
            "'{}' does not take named arguments",
            func.name()
        )));
    }
    Ok(())
}
