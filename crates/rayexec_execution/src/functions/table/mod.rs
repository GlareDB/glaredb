pub mod builtin;
pub mod inout;
pub mod inputs;
pub mod out;

use std::fmt::Debug;

use dyn_clone::DynClone;
use futures::future::BoxFuture;
use futures::FutureExt;
use inout::TableInOutFunction;
use inputs::TableFunctionInputs;
use out::TableOutFunction;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;

use crate::database::DatabaseContext;
use crate::logical::statistics::StatisticsValue;
use crate::storage::table_storage::DataTable;

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
        &self,
        context: &'a DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>>;

    fn initialize<'a>(
        &self,
        context: &'a DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        unimplemented!()
    }

    fn reinitialize<'a>(
        &self,
        _context: &'a DatabaseContext,
        state: TableFunctionState,
    ) -> BoxFuture<'a, Result<TableFunctionState>> {
        async move { Ok(state) }.boxed()
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction>>;
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

impl Eq for dyn TableFunction {}

#[derive(Debug)]
pub struct TableFunctionState {
    pub table_function: Box<dyn TableFunction>,
    pub inputs: TableFunctionInputs,
    pub out_function: Option<Box<dyn TableOutFunction>>,
    pub inout_function: Option<Box<dyn TableInOutFunction>>,
    pub cardinality: StatisticsValue<usize>,
    pub schema: Schema,
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
    fn reinitialize(&self) -> BoxFuture<Result<()>> {
        async move { Ok(()) }.boxed()
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()>;

    /// Returns a reference to the table function that initialized this
    /// function.
    fn table_function(&self) -> &dyn TableFunction;

    /// Get the schema for the function output.
    fn schema(&self) -> Schema;

    /// Get the cardinality of the output.
    fn cardinality(&self) -> StatisticsValue<usize> {
        StatisticsValue::Unknown
    }

    /// Return a data table representing the function output.
    ///
    /// An engine runtime is provided for table funcs that return truly async
    /// data tables.
    fn datatable(&self) -> Result<Box<dyn DataTable>>;
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

impl Eq for dyn PlannedTableFunction {}

impl Clone for Box<dyn PlannedTableFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}
