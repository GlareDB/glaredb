pub mod builtin;
pub mod inout;
pub mod inputs;
pub mod scan;

use std::collections::HashMap;
use std::fmt::Debug;

use dyn_clone::DynClone;
use futures::future::BoxFuture;
use futures::FutureExt;
use inout::TableInOutFunction;
use inputs::TableFunctionInputs;
use rayexec_bullet::field::Schema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::Result;
use scan::TableScanFunction;

use super::FunctionInfo;
use crate::database::DatabaseContext;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableList;
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
pub trait TableFunction: FunctionInfo + Debug + Sync + Send + DynClone {
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
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction2>>> {
        unimplemented!()
    }

    fn initialize<'a>(
        &self,
        _context: &'a DatabaseContext,
        _args: TableFunctionInputs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction2>>> {
        unimplemented!()
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction2>> {
        unimplemented!()
    }

    fn planner(&self) -> TableFunctionPlanner {
        unimplemented!()
    }
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
pub enum TableFunctionPlanner<'a> {
    InOut(&'a dyn InOutPlanner),
    Scan(&'a dyn ScanPlanner),
}

pub trait InOutPlanner: Debug {
    /// Plans an in/out function with possibly dynamic positional inputs.
    fn plan(
        &self,
        table_list: &TableList,
        positional_inputs: Vec<Expression>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> Result<PlannedTableFunction>;
}

pub trait ScanPlanner: Debug {
    /// Plans an table scan function.
    ///
    /// This only accepts constant arguments as it's meant to be used when
    /// reading tables from an external resource. Functions like `read_parquet`
    /// or `read_postgres` should implement this.
    fn plan<'a>(
        &self,
        context: &'a DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction>>;
}

#[derive(Debug, Clone)]
pub struct PlannedTableFunction {
    /// The function that did the planning.
    pub function: Box<dyn TableFunction>,
    /// Unnamed positional arguments.
    pub positional_inputs: Vec<Expression>,
    /// Named arguments.
    pub named_inputs: HashMap<String, OwnedScalarValue>, // Requiring constant values for named args is currently a limitation.
    /// The function implementation.
    ///
    /// The variant used here should match the variant of the planner that
    /// `function` returns from its `planner` method.
    pub function_impl: TableFunctionImpl,
    /// Output cardinality of the function.
    pub cardinality: StatisticsValue<usize>,
    /// Output schema of the function.
    pub schema: Schema,
}

impl PartialEq for PlannedTableFunction {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.positional_inputs == other.positional_inputs
            && self.named_inputs == other.named_inputs
            && self.schema == other.schema
    }
}

impl Eq for PlannedTableFunction {}

#[derive(Debug, Clone)]
pub enum TableFunctionImpl {
    /// Table function that produces a table as its output.
    Scan(Box<dyn TableScanFunction>),
    /// A table function that accepts dynamic arguments and produces a table
    /// output.
    InOut(Box<dyn TableInOutFunction>),
}

pub trait PlannedTableFunction2: Debug + Sync + Send + DynClone {
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

impl PartialEq<dyn PlannedTableFunction2> for Box<dyn PlannedTableFunction2 + '_> {
    fn eq(&self, other: &dyn PlannedTableFunction2) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn PlannedTableFunction2 + '_ {
    fn eq(&self, other: &dyn PlannedTableFunction2) -> bool {
        self.table_function() == other.table_function() && self.schema() == other.schema()
    }
}

impl Eq for dyn PlannedTableFunction2 {}

impl Clone for Box<dyn PlannedTableFunction2> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}
