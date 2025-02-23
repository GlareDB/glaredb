pub mod builtin;
pub mod file_scan;
pub mod inout;
pub mod multi_file;
pub mod scan;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use dyn_clone::DynClone;
use futures::future::BoxFuture;
use inout::TableInOutFunction;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::s3::credentials::AwsCredentials;
use rayexec_io::s3::S3Location;

use super::FunctionInfo;
use crate::arrays::field::Schema;
use crate::arrays::scalar::ScalarValue;
use crate::database::DatabaseContext;
use crate::execution::operators::source::operation::SourceOperation;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableList;
use crate::logical::statistics::StatisticsValue;

/// A generic table function provides a way to dispatch to a more specialized
/// table functions.
///
/// For example, the generic function 'read_csv' might have specialized versions
/// for reading a csv from the local file system, another for reading from
/// object store, etc.
///
/// The specialized variant should be determined by function argument inputs.
pub trait TableFunction: FunctionInfo + Debug + Sync + Send + DynClone {
    /// Return a planner that will produce a planned table function.
    fn planner(&self) -> TableFunctionPlanner;
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

/// The types of table function planners supported.
#[derive(Debug)]
pub enum TableFunctionPlanner<'a> {
    /// Produces a table function that accept inputs and produce outputs.
    InOut(&'a dyn InOutPlanner),
    /// Produces a table function that acts as just a scan.
    Scan(&'a dyn ScanPlanner),
}

pub trait InOutPlanner: Debug {
    /// Plans an in/out function with possibly dynamic positional inputs.
    fn plan(
        &self,
        table_list: &TableList,
        positional_inputs: Vec<Expression>,
        named_inputs: HashMap<String, ScalarValue>,
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
        positional: Vec<ScalarValue>,
        named: HashMap<String, ScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction>>;
}

#[derive(Debug, Clone)]
pub struct PlannedTableFunction {
    /// The function that did the planning.
    pub function: Box<dyn TableFunction>,
    /// Unnamed positional arguments.
    pub positional: Vec<Expression>,
    /// Named arguments.
    pub named: HashMap<String, ScalarValue>, // Requiring constant values for named args is currently a limitation.
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
            && self.positional == other.positional
            && self.named == other.named
            && self.schema == other.schema
    }
}

impl Eq for PlannedTableFunction {}

#[derive(Debug, Clone)]
pub enum TableFunctionImpl {
    /// Table function that produces a table as its output.
    // TODO: Try to remove the Arc+Mutex.
    //
    // Currently expressions can be cloned mostly for ease of implementation of
    // optimizer rules. The Arc+Mutex here is to allow that without needing to
    // do a larger refactor right now.
    //
    // There will only be a single instance of this object after all the
    // planning/optimization. The mutex also shouldn't be that expensive since
    // only a single thread locks it when creating the states, and it's only
    // locked once.
    Scan(Arc<Mutex<dyn SourceOperation>>),
    /// A table function that accepts dynamic arguments and produces a table
    /// output.
    InOut(Box<dyn TableInOutFunction>),
}

impl TableFunctionImpl {
    pub fn new_scan<S>(source: S) -> Self
    where
        S: SourceOperation,
    {
        TableFunctionImpl::Scan(Arc::new(Mutex::new(source)))
    }
}

/// Try to get a file location and access config from the table args.
// TODO: Secrets provider that we pass in allowing us to get creds from some
// secrets store.
pub fn try_location_and_access_config_from_args(
    func: &impl TableFunction,
    positional: &[ScalarValue],
    named: &HashMap<String, ScalarValue>,
) -> Result<(FileLocation, AccessConfig)> {
    let loc = match positional.first() {
        Some(loc) => {
            let loc = loc.try_as_str()?;
            FileLocation::parse(loc)
        }
        None => {
            return Err(RayexecError::new(format!(
                "Expected at least one position argument for function {}",
                func.name(),
            )))
        }
    };

    let conf = match &loc {
        FileLocation::Url(url) => {
            if S3Location::is_s3_location(url) {
                let key_id = try_get_named(func, "key_id", named)?
                    .try_as_str()?
                    .to_string();
                let secret = try_get_named(func, "secret", named)?
                    .try_as_str()?
                    .to_string();
                let region = try_get_named(func, "region", named)?
                    .try_as_str()?
                    .to_string();

                AccessConfig::S3 {
                    credentials: AwsCredentials { key_id, secret },
                    region,
                }
            } else {
                AccessConfig::None
            }
        }
        FileLocation::Path(_) => AccessConfig::None,
    };

    Ok((loc, conf))
}

pub fn try_get_named<'a>(
    func: &impl TableFunction,
    name: &str,
    named: &'a HashMap<String, ScalarValue>,
) -> Result<&'a ScalarValue> {
    named.get(name).ok_or_else(|| {
        RayexecError::new(format!(
            "Expected named argument '{name}' for function {}",
            func.name()
        ))
    })
}

pub fn try_get_positional<'a>(
    func: &impl TableFunction,
    pos: usize,
    positional: &'a [ScalarValue],
) -> Result<&'a ScalarValue> {
    positional.get(pos).ok_or_else(|| {
        RayexecError::new(format!(
            "Expected argument at position {pos} for function {}",
            func.name()
        ))
    })
}
