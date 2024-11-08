pub mod series;
pub mod system;

use std::collections::HashMap;
use std::fmt::Debug;

use dyn_clone::DynClone;
use futures::future::BoxFuture;
use futures::FutureExt;
use once_cell::sync::Lazy;
use rayexec_bullet::field::Schema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::s3::credentials::AwsCredentials;
use rayexec_io::s3::S3Location;
use serde::{Deserialize, Serialize};
use series::GenerateSeries;
use system::{ListDatabases, ListSchemas, ListTables};

use crate::database::DatabaseContext;
use crate::logical::statistics::StatisticsValue;
use crate::storage::table_storage::DataTable;

pub static BUILTIN_TABLE_FUNCTIONS: Lazy<Vec<Box<dyn TableFunction>>> = Lazy::new(|| {
    vec![
        Box::new(GenerateSeries),
        // Various list system object functions.
        Box::new(ListDatabases::new()),
        Box::new(ListSchemas::new()),
        Box::new(ListTables::new()),
    ]
});

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableFunctionArgs {
    /// Named arguments to a table function.
    pub named: HashMap<String, OwnedScalarValue>,

    /// Positional arguments to a table function.
    pub positional: Vec<OwnedScalarValue>,
}

impl TableFunctionArgs {
    /// Try to get a file location and access config from the table args.
    // TODO: Secrets provider that we pass in allowing us to get creds from some
    // secrets store.
    pub fn try_location_and_access_config(&self) -> Result<(FileLocation, AccessConfig)> {
        let loc = match self.positional.first() {
            Some(loc) => {
                let loc = loc.try_as_str()?;
                FileLocation::parse(loc)
            }
            None => return Err(RayexecError::new("Expected at least one position argument")),
        };

        let conf = match &loc {
            FileLocation::Url(url) => {
                if S3Location::is_s3_location(url) {
                    let key_id = self.try_get_named("key_id")?.try_as_str()?.to_string();
                    let secret = self.try_get_named("secret")?.try_as_str()?.to_string();
                    let region = self.try_get_named("region")?.try_as_str()?.to_string();

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

    pub fn try_get_named(&self, name: &str) -> Result<&OwnedScalarValue> {
        self.named
            .get(name)
            .ok_or_else(|| RayexecError::new(format!("Expected named argument '{name}'")))
    }
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
    fn plan_and_initialize(
        &self,
        context: &DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>>;

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
    ///
    /// Admittedly passing a runtime here feels a bit weird, but I don't think
    /// is a terrible solution. This might change as we implement more data
    /// sources.
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

pub fn check_named_args_is_empty(func: &dyn TableFunction, args: &TableFunctionArgs) -> Result<()> {
    if !args.named.is_empty() {
        return Err(RayexecError::new(format!(
            "'{}' does not take named arguments",
            func.name()
        )));
    }
    Ok(())
}
