use std::collections::HashMap;
use std::fmt::Debug;

use dyn_clone::DynClone;
use rayexec_error::{RayexecError, Result};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::s3::credentials::AwsCredentials;
use rayexec_io::s3::S3Location;
use serde::{Deserialize, Serialize};

use crate::arrays::field::Schema;
use crate::arrays::scalar::OwnedScalarValue;
use crate::execution::operators::sink::operation::PartitionSink;

pub const FORMAT_OPT_KEY: &str = "format";

/// Arguments provided via a COPY TO statement.
///
/// Only named arguments are supported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyToArgs {
    pub named: HashMap<String, OwnedScalarValue>,
}

impl CopyToArgs {
    /// Try to get an access configuration from the arguments.
    ///
    /// This should follow the logic in table function arguments. The main
    /// difference here is that the file location is not part of an argument,
    /// and so we have to provide it separately.
    pub fn try_access_config_for_location(&self, loc: &FileLocation) -> Result<AccessConfig> {
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

        Ok(conf)
    }

    /// Try to remove the value corresponding to the FORMAT option in a COPY TO
    /// statement, returning it.
    ///
    /// We remove from the map so that function implemenations can more easily
    /// ensure that it can handle all args, and not have to worry about a left
    /// over FORMAT option.
    pub fn try_remove_format(&mut self) -> Option<OwnedScalarValue> {
        self.named.remove(FORMAT_OPT_KEY)
    }

    pub fn try_get_named(&self, name: &str) -> Result<&OwnedScalarValue> {
        self.named
            .get(name)
            .ok_or_else(|| RayexecError::new(format!("Missing COPY TO argument: '{name}'")))
    }
}

pub trait CopyToFunction: Debug + Sync + Send + DynClone {
    /// Name of the copy to function.
    fn name(&self) -> &'static str;

    /// Create a COPY TO destination that will write to the given location.
    // TODO: Additional COPY TO args once we have them.
    fn create_sinks(
        &self,
        schema: Schema,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        unimplemented!()
    }
}

impl Clone for Box<dyn CopyToFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

impl PartialEq<dyn CopyToFunction> for Box<dyn CopyToFunction + '_> {
    fn eq(&self, other: &dyn CopyToFunction) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq for dyn CopyToFunction + '_ {
    fn eq(&self, other: &dyn CopyToFunction) -> bool {
        self.name() == other.name()
    }
}

impl Eq for dyn CopyToFunction {}
