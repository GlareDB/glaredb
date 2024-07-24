use dyn_clone::DynClone;
use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::s3::credentials::AwsCredentials;
use rayexec_io::s3::S3Location;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::runtime::ExecutionRuntime;

/// Arguments provided via a COPY TO statement.
///
/// Only named arguments are supported.
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
        runtime: &Arc<dyn ExecutionRuntime>,
        schema: Schema,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn CopyToSink>>>;
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

pub trait CopyToSink: Debug + Send {
    /// Push a batch to the sink.
    ///
    /// Batches are pushed in the order they're received in.
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>>;

    /// Finalize the sink.
    ///
    /// Called once only after all batches have been pushed. If there's any
    /// pending work that needs to happen (flushing), it should happen here.
    /// Once this returns, the sink is complete.
    fn finalize(&mut self) -> BoxFuture<'_, Result<()>>;
}
