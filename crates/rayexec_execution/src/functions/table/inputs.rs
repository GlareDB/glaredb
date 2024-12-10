use std::collections::HashMap;
use std::fmt::Debug;

use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::s3::credentials::AwsCredentials;
use rayexec_io::s3::S3Location;
use serde::{Deserialize, Serialize};

use super::TableFunction;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableFunctionInputs {
    /// Named arguments to a table function.
    pub named: HashMap<String, OwnedScalarValue>,

    /// Positional arguments to a table function.
    pub positional: Vec<OwnedScalarValue>,
}

impl TableFunctionInputs {
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

    pub fn try_get_position(&self, pos: usize) -> Result<&OwnedScalarValue> {
        self.positional
            .get(pos)
            .ok_or_else(|| RayexecError::new(format!("Expected argument at position {pos}")))
    }
}

pub fn check_named_args_is_empty(
    func: &dyn TableFunction,
    args: &TableFunctionInputs,
) -> Result<()> {
    if !args.named.is_empty() {
        return Err(RayexecError::new(format!(
            "'{}' does not take named arguments",
            func.name()
        )));
    }
    Ok(())
}
