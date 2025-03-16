use std::fmt::Debug;

use glaredb_error::Result;

#[derive(Debug, Clone, Copy)]
pub enum OptionValue<'a> {
    String(&'a str),
    Int(i64),
}

pub trait OptionMap {
    /// Try to get an option value with the given key.
    fn get<'a>(&'a self, key: &str) -> Option<OptionValue<'a>>;
}

/// Describes configuration for accessing files.
pub trait AccessConfig: Debug + Sized {
    /// Construct an access config from the given unnamed and named options.
    fn from_options(unnamed: &[OptionValue], named: impl OptionMap) -> Result<Self>;
}
