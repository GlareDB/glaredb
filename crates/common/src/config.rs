use config::{builder::DefaultState, Config, ConfigBuilder, Environment, File, FileFormat};
use once_cell::sync::OnceCell;
use serde::Deserialize;

use crate::access::AccessConfig;
use crate::errors::Result;

const DEFAULT_CONFIG_FILE: &str = "config/default.toml";
const PREFIX: &str = "GLAREDB";
const SEPARATOR: &str = "__";

pub static CONFIG: OnceCell<DbConfig> = OnceCell::new();

/// Configuration for GlareDB. Default config file items can be overriden by using environment
/// variables with the following prefix, `GLAREDB__`.
#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub access: AccessConfig,
}

impl DbConfig {
    pub fn new() -> Result<Self> {
        let config = Self::base(None).build()?;
        Ok(config.try_deserialize()?)
    }

    /// Generates the base configuration for glaredb from `DEFAULT_CONFIG_FILE` and all environment
    /// variables prefixed with `GLAREDB__`. The rest of the environment are based on the
    /// deserialized version of `DbConfig`
    pub fn base(config: Option<String>) -> ConfigBuilder<DefaultState> {
        let default_config = File::new(DEFAULT_CONFIG_FILE, FileFormat::Toml).required(true);
        let env_config = Environment::with_prefix(PREFIX)
            .separator(SEPARATOR)
            .ignore_empty(true)
            .keep_prefix(false);

        let mut config_builder = Config::builder().add_source(default_config);

        if let Some(config) = config {
            let config = File::new(&config, FileFormat::Toml).required(true);
            config_builder = config_builder.add_source(config);
        }

        config_builder.add_source(env_config)
    }
}
