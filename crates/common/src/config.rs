use once_cell::sync::OnceCell;

use config::{builder::DefaultState, Config, ConfigBuilder, ConfigError, File, FileFormat};
use serde::Deserialize;

use crate::access::AccessConfig;

const DEFAULT_CONFIG_FILE: &str = "config/default.toml";
const GLAREDB_CONFIG_FILE: &str = "glaredb.toml";

pub static CONFIG: OnceCell<DbConfig> = OnceCell::new();

// The library's required configuration.
#[derive(Default, Debug, Deserialize)]
pub struct DbConfig {
    pub access: AccessConfig,
}

impl DbConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let config = Self::base(None).build()?;
        config.try_deserialize()
    }

    pub fn base(config: Option<String>) -> ConfigBuilder<DefaultState> {
        let default_config = File::new(DEFAULT_CONFIG_FILE, FileFormat::Toml).required(true);
        let glaredb_config = File::new(GLAREDB_CONFIG_FILE, FileFormat::Toml).required(false);

        let mut config_builder = Config::builder()
            .add_source(default_config)
            .add_source(glaredb_config);

        if let Some(config) = config {
            let config = File::new(&config, FileFormat::Toml).required(true);
            config_builder = config_builder.add_source(config);
        }

        config_builder
    }
}
