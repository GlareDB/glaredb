use std::fs;

use common::config::DbConfig;

/// Relative path to default.toml
const DEFAULT_CONFIG_TOML: &str = "../../config/default.toml";

fn main() {
    let default_config = DbConfig::default();

    let toml_string =
        toml::to_string(&default_config).expect("Could not encode default config to TOML value");

    fs::write(DEFAULT_CONFIG_TOML, toml_string).expect("Could not write to file!");
}
