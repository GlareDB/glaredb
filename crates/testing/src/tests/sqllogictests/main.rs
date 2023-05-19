use std::{env, path::Path};

use anyhow::Result;
use testing::slt::SltRunner;
use tokio_postgres::Config;

fn main() -> Result<()> {
    SltRunner::new()
        .test_files_dir(Path::new("testdata"))?
        .pre_test_hook(
            "sqllogictests/functions/postgres",
            pre_test_functions_postgres,
        )?
        .run()
}

fn pre_test_functions_postgres(config: &Config) -> Result<()> {
    // Set the current database to test database
    env::set_var(
        "SLT_FUNCTIONS_POSTGRES_CURRENT_DATABASE",
        config.get_dbname().unwrap(),
    );
    Ok(())
}
