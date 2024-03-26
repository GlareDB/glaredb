use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use glob::Pattern;
use regex::{Captures, Regex};
use sqllogictest::{parse_with_name, ColumnType, Injected, Record, Runner};
use tokio_postgres::Config;

use crate::clients::TestClient;

#[async_trait]
pub trait Hook: Send + Sync {
    async fn pre(
        &self,
        _config: &Config,
        _client: TestClient,
        _vars: &mut HashMap<String, String>,
    ) -> Result<bool> {
        // TODO: make enum for skip/continue rather than booleans
        Ok(true)
    }

    async fn post(
        &self,
        _config: &Config,
        _client: TestClient,
        _vars: &HashMap<String, String>,
    ) -> Result<()> {
        Ok(())
    }
}

pub type TestHook = Arc<dyn Hook>;

/// List of hooks that should be ran for tests that match a pattern.
///
/// For example, a pattern "*" will run a hook against all tests, while
/// "*/tunnels/ssh" would only run hooks for the ssh tunnels tests.
pub type TestHooks = Vec<(Pattern, TestHook)>;

#[async_trait]
pub trait FnTest: Send + Sync {
    async fn run(
        &self,
        config: &Config,
        client: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<()>;
}

const ENV_REGEX: &str = r"\$\{\s*(\w+)\s*\}";

pub enum Test {
    File(PathBuf),
    FnTest(Box<dyn FnTest>),
}

impl Debug for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(path) => write!(f, "File({path:?})"),
            Self::FnTest(_) => write!(f, "FnTest"),
        }
    }
}

impl Test {
    pub async fn execute(
        self,
        config: &Config,
        client: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<()> {
        match self {
            Self::File(path) => {
                let regx = Regex::new(ENV_REGEX).unwrap();
                let records = parse_file(&regx, &path, vars)?;

                let mut runner = Runner::new(|| {
                    let client = client.clone();
                    async { Ok(client) }
                });

                runner
                    .run_multi_async(records)
                    .await
                    .map_err(|e| anyhow!("test fail: {}", e))
            }
            Self::FnTest(fn_test) => fn_test.run(config, client, vars).await,
        }
    }
}

fn parse_file<T: ColumnType>(
    regx: &Regex,
    path: &Path,
    vars: &HashMap<String, String>,
) -> Result<Vec<Record<T>>> {
    let script = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("Error while opening `{}`: {}", path.to_string_lossy(), e))?;

    // Replace all occurances of ${some_env_var} with actual values
    // from the environment.
    let mut err = None;
    let script = regx.replace_all(&script, |caps: &Captures| {
        let env_var = &caps[1];
        // Try if there's a local var with the key. Fallback to environment
        // variable.
        if let Some(var) = vars.get(env_var) {
            return var.to_string();
        }
        match std::env::var(env_var) {
            Ok(v) => v,
            Err(error) => {
                let error = anyhow!("Error fetching environment variable `{env_var}`: {error}");
                let err_msg = error.to_string();
                err = Some(error);
                err_msg
            }
        }
    });
    if let Some(err) = err {
        return Err(err);
    }

    let mut records = vec![];

    let script_name = path.to_str().unwrap();
    let parsed_records = parse_with_name(&script, script_name).map_err(|e| {
        anyhow!(
            "Error while parsing `{}`: {}",
            path.to_string_lossy(),
            e.kind()
        )
    })?;

    for rec in parsed_records {
        records.push(rec);

        // What we just pushed
        let rec = records.last().unwrap();

        // Includes are not actually processed by the runner. It's more of a
        // pre-processor, so we process them during the parse stage.
        //
        // This code was borrowed from `parse_file` function since the inner
        // function is private.

        if let Record::Include { filename, .. } = rec {
            let complete_filename = {
                let mut path_buf = path.to_path_buf();
                path_buf.pop();
                path_buf.push(filename.clone());
                path_buf.as_os_str().to_string_lossy().to_string()
            };

            for included_file in glob::glob(&complete_filename)
                .map_err(|e| anyhow!("Invalid include file at {}: {}", path.to_string_lossy(), e))?
                .filter_map(Result::ok)
            {
                let included_file = included_file.as_os_str().to_string_lossy().to_string();

                records.push(Record::Injected(Injected::BeginInclude(
                    included_file.clone(),
                )));
                records.extend(parse_file(regx, &PathBuf::from(&included_file), vars)?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}
