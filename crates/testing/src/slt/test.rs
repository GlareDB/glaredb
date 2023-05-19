use anyhow::{anyhow, Result};
use async_trait::async_trait;
use glob::Pattern;
use regex::{Captures, Regex};
use sqllogictest::{
    parse_with_name, AsyncDB, ColumnType, DBOutput, DefaultColumnType, Injected, Record, Runner,
};
use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio_postgres::{Client, Config, SimpleQueryMessage};

pub type TestHook = fn(&Config) -> Result<()>;

pub type TestHooks = Vec<(Pattern, TestHook)>;

const ENV_REGEX: &str = r"\$\{\s*(\w+)\s*\}";

#[derive(Debug)]
pub enum Test {
    File(PathBuf),
    // TODO: Test function
}

impl Test {
    pub async fn execute(self, runner: &mut Runner<TestClient>) -> Result<()> {
        match self {
            Self::File(path) => {
                let regx = Regex::new(ENV_REGEX).unwrap();
                let records = parse_file(&regx, &path)?;
                runner
                    .run_multi_async(records)
                    .await
                    .map_err(|e| anyhow!("test fail: {}", e))?;
            }
        };
        Ok(())
    }
}

fn parse_file<T: ColumnType>(regx: &Regex, path: &Path) -> Result<Vec<Record<T>>> {
    let script = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("Error while opening `{}`: {}", path.to_string_lossy(), e))?;

    // Replace all occurances of ${some_env_var} with actual values
    // from the environment.
    let mut err = None;
    let script = regx.replace_all(&script, |caps: &Captures| {
        let env_var = &caps[1];
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
                records.extend(parse_file(regx, &PathBuf::from(&included_file))?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}

pub struct TestClient {
    pub client: Client,
}

#[async_trait]
impl AsyncDB for TestClient {
    type Error = tokio_postgres::Error;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut output = Vec::new();
        let mut num_columns = 0;
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            match row {
                SimpleQueryMessage::Row(row) => {
                    num_columns = row.len();
                    let mut row_output = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    row_output.push("(empty)".to_string());
                                } else {
                                    row_output.push(v.to_string());
                                }
                            }
                            None => row_output.push("NULL".to_string()),
                        }
                    }
                    output.push(row_output);
                }
                SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
        }

        if output.is_empty() && num_columns == 0 {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text; num_columns],
                rows: output,
            })
        }
    }

    fn engine_name(&self) -> &str {
        "glaredb"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}
