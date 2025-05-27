use std::path::Path;

use glaredb_error::Result;
use harness::sqlfile::bench_parser::BenchRecord;

/// Describes a benchmark to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Benchmark {
    /// Setup queries to run prior to running the actual benchmark queries.
    pub setup: Vec<String>,
    /// The benchmark queries.
    pub queries: Vec<String>,
}

impl Benchmark {
    /// Construct a benchmark run from a file.
    ///
    /// The content should include setup queries and benchmark queries.
    ///
    /// Setup queries are annotated with "setup":
    ///
    /// ```text
    /// setup
    /// CREATE TABLE ...
    /// ```
    ///
    /// Benchmark queries are annotated with "run":
    ///
    /// ```text
    /// run
    /// SELECT * FROM ...
    /// ```
    ///
    /// Any number of setup and benchmark queries can be provided, however a
    /// setup query cannot be defined after a benchmark query has already been
    /// defined.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)?;

        let path_str = path.to_string_lossy();
        let records = BenchRecord::parse_many(&path_str, &contents)?;

        let mut setup = Vec::new();
        let mut queries = Vec::new();

        for record in records {
            match record {
                BenchRecord::Setup(record) => {
                    if !queries.is_empty() {
                        return record
                            .loc
                            .emit_error("Cannot have 'setup' after a bechmark query");
                    }
                    setup.push(record.sql.join("\n"));
                }
                BenchRecord::Run(record) => {
                    queries.push(record.sql.join("\n"));
                }
            }
        }

        Ok(Benchmark { setup, queries })
    }
}
