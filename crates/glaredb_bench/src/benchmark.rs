use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use glaredb_error::{DbError, Result, ResultExt};

/// Describes a benchmark to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Benchmark {
    /// Setup queries to run prior to running the actual benchmark queries.
    pub setup: Vec<String>,
    /// The benchmark queries.
    pub queries: Vec<String>,
}

impl Benchmark {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Self::from_buf_read(reader)
    }

    /// Construct a benchmark run from some reader.
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
    pub fn from_buf_read(reader: impl BufRead) -> Result<Self> {
        let lines = reader.lines();

        let mut setup = Vec::new();
        let mut queries = Vec::new();

        let mut current_section: Option<&mut Vec<String>> = None;

        let mut setup_finished = false;
        let mut query_buffer = String::new();

        for line in lines {
            let line = line.context("failed to get line")?;
            let trimmed = line.trim();

            if trimmed.is_empty() || trimmed.starts_with('#') {
                // Ignore empty lines and comments
                continue;
            }

            match trimmed {
                "setup" => {
                    // Files should be read and executed top-to-bottom, enforce
                    // that by not allowing a setup to happen after reading in a
                    // 'run' query.
                    if setup_finished {
                        return Err(DbError::new(
                            "'setup' queries must come before 'run' queries",
                        ));
                    }

                    flush_query_buffer(&mut query_buffer, &mut current_section)?;
                    current_section = Some(&mut setup);
                }
                "run" => {
                    setup_finished = true;
                    flush_query_buffer(&mut query_buffer, &mut current_section)?;
                    current_section = Some(&mut queries);
                }
                _ => {
                    // Append the line to the query buffer
                    if !query_buffer.is_empty() {
                        query_buffer.push('\n');
                    }
                    // Push the original line to preserve whitespace.
                    query_buffer.push_str(&line);
                }
            }
        }

        flush_query_buffer(&mut query_buffer, &mut current_section)?;

        // Setup can be empty, but benchmark queries cannot.
        if queries.is_empty() {
            return Err(DbError::new("No benchmark queries"));
        }

        Ok(Benchmark { setup, queries })
    }
}

/// Flush the query buffer to the current section
fn flush_query_buffer(
    query_buffer: &mut String,
    current_section: &mut Option<&mut Vec<String>>,
) -> Result<()> {
    if !query_buffer.is_empty() {
        if let Some(section) = current_section {
            section.push(query_buffer.trim().to_string());
            query_buffer.clear();
        } else {
            return Err(DbError::new(format!(
                "Query found outside of any section: {query_buffer}",
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn single_setup_and_run() {
        let contents = r#"
# This is a comment.

setup
CREATE TABLE hello (a INT)

run
SELECT * FROM hello
"#;

        let bench = Benchmark::from_buf_read(Cursor::new(contents)).unwrap();
        let expected = Benchmark {
            setup: vec!["CREATE TABLE hello (a INT)".to_string()],
            queries: vec!["SELECT * FROM hello".to_string()],
        };

        assert_eq!(expected, bench);
    }

    #[test]
    fn single_run() {
        let contents = r#"run
SELECT * FROM hello
"#;

        let bench = Benchmark::from_buf_read(Cursor::new(contents)).unwrap();
        let expected = Benchmark {
            setup: vec![],
            queries: vec!["SELECT * FROM hello".to_string()],
        };

        assert_eq!(expected, bench);
    }

    #[test]
    fn missing_run() {
        let contents = r#"
# This is a comment.

setup
CREATE TABLE hello (a INT)
"#;

        let _ = Benchmark::from_buf_read(Cursor::new(contents)).unwrap_err();
    }

    #[test]
    fn multiple_setups_and_runs() {
        let contents = r#"
# This is a comment.

setup
CREATE TABLE hello (a INT)

setup
CREATE TABLE hello2 (a INT)

run
SELECT * FROM hello

run
SELECT * FROM hello2
"#;

        let bench = Benchmark::from_buf_read(Cursor::new(contents)).unwrap();
        let expected = Benchmark {
            setup: vec![
                "CREATE TABLE hello (a INT)".to_string(),
                "CREATE TABLE hello2 (a INT)".to_string(),
            ],
            queries: vec![
                "SELECT * FROM hello".to_string(),
                "SELECT * FROM hello2".to_string(),
            ],
        };

        assert_eq!(expected, bench);
    }

    #[test]
    fn setup_after_run() {
        let contents = r#"
setup
CREATE TABLE hello (a INT)

run
SELECT * FROM hello

setup
CREATE TABLE hello2 (a INT)
"#;

        let _ = Benchmark::from_buf_read(Cursor::new(contents)).unwrap_err();
    }

    #[test]
    fn multiline_setup() {
        let contents = r#"
setup
CREATE TABLE
  hello (a INT)

run
SELECT * FROM hello
"#;

        let bench = Benchmark::from_buf_read(Cursor::new(contents)).unwrap();
        let expected = Benchmark {
            setup: vec!["CREATE TABLE\n  hello (a INT)".to_string()],
            queries: vec!["SELECT * FROM hello".to_string()],
        };

        assert_eq!(expected, bench);
    }
}
