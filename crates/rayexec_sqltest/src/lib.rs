use async_trait::async_trait;
use futures::StreamExt;
use rayexec_bullet::{
    batch::Batch,
    field::{DataType, Schema},
    format::{FormatOptions, Formatter},
};
use rayexec_error::{RayexecError, Result};
use rayexec_execution::engine::{session::Session, Engine};
use sqllogictest::DefaultColumnType;
use std::path::Path;

pub async fn run_test(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let mut runner = sqllogictest::Runner::new(|| async { TestSession::try_new() });
    runner
        .run_file_async(path)
        .await
        .map_err(|e| RayexecError::with_source("Failed to run SLT".to_string(), Box::new(e)))?;
    Ok(())
}

#[derive(Debug)]
#[allow(dead_code)]
struct TestSession {
    engine: Engine,
    session: Session,
}

impl TestSession {
    fn try_new() -> Result<Self> {
        let engine = Engine::try_new()?;
        let session = engine.new_session()?;
        Ok(TestSession { engine, session })
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for TestSession {
    type Error = RayexecError;
    type ColumnType = DefaultColumnType;

    async fn run(
        &mut self,
        sql: &str,
    ) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        let mut rows = Vec::new();
        let mut results = self.session.simple(sql)?;
        if results.len() != 1 {
            return Err(RayexecError::new(format!(
                "Unexpected number of results for '{sql}': {}",
                results.len()
            )));
        }

        let typs = schema_to_types(&results[0].output_schema);
        while let Some(result) = results[0].stream.next().await {
            let batch = result?;
            rows.extend(batch_to_rows(batch)?);
        }

        Ok(sqllogictest::DBOutput::Rows { types: typs, rows })
    }

    fn engine_name(&self) -> &str {
        "rayexec"
    }
}

/// Convert a batch into a vector of rows.
fn batch_to_rows(batch: Batch) -> Result<Vec<Vec<String>>> {
    const OPTS: FormatOptions = FormatOptions {
        null: "NULL",
        empty_string: "(empty)",
    };
    let formatter = Formatter::new(OPTS);

    let mut rows = Vec::new();

    for row_idx in 0..batch.num_rows() {
        let row = batch.row(row_idx).expect("row to exist");

        let col_strings: Vec<_> = row
            .iter()
            .map(|col| formatter.format_scalar_value(col.clone()).to_string())
            .collect();

        match transform_multiline_cols_to_rows(&col_strings) {
            Some(new_rows) => rows.extend(new_rows),
            None => rows.push(col_strings),
        }
    }

    Ok(rows)
}

/// Transforms a row with a potentially multiline column into multiple rows with
/// each column containing a single line.
///
/// Columns that don't have content for that row will instead have a '.'.
///
/// For example, the following output:
/// ```text
/// +-------------+---------------------------------------------+
/// | type        | plan                                        |
/// +-------------+---------------------------------------------+
/// | logical     | Order (expressions = [#0 DESC NULLS FIRST]) |
/// |             |   Projection (expressions = [#0])           |
/// |             |     ExpressionList                          |
/// +-------------+---------------------------------------------+
/// | pipeline    | Pipeline 1                                  |
/// |             |   Pipeline 2                                |
/// +-------------+---------------------------------------------+
/// ```
///
/// Would get trasnformed into:
/// ```text
/// logical   Order (expressions = [#0 DESC NULLS FIRST])
/// .           Projection (expressions = [#0])
/// .             ExpressionList
/// pipeline  Pipeline 1
/// .           Pipeline 2
/// ```
/// Where each line is a new "row".
///
/// This allows for nicely formatted SLTs for queries that return multiline
/// results (like EXPLAIN).
fn transform_multiline_cols_to_rows<S: AsRef<str>>(cols: &[S]) -> Option<Vec<Vec<String>>> {
    let max = cols.iter().fold(0, |curr, col| {
        let col_lines = col.as_ref().lines().count();
        if col_lines > curr {
            col_lines
        } else {
            curr
        }
    });

    if max > 1 {
        let mut new_rows = Vec::new();
        for row_idx in 0..max {
            let new_row: Vec<_> = cols
                .iter()
                .map(|col| col.as_ref().lines().nth(row_idx).unwrap_or(".").to_string())
                .collect();
            new_rows.push(new_row)
        }
        Some(new_rows)
    } else {
        None
    }
}

fn schema_to_types(schema: &Schema) -> Vec<DefaultColumnType> {
    let mut typs = Vec::new();
    for field in &schema.fields {
        let typ = match field.datatype {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DefaultColumnType::Integer,
            DataType::Float32 | DataType::Float64 => DefaultColumnType::FloatingPoint,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Boolean => DefaultColumnType::Text,
            _ => DefaultColumnType::Any,
        };
        typs.push(typ);
    }

    typs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_not_needed() {
        let orig_row = &["col1", "col2", "col3"];
        let out = transform_multiline_cols_to_rows(orig_row);
        assert_eq!(None, out);
    }

    #[test]
    fn transform_multiline_col() {
        let orig_row = &["col1", "col2\ncol2a\ncol2b", "col3"];
        let out = transform_multiline_cols_to_rows(orig_row);

        let expected = vec![
            vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            vec![".".to_string(), "col2a".to_string(), ".".to_string()],
            vec![".".to_string(), "col2b".to_string(), ".".to_string()],
        ];

        assert_eq!(Some(expected), out);
    }

    #[test]
    fn transform_multiple_multiline_cols() {
        let orig_row = &["col1", "col2\ncol2a\ncol2b", "col3\ncol3a"];
        let out = transform_multiline_cols_to_rows(orig_row);

        let expected = vec![
            vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            vec![".".to_string(), "col2a".to_string(), "col3a".to_string()],
            vec![".".to_string(), "col2b".to_string(), ".".to_string()],
        ];

        assert_eq!(Some(expected), out);
    }
}
