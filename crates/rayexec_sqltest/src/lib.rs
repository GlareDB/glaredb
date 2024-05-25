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
use std::path::PathBuf;
use tracing::{debug, info};

pub async fn run_tests(paths: Vec<PathBuf>) -> Result<()> {
    for path in paths {
        run_test(path).await?;
    }
    Ok(())
}

async fn run_test(path: PathBuf) -> Result<()> {
    debug!(?path, "running slt file");
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
        info!(%sql, "running query");

        let mut rows = Vec::new();
        let mut output = self.session.execute(sql)?;
        let typs = schema_to_types(&output.output_schema);
        while let Some(batch) = output.stream.next().await {
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

        rows.push(col_strings);
    }

    Ok(rows)
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
