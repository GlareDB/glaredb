use arrow::{
    array::StringArray,
    datatypes::DataType,
    util::display::{ArrayFormatter, FormatOptions},
};
use async_trait::async_trait;
use futures::StreamExt;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::{
    engine::{session::Session, Engine},
    types::batch::{DataBatch, DataBatchSchema},
};
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
        .map_err(|e| RayexecError::with_source("run file", Box::new(e)))?;
    Ok(())
}

#[derive(Debug)]
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

/// String representing null values in the ouput.
const NULL_STR: &'static str = "NULL";

/// String representing empty string values in the output.
const EMPTY_STR: &'static str = "(empty)";

const ARROW_FORMAT_OPTIONS: FormatOptions = FormatOptions::new().with_null(NULL_STR);

/// Convert a batch into a vector of rows.
fn batch_to_rows(batch: DataBatch) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();

    for row_idx in 0..batch.num_rows() {
        let mut row = Vec::with_capacity(batch.columns().len());
        for col in batch.columns().iter() {
            let s = if col.is_valid(row_idx) {
                match col.data_type() {
                    DataType::Null => NULL_STR.to_string(),
                    DataType::Utf8 => {
                        let val = col
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row_idx);
                        if val.is_empty() {
                            EMPTY_STR.to_string()
                        } else {
                            val.to_string()
                        }
                    }
                    _ => ArrayFormatter::try_new(col.as_ref(), &ARROW_FORMAT_OPTIONS)?
                        .value(row_idx)
                        .to_string(),
                }
            } else {
                NULL_STR.to_string()
            };

            row.push(s);
        }
        rows.push(row);
    }

    Ok(rows)
}

fn schema_to_types(schema: &DataBatchSchema) -> Vec<DefaultColumnType> {
    let mut typs = Vec::new();
    for data_type in schema.get_types() {
        let typ = match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DefaultColumnType::Integer,
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => DefaultColumnType::FloatingPoint,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Boolean => DefaultColumnType::Text,
            _ => DefaultColumnType::Any,
        };
        typs.push(typ);
    }

    typs
}
