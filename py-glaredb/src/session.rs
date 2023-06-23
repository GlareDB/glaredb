use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use pgrepr::format::Format;
use pyo3::prelude::*;
use sqlexec::{parser, session::ExecutionResult};

use crate::{utils::wait_for_future, LocalSession};

#[pymethods]
impl LocalSession {
    fn sql(&mut self, py: Python<'_>, query: &str) -> PyResult<()> {
        const UNNAMED: String = String::new();

        let statements = parser::parse_sql(query).unwrap();
        wait_for_future(py, async move {
            for stmt in statements {
                self.sess
                    .prepare_statement(UNNAMED, Some(stmt), Vec::new())
                    .await
                    .unwrap();
                let prepared = self.sess.get_prepared_statement(&UNNAMED).unwrap();
                let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
                self.sess
                    .bind_statement(
                        UNNAMED,
                        &UNNAMED,
                        Vec::new(),
                        vec![Format::Text; num_fields],
                    )
                    .unwrap();
                let result = self.sess.execute_portal(&UNNAMED, 0).await.unwrap();
                match result {
                    ExecutionResult::Query { stream, .. }
                    | ExecutionResult::ShowVariable { stream } => {
                        let batches = process_stream(stream).await.unwrap();
                        for batch in batches {
                            println!("columns = {:?}", batch.columns());
                        }
                    }
                    other => println!("{:?}", other),
                }
            }
        });

        todo!()
    }
}

async fn process_stream(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    let batches = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}
