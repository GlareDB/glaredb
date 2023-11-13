use arrow_util::pretty;

use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;

use sqlexec::session::ExecutionResult;

use crate::error::JsGlareDbError;

pub(crate) struct JsExecutionResult(pub(crate) ExecutionResult);

impl JsExecutionResult {
    pub(crate) async fn execute(&mut self) -> napi::Result<()> {
        match &mut self.0 {
            ExecutionResult::Query { stream, .. } => {
                while let Some(r) = stream.next().await {
                    let _ = r.map_err(JsGlareDbError::from)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(crate) async fn to_arrow_inner(&mut self) -> napi::Result<Vec<u8>> {
        let res = match &mut self.0 {
            ExecutionResult::Query { stream, .. } => {
                let mut data_batch = vec![];
                let cursor = std::io::Cursor::new(&mut data_batch);
                let mut writer = FileWriter::try_new(cursor, stream.schema().as_ref())
                    .map_err(JsGlareDbError::from)?;

                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(JsGlareDbError::from)?;
                    writer.write(&batch).map_err(JsGlareDbError::from)?;
                }

                writer.finish().map_err(JsGlareDbError::from)?;
                drop(writer);

                data_batch
            }
            _ => vec![],
        };
        Ok(res)
    }

    // TEMPORARILY DISABLED -- we need to figure out how to convert the raw record batches to JS objects.
    // pub(crate) async fn record_batches(&mut self) -> napi::Result<Vec<JsRecordBatch>> {
    //     let mut batches = vec![];
    //     match &mut self.0 {
    //         ExecutionResult::Query { stream, .. } => {
    //             while let Some(r) = stream.next().await {
    //                 let batch = r.map_err(JsGlareDbError::from)?;
    //                 batches.push(JsRecordBatch::from(batch));
    //             }
    //             Ok(batches)
    //         }
    //         _ => Ok(batches),
    //     }
    // }

    pub(crate) async fn show(&mut self) -> napi::Result<()> {
        print_batch(&mut self.0).await?;
        Ok(())
    }
}

async fn print_batch(result: &mut ExecutionResult) -> napi::Result<()> {
    match result {
        ExecutionResult::Query { stream, .. } => {
            let schema = stream.schema();
            let batches = stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<RecordBatch>, _>>()
                .map_err(JsGlareDbError::from)?;

            let disp =
                pretty::pretty_format_batches(&schema, &batches, Some(pretty::term_width()), None)
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;

            println!("{}", disp);
            Ok(())
        }
        _ => Err(napi::Error::from_reason("Not able to show executed result")),
    }
}
