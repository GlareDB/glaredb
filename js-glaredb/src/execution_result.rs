use arrow_util::pretty::pretty_format_batches;

use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;

use sqlexec::session::ExecutionResult;

use crate::error::JsGlareDbError;
use crate::record_batch::JsRecordBatch;

#[napi]
pub struct JsExecutionResult(pub(crate) ExecutionResult);

#[napi]
impl JsExecutionResult {
  #[napi]
  pub async unsafe fn execute(&mut self) -> napi::Result<()> {
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

  #[napi]
  pub async unsafe fn to_arrow_inner(&mut self) -> napi::Result<Vec<u8>> {
    let res = match &mut self.0 {
      ExecutionResult::Query { stream, .. } => {
        let batch = stream.next().await.unwrap().map_err(JsGlareDbError::from)?;

        let mut data_batch = vec![];
        let cursor = std::io::Cursor::new(&mut data_batch);
        let mut writer = FileWriter::try_new(cursor, batch.schema().as_ref()).unwrap();

        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        data_batch
      }
      _ => vec![],
    };
    Ok(res)
  }

  #[napi]
  pub async unsafe fn record_batches(&mut self) -> napi::Result<Vec<JsRecordBatch>> {
    let mut batches = vec![];
    match &mut self.0 {
      ExecutionResult::Query { stream, .. } => {
        while let Some(r) = stream.next().await {
          let batch = r.map_err(JsGlareDbError::from)?;
          batches.push(JsRecordBatch::from(batch));
        }
        Ok(batches)
      }
      _ => Ok(batches),
    }
  }

  pub async fn show(&mut self) -> napi::Result<()> {
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

      let disp = pretty_format_batches(&schema, &batches, None, None)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;

      println!("{}", disp);
      Ok(())
    }
    _ => Err(napi::Error::from_reason("Not able to show executed result")),
  }
}
