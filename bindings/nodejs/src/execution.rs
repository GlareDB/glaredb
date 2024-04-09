use std::sync::{Arc, Mutex};

use arrow_util::pretty;
use futures::stream::StreamExt;
use glaredb::RecordBatch;
use sqlexec::session::ExecutionResult;

use crate::error::JsGlareDbError;

#[napi]
#[derive(Clone, Debug)]
pub struct JsExecution {
    op: Arc<Mutex<glaredb::Operation>>,
}

impl From<glaredb::Operation> for JsExecution {
    fn from(opt: glaredb::Operation) -> Self {
        Self {
            op: Arc::new(Mutex::new(opt)),
        }
    }
}

impl JsExecution {
    pub(crate) async fn legacy_execute(&mut self) -> napi::Result<()> {
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

    pub(crate) async fn legacy_to_arrow_inner(&mut self) -> napi::Result<Vec<u8>> {
        let mut stream = self.op.lock().unwrap().execute().await?;

        let mut data_batch = vec![];
        let cursor = std::io::Cursor::new(&mut data_batch);
        let mut writer =
            FileWriter::try_new(cursor, stream.schema().as_ref()).map_err(JsGlareDbError::from)?;

        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(JsGlareDbError::from)?;
            writer.write(&batch).map_err(JsGlareDbError::from)?;
        }

        writer.finish().map_err(JsGlareDbError::from)?;
        drop(writer);

        Ok(data_batch)
    }

    pub(crate) async fn legacy_show(&mut self) -> napi::Result<()> {
        print_batch(&mut self.0).await?;
        Ok(())
    }
}

#[napi]
impl JsExecution {
    #[napi(catch_unwind)]
    pub fn to_string(&self) -> napi::Result<String> {
        Ok(format!("{:?}", self.op.lock().unwrap()))
    }

    #[napi(catch_unwind)]
    pub async fn show(&self) -> napi::Result<()> {
        let _res = self
            .op
            .lock()
            .unwrap()
            .execute()
            .await
            .map_err(JsGlareDbError::from)?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub async fn execute(&self) -> napi::Result<()> {
        self.execute_inner().await?.execute().await?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub async fn to_ipc(&self) -> napi::Result<napi::bindgen_prelude::Buffer> {
        let inner = self.execute_inner().await?.to_arrow_inner().await?;
        Ok(inner.into())
    }

    #[napi(ts_return_type = "pl.DataFrame")]
    /// Convert to a Polars DataFrame.
    /// "nodejs-polars" must be installed as a peer dependency.
    /// See https://www.npmjs.com/package/nodejs-polars
    pub async fn to_polars(&self) -> napi::Result<()> {
        // TODO: implement this in rust if possible?
        // Currently, this is monkeypatched in glaredb.js
        unimplemented!("to_polars")
    }

    #[napi(ts_return_type = "arrow.Table<any>")]
    /// Convert to an "apache-arrow" Table.
    /// "apache-arrow" must be installed as a peer dependency.
    /// See https://www.npmjs.com/package/apache-arrow
    pub async fn to_arrow(&self) -> napi::Result<()> {
        // TODO: implement this in rust if possible?
        // Currently, this is monkeypatched in glaredb.js
        unimplemented!("to_arrow")
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

            let disp = pretty::pretty_format_batches(
                &schema,
                &batches,
                Some(terminal_util::term_width()),
                None,
            )
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;

            println!("{}", disp);
            Ok(())
        }
        _ => Err(napi::Error::from_reason("Not able to show executed result")),
    }
}
