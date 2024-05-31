use std::sync::{Arc, Mutex};

use arrow_util::pretty;
use datafusion::arrow::ipc::writer::FileWriter;
use futures::stream::StreamExt;
use glaredb::{DatabaseError, RecordStream, SendableRecordBatchStream};

use crate::error::JsDatabaseError;

#[napi]
#[derive(Clone, Debug)]
pub struct JsExecutionOutput {
    op: Arc<Mutex<glaredb::Operation>>,
}

impl From<glaredb::Operation> for JsExecutionOutput {
    fn from(opt: glaredb::Operation) -> Self {
        Self {
            op: Arc::new(Mutex::new(opt)),
        }
    }
}

impl JsExecutionOutput {
    pub(crate) async fn to_arrow_inner(&self) -> napi::Result<Vec<u8>> {
        let mut op = self.op.lock().unwrap().clone();
        Ok(async move {
            let mut stream = op.resolve().await?;
            let mut data_batch = Vec::new();
            let cursor = std::io::Cursor::new(&mut data_batch);
            let mut writer = FileWriter::try_new(cursor, stream.schema().as_ref())
                .map_err(glaredb::DatabaseError::from)?;

            while let Some(batch) = stream.next().await {
                writer
                    .write(&batch.map_err(DatabaseError::from)?)
                    .map_err(DatabaseError::from)?;
            }

            writer.finish().map_err(DatabaseError::from)?;
            drop(writer);

            Ok::<Vec<u8>, JsDatabaseError>(data_batch)
        }
        .await?)
    }
}

#[napi]
impl JsExecutionOutput {
    #[napi(catch_unwind)]
    pub fn to_string(&self) -> napi::Result<String> {
        Ok(format!("{:?}", self.op.lock().unwrap()))
    }

    #[napi(catch_unwind)]
    pub async fn show(&self) -> napi::Result<()> {
        let mut op = self.op.lock().unwrap().clone();
        Ok(async move { print_record_batches(op.resolve().await?).await }.await?)
    }

    #[napi(catch_unwind)]
    pub async fn execute(&self) -> napi::Result<()> {
        let mut op = self.op.lock().unwrap().clone();
        Ok(async move { Ok::<_, JsDatabaseError>(op.call().check().await?) }.await?)
    }

    #[napi(catch_unwind)]
    pub async fn to_ipc(&self) -> napi::Result<napi::bindgen_prelude::Buffer> {
        Ok(self.to_arrow_inner().await?.into())
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

async fn print_record_batches(stream: SendableRecordBatchStream) -> Result<(), JsDatabaseError> {
    let schema = stream.schema();
    let mut stream: RecordStream = stream.into();
    let batches = stream.to_vec().await?;

    let disp =
        pretty::pretty_format_batches(&schema, &batches, Some(terminal_util::term_width()), None)
            .map_err(DatabaseError::from)?;

    println!("{}", disp);
    Ok(())
}
