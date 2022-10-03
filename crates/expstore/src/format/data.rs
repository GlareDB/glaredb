//! Reading/writing user data files.
use crate::errors::{internal, Result};
use crate::file::MirroredFile;
use crate::format::FinishError;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use std::io::{self, Read, Seek, Write};

const RECORD_BATCH_MARKER: &[u8] = b"glar";

/// Write out user data to some underlying file.
#[derive(Debug)]
pub struct DataWriter {
    file: MirroredFile,
}

impl DataWriter {
    /// Create a new data writer with the given file.
    pub fn with_file(file: MirroredFile) -> DataWriter {
        DataWriter { file }
    }

    /// Write record batches to the underlying file. This will completely
    /// overwrite existing data.
    pub fn write_data(&mut self, iter: impl IntoIterator<Item = RecordBatch>) -> Result<()> {
        // TODO:
        // - Write some sort of index in footer.
        // - Possibly make this async.
        // - More efficient format. Currently just arrow ipc.

        let file = &mut self.file;
        file.seek(io::SeekFrom::Start(0))?;

        let mut iter = iter.into_iter();
        let schema = match iter.next() {
            Some(batch) => {
                let schema = batch.schema();
                self.write_batch(batch, &schema)?;
                schema
            }
            None => return Ok(()),
        };

        for batch in iter.into_iter() {
            self.write_batch(batch, &schema)?;
        }

        Ok(())
    }

    fn write_batch(&mut self, batch: RecordBatch, schema: &Schema) -> Result<()> {
        self.file.write_all(RECORD_BATCH_MARKER)?;
        let mut ipc_writer = FileWriter::try_new(&mut self.file, schema)?;
        ipc_writer.write(&batch)?;
        ipc_writer.finish()?;
        Ok(())
    }

    /// Finish writing this file.
    pub fn finish(mut self) -> Result<MirroredFile, FinishError<Self>> {
        match self.file.flush() {
            Ok(_) => Ok(self.file),
            Err(e) => Err(FinishError {
                writer: self,
                error: e.into(),
            }),
        }
    }
}
