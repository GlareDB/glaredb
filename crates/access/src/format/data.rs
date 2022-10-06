//! Reading/writing user data files.
use crate::errors::{internal, Result};
use crate::format::FinishError;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use std::io::{self, Read, Seek, Write};

/// Write out user data to some underlying file.
#[derive(Debug)]
pub struct DataWriter<W> {
    writer: W,
}

impl<W: Write + Seek> DataWriter<W> {
    /// Create a new data writer with the given file.
    pub fn with_writer(writer: W) -> Self {
        DataWriter { writer }
    }

    /// Write record batches to the underlying file. This will completely
    /// overwrite existing data.
    pub fn write_data(&mut self, iter: impl IntoIterator<Item = RecordBatch>) -> Result<()> {
        // TODO:
        // - Write some sort of index in footer.
        // - Possibly make this async.
        // - More efficient format. Currently just arrow ipc.

        self.writer.seek(io::SeekFrom::Start(0))?;

        let mut iter = iter.into_iter();
        let batch = match iter.next() {
            Some(batch) => batch,
            None => return Ok(()), // TODO: Maybe error?
        };

        let mut ipc_writer = FileWriter::try_new(&mut self.writer, &batch.schema())?;
        ipc_writer.write(&batch)?;

        for batch in iter {
            ipc_writer.write(&batch)?;
        }
        ipc_writer.finish()?;

        Ok(())
    }

    /// Finish writing this file.
    pub fn finish(mut self) -> Result<W, FinishError<Self>> {
        match self.writer.flush() {
            Ok(_) => Ok(self.writer),
            Err(e) => Err(FinishError {
                writer: self,
                error: e.into(),
            }),
        }
    }
}

#[derive(Debug)]
pub struct DataReader<R: Read + Seek> {
    ipc_reader: FileReader<R>,
}

impl<R: Read + Seek> DataReader<R> {
    pub fn with_file(mut reader: R) -> Result<Self> {
        reader.seek(io::SeekFrom::Start(0))?;
        let ipc_reader = FileReader::try_new(reader, None)?;
        Ok(DataReader { ipc_reader })
    }

    pub fn read_batch_at(&mut self, idx: usize) -> Result<RecordBatch> {
        self.ipc_reader.set_index(idx)?;
        let batch = self
            .ipc_reader
            .next()
            .ok_or_else(|| internal!("missing batch for index: {}", idx))??;

        Ok(batch)
    }
}
