//! Reading/writing user data files.
use crate::errors::{internal, Result};
use crate::file::MirroredFile;
use crate::format::FinishError;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use std::io::{self, Seek, Write};
use std::path::PathBuf;

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

        self.file.seek(io::SeekFrom::Start(0))?;

        let mut iter = iter.into_iter();
        let batch = match iter.next() {
            Some(batch) => batch,
            None => return Ok(()), // TODO: Maybe error?
        };

        let mut ipc_writer = FileWriter::try_new(&mut self.file, &batch.schema())?;
        ipc_writer.write(&batch)?;

        for batch in iter {
            ipc_writer.write(&batch)?;
        }
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

#[derive(Debug)]
pub struct DataReader {
    /// Local path for debugging.
    path: PathBuf,
    ipc_reader: FileReader<MirroredFile>,
}

impl DataReader {
    pub fn with_file(mut file: MirroredFile) -> Result<DataReader> {
        let path = file.local_path().to_path_buf();
        file.seek(io::SeekFrom::Start(0))?;
        let ipc_reader = FileReader::try_new(file, None)?;
        Ok(DataReader { path, ipc_reader })
    }

    pub fn read_batch_at(&mut self, idx: usize) -> Result<RecordBatch> {
        self.ipc_reader.set_index(idx)?;
        let batch = self
            .ipc_reader
            .next()
            .ok_or_else(|| internal!("missing batch for index: {}, {:?}", idx, self.path))??;

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::testutil;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn create_test_batch(val: i32) -> RecordBatch {
        let id_array = Int32Array::from(vec![val; 10]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap()
    }

    #[tokio::test]
    async fn simple() {
        let cache = testutil::new_local_cache();
        let mirrored = testutil::new_temp_mirrored_file(cache, "data").await;

        let mut writer = DataWriter::with_file(mirrored);
        let b1 = create_test_batch(1);
        let b2 = create_test_batch(2);
        writer.write_data([b1.clone(), b2.clone()]).unwrap();

        let mirrored = writer.finish().unwrap();
        let mut reader = DataReader::with_file(mirrored).unwrap();

        let got = reader.read_batch_at(0).unwrap();
        assert_eq!(b1, got);
        let got = reader.read_batch_at(1).unwrap();
        assert_eq!(b2, got);
    }
}
