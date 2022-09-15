//! Helpers for serialization/deserialization for arrow types.
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use datafusion::arrow::record_batch::RecordBatch;

use std::io::{Read, Write};

pub fn serialize_schema<W: Write>(w: W, schema: &Schema) -> Result<(), serde_json::Error> {
    serde_json::to_writer(w, schema)
}

pub fn deserialize_schema<R: Read>(r: R) -> Result<Schema, serde_json::Error> {
    serde_json::from_reader(r)
}

pub fn serialize_batch_to_ipc<W: Write, S: AsRef<Schema>>(
    w: W,
    schema: S,
    batch: &RecordBatch,
) -> Result<(), ArrowError> {
    let mut writer = StreamWriter::try_new(w, schema.as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(())
}

pub fn deserialize_batches_from_ipc<R: Read>(r: R) -> Result<Vec<RecordBatch>, ArrowError> {
    let reader = StreamReader::try_new(r, None)?;
    let batches = reader.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};

    #[test]
    fn schema_roundtrip() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let mut buf = Vec::new();
        serialize_schema(&mut buf, &schema).unwrap();
        let got = deserialize_schema(&buf[..]).unwrap();

        assert_eq!(schema, got);
    }
}
