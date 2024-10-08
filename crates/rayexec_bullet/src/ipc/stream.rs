use std::io::{ErrorKind, Read, Write};

use flatbuffers::FlatBufferBuilder;
use rayexec_error::{RayexecError, Result, ResultExt};

use super::batch::batch_to_ipc;
use super::gen::message::{self, MessageBuilder, MessageHeader};
use super::gen::schema::MetadataVersion;
use super::schema::schema_to_ipc;
use super::IpcConfig;
use crate::batch::Batch;
use crate::field::Schema;
use crate::ipc::batch::ipc_to_batch;
use crate::ipc::schema::ipc_to_schema;

/// Marker at the beginning of an encapsulated message.
pub(crate) const CONTINUATION_MARKER: u32 = 0xFFFFFFFF;

const WRITE_PAD: &[u8; 8] = &[0; 8];

#[derive(Debug)]
#[allow(dead_code)]
pub struct StreamReader<R: Read> {
    reader: R,
    buf: Vec<u8>,
    data_buf: Vec<u8>,
    schema: Schema,
    conf: IpcConfig,
}

impl<R: Read> StreamReader<R> {
    /// Try to create a new stream reader.
    ///
    /// This will attempt to read the first message a schema. Every message
    /// afterwards can be assumed to be a batch with that schema.
    pub fn try_new(mut reader: R, conf: IpcConfig) -> Result<Self> {
        let mut buf = Vec::new();

        let did_read = read_encapsulated_header(&mut reader, &mut buf)?;
        if !did_read {
            return Err(RayexecError::new("Unexpected end of stream"));
        }

        let message =
            message::root_as_message(&buf[8..]).context("Failed to read flat buffer for schema")?;
        let schema_ipc = match message.header_as_schema() {
            Some(ipc) => ipc,
            None => {
                return Err(RayexecError::new(format!(
                    "Unexpected header type: {:?}",
                    message.header_type()
                )))
            }
        };

        let schema = ipc_to_schema(schema_ipc, &conf)?;

        Ok(StreamReader {
            reader,
            buf,
            data_buf: Vec::new(),
            schema,
            conf,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn try_next_batch(&mut self) -> Result<Option<Batch>> {
        self.buf.clear();
        let did_read = read_encapsulated_header(&mut self.reader, &mut self.buf)?;
        if !did_read {
            return Ok(None);
        }

        let message = message::root_as_message(&self.buf[8..])
            .context("Failed to read flat buffer for batch")?;
        let batch_ipc = match message.header_as_record_batch() {
            Some(ipc) => ipc,
            None => {
                // TODO: Dictionaries.
                return Err(RayexecError::new(format!(
                    "Unexpected header type: {:?}",
                    message.header_type()
                )));
            }
        };

        // Read batch data.
        self.data_buf.clear();
        self.data_buf.resize(message.bodyLength() as usize, 0);
        self.reader.read_exact(&mut self.data_buf)?;

        // TODO: Decompression.

        let batch = ipc_to_batch(batch_ipc, &self.data_buf, &self.schema, &self.conf)?;

        Ok(Some(batch))
    }
}

/// Reads an encapsulated message header.
///
/// May return Ok(false) if the stream is complete. A stream is complete if
/// either the stream returns and EOF, or writes a 0 size metatadata length.
fn read_encapsulated_header(reader: &mut impl Read, buf: &mut Vec<u8>) -> Result<bool> {
    buf.truncate(0);
    buf.resize(4, 0);

    match reader.read_exact(buf) {
        Ok(_) => (),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(false),
        Err(e) => return Err(e.into()),
    };

    if buf[0..4] != CONTINUATION_MARKER.to_le_bytes() {
        return Err(RayexecError::new(format!(
            "Unexpected bytes at beginning of reader: {buf:?}"
        )));
    }

    buf.resize(8, 0);
    let metadata_size = {
        reader.read_exact(&mut buf[4..8])?;
        i32::from_le_bytes(buf[4..8].try_into().unwrap())
    };

    if metadata_size == 0 {
        return Ok(false);
    }

    buf.resize(metadata_size as usize + 8, 0);
    reader.read_exact(&mut buf[8..])?;

    Ok(true)
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct StreamWriter<W: Write> {
    writer: W,
    data_buf: Vec<u8>,
    conf: IpcConfig,
}

impl<W: Write> StreamWriter<W> {
    pub fn try_new(mut writer: W, schema: &Schema, conf: IpcConfig) -> Result<Self> {
        let mut builder = FlatBufferBuilder::new();
        let schema_ipc = schema_to_ipc(schema, &mut builder)?.as_union_value();

        let mut message = MessageBuilder::new(&mut builder);
        message.add_version(MetadataVersion::V5);
        message.add_header_type(MessageHeader::Schema);
        message.add_bodyLength(0);
        message.add_header(schema_ipc);
        let message = message.finish();

        builder.finish(message, None);
        let buf = builder.finished_data();

        write_encapsulated_header(&mut writer, buf)?;

        writer.flush()?;

        Ok(StreamWriter {
            writer,
            data_buf: Vec::new(),
            conf,
        })
    }

    pub fn write_batch(&mut self, batch: &Batch) -> Result<()> {
        let mut builder = FlatBufferBuilder::new();

        self.data_buf.clear();
        let batch_ipc = batch_to_ipc(batch, &mut self.data_buf, &mut builder)?.as_union_value();

        if self.data_buf.len() % 8 != 0 {
            self.data_buf
                .extend_from_slice(&WRITE_PAD[self.data_buf.len() % 8..]);
        }

        let mut message = MessageBuilder::new(&mut builder);
        message.add_version(MetadataVersion::V5);
        message.add_header_type(MessageHeader::RecordBatch);
        message.add_bodyLength(self.data_buf.len() as i64);
        message.add_header(batch_ipc);
        let message = message.finish();

        builder.finish(message, None);
        let buf = builder.finished_data();

        write_encapsulated_header(&mut self.writer, buf)?;

        self.writer.write_all(&self.data_buf)?;

        self.writer.flush()?;

        Ok(())
    }

    pub fn into_writer(self) -> W {
        self.writer
    }
}

fn write_encapsulated_header(writer: &mut impl Write, buf: &[u8]) -> Result<()> {
    writer.write_all(&CONTINUATION_MARKER.to_be_bytes())?;

    let mut metadata_size = buf.len();
    if buf.len() % 8 != 0 {
        metadata_size += 8 - buf.len() % 8;
    }

    writer.write_all(&i32::to_le_bytes(metadata_size as i32))?;
    writer.write_all(buf)?;

    if buf.len() % 8 != 0 {
        let idx = buf.len() % 8;
        writer.write_all(&WRITE_PAD[idx..])?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read, Write};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::batch::Batch;
    use crate::datatype::DataType;
    use crate::field::{Field, Schema};

    struct SharedVecWriter {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    struct SharedVecReader {
        n: usize,
        buf: Arc<Mutex<Vec<u8>>>,
    }

    fn test_reader_writer() -> (SharedVecReader, SharedVecWriter) {
        let shared = Arc::new(Mutex::new(Vec::new()));
        (
            SharedVecReader {
                n: 0,
                buf: shared.clone(),
            },
            SharedVecWriter { buf: shared },
        )
    }

    impl Write for SharedVecWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut v = self.buf.lock().unwrap();
            v.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Read for SharedVecReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let v = self.buf.lock().unwrap();
            let n = (&v[self.n..]).read(buf)?;
            self.n += n;
            Ok(n)
        }
    }

    #[test]
    fn read_no_batches() {
        let (reader, writer) = test_reader_writer();

        let schema = Schema::new([Field::new("c1", DataType::UInt32, true)]);

        let _ = StreamWriter::try_new(writer, &schema, IpcConfig::default()).unwrap();
        let mut s_reader = StreamReader::try_new(reader, IpcConfig::default()).unwrap();

        let got = s_reader.try_next_batch().unwrap();
        assert_eq!(None, got);
    }

    #[test]
    fn roundtrip_simple() {
        let (reader, writer) = test_reader_writer();

        let schema = Schema::new([
            Field::new("c1", DataType::UInt32, true),
            Field::new("c2", DataType::Int64, true),
        ]);

        let mut s_writer = StreamWriter::try_new(writer, &schema, IpcConfig::default()).unwrap();

        let batch = Batch::try_new([
            FromIterator::<u32>::from_iter([1, 2, 3]),
            FromIterator::<i64>::from_iter([7, 8, 9]),
        ])
        .unwrap();
        s_writer.write_batch(&batch).unwrap();

        let mut s_reader = StreamReader::try_new(reader, IpcConfig::default()).unwrap();
        let got = s_reader.try_next_batch().unwrap().unwrap();

        assert_eq!(batch, got);
    }

    #[test]
    fn roundtrip_multiple() {
        let (reader, writer) = test_reader_writer();

        let schema = Schema::new([Field::new("c1", DataType::UInt32, true)]);

        let mut s_writer = StreamWriter::try_new(writer, &schema, IpcConfig::default()).unwrap();
        let mut s_reader = StreamReader::try_new(reader, IpcConfig::default()).unwrap();

        let batch1 = Batch::try_new([FromIterator::<u32>::from_iter([1, 2, 3])]).unwrap();

        s_writer.write_batch(&batch1).unwrap();

        let got1 = s_reader.try_next_batch().unwrap().unwrap();

        let batch2 = Batch::try_new([FromIterator::<u32>::from_iter([4, 5])]).unwrap();

        s_writer.write_batch(&batch2).unwrap();

        let got2 = s_reader.try_next_batch().unwrap().unwrap();

        assert_eq!(batch1, got1);
        assert_eq!(batch2, got2);
    }

    #[test]
    fn write_encapsulated_header_unpadded() {
        let mut out = Vec::new();
        write_encapsulated_header(&mut out, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        assert_eq!(
            CONTINUATION_MARKER,
            u32::from_le_bytes(out[0..4].try_into().unwrap())
        );
        assert_eq!(0, out.len() % 8);
        assert_eq!(8, i32::from_le_bytes(out[4..8].try_into().unwrap()));
        assert_eq!(&[1, 2, 3, 4, 5, 6, 7, 8], &out[out.len() - 8..]);
    }

    #[test]
    fn write_encapsulated_header_padded() {
        let mut out = Vec::new();
        write_encapsulated_header(&mut out, &[1, 2, 3, 4, 5]).unwrap();

        assert_eq!(
            CONTINUATION_MARKER,
            u32::from_le_bytes(out[0..4].try_into().unwrap())
        );
        assert_eq!(0, out.len() % 8);
        assert_eq!(8, i32::from_le_bytes(out[4..8].try_into().unwrap()));
        assert_eq!(&[1, 2, 3, 4, 5, 0, 0, 0], &out[out.len() - 8..]);
    }
}
