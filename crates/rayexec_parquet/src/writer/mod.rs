use bytes::Bytes;
use parquet::{
    column::{
        page::{CompressedPage, PageWriteSpec, PageWriter},
        writer::{get_column_writer, ColumnCloseResult, ColumnWriter},
    },
    data_type::ByteArray,
    errors::ParquetError,
    file::{
        metadata::ColumnChunkMetaData,
        properties::{WriterProperties, WriterPropertiesPtr},
        writer::{write_page, SerializedFileWriter},
    },
    format::FileMetaData,
    schema::types::SchemaDescriptor,
};
use rayexec_bullet::{array::Array, batch::Batch, datatype::DataType, field::Schema};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use rayexec_io::FileSink;
use std::{fmt, sync::Arc};

use crate::schema::to_parquet_schema;

/// Writes batches out to a parquet file.
///
/// During writes, a complete row group is buffered in memory. Once that row
/// group is complete, it'll automatically be flushed out to the file sink.
pub struct AsyncBatchWriter {
    /// Underlying sink.
    sink: Box<dyn FileSink>,
    /// Schema of parquet file we're writing.
    schema: Schema,
    /// Write properties.
    props: Arc<WriterProperties>,
    /// In-memory writer.
    writer: SerializedFileWriter<Vec<u8>>,
    /// Current row group we're working on.
    current_row_group: RowGroupWriter,
}

impl AsyncBatchWriter {
    pub fn try_new(sink: Box<dyn FileSink>, schema: Schema) -> Result<Self> {
        let props = Arc::new(WriterProperties::new());
        let parquet_schema = to_parquet_schema(&schema)?;
        let writer =
            SerializedFileWriter::new(Vec::new(), parquet_schema.root_schema_ptr(), props.clone())
                .context("failed to build writer")?;

        let current_row_group = RowGroupWriter::try_new(writer.schema_descr(), &schema, &props)?;

        Ok(AsyncBatchWriter {
            sink,
            schema,
            props,
            writer,
            current_row_group,
        })
    }

    /// Encode and write a batch to the underlying file sink.
    pub async fn write(&mut self, batch: &Batch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.current_row_group.write(batch)?;

        // TODO: Slice buffer before right to make sure number of rows in row
        // groups are exact.
        if self.current_row_group.num_rows >= self.props.max_row_group_size() {
            self.flush_row_group()?;
            self.flush_writer_buffer().await?;
        }

        Ok(())
    }

    pub async fn finish(&mut self) -> Result<FileMetaData> {
        self.flush_row_group()?;
        let meta = self.writer.finish().context("failed to finish")?;

        self.flush_writer_buffer().await?;
        self.sink.finish().await?;

        Ok(meta)
    }

    fn flush_row_group(&mut self) -> Result<()> {
        let new_row_group = RowGroupWriter::try_new(
            self.writer.schema_descr(),
            &self.schema,
            self.writer.properties(),
        )?;

        let current_row_group = std::mem::replace(&mut self.current_row_group, new_row_group);

        // Row group corresponding to the serialized file writer. Appended
        // to directly from current (buffered) row group.
        let mut next = self.writer.next_row_group().context("next row group")?;

        let results = current_row_group.close()?;
        for (result, buffer) in results {
            // TODO: Could be cool to reuse this, but...
            let bytes = Bytes::from(buffer.0);
            // TODO: This uses the whack ChunkReader trait.
            next.append_column(&bytes, result)
                .context("failed to append column")?;
        }

        next.close().context("failed to close row group")?;

        Ok(())
    }

    /// Take the underlying buffer and flush it to the file sink. Does not alter
    /// state of the writer, so it can continue on as normal.
    async fn flush_writer_buffer(&mut self) -> Result<()> {
        let buf = std::mem::take(self.writer.inner_mut());
        self.sink.write_all(buf.into()).await?;
        Ok(())
    }
}

impl fmt::Debug for AsyncBatchWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncBatchWriter").finish_non_exhaustive()
    }
}

struct RowGroupWriter {
    column_writers: Vec<ColumnWriter<BufferedPageWriter>>,
    /// Number of rows currently serialized in the row group.
    num_rows: usize,
}

impl RowGroupWriter {
    fn try_new(
        parquet_schema: &SchemaDescriptor,
        schema: &Schema,
        props: &WriterPropertiesPtr,
    ) -> Result<Self> {
        let mut leaves = parquet_schema.columns().iter();
        let mut writers = Vec::with_capacity(schema.fields.len());

        for field in schema.fields.iter() {
            match &field.datatype {
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal64(_)
                | DataType::Decimal128(_)
                | DataType::Utf8
                | DataType::LargeUtf8 => {
                    let page_writer = BufferedPageWriter {
                        buf: ColumnBuffer(Vec::new()), // TODO: Could reuse across row groups.
                    };
                    let desc = leaves
                        .next()
                        .ok_or_else(|| RayexecError::new("Missing column desc"))?;
                    let writer = get_column_writer(desc.clone(), props.clone(), page_writer);
                    writers.push(writer);
                }
                other => not_implemented!("writer datatype {other}"),
            }
        }

        Ok(RowGroupWriter {
            column_writers: writers,
            num_rows: 0,
        })
    }

    fn write(&mut self, batch: &Batch) -> Result<()> {
        for (writer, col) in self.column_writers.iter_mut().zip(batch.columns()) {
            write_array(writer, col)?;
        }

        self.num_rows += batch.num_rows();

        Ok(())
    }

    /// Close the writers and collect all buffers for each column in the row
    /// group.
    fn close(self) -> Result<Vec<(ColumnCloseResult, ColumnBuffer)>> {
        self.column_writers
            .into_iter()
            .map(|w| {
                let (r, page_writer) = w.close()?;
                Ok((r, page_writer.buf))
            })
            .collect::<Result<Vec<_>, ParquetError>>()
            .context("failed to close columns")
    }
}

#[derive(Debug)]
struct ColumnBuffer(Vec<u8>);

#[derive(Debug)]
struct BufferedPageWriter {
    buf: ColumnBuffer,
}

impl PageWriter for BufferedPageWriter {
    fn write_page(&mut self, page: CompressedPage) -> Result<PageWriteSpec, ParquetError> {
        let offset = self.buf.0.len();
        let mut spec = write_page(page, &mut self.buf.0)?;
        spec.offset = offset as u64;
        Ok(spec)
    }

    fn write_metadata(&mut self, _metadata: &ColumnChunkMetaData) -> Result<(), ParquetError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), ParquetError> {
        Ok(())
    }
}

fn array_type_err(arr: &Array) -> RayexecError {
    RayexecError::new(format!("Unexpected array type: {}", arr.datatype()))
}

/// Write an array into the column writer.
// TODO: Validity.
fn write_array<P: PageWriter>(
    writer: &mut ColumnWriter<P>,
    array: impl AsRef<Array>,
) -> Result<()> {
    let array = array.as_ref();
    match writer {
        ColumnWriter::BoolColumnWriter(writer) => match array {
            Array::Boolean(arr) => {
                // TODO: This could be `AsRef`ed
                let bools: Vec<_> = arr.values().iter().collect();
                writer
                    .write_batch(&bools, None, None)
                    .context("failed to write bools")?; // TODO: Def, rep
                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        ColumnWriter::Int64ColumnWriter(writer) => match array {
            Array::Int64(arr) => {
                writer
                    .write_batch(arr.values().as_ref(), None, None)
                    .context("failed to write int64s")?;
                Ok(())
            }
            Array::UInt64(arr) => {
                // Allow overflow.
                // TODO: AsRef instead of needing to collect.
                let vals: Vec<_> = arr.values().as_ref().iter().map(|v| *v as i64).collect();
                writer
                    .write_batch(&vals, None, None)
                    .context("failed to write uint64s")?;
                Ok(())
            }
            Array::Decimal64(arr) => {
                writer
                    .write_batch(arr.get_primitive().values().as_ref(), None, None)
                    .context("failed to write decimal64s")?;
                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        ColumnWriter::FloatColumnWriter(writer) => match array {
            Array::Float32(arr) => {
                writer
                    .write_batch(arr.values().as_ref(), None, None)
                    .context("failed to float32s")?;
                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        ColumnWriter::DoubleColumnWriter(writer) => match array {
            Array::Float64(arr) => {
                writer
                    .write_batch(arr.values().as_ref(), None, None)
                    .context("failed to write float64s")?;
                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        ColumnWriter::ByteArrayColumnWriter(writer) => match array {
            Array::Utf8(arr) => {
                // TODO: Try not to copy here. There's a hard requirement on the
                // physical type being `Bytes`, and so a conversion needs to
                // happen somewhere.
                let data = Bytes::copy_from_slice(arr.data().as_ref());
                let values: Vec<ByteArray> = arr
                    .offsets()
                    .as_ref()
                    .windows(2)
                    .map(|indices| {
                        let start = indices[0] as usize;
                        let end = indices[1] as usize;
                        data.slice(start..end).into()
                    })
                    .collect();

                writer
                    .write_batch(&values, None, None)
                    .context("failed to write strings")?;

                Ok(())
            }
            other => Err(array_type_err(other)),
        },
        _other => not_implemented!("writer not implemented"),
    }
}
