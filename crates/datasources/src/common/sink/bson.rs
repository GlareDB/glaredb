use crate::common::errors::Result;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchWriter};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::{DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::{fmt::Debug, fmt::Display, io::Write, sync::Arc};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::SharedBuffer;
use arrow_array::{types::*, Array, StructArray};
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};
const BUFFER_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug)]
pub struct BsonSink {
    store: Arc<dyn ObjectStore>,
    loc: ObjectPath,
}

impl Display for BsonSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BsonSink({}:{})", self.store, self.loc)
    }
}

impl DisplayAs for BsonSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "BsonSink({}:{})", self.store, self.loc),
            DisplayFormatType::Verbose => write!(f, "{self}"),
        }
    }
}

impl BsonSink {
    pub fn from_obj_store(store: Arc<dyn ObjectStore>, loc: impl Into<ObjectPath>) -> BsonSink {
        BsonSink {
            store,
            loc: loc.into(),
        }
    }

    async fn stream_into_inner(&self, stream: SendableRecordBatchStream) -> Result<usize> {
        self.formatted_stream(stream).await
    }

    async fn formatted_stream(&self, mut stream: SendableRecordBatchStream) -> Result<usize> {
        let (_id, obj_handle) = self.store.put_multipart(&self.loc).await?;
        let mut writer = AsyncBsonWriter::new(obj_handle, BUFFER_SIZE);
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            writer.write_batch(batch).await?;
        }
        writer.finish().await
    }
}

#[async_trait]
impl DataSink for BsonSink {
    async fn write_all(
        &self,
        data: Vec<SendableRecordBatchStream>,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let mut count = 0;
        for stream in data {
            count += self
                .stream_into_inner(stream)
                .await
                .map(|x| x as u64)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        Ok(count)
    }
}

/// Wrapper around Arrow's Bson writer to provide async write support.
///
/// Modeled after the parquet crate's `AsyncArrowWriter`.
struct AsyncBsonWriter<W> {
    async_writer: W,
    sync_writer: BsonWriter<SharedBuffer>,
    buffer: SharedBuffer,
    row_count: usize,
}

pub struct BsonWriter<W>
where
    W: Write,
{
    /// Underlying writer to use to write bytes
    writer: W,
}

impl<W> BsonWriter<W>
where
    W: Write,
{
    /// Construct a new writer
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Write a single BSON row to the output writer
    pub fn write_row(&mut self, row: &bson::Document) -> Result<(), ArrowError> {
        row.to_writer(&mut self.writer)
            .map_err(|e| ArrowError::from_external_error(Box::new(e)))
    }

    /// Convert the `RecordBatch` into BSON rows, and write them to
    /// the output
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let coverter = BsonBatchConverter::new(
            StructArray::from(batch.to_owned()),
            batch.schema().fields().to_owned(),
        );

        for doc in coverter {
            self.write_row(&doc)?;
        }

        Ok(())
    }

    /// Convert the [`RecordBatch`] into BSON rows, and write them to
    /// the output
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for batch in batches {
            self.write(batch)?;
        }
        Ok(())
    }

    /// Finishes the output stream.
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        Ok(())
    }

    /// Unwraps this `Writer<W>`, returning the underlying writer
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> RecordBatchWriter for BsonWriter<W>
where
    W: Write,
{
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.write(batch)
    }

    fn close(mut self) -> Result<(), ArrowError> {
        self.finish()
    }
}

impl<W: AsyncWrite + Unpin + Send> AsyncBsonWriter<W> {
    fn new(async_writer: W, buf_size: usize) -> Self {
        let buf = SharedBuffer::with_capacity(buf_size);
        let sync_writer = BsonWriter::new(buf.clone());
        AsyncBsonWriter {
            async_writer,
            sync_writer,
            buffer: buf,
            row_count: 0,
        }
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        self.sync_writer.write(&batch)?;
        self.try_flush(false).await?;
        self.row_count += num_rows;
        Ok(())
    }

    async fn finish(mut self) -> Result<usize> {
        self.sync_writer.finish()?;
        self.try_flush(true).await?;
        self.async_writer.shutdown().await?;
        Ok(self.row_count)
    }

    async fn try_flush(&mut self, force: bool) -> Result<()> {
        let mut buf = self.buffer.buffer.try_lock().unwrap();
        if !force && buf.len() < buf.capacity() / 2 {
            return Ok(());
        }

        self.async_writer.write_all(&buf).await?;
        self.async_writer.flush().await?;

        buf.clear();

        Ok(())
    }
}

struct BsonBatchConverter {
    batch: StructArray,
    schema: Vec<String>,
    row: usize,
    started: bool,
    columns: Vec<Vec<bson::Bson>>,
}

impl BsonBatchConverter {
    fn new(batch: StructArray, fields: arrow_schema::Fields) -> Self {
        let mut field_names = Vec::<String>::with_capacity(fields.len());
        for name in &fields {
            field_names.push(name.to_string())
        }

        Self {
            batch: batch.clone(),
            schema: field_names,
            row: 0,
            started: true,
            columns: Vec::<Vec<bson::Bson>>::with_capacity(batch.num_columns()),
        }
    }

    fn setup(&mut self) -> Result<(), ArrowError> {
        for col in self.batch.columns().iter() {
            self.columns
                .push(array_to_bson(col).map_err(|e| ArrowError::from_external_error(Box::new(e)))?)
        }
        Ok(())
    }
}

impl Iterator for BsonBatchConverter {
    type Item = bson::Document;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.setup().ok()?;
        }
        if self.row > self.batch.len() {
            return None;
        }

        let mut doc = bson::Document::new();
        for (i, field) in self.schema.iter().enumerate() {
            doc.insert(field.to_string(), self.columns[i][self.row].to_owned());
        }

        self.row += 1;
        Some(doc)
    }
}

fn array_to_bson(array: &ArrayRef) -> Result<Vec<bson::Bson>, ArrowError> {
    let mut out = Vec::<bson::Bson>::with_capacity(array.len());
    let dt = array.data_type().to_owned();
    match dt {
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default() as i32))),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default() as i32))),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::UInt8 => array
            .as_primitive::<UInt8Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default() as i32))),
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default() as i32))),
        DataType::UInt32 => array
            .as_primitive::<UInt32Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default() as i64))),
        DataType::UInt64 => array
            .as_primitive::<UInt64Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default() as i64))),
        DataType::Utf8 | DataType::LargeUtf8 => arrow_array::cast::as_string_array(array)
            .iter()
            .for_each(|val| match val {
                Some(v) => out.push(bson::Bson::String(v.to_string())),
                None => out.push(bson::Bson::Null),
            }),
        DataType::Float16 => array
            .as_primitive::<Float16Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Double(val.unwrap_or_default().to_f64()))),
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Double(val.unwrap_or_default() as f64))),
        DataType::Float64 => array
            .as_primitive::<Float64Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Double(val.unwrap_or_default()))),
        DataType::Decimal128(_, _) => array
            .as_primitive::<Decimal128Type>()
            .iter()
            // TODO: this is probably not correct:
            .for_each(|val| {
                out.push(bson::ser::to_bson(&val.unwrap_or_default()).expect("decimal128"))
            }),
        DataType::Null => {
            for _ in 0..array.len() {
                out.push(bson::Bson::Null)
            }
        }
        DataType::Boolean => array
            .as_boolean()
            .iter()
            .for_each(|val| out.push(bson::Bson::Boolean(val.unwrap_or_default()))),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().iter().for_each(|val| {
            out.push(bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: val.unwrap_or_default().to_vec(),
            }))
        }),
        DataType::Binary => array
            .as_bytes::<GenericBinaryType<i32>>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: val.unwrap_or_default().to_vec(),
                }))
            }),
        DataType::LargeBinary => {
            array
                .as_bytes::<GenericBinaryType<i64>>()
                .iter()
                .for_each(|val| {
                    out.push(bson::Bson::Binary(bson::Binary {
                        subtype: bson::spec::BinarySubtype::Generic,
                        bytes: val.unwrap_or_default().to_vec(),
                    }))
                })
        }
        DataType::Date32 => array
            .as_primitive::<Date32Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Date64 => array
            .as_primitive::<Date64Type>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Interval(IntervalUnit::DayTime) => array
            .as_primitive::<IntervalDayTimeType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Interval(IntervalUnit::YearMonth) => array
            .as_primitive::<IntervalYearMonthType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => {
            return Err(ArrowError::CastError(
                "calendar type is not representable in BSON".to_string(),
            ))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_primitive::<TimestampMillisecondType>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::DateTime(bson::datetime::DateTime::from_millis(
                    val.unwrap_or_default(),
                )))
            }),
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_primitive::<TimestampSecondType>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::DateTime(bson::datetime::DateTime::from_millis(
                    val.unwrap_or_default() * 1000,
                )))
            }),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::DateTime(bson::datetime::DateTime::from_millis(
                    val.unwrap_or_default() / 100,
                )))
            }),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .for_each(|val| {
                out.push(bson::Bson::DateTime(bson::datetime::DateTime::from_millis(
                    val.unwrap_or_default() / 100_000,
                )))
            }),
        DataType::Time32(TimeUnit::Second) => array
            .as_primitive::<Time32SecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Time32(TimeUnit::Millisecond) => array
            .as_primitive::<Time32MillisecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int32(val.unwrap_or_default()))),
        DataType::Time32(TimeUnit::Nanosecond)
        | DataType::Time32(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Second)
        | DataType::Time64(TimeUnit::Millisecond) => {
            return Err(ArrowError::CastError(
                "unreasonable time value conversion BSON".to_string(),
            ))
        }
        DataType::Time64(TimeUnit::Microsecond) => array
            .as_primitive::<Time64MicrosecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Time64(TimeUnit::Nanosecond) => array
            .as_primitive::<Time64NanosecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Duration(TimeUnit::Second) => array
            .as_primitive::<DurationSecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Duration(TimeUnit::Millisecond) => array
            .as_primitive::<DurationMillisecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Duration(TimeUnit::Microsecond) => array
            .as_primitive::<DurationMicrosecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::Duration(TimeUnit::Nanosecond) => array
            .as_primitive::<DurationNanosecondType>()
            .iter()
            .for_each(|val| out.push(bson::Bson::Int64(val.unwrap_or_default()))),
        DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_) => {
            out.push(bson::Bson::Array(array_to_bson(array)?))
        }
        DataType::Struct(fields) => {
            let converter = BsonBatchConverter::new(array.as_struct().to_owned(), fields);

            for doc in converter {
                out.push(bson::Bson::Document(doc))
            }
        }
        DataType::Map(_, _) => {
            let struct_array = array.as_map().entries();
            let converter =
                BsonBatchConverter::new(struct_array.to_owned(), struct_array.fields().to_owned());

            for doc in converter {
                out.push(bson::Bson::Document(doc))
            }
        }
        DataType::Dictionary(_, _) => out.push(bson::Bson::Array(array_to_bson(
            array.as_any_dictionary().values(),
        )?)),
        DataType::Decimal256(_, _) | DataType::RunEndEncoded(_, _) | DataType::Union(_, _) => {
            return Err(ArrowError::CastError(
                "type is not representable in BSON".to_string(),
            ))
        }
    };
    Ok(out)
}
