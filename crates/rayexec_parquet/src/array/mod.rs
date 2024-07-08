pub mod primitive;
pub mod varlen;

use bytes::{Buf, Bytes};
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use parquet::column::page::PageReader;
use parquet::column::reader::GenericColumnReader;
use parquet::data_type::{
    BoolType, ByteArrayType, DataType as ParquetDataType, DoubleType, FloatType, Int32Type,
    Int64Type, Int96Type,
};
use parquet::file::reader::{ChunkReader, Length, SerializedPageReader};
use parquet::schema::types::ColumnDescPtr;
use primitive::PrimitiveArrayReader;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::{DataType, TimeUnit};
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_io::AsyncReader;
use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::sync::Arc;
use varlen::VarlenArrayReader;

use crate::metadata::Metadata;

pub trait ArrayBuilder<P: PageReader>: Send {
    /// Consume the current buffer and build an array.
    fn build(&mut self) -> Result<Array>;

    /// Sets the page reader the builder should now be reading from.
    fn set_page_reader(&mut self, page_reader: P) -> Result<()>;

    /// Read `n` number of rows from the page reader, returning the actual
    /// number of rows read.
    fn read_rows(&mut self, n: usize) -> Result<usize>;
}

/// Create a new array builder based on the provided type.
pub fn builder_for_type<P>(
    datatype: DataType,
    desc: ColumnDescPtr,
) -> Result<Box<dyn ArrayBuilder<P>>>
where
    P: PageReader + 'static,
{
    match &datatype {
        DataType::Boolean => Ok(Box::new(PrimitiveArrayReader::<BoolType, P>::new(
            datatype, desc,
        ))),
        DataType::Int32 => Ok(Box::new(PrimitiveArrayReader::<Int32Type, P>::new(
            datatype, desc,
        ))),
        DataType::Int64 => Ok(Box::new(PrimitiveArrayReader::<Int64Type, P>::new(
            datatype, desc,
        ))),
        DataType::Timestamp(meta) if meta.unit == TimeUnit::Nanosecond => Ok(Box::new(
            PrimitiveArrayReader::<Int96Type, P>::new(datatype, desc),
        )),
        DataType::Float32 => Ok(Box::new(PrimitiveArrayReader::<FloatType, P>::new(
            datatype, desc,
        ))),
        DataType::Float64 => Ok(Box::new(PrimitiveArrayReader::<DoubleType, P>::new(
            datatype, desc,
        ))),
        DataType::Date32 => Ok(Box::new(PrimitiveArrayReader::<Int32Type, P>::new(
            datatype, desc,
        ))),
        DataType::Decimal64(_) => Ok(Box::new(PrimitiveArrayReader::<Int64Type, P>::new(
            datatype, desc,
        ))),
        DataType::Utf8 => Ok(Box::new(VarlenArrayReader::<ByteArrayType, P>::new(
            datatype, desc,
        ))),
        DataType::Binary => Ok(Box::new(VarlenArrayReader::<ByteArrayType, P>::new(
            datatype, desc,
        ))),
        other => Err(RayexecError::new(format!(
            "Unimplemented parquet array builder: {other:?}"
        ))),
    }
}

/// Trait for converting a buffer of values into an array.
pub trait IntoArray {
    fn into_array(self, def_levels: Option<Vec<i16>>) -> Array;
}

pub fn def_levels_into_bitmap(def_levels: Vec<i16>) -> Bitmap {
    Bitmap::from_iter(def_levels.into_iter().map(|v| match v {
        0 => false,
        1 => true,
        other => unimplemented!("nested unimplemented, def level: {other}"),
    }))
}

pub struct AsyncBatchReader<R: AsyncReader> {
    /// Reader we're reading from.
    reader: R,

    /// Row groups we'll be reading for.
    row_groups: VecDeque<usize>,

    /// Row group we're currently working.
    ///
    /// Initialized to None
    current_row_group: Option<usize>,

    /// Parquet metadata.
    metadata: Arc<Metadata>,

    /// Desired batch size.
    batch_size: usize,

    /// Builders for each column in the batch.
    builders: Vec<Box<dyn ArrayBuilder<SerializedPageReader<InMemoryColumnChunk>>>>,

    /// Columns chunks we've read.
    column_chunks: Vec<Option<InMemoryColumnChunk>>,
}

impl<R: AsyncReader + 'static> AsyncBatchReader<R> {
    pub fn try_new(
        reader: R,
        row_groups: VecDeque<usize>,
        metadata: Arc<Metadata>,
        schema: &Schema,
        batch_size: usize,
    ) -> Result<Self> {
        let column_chunks = (0..schema.fields.len()).map(|_| None).collect();
        let mut builders = Vec::with_capacity(schema.fields.len());

        for (datatype, column_chunk_meta) in schema
            .iter()
            .map(|f| f.datatype.clone())
            .zip(metadata.parquet_metadata.row_group(0).columns())
        {
            let builder = builder_for_type(datatype, column_chunk_meta.column_descr_ptr())?;
            builders.push(builder)
        }

        Ok(AsyncBatchReader {
            reader,
            row_groups,
            current_row_group: None,
            metadata,
            batch_size,
            column_chunks,
            builders,
        })
    }

    pub fn into_stream(self) -> BoxStream<'static, Result<Batch>> {
        let stream = stream::try_unfold(self, |mut reader| async move {
            match reader.read_next().await {
                Ok(Some(batch)) => Ok(Some((batch, reader))),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        });
        stream.boxed()
    }

    pub async fn read_next(&mut self) -> Result<Option<Batch>> {
        if self.current_row_group.is_none() {
            match self.row_groups.pop_front() {
                Some(group) => {
                    self.current_row_group = Some(group);
                    self.fetch_column_chunks().await?;
                    self.set_page_readers()?;
                }
                None => return Ok(None),
            }
        }

        loop {
            match self.maybe_read_batch()? {
                Some(batch) => return Ok(Some(batch)),
                None => {
                    self.current_row_group = Some(match self.row_groups.pop_front() {
                        Some(group) => group,
                        None => return Ok(None),
                    });

                    // Need to read the next set of column chunks.
                    self.fetch_column_chunks().await?;
                    self.set_page_readers()?;
                }
            }
        }
    }

    /// Try to read the next batch from the array builders.
    ///
    /// Returns Ok(None) when there's nothing left to read.
    fn maybe_read_batch(&mut self) -> Result<Option<Batch>> {
        for builder in self.builders.iter_mut() {
            builder.read_rows(self.batch_size)?;
        }

        let arrays = self
            .builders
            .iter_mut()
            .map(|builder| builder.build())
            .collect::<Result<Vec<_>>>()?;

        let batch = Batch::try_new(arrays)?;

        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    fn set_page_readers(&mut self) -> Result<()> {
        let mut builders = std::mem::take(&mut self.builders);
        for (idx, builder) in builders.iter_mut().enumerate() {
            let page_reader = self.take_serialized_page_reader(idx)?;
            builder.set_page_reader(page_reader)?;
        }
        self.builders = builders;

        Ok(())
    }

    /// Fetches the column chunks for the current row group.
    async fn fetch_column_chunks(&mut self) -> Result<()> {
        for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
            let col = self
                .metadata
                .parquet_metadata
                .row_group(self.current_row_group.expect("current row group to be set"))
                .column(idx);
            let (start, len) = col.byte_range();

            // TODO: Parallel reads.
            let buf = self.reader.read_range(start as usize, len as usize).await?;

            *chunk = Some(InMemoryColumnChunk {
                offset: start as usize,
                buf,
            })
        }

        Ok(())
    }

    /// Take the underlying buffer for a column and convert into a page reader.
    fn take_serialized_page_reader(
        &mut self,
        col: usize,
    ) -> Result<SerializedPageReader<InMemoryColumnChunk>> {
        let row_group = self.current_row_group.expect("current row group to be set");
        let locations = self
            .metadata
            .parquet_metadata
            .offset_index()
            .map(|row_groups| row_groups[row_group][col].clone());

        let row_group_meta = self.metadata.parquet_metadata.row_group(row_group);

        let chunk = match std::mem::take(&mut self.column_chunks[col]) {
            Some(chunk) => Arc::new(chunk),
            None => return Err(RayexecError::new("Expected column chunk")),
        };

        let page_reader = SerializedPageReader::new(
            chunk,
            row_group_meta.column(col),
            row_group_meta.num_rows() as usize,
            locations,
        )
        .context("failed to create serialize page reader")?;

        Ok(page_reader)
    }
}

impl<R: AsyncReader> fmt::Debug for AsyncBatchReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncBatchReader")
            .field("row_groups", &self.row_groups)
            .field("current_row_group", &self.current_row_group)
            .finish_non_exhaustive()
    }
}

/// In-memory column chunk buffer.
#[derive(Debug, PartialEq, Eq)]
struct InMemoryColumnChunk {
    /// The offset of the column chunk in the file. This is used to properly
    /// adapt this to the `ChunkReader` trait which is file oriented.
    offset: usize,

    /// The actual column data.
    buf: Bytes,
}

impl InMemoryColumnChunk {
    fn get(&self, start: usize) -> Bytes {
        let start = start - self.offset;
        self.buf.slice(start..)
    }
}

impl ChunkReader for InMemoryColumnChunk {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(self.get(start as usize).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        Ok(self.get(start as usize).slice(..length))
    }
}

impl Length for InMemoryColumnChunk {
    fn len(&self) -> u64 {
        self.buf.len() as u64
    }
}

#[derive(Debug)]
pub struct ValuesReader<T: ParquetDataType, P: PageReader> {
    desc: ColumnDescPtr,
    reader: Option<GenericColumnReader<T, P>>,

    values: Vec<T::T>,
    def_levels: Option<Vec<i16>>,
    rep_levels: Option<Vec<i16>>,
}

impl<T, P> ValuesReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
{
    pub fn new(desc: ColumnDescPtr) -> Self {
        let def_levels = if desc.max_def_level() > 0 {
            Some(Vec::new())
        } else {
            None
        };
        let rep_levels = if desc.max_rep_level() > 0 {
            Some(Vec::new())
        } else {
            None
        };

        Self {
            desc,
            reader: None,
            values: Vec::new(),
            def_levels,
            rep_levels,
        }
    }

    pub fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        let reader = GenericColumnReader::new(self.desc.clone(), page_reader);
        self.reader = Some(reader);

        Ok(())
    }

    pub fn read_records(&mut self, num_records: usize) -> Result<usize> {
        let reader = match &mut self.reader {
            Some(reader) => reader,
            None => return Err(RayexecError::new("Expected reader to be Some")),
        };

        let mut num_read = 0;
        loop {
            let to_read = num_records - num_read;
            let (records_read, values_read, levels_read) = reader
                .read_records(
                    to_read,
                    self.def_levels.as_mut(),
                    self.rep_levels.as_mut(),
                    &mut self.values,
                )
                .context("read records")?;

            // Pad nulls.
            if values_read < levels_read {
                // TODO: Need to revisit how we handle definition
                // levels/repetition levels and nulls.
                self.values.resize(levels_read, T::T::default());
            }

            num_read += records_read;

            if num_read == num_records || !reader.has_next().context("check next page")? {
                break;
            }
        }

        Ok(num_read)
    }

    pub fn take_values(&mut self) -> Vec<T::T> {
        std::mem::take(&mut self.values)
    }

    pub fn take_def_levels(&mut self) -> Option<Vec<i16>> {
        // We want to take the inner array and replace it with an empty array.
        // Calling `take` on an option would replace Some with None.
        self.def_levels.as_mut().map(std::mem::take)
    }

    pub fn take_rep_levels(&mut self) -> Option<Vec<i16>> {
        // See above.
        self.rep_levels.as_mut().map(std::mem::take)
    }
}
