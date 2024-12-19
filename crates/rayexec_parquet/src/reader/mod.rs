pub mod primitive;
pub mod varlen;

use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::column::reader::decoder::{
    ColumnValueDecoder,
    DefinitionLevelDecoder,
    RepetitionLevelDecoder,
};
use parquet::column::reader::GenericColumnReader;
use parquet::data_type::Int96;
use parquet::file::reader::{ChunkReader, Length, SerializedPageReader};
use parquet::schema::types::ColumnDescPtr;
use primitive::PrimitiveArrayReader;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::storage::table_storage::Projections;
use rayexec_io::FileSource;
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
    batch_size: usize,
    datatype: DataType,
    physical: PhysicalType,
    desc: ColumnDescPtr,
) -> Result<Box<dyn ArrayBuilder<P>>>
where
    P: PageReader + 'static,
{
    match (&datatype, physical) {
        (DataType::Boolean, _) => Ok(Box::new(PrimitiveArrayReader::<bool, P>::new(
            batch_size, datatype, desc,
        ))),
        (DataType::Int16, _) => Ok(Box::new(PrimitiveArrayReader::<i32, P>::new(
            batch_size, datatype, desc,
        ))),
        (DataType::Int32, _) => Ok(Box::new(PrimitiveArrayReader::<i32, P>::new(
            batch_size, datatype, desc,
        ))),
        (DataType::UInt16, PhysicalType::INT32) => Ok(Box::new(
            PrimitiveArrayReader::<i32, P>::new(batch_size, datatype, desc),
        )),
        (DataType::Int64, _) => Ok(Box::new(PrimitiveArrayReader::<i64, P>::new(
            batch_size, datatype, desc,
        ))),
        (DataType::Timestamp(_), PhysicalType::INT64) => Ok(Box::new(
            PrimitiveArrayReader::<i64, P>::new(batch_size, datatype, desc),
        )),
        (DataType::Timestamp(_), PhysicalType::INT96) => {
            Ok(Box::new(PrimitiveArrayReader::<Int96, P>::new(
                batch_size, datatype, desc,
            )))
        }
        (DataType::Float32, _) => Ok(Box::new(PrimitiveArrayReader::<f32, P>::new(
            batch_size, datatype, desc,
        ))),
        (DataType::Float64, _) => Ok(Box::new(PrimitiveArrayReader::<f64, P>::new(
            batch_size, datatype, desc,
        ))),
        (DataType::Date32, _) => Ok(Box::new(PrimitiveArrayReader::<i32, P>::new(
            batch_size, datatype, desc,
        ))),
        (DataType::Decimal64(_), PhysicalType::INT32) => Ok(Box::new(
            PrimitiveArrayReader::<i32, P>::new(batch_size, datatype, desc),
        )),
        (DataType::Decimal64(_), PhysicalType::INT64) => Ok(Box::new(
            PrimitiveArrayReader::<i64, P>::new(batch_size, datatype, desc),
        )),
        (DataType::Decimal128(_), PhysicalType::INT32) => {
            Ok(Box::new(PrimitiveArrayReader::<i32, P>::new(
                batch_size, datatype, desc,
            )))
        }
        (DataType::Decimal128(_), PhysicalType::INT64) => {
            Ok(Box::new(PrimitiveArrayReader::<i64, P>::new(
                batch_size, datatype, desc,
            )))
        }
        (DataType::Utf8, _) => Ok(Box::new(VarlenArrayReader::<P>::new(
            batch_size, datatype, desc,
        ))),
        (DataType::Binary, _) => Ok(Box::new(VarlenArrayReader::<P>::new(
            batch_size, datatype, desc,
        ))),
        other => Err(RayexecError::new(format!(
            "Unimplemented parquet array builder: {other:?}"
        ))),
    }
}

/// Trait for converting a buffer of values into array data.
pub trait IntoArrayData {
    fn into_array_data(self) -> ArrayData;
}

pub fn def_levels_into_bitmap(def_levels: Vec<i16>) -> Bitmap {
    Bitmap::from_iter(def_levels.into_iter().map(|v| match v {
        0 => false,
        1 => true,
        other => unimplemented!("nested unimplemented, def level: {other}"),
    }))
}

/// Insert null (meaningless) values into the vec according to the validity
/// bitmap.
///
/// The resulting vec will have its length equal to the bitmap's length.
pub fn insert_null_values<T>(values: &mut Vec<T>, bitmap: &Bitmap)
where
    T: Copy + Default,
{
    values.resize(bitmap.len(), T::default());

    for (current_idx, new_idx) in (0..values.len()).rev().zip(bitmap.index_iter().rev()) {
        if current_idx <= new_idx {
            break;
        }
        values[new_idx] = values[current_idx];
    }
}

pub struct AsyncBatchReader<R: FileSource> {
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
    /// All column states for columns we're reading.
    column_states: Vec<ColumnState>,
}

struct ColumnState {
    /// Index of the column in the parquet file.
    column_idx: usize,
    /// Builder for this column.
    builder: Box<dyn ArrayBuilder<SerializedPageReader<InMemoryColumnChunk>>>,
    /// In-memory buffer for reading this column.
    column_chunk: Option<InMemoryColumnChunk>,
}

impl<R: FileSource + 'static> AsyncBatchReader<R> {
    pub fn try_new(
        reader: R,
        row_groups: VecDeque<usize>,
        metadata: Arc<Metadata>,
        schema: &Schema,
        batch_size: usize,
        projections: Projections,
    ) -> Result<Self> {
        // Create projection bitmap.
        //
        // TODO: This will need to change to accomodate structs in parquet
        // files.
        let bitmap = match &projections.column_indices {
            Some(indices) => {
                let mut bitmap = Bitmap::new_with_all_false(schema.fields.len());
                for &idx in indices {
                    bitmap.set(idx, true);
                }
                bitmap
            }
            None => Bitmap::new_with_all_true(schema.fields.len()),
        };

        let mut states = Vec::with_capacity(schema.fields.len());

        for (col_idx, ((datatype, column_chunk_meta), projected)) in schema
            .iter()
            .map(|f| f.datatype.clone())
            .zip(metadata.decoded_metadata.row_group(0).columns())
            .zip(bitmap.iter())
            .enumerate()
        {
            if projected {
                let physical = column_chunk_meta.column_type();
                let builder = builder_for_type(
                    batch_size,
                    datatype,
                    physical,
                    column_chunk_meta.column_descr_ptr(),
                )?;

                let state = ColumnState {
                    column_idx: col_idx,
                    builder,
                    column_chunk: None,
                };

                states.push(state)
            }
        }

        Ok(AsyncBatchReader {
            reader,
            row_groups,
            current_row_group: None,
            metadata,
            batch_size,
            column_states: states,
        })
    }

    pub async fn read_next(&mut self) -> Result<Option<Batch>> {
        if self.current_row_group.is_none() {
            match self.row_groups.pop_front() {
                Some(group) => {
                    // DO TABLE FILTERS HERE.

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
        for state in self.column_states.iter_mut() {
            state.builder.read_rows(self.batch_size)?;
        }
        let arrays = self
            .column_states
            .iter_mut()
            .map(|state| state.builder.build())
            .collect::<Result<Vec<_>>>()?;

        let batch = Batch::try_new(arrays)?;

        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    fn set_page_readers(&mut self) -> Result<()> {
        for state in self.column_states.iter_mut() {
            let row_group = self.current_row_group.expect("current row group to be set");
            let locations = self
                .metadata
                .decoded_metadata
                .offset_index()
                .map(|row_groups| row_groups[row_group][state.column_idx].clone());

            let row_group_meta = self.metadata.decoded_metadata.row_group(row_group);

            let chunk = match std::mem::take(&mut state.column_chunk) {
                Some(chunk) => Arc::new(chunk),
                None => return Err(RayexecError::new("Expected column chunk")),
            };

            let page_reader = SerializedPageReader::new(
                chunk,
                row_group_meta.column(state.column_idx),
                row_group_meta.num_rows() as usize,
                locations,
            )
            .context("failed to create serialize page reader")?;

            state.builder.set_page_reader(page_reader)?;
        }

        Ok(())
    }

    /// Fetches the column chunks for the current row group.
    async fn fetch_column_chunks(&mut self) -> Result<()> {
        for state in self.column_states.iter_mut() {
            // We already have data for this.
            if state.column_chunk.is_some() {
                continue;
            }

            let col = self
                .metadata
                .decoded_metadata
                .row_group(self.current_row_group.expect("current row group to be set"))
                .column(state.column_idx);
            let (start, len) = col.byte_range();

            // TODO: Parallel reads.
            let buf = self.reader.read_range(start as usize, len as usize).await?;

            state.column_chunk = Some(InMemoryColumnChunk {
                offset: start as usize,
                buf,
            })
        }

        Ok(())
    }
}

impl<R: FileSource> fmt::Debug for AsyncBatchReader<R> {
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
pub struct ValuesReader<V: ColumnValueDecoder, P: PageReader> {
    description: ColumnDescPtr,
    reader: Option<GenericColumnReader<V, P>>,

    def_levels: Option<Vec<i16>>,
    rep_levels: Option<Vec<i16>>,
}

impl<V, P> ValuesReader<V, P>
where
    V: ColumnValueDecoder,
    P: PageReader,
{
    pub fn new(description: ColumnDescPtr) -> Self {
        let def_levels = if description.max_def_level() > 0 {
            Some(Vec::new())
        } else {
            None
        };
        let rep_levels = if description.max_rep_level() > 0 {
            Some(Vec::new())
        } else {
            None
        };

        Self {
            description,
            reader: None,
            def_levels,
            rep_levels,
        }
    }

    pub fn set_page_reader(&mut self, values_decoder: V, page_reader: P) -> Result<()> {
        let def_level_decoder = (self.description.max_def_level() != 0)
            .then(|| DefinitionLevelDecoder::new(self.description.max_def_level()));

        let rep_level_decoder = (self.description.max_rep_level() != 0)
            .then(|| RepetitionLevelDecoder::new(self.description.max_rep_level()));

        let reader = GenericColumnReader::new_with_decoders(
            self.description.clone(),
            page_reader,
            values_decoder,
            def_level_decoder,
            rep_level_decoder,
        );
        self.reader = Some(reader);

        Ok(())
    }

    pub fn read_records(&mut self, num_records: usize, values: &mut V::Buffer) -> Result<usize> {
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
                    values,
                )
                .context("read records")?;

            // Pad nulls.
            if values_read < levels_read {
                // TODO: Need to revisit how we handle definition
                // levels/repetition levels and nulls.
                // values.resize(levels_read, T::T::default());
            }

            num_read += records_read;

            if num_read == num_records || !reader.has_next().context("check next page")? {
                break;
            }
        }

        Ok(num_read)
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
