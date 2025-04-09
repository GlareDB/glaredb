//! Parquet metadata structures
//!
//! * [`ParquetMetaData`]: Top level metadata container, read from the Parquet
//!   file footer.
//!
//! * [`FileMetaData`]: File level metadata such as schema, row counts and
//!   version.
//!
//! * [`RowGroupMetaData`]: Metadata for each Row Group with a File, such as
//!   location and number of rows, and column chunks.
//!
//! * [`ColumnChunkMetaData`]: Metadata for each column chunk (primitive leaf)
//!   within a Row Group including encoding and compression information,
//!   number of values, statistics, etc.

pub mod loader;
pub mod page_encoding_stats;
pub mod page_index;
pub mod properties;
pub mod statistics;

use std::ops::Range;
use std::sync::Arc;

use glaredb_error::{DbError, Result};
use page_encoding_stats::PageEncodingStats;
use statistics::Statistics;

use crate::basic::{ColumnOrder, Compression, Encoding, Type};
use crate::format::{
    self,
    BoundaryOrder,
    ColumnChunk,
    ColumnIndex,
    ColumnMetaData,
    OffsetIndex,
    PageLocation,
    RowGroup,
    SortingColumn,
};
use crate::metadata::page_index::index::Index;
use crate::schema::types::{ColumnDescriptor, ColumnPath, GroupType, SchemaDescriptor};

/// The length of the parquet footer in bytes
pub const FOOTER_SIZE: usize = 8;

/// Minimum file size for a valid parquet file.
pub const MIN_FILE_SIZE: usize = 12;

/// Magic value for parquet files.
pub const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Magic value for encrypted parquet files.
pub const PARQUET_MAGIC_ENC: &[u8; 4] = b"PARE";

/// [`Index`] for each row group of each column.
///
/// `column_index[row_group_number][column_number]` holds the
/// [`Index`] corresponding to column `column_number` of row group
/// `row_group_number`.
///
/// For example `column_index[2][3]` holds the [`Index`] for the forth
/// column in the third row group of the parquet file.
pub type ParquetColumnIndex = Vec<Vec<Index>>;

/// [`PageLocation`] for each data page of each row group of each column.
///
/// `offset_index[row_group_number][column_number][page_number]` holds
/// the [`PageLocation`] corresponding to page `page_number` of column
/// `column_number`of row group `row_group_number`.
///
/// For example `offset_index[2][3][4]` holds the [`PageLocation`] for
/// the fifth page of the forth column in the third row group of the
/// parquet file.
pub type ParquetOffsetIndex = Vec<Vec<Vec<PageLocation>>>;

/// Global Parquet metadata, including [`FileMetaData`], [`RowGroupMetaData`].
///
/// This structure is stored in the footer of Parquet files, in the format
/// defined by [`parquet.thrift`]. It contains:
///
/// * File level metadata: [`FileMetaData`]
/// * Row Group level metadata: [`RowGroupMetaData`]
/// * (Optional) "Page Index" structures: [`ParquetColumnIndex`] and [`ParquetOffsetIndex`]
///
/// [`parquet.thrift`]: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
#[derive(Debug, Clone)]
pub struct ParquetMetaData {
    /// File level metadata
    pub file_metadata: FileMetaData,
    /// Row group metadata
    pub row_groups: Vec<RowGroupMetaData>,
    /// Page level index for each page in each column chunk
    pub column_index: Option<ParquetColumnIndex>,
    /// Offset index for all each page in each column chunk
    pub offset_index: Option<ParquetOffsetIndex>,
}

impl ParquetMetaData {
    /// Creates Parquet metadata from file metadata and a list of row
    /// group metadata
    pub fn new(file_metadata: FileMetaData, row_groups: Vec<RowGroupMetaData>) -> Self {
        ParquetMetaData {
            file_metadata,
            row_groups,
            column_index: None,
            offset_index: None,
        }
    }

    /// Creates Parquet metadata from file metadata, a list of row
    /// group metadata, and the column index structures.
    pub fn new_with_page_index(
        file_metadata: FileMetaData,
        row_groups: Vec<RowGroupMetaData>,
        column_index: Option<ParquetColumnIndex>,
        offset_index: Option<ParquetOffsetIndex>,
    ) -> Self {
        ParquetMetaData {
            file_metadata,
            row_groups,
            column_index,
            offset_index,
        }
    }

    /// Returns number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.row_groups.len()
    }
}

/// File level metadata for a Parquet file.
///
/// Includes the version of the file, metadata, number of rows, schema, and
/// column orders
#[derive(Debug, Clone)]
pub struct FileMetaData {
    /// Version of this file.
    pub version: i32,
    /// Number of rows in the file.
    pub num_rows: i64,
    /// String message for application that wrote this file.
    ///
    /// This should have the following format:
    /// `<application> version <application version> (build <application build hash>)`.
    ///
    /// ```shell
    /// parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)
    /// ```
    pub created_by: Option<String>,
    /// Returns key_value_metadata of this file.
    pub key_value_metadata: Option<Vec<format::KeyValue>>,
    /// A reference to schema descriptor.
    pub schema_descr: Arc<SchemaDescriptor>,
    /// Column (sort) order used for `min` and `max` values of each column in this file.
    ///
    /// Each column order corresponds to one column, determined by its position in the
    /// list, matching the position of the column in the schema.
    ///
    /// When `None`, there are no column orders available, and each column
    /// should be assumed to have undefined (legacy) column order.
    pub column_orders: Option<Vec<ColumnOrder>>,
}

impl FileMetaData {
    /// Creates new file metadata.
    pub fn new(
        version: i32,
        num_rows: i64,
        created_by: Option<String>,
        key_value_metadata: Option<Vec<format::KeyValue>>,
        schema_descr: Arc<SchemaDescriptor>,
        column_orders: Option<Vec<ColumnOrder>>,
    ) -> Self {
        FileMetaData {
            version,
            num_rows,
            created_by,
            key_value_metadata,
            schema_descr,
            column_orders,
        }
    }

    /// Returns Parquet [`GroupType`] that describes schema in this file.
    ///
    /// [`Type`]: crate::schema::types::Type
    pub fn schema(&self) -> &GroupType {
        self.schema_descr.schema_type()
    }

    /// Returns column order for `i`th column in this file.
    /// If column orders are not available, returns undefined (legacy) column order.
    pub fn column_order(&self, i: usize) -> ColumnOrder {
        self.column_orders
            .as_ref()
            .map(|data| data[i])
            .unwrap_or(ColumnOrder::UNDEFINED)
    }
}

/// Metadata for a row group
///
/// Includes [`ColumnChunkMetaData`] for each column in the row group, the
/// number of rows the total byte size of the row group, and the
/// [`SchemaDescriptor`] for the row group.
#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupMetaData {
    /// Metadata for columns in this row group.
    pub columns: Vec<ColumnChunkMetaData>,
    /// Number of rows in this row group.
    pub num_rows: i64,
    /// The sort ordering of the rows in this RowGroup if any
    pub sorting_columns: Option<Vec<SortingColumn>>,
    /// Total byte size of all uncompressed column data in this row group.
    pub total_byte_size: i64,
    pub schema_descr: Arc<SchemaDescriptor>,
    /// File offset of this row group in file.
    ///
    /// We can't infer from file offset of first column since there may empty
    /// columns in row group.
    pub file_offset: Option<i64>,
    /// Ordinal position of this row group in file
    ///
    /// For example if this is the first row group in the file, this will be 0.
    /// If this is the second row group in the file, this will be 1.
    pub ordinal: Option<i16>,
}

impl RowGroupMetaData {
    /// Returns builder for row group metadata.
    pub fn builder(schema_descr: Arc<SchemaDescriptor>) -> RowGroupMetaDataBuilder {
        RowGroupMetaDataBuilder::new(schema_descr)
    }

    /// Number of columns in this row group.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Total size of all compressed column data in this row group.
    pub fn compressed_size(&self) -> i64 {
        self.columns.iter().map(|c| c.total_compressed_size).sum()
    }

    /// Method to convert from Thrift.
    pub fn from_thrift(
        schema_descr: Arc<SchemaDescriptor>,
        mut rg: RowGroup,
    ) -> Result<RowGroupMetaData> {
        if schema_descr.num_columns() != rg.columns.len() {
            return Err(DbError::new(format!(
                "Column count mismatch. Schema has {} columns while Row Group has {}",
                schema_descr.num_columns(),
                rg.columns.len()
            )));
        }
        let total_byte_size = rg.total_byte_size;
        let num_rows = rg.num_rows;
        let mut columns = vec![];
        for (c, d) in rg.columns.drain(0..).zip(schema_descr.columns()) {
            let cc = ColumnChunkMetaData::from_thrift(d.clone(), c)?;
            columns.push(cc);
        }
        let sorting_columns = rg.sorting_columns;
        Ok(RowGroupMetaData {
            columns,
            num_rows,
            sorting_columns,
            total_byte_size,
            schema_descr,
            file_offset: rg.file_offset,
            ordinal: rg.ordinal,
        })
    }

    /// Method to convert to Thrift.
    pub fn to_thrift(&self) -> RowGroup {
        RowGroup {
            columns: self.columns.iter().map(|v| v.to_thrift()).collect(),
            total_byte_size: self.total_byte_size,
            num_rows: self.num_rows,
            sorting_columns: self.sorting_columns.clone(),
            file_offset: self.file_offset,
            total_compressed_size: Some(self.compressed_size()),
            ordinal: self.ordinal,
        }
    }

    /// Converts this [`RowGroupMetaData`] into a [`RowGroupMetaDataBuilder`]
    pub fn into_builder(self) -> RowGroupMetaDataBuilder {
        RowGroupMetaDataBuilder(self)
    }
}

/// Builder for row group metadata.
// TODO: Remove
pub struct RowGroupMetaDataBuilder(RowGroupMetaData);

impl RowGroupMetaDataBuilder {
    /// Creates new builder from schema descriptor.
    fn new(schema_descr: Arc<SchemaDescriptor>) -> Self {
        Self(RowGroupMetaData {
            columns: Vec::with_capacity(schema_descr.num_columns()),
            schema_descr,
            file_offset: None,
            num_rows: 0,
            sorting_columns: None,
            total_byte_size: 0,
            ordinal: None,
        })
    }

    /// Sets number of rows in this row group.
    pub fn set_num_rows(mut self, value: i64) -> Self {
        self.0.num_rows = value;
        self
    }

    /// Sets the sorting order for columns
    pub fn set_sorting_columns(mut self, value: Option<Vec<SortingColumn>>) -> Self {
        self.0.sorting_columns = value;
        self
    }

    /// Sets total size in bytes for this row group.
    pub fn set_total_byte_size(mut self, value: i64) -> Self {
        self.0.total_byte_size = value;
        self
    }

    /// Sets column metadata for this row group.
    pub fn set_column_metadata(mut self, value: Vec<ColumnChunkMetaData>) -> Self {
        self.0.columns = value;
        self
    }

    /// Sets ordinal for this row group.
    pub fn set_ordinal(mut self, value: i16) -> Self {
        self.0.ordinal = Some(value);
        self
    }

    pub fn set_file_offset(mut self, value: i64) -> Self {
        self.0.file_offset = Some(value);
        self
    }

    /// Builds row group metadata.
    pub fn build(self) -> Result<RowGroupMetaData> {
        if self.0.schema_descr.num_columns() != self.0.columns.len() {
            return Err(DbError::new(format!(
                "Column length mismatch: {} != {}",
                self.0.schema_descr.num_columns(),
                self.0.columns.len()
            )));
        }

        Ok(self.0)
    }
}

/// Metadata for a column chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChunkMetaData {
    pub column_descr: ColumnDescriptor,
    /// All encodings used for this column.
    pub encodings: Vec<Encoding>,
    /// File where the column chunk is stored.
    ///
    /// If not set, assumed to belong to the same file as the metadata.
    /// This path is relative to the current file.
    pub file_path: Option<String>,
    /// Byte offset in `file_path()`.
    pub file_offset: i64,
    /// Total number of values in this column chunk.
    pub num_values: i64,
    /// Compression for this column.
    pub compression: Compression,
    /// Total byte size of all compressed, and potentially encrypted, pages in
    /// this column chunk (including the headers)
    pub total_compressed_size: i64,
    /// The total uncompressed data size of this column chunk.
    pub total_uncompressed_size: i64,
    /// The offset for the column data.
    pub data_page_offset: i64,
    /// The offset for the index page.
    pub index_page_offset: Option<i64>,
    /// The offset for the dictionary page, if any.
    pub dictionary_page_offset: Option<i64>,
    /// Statistics that are set for this column chunk, or `None` if no
    /// statistics are available.
    pub statistics: Option<Statistics>,
    /// The offset for the page encoding stats, or `None` if no page
    /// encoding stats are available.
    pub encoding_stats: Option<Vec<PageEncodingStats>>,
    /// The offset for the bloom filter.
    pub bloom_filter_offset: Option<i64>,
    /// The offset for the bloom filter length
    pub bloom_filter_length: Option<i32>,
    /// The offset for the offset index.
    pub offset_index_offset: Option<i64>,
    /// The offset for the offset index length.
    pub offset_index_length: Option<i32>,
    /// The offset for the column index.
    pub column_index_offset: Option<i64>,
    /// The offset for the column index length.
    pub column_index_length: Option<i32>,
}

/// Represents common operations for a column chunk.
impl ColumnChunkMetaData {
    /// Returns builder for column chunk metadata.
    pub fn builder(column_descr: ColumnDescriptor) -> ColumnChunkMetaDataBuilder {
        ColumnChunkMetaDataBuilder::new(column_descr)
    }

    /// Type of this column. Must be primitive.
    pub fn column_type(&self) -> Type {
        self.column_descr.physical_type()
    }

    /// Path (or identifier) of this column.
    pub fn column_path(&self) -> &ColumnPath {
        self.column_descr.path()
    }

    /// Returns the offset and length in bytes of the column chunk within the file
    pub fn byte_range(&self) -> (u64, u64) {
        let col_start = match self.dictionary_page_offset {
            Some(dictionary_page_offset) => dictionary_page_offset,
            None => self.data_page_offset,
        };
        let col_len = self.total_compressed_size;
        assert!(
            col_start >= 0 && col_len >= 0,
            "column start and length should not be negative"
        );
        (col_start as u64, col_len as u64)
    }

    /// Returns the range for the offset index if any
    #[allow(unused)]
    pub(crate) fn column_index_range(&self) -> Option<Range<usize>> {
        let offset = usize::try_from(self.column_index_offset?).ok()?;
        let length = usize::try_from(self.column_index_length?).ok()?;
        Some(offset..(offset + length))
    }

    /// Returns the range for the offset index if any
    #[allow(unused)]
    pub(crate) fn offset_index_range(&self) -> Option<Range<usize>> {
        let offset = usize::try_from(self.offset_index_offset?).ok()?;
        let length = usize::try_from(self.offset_index_length?).ok()?;
        Some(offset..(offset + length))
    }

    /// Method to convert from Thrift.
    pub fn from_thrift(column_descr: ColumnDescriptor, cc: ColumnChunk) -> Result<Self> {
        if cc.meta_data.is_none() {
            return Err(DbError::new("Expected to have column metadata"));
        }
        let mut col_metadata: ColumnMetaData = cc.meta_data.unwrap();
        let column_type = Type::try_from(col_metadata.type_)?;
        let encodings = col_metadata
            .encodings
            .drain(0..)
            .map(Encoding::try_from)
            .collect::<Result<_>>()?;
        let compression = Compression::try_from(col_metadata.codec)?;
        let file_path = cc.file_path;
        let file_offset = cc.file_offset;
        let num_values = col_metadata.num_values;
        let total_compressed_size = col_metadata.total_compressed_size;
        let total_uncompressed_size = col_metadata.total_uncompressed_size;
        let data_page_offset = col_metadata.data_page_offset;
        let index_page_offset = col_metadata.index_page_offset;
        let dictionary_page_offset = col_metadata.dictionary_page_offset;
        let statistics = statistics::from_thrift(column_type, col_metadata.statistics)?;
        let encoding_stats = col_metadata
            .encoding_stats
            .as_ref()
            .map(|vec| {
                vec.iter()
                    .map(page_encoding_stats::try_from_thrift)
                    .collect::<Result<_>>()
            })
            .transpose()?;
        let bloom_filter_offset = col_metadata.bloom_filter_offset;
        let bloom_filter_length = col_metadata.bloom_filter_length;
        let offset_index_offset = cc.offset_index_offset;
        let offset_index_length = cc.offset_index_length;
        let column_index_offset = cc.column_index_offset;
        let column_index_length = cc.column_index_length;

        let result = ColumnChunkMetaData {
            column_descr,
            encodings,
            file_path,
            file_offset,
            num_values,
            compression,
            total_compressed_size,
            total_uncompressed_size,
            data_page_offset,
            index_page_offset,
            dictionary_page_offset,
            statistics,
            encoding_stats,
            bloom_filter_offset,
            bloom_filter_length,
            offset_index_offset,
            offset_index_length,
            column_index_offset,
            column_index_length,
        };
        Ok(result)
    }

    /// Method to convert to Thrift.
    pub fn to_thrift(&self) -> ColumnChunk {
        let column_metadata = self.to_column_metadata_thrift();

        ColumnChunk {
            file_path: self.file_path.clone(),
            file_offset: self.file_offset,
            meta_data: Some(column_metadata),
            offset_index_offset: self.offset_index_offset,
            offset_index_length: self.offset_index_length,
            column_index_offset: self.column_index_offset,
            column_index_length: self.column_index_length,
            crypto_metadata: None,
            encrypted_column_metadata: None,
        }
    }

    /// Method to convert to Thrift `ColumnMetaData`
    pub fn to_column_metadata_thrift(&self) -> ColumnMetaData {
        ColumnMetaData {
            type_: self.column_type().into(),
            encodings: self.encodings.iter().map(|&v| v.into()).collect(),
            path_in_schema: self.column_path().as_ref().to_vec(),
            codec: self.compression.into(),
            num_values: self.num_values,
            total_uncompressed_size: self.total_uncompressed_size,
            total_compressed_size: self.total_compressed_size,
            key_value_metadata: None,
            data_page_offset: self.data_page_offset,
            index_page_offset: self.index_page_offset,
            dictionary_page_offset: self.dictionary_page_offset,
            statistics: statistics::to_thrift(self.statistics.as_ref()),
            encoding_stats: self
                .encoding_stats
                .as_ref()
                .map(|vec| vec.iter().map(page_encoding_stats::to_thrift).collect()),
            bloom_filter_offset: self.bloom_filter_offset,
            bloom_filter_length: self.bloom_filter_length,
        }
    }

    /// Converts this [`ColumnChunkMetaData`] into a [`ColumnChunkMetaDataBuilder`]
    pub fn into_builder(self) -> ColumnChunkMetaDataBuilder {
        ColumnChunkMetaDataBuilder(self)
    }
}

/// Builder for column chunk metadata.
pub struct ColumnChunkMetaDataBuilder(ColumnChunkMetaData);

impl ColumnChunkMetaDataBuilder {
    /// Creates new column chunk metadata builder.
    fn new(column_descr: ColumnDescriptor) -> Self {
        Self(ColumnChunkMetaData {
            column_descr,
            encodings: Vec::new(),
            file_path: None,
            file_offset: 0,
            num_values: 0,
            compression: Compression::UNCOMPRESSED,
            total_compressed_size: 0,
            total_uncompressed_size: 0,
            data_page_offset: 0,
            index_page_offset: None,
            dictionary_page_offset: None,
            statistics: None,
            encoding_stats: None,
            bloom_filter_offset: None,
            bloom_filter_length: None,
            offset_index_offset: None,
            offset_index_length: None,
            column_index_offset: None,
            column_index_length: None,
        })
    }

    /// Sets list of encodings for this column chunk.
    pub fn set_encodings(mut self, encodings: Vec<Encoding>) -> Self {
        self.0.encodings = encodings;
        self
    }

    /// Sets optional file path for this column chunk.
    pub fn set_file_path(mut self, value: String) -> Self {
        self.0.file_path = Some(value);
        self
    }

    /// Sets file offset in bytes.
    pub fn set_file_offset(mut self, value: i64) -> Self {
        self.0.file_offset = value;
        self
    }

    /// Sets number of values.
    pub fn set_num_values(mut self, value: i64) -> Self {
        self.0.num_values = value;
        self
    }

    /// Sets compression.
    pub fn set_compression(mut self, value: Compression) -> Self {
        self.0.compression = value;
        self
    }

    /// Sets total compressed size in bytes.
    pub fn set_total_compressed_size(mut self, value: i64) -> Self {
        self.0.total_compressed_size = value;
        self
    }

    /// Sets total uncompressed size in bytes.
    pub fn set_total_uncompressed_size(mut self, value: i64) -> Self {
        self.0.total_uncompressed_size = value;
        self
    }

    /// Sets data page offset in bytes.
    pub fn set_data_page_offset(mut self, value: i64) -> Self {
        self.0.data_page_offset = value;
        self
    }

    /// Sets optional dictionary page ofset in bytes.
    pub fn set_dictionary_page_offset(mut self, value: Option<i64>) -> Self {
        self.0.dictionary_page_offset = value;
        self
    }

    /// Sets optional index page offset in bytes.
    pub fn set_index_page_offset(mut self, value: Option<i64>) -> Self {
        self.0.index_page_offset = value;
        self
    }

    /// Sets statistics for this column chunk.
    pub fn set_statistics(mut self, value: Statistics) -> Self {
        self.0.statistics = Some(value);
        self
    }

    /// Sets page encoding stats for this column chunk.
    pub fn set_page_encoding_stats(mut self, value: Vec<PageEncodingStats>) -> Self {
        self.0.encoding_stats = Some(value);
        self
    }

    /// Sets optional bloom filter offset in bytes.
    pub fn set_bloom_filter_offset(mut self, value: Option<i64>) -> Self {
        self.0.bloom_filter_offset = value;
        self
    }

    /// Sets optional bloom filter length in bytes.
    pub fn set_bloom_filter_length(mut self, value: Option<i32>) -> Self {
        self.0.bloom_filter_length = value;
        self
    }

    /// Sets optional offset index offset in bytes.
    pub fn set_offset_index_offset(mut self, value: Option<i64>) -> Self {
        self.0.offset_index_offset = value;
        self
    }

    /// Sets optional offset index length in bytes.
    pub fn set_offset_index_length(mut self, value: Option<i32>) -> Self {
        self.0.offset_index_length = value;
        self
    }

    /// Sets optional column index offset in bytes.
    pub fn set_column_index_offset(mut self, value: Option<i64>) -> Self {
        self.0.column_index_offset = value;
        self
    }

    /// Sets optional column index length in bytes.
    pub fn set_column_index_length(mut self, value: Option<i32>) -> Self {
        self.0.column_index_length = value;
        self
    }

    /// Builds column chunk metadata.
    pub fn build(self) -> Result<ColumnChunkMetaData> {
        Ok(self.0)
    }
}

/// Builder for column index
pub struct ColumnIndexBuilder {
    null_pages: Vec<bool>,
    min_values: Vec<Vec<u8>>,
    max_values: Vec<Vec<u8>>,
    null_counts: Vec<i64>,
    boundary_order: BoundaryOrder,
    // If one page can't get build index, need to ignore all index in this column
    valid: bool,
}

impl Default for ColumnIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnIndexBuilder {
    pub fn new() -> Self {
        ColumnIndexBuilder {
            null_pages: Vec::new(),
            min_values: Vec::new(),
            max_values: Vec::new(),
            null_counts: Vec::new(),
            boundary_order: BoundaryOrder::UNORDERED,
            valid: true,
        }
    }

    pub fn append(
        &mut self,
        null_page: bool,
        min_value: Vec<u8>,
        max_value: Vec<u8>,
        null_count: i64,
    ) {
        self.null_pages.push(null_page);
        self.min_values.push(min_value);
        self.max_values.push(max_value);
        self.null_counts.push(null_count);
    }

    pub fn set_boundary_order(&mut self, boundary_order: BoundaryOrder) {
        self.boundary_order = boundary_order;
    }

    pub fn to_invalid(&mut self) {
        self.valid = false;
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Build and get the thrift metadata of column index
    pub fn build_to_thrift(self) -> ColumnIndex {
        ColumnIndex::new(
            self.null_pages,
            self.min_values,
            self.max_values,
            self.boundary_order,
            self.null_counts,
        )
    }
}

/// Builder for offset index
pub struct OffsetIndexBuilder {
    offset_array: Vec<i64>,
    compressed_page_size_array: Vec<i32>,
    first_row_index_array: Vec<i64>,
    current_first_row_index: i64,
}

impl Default for OffsetIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetIndexBuilder {
    pub fn new() -> Self {
        OffsetIndexBuilder {
            offset_array: Vec::new(),
            compressed_page_size_array: Vec::new(),
            first_row_index_array: Vec::new(),
            current_first_row_index: 0,
        }
    }

    pub fn append_row_count(&mut self, row_count: i64) {
        let current_page_row_index = self.current_first_row_index;
        self.first_row_index_array.push(current_page_row_index);
        self.current_first_row_index += row_count;
    }

    pub fn append_offset_and_size(&mut self, offset: i64, compressed_page_size: i32) {
        self.offset_array.push(offset);
        self.compressed_page_size_array.push(compressed_page_size);
    }

    /// Build and get the thrift metadata of offset index
    pub fn build_to_thrift(self) -> OffsetIndex {
        let locations = self
            .offset_array
            .iter()
            .zip(self.compressed_page_size_array.iter())
            .zip(self.first_row_index_array.iter())
            .map(|((offset, size), row_index)| PageLocation::new(*offset, *size, *row_index))
            .collect::<Vec<_>>();
        OffsetIndex::new(locations)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::basic::PageType;
    use crate::schema::types::{SchemaDescriptor, SchemaType};

    #[test]
    fn test_row_group_metadata_thrift_conversion() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(ptr.clone()).build().unwrap();
            columns.push(column);
        }
        let row_group_meta = RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .set_ordinal(1)
            .build()
            .unwrap();

        let row_group_exp = row_group_meta.to_thrift();
        let row_group_res = RowGroupMetaData::from_thrift(schema_descr, row_group_exp.clone())
            .unwrap()
            .to_thrift();

        assert_eq!(row_group_res, row_group_exp);
    }

    #[test]
    fn test_row_group_metadata_thrift_conversion_empty() {
        let schema_descr = get_test_schema_descr();
        RowGroupMetaData::builder(schema_descr).build().unwrap_err();
    }

    /// Test reading a corrupted Parquet file with 3 columns in its schema but only 2 in its row group
    #[test]
    fn test_row_group_metadata_thrift_corrupted() {
        let schema_descr_2cols = Arc::new(SchemaDescriptor::new(Arc::new(
            SchemaType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        SchemaType::primitive_type_builder("a", Type::INT32)
                            .build()
                            .unwrap(),
                    )
                    .into(),
                    Arc::new(
                        SchemaType::primitive_type_builder("b", Type::INT32)
                            .build()
                            .unwrap(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        )));

        let schema_descr_3cols = Arc::new(SchemaDescriptor::new(Arc::new(
            SchemaType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        SchemaType::primitive_type_builder("a", Type::INT32)
                            .build()
                            .unwrap(),
                    )
                    .into(),
                    Arc::new(
                        SchemaType::primitive_type_builder("b", Type::INT32)
                            .build()
                            .unwrap(),
                    )
                    .into(),
                    Arc::new(
                        SchemaType::primitive_type_builder("c", Type::INT32)
                            .build()
                            .unwrap(),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        )));

        let row_group_meta_2cols = RowGroupMetaData::builder(schema_descr_2cols.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(vec![
                ColumnChunkMetaData::builder(schema_descr_2cols.column(0).clone())
                    .build()
                    .unwrap(),
                ColumnChunkMetaData::builder(schema_descr_2cols.column(1).clone())
                    .build()
                    .unwrap(),
            ])
            .set_ordinal(1)
            .build()
            .unwrap();

        RowGroupMetaData::from_thrift(schema_descr_3cols, row_group_meta_2cols.to_thrift())
            .unwrap_err();
    }

    #[test]
    fn test_column_chunk_metadata_thrift_conversion() {
        let column_descr = get_test_schema_descr().column(0).clone();

        let col_metadata = ColumnChunkMetaData::builder(column_descr.clone())
            .set_encodings(vec![Encoding::PLAIN, Encoding::RLE])
            .set_file_path("file_path".to_owned())
            .set_file_offset(100)
            .set_num_values(1000)
            .set_compression(Compression::SNAPPY)
            .set_total_compressed_size(2000)
            .set_total_uncompressed_size(3000)
            .set_data_page_offset(4000)
            .set_dictionary_page_offset(Some(5000))
            .set_page_encoding_stats(vec![
                PageEncodingStats {
                    page_type: PageType::DATA_PAGE,
                    encoding: Encoding::PLAIN,
                    count: 3,
                },
                PageEncodingStats {
                    page_type: PageType::DATA_PAGE,
                    encoding: Encoding::RLE,
                    count: 5,
                },
            ])
            .set_bloom_filter_offset(Some(6000))
            .set_bloom_filter_length(Some(25))
            .set_offset_index_offset(Some(7000))
            .set_offset_index_length(Some(25))
            .set_column_index_offset(Some(8000))
            .set_column_index_length(Some(25))
            .build()
            .unwrap();

        let col_chunk_res =
            ColumnChunkMetaData::from_thrift(column_descr.clone(), col_metadata.to_thrift())
                .unwrap();

        assert_eq!(col_chunk_res, col_metadata);
    }

    #[test]
    fn test_column_chunk_metadata_thrift_conversion_empty() {
        let column_descr = get_test_schema_descr().column(0).clone();

        let col_metadata = ColumnChunkMetaData::builder(column_descr.clone())
            .build()
            .unwrap();

        let col_chunk_exp = col_metadata.to_thrift();
        let col_chunk_res =
            ColumnChunkMetaData::from_thrift(column_descr.clone(), col_chunk_exp.clone())
                .unwrap()
                .to_thrift();

        assert_eq!(col_chunk_res, col_chunk_exp);
    }

    #[test]
    fn test_compressed_size() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for column_descr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(column_descr.clone())
                .set_total_compressed_size(500)
                .set_total_uncompressed_size(700)
                .build()
                .unwrap();
            columns.push(column);
        }
        let row_group_meta = RowGroupMetaData::builder(schema_descr)
            .set_num_rows(1000)
            .set_column_metadata(columns)
            .build()
            .unwrap();

        let compressed_size_res: i64 = row_group_meta.compressed_size();
        let compressed_size_exp: i64 = 1000;

        assert_eq!(compressed_size_res, compressed_size_exp);
    }

    /// Returns sample schema descriptor so we can create column metadata.
    fn get_test_schema_descr() -> Arc<SchemaDescriptor> {
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    SchemaType::primitive_type_builder("a", Type::INT32)
                        .build()
                        .unwrap(),
                )
                .into(),
                Arc::new(
                    SchemaType::primitive_type_builder("b", Type::INT32)
                        .build()
                        .unwrap(),
                )
                .into(),
            ])
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }
}
