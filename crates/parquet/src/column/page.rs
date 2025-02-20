// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains Parquet Page definitions.

use rayexec_execution::arrays::array::buffer_manager::BufferManager;

use crate::basic::{Encoding, PageType};
use crate::column_reader::ColumnData;
use crate::errors::{ParquetError, ParquetResult};
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::statistics::Statistics;
use crate::format::PageHeader;

/// Parquet Page definition.
///
/// List of supported pages.
/// These are 1-to-1 mapped from the equivalent Thrift definitions.
pub enum Page {
    DataPage {
        num_values: u32,
        encoding: Encoding,
        def_level_encoding: Encoding,
        rep_level_encoding: Encoding,
        statistics: Option<Statistics>,
    },
    DataPageV2 {
        num_values: u32,
        encoding: Encoding,
        num_nulls: u32,
        num_rows: u32,
        def_levels_byte_len: u32,
        rep_levels_byte_len: u32,
        is_compressed: bool,
        statistics: Option<Statistics>,
    },
    DictionaryPage {
        num_values: u32,
        encoding: Encoding,
        is_sorted: bool,
    },
}

impl Page {
    /// Returns [`PageType`] for this page.
    pub fn page_type(&self) -> PageType {
        match self {
            Page::DataPage { .. } => PageType::DATA_PAGE,
            Page::DataPageV2 { .. } => PageType::DATA_PAGE_V2,
            Page::DictionaryPage { .. } => PageType::DICTIONARY_PAGE,
        }
    }

    /// Returns number of values in this page.
    pub fn num_values(&self) -> u32 {
        match self {
            Page::DataPage { num_values, .. } => *num_values,
            Page::DataPageV2 { num_values, .. } => *num_values,
            Page::DictionaryPage { num_values, .. } => *num_values,
        }
    }

    /// Returns this page [`Encoding`].
    pub fn encoding(&self) -> Encoding {
        match self {
            Page::DataPage { encoding, .. } => *encoding,
            Page::DataPageV2 { encoding, .. } => *encoding,
            Page::DictionaryPage { encoding, .. } => *encoding,
        }
    }

    /// Returns optional [`Statistics`].
    pub fn statistics(&self) -> Option<&Statistics> {
        match self {
            Page::DataPage { ref statistics, .. } => statistics.as_ref(),
            Page::DataPageV2 { ref statistics, .. } => statistics.as_ref(),
            Page::DictionaryPage { .. } => None,
        }
    }
}

/// Helper struct to represent pages with potentially compressed buffer (data
/// page v1) or compressed and concatenated buffer (def levels + rep levels +
/// compressed values for data page v2).
///
/// The difference with `Page` is that `Page` buffer is always uncompressed.
pub struct CompressedPage {
    compressed_page: Page,
    uncompressed_size: usize,
}

impl CompressedPage {
    /// Creates `CompressedPage` from a page with potentially compressed buffer and
    /// uncompressed size.
    pub fn new(compressed_page: Page, uncompressed_size: usize) -> Self {
        Self {
            compressed_page,
            uncompressed_size,
        }
    }

    /// Returns page type.
    pub fn page_type(&self) -> PageType {
        self.compressed_page.page_type()
    }

    /// Returns underlying page with potentially compressed buffer.
    pub fn compressed_page(&self) -> &Page {
        &self.compressed_page
    }

    /// Returns uncompressed size in bytes.
    pub fn uncompressed_size(&self) -> usize {
        self.uncompressed_size
    }

    /// Number of values in page.
    pub fn num_values(&self) -> u32 {
        self.compressed_page.num_values()
    }

    /// Returns encoding for values in page.
    pub fn encoding(&self) -> Encoding {
        self.compressed_page.encoding()
    }
}

/// Contains page write metrics.
pub struct PageWriteSpec {
    pub page_type: PageType,
    pub uncompressed_size: usize,
    pub compressed_size: usize,
    pub num_values: u32,
    pub offset: u64,
    pub bytes_written: u64,
}

/// Contains metadata for a page
#[derive(Debug, Clone)]
pub struct PageMetadata {
    /// The number of rows within the page if known
    pub num_rows: Option<usize>,
    /// The number of levels within the page if known
    pub num_levels: Option<usize>,
    /// Returns true if the page is a dictionary page
    pub is_dict: bool,
}

impl TryFrom<&PageHeader> for PageMetadata {
    type Error = ParquetError;

    fn try_from(value: &PageHeader) -> std::result::Result<Self, Self::Error> {
        match value.type_ {
            crate::format::PageType::DATA_PAGE => {
                let header = value.data_page_header.as_ref().unwrap();
                Ok(PageMetadata {
                    num_rows: None,
                    num_levels: Some(header.num_values as _),
                    is_dict: false,
                })
            }
            crate::format::PageType::DICTIONARY_PAGE => Ok(PageMetadata {
                num_rows: None,
                num_levels: None,
                is_dict: true,
            }),
            crate::format::PageType::DATA_PAGE_V2 => {
                let header = value.data_page_header_v2.as_ref().unwrap();
                Ok(PageMetadata {
                    num_rows: Some(header.num_rows as _),
                    num_levels: Some(header.num_values as _),
                    is_dict: false,
                })
            }
            other => Err(ParquetError::General(format!(
                "page type {other:?} cannot be converted to PageMetadata"
            ))),
        }
    }
}

/// API for reading pages from a column chunk.
/// This offers a iterator like API to get the next page.
pub trait PageReader: Send {
    /// Read the next page in the column chunk.
    ///
    /// The column data provided contains the chunk to read the page from, and a
    /// buffer for writing the decompressed page to.
    ///
    /// Returns `None` if there are no pages left.
    fn read_next_page<B>(&mut self, column_data: &mut ColumnData<B>) -> ParquetResult<Option<Page>>
    where
        B: BufferManager;

    /// Gets metadata about the next page, returns an error if no column index
    /// information
    fn peek_next_page<B>(
        &mut self,
        column_data: &ColumnData<B>,
    ) -> ParquetResult<Option<PageMetadata>>
    where
        B: BufferManager;

    /// Skips reading the next page, returns an error if no column index
    /// information
    fn skip_next_page<B>(&mut self, column_data: &ColumnData<B>) -> ParquetResult<()>
    where
        B: BufferManager;

    /// Returns `true` if the next page can be assumed to contain the start of a
    /// new record
    ///
    /// Prior to parquet V2 the specification was ambiguous as to whether a
    /// single record could be split across multiple pages, and prior to
    /// [(#4327)] the Rust writer would do this in certain situations. However,
    /// correctly interpreting the offset index relies on this assumption
    /// holding [(#4943)], and so this mechanism is provided for a
    /// [`PageReader`] to signal this to the calling context
    ///
    /// [(#4327)]: https://github.com/apache/arrow-rs/pull/4327
    /// [(#4943)]: https://github.com/apache/arrow-rs/pull/4943
    fn at_record_boundary<B>(&mut self, column_data: &ColumnData<B>) -> ParquetResult<bool>
    where
        B: BufferManager,
    {
        Ok(self.peek_next_page(column_data)?.is_none())
    }
}

/// API for writing pages in a column chunk.
///
/// It is reasonable to assume that all pages will be written in the correct order, e.g.
/// dictionary page followed by data pages, or a set of data pages, etc.
pub trait PageWriter: Send {
    /// Writes a page into the output stream/sink.
    /// Returns `PageWriteSpec` that contains information about written page metrics,
    /// including number of bytes, size, number of values, offset, etc.
    ///
    /// This method is called for every compressed page we write into underlying buffer,
    /// either data page or dictionary page.
    fn write_page(&mut self, page: CompressedPage) -> ParquetResult<PageWriteSpec>;

    /// Writes column chunk metadata into the output stream/sink.
    ///
    /// This method is called once before page writer is closed, normally when writes are
    /// finalised in column writer.
    fn write_metadata(&mut self, metadata: &ColumnChunkMetaData) -> ParquetResult<()>;

    /// Closes resources and flushes underlying sink.
    /// Page writer should not be used after this method is called.
    fn close(&mut self) -> ParquetResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page() {
        let data_page = Page::DataPage {
            num_values: 10,
            encoding: Encoding::PLAIN,
            def_level_encoding: Encoding::RLE,
            rep_level_encoding: Encoding::RLE,
            statistics: Some(Statistics::int32(Some(1), Some(2), None, 1, true)),
        };
        assert_eq!(data_page.page_type(), PageType::DATA_PAGE);
        assert_eq!(data_page.num_values(), 10);
        assert_eq!(data_page.encoding(), Encoding::PLAIN);
        assert_eq!(
            data_page.statistics(),
            Some(&Statistics::int32(Some(1), Some(2), None, 1, true))
        );

        let data_page_v2 = Page::DataPageV2 {
            num_values: 10,
            encoding: Encoding::PLAIN,
            num_nulls: 5,
            num_rows: 20,
            def_levels_byte_len: 30,
            rep_levels_byte_len: 40,
            is_compressed: false,
            statistics: Some(Statistics::int32(Some(1), Some(2), None, 1, true)),
        };
        assert_eq!(data_page_v2.page_type(), PageType::DATA_PAGE_V2);
        assert_eq!(data_page_v2.num_values(), 10);
        assert_eq!(data_page_v2.encoding(), Encoding::PLAIN);
        assert_eq!(
            data_page_v2.statistics(),
            Some(&Statistics::int32(Some(1), Some(2), None, 1, true))
        );

        let dict_page = Page::DictionaryPage {
            num_values: 10,
            encoding: Encoding::PLAIN,
            is_sorted: false,
        };
        assert_eq!(dict_page.page_type(), PageType::DICTIONARY_PAGE);
        assert_eq!(dict_page.num_values(), 10);
        assert_eq!(dict_page.encoding(), Encoding::PLAIN);
        assert_eq!(dict_page.statistics(), None);
    }

    #[test]
    fn test_compressed_page() {
        let data_page = Page::DataPage {
            num_values: 10,
            encoding: Encoding::PLAIN,
            def_level_encoding: Encoding::RLE,
            rep_level_encoding: Encoding::RLE,
            statistics: Some(Statistics::int32(Some(1), Some(2), None, 1, true)),
        };

        let cpage = CompressedPage::new(data_page, 5);

        assert_eq!(cpage.page_type(), PageType::DATA_PAGE);
        assert_eq!(cpage.uncompressed_size(), 5);
        assert_eq!(cpage.num_values(), 10);
        assert_eq!(cpage.encoding(), Encoding::PLAIN);
    }
}
