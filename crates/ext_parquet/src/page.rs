//! Sane page types.
use glaredb_error::{DbError, Result, not_implemented};

use crate::basic::{Encoding, Type};
use crate::format;
use crate::metadata::statistics::{self, Statistics};

#[derive(Debug, PartialEq)]
pub struct DataPageHeader {
    pub num_values: u32,
    pub encoding: Encoding,
    pub def_level_encoding: Encoding,
    pub rep_level_encoding: Encoding,
    pub statistics: Option<Statistics>,
}

#[derive(Debug, PartialEq)]
pub struct DataPageHeaderV2 {
    pub num_values: u32,
    pub encoding: Encoding,
    pub num_nulls: u32,
    pub num_rows: u32,
    pub def_levels_byte_len: u32,
    pub rep_levels_byte_len: u32,
    pub is_compressed: bool,
    pub statistics: Option<Statistics>,
}

#[derive(Debug, PartialEq)]
pub struct DictionaryPageHeader {
    pub num_values: u32,
    pub encoding: Encoding,
    pub is_sorted: bool,
}

#[derive(Debug, PartialEq)]
pub enum PageType {
    DataPage(DataPageHeader),
    DataPageV2(DataPageHeaderV2),
    Dictionary(DictionaryPageHeader),
}

#[derive(Debug, PartialEq)]
pub struct PageMetadata {
    pub uncompressed_page_size: i32,
    pub compressed_page_size: i32,
    pub crc: Option<i32>,
}

#[derive(Debug, PartialEq)]
pub struct PageHeader {
    pub metadata: PageMetadata,
    pub page_type: PageType,
}

impl PageHeader {
    pub fn try_from_format(header: format::PageHeader, physical_type: Type) -> Result<Self> {
        let page_type = match header.type_ {
            format::PageType::DICTIONARY_PAGE => {
                let dict_header = header
                    .dictionary_page_header
                    .as_ref()
                    .ok_or_else(|| DbError::new("Missing dictionary page header"))?;
                let is_sorted = dict_header.is_sorted.unwrap_or(false);
                PageType::Dictionary(DictionaryPageHeader {
                    num_values: dict_header.num_values.try_into()?,
                    encoding: Encoding::try_from(dict_header.encoding)?,
                    is_sorted,
                })
            }
            format::PageType::DATA_PAGE => {
                let header = header
                    .data_page_header
                    .ok_or_else(|| DbError::new("Missing V1 data page header"))?;
                PageType::DataPage(DataPageHeader {
                    num_values: header.num_values.try_into()?,
                    encoding: Encoding::try_from(header.encoding)?,
                    def_level_encoding: Encoding::try_from(header.definition_level_encoding)?,
                    rep_level_encoding: Encoding::try_from(header.repetition_level_encoding)?,
                    statistics: statistics::from_thrift(physical_type, header.statistics)?,
                })
            }
            format::PageType::DATA_PAGE_V2 => {
                let header = header
                    .data_page_header_v2
                    .ok_or_else(|| DbError::new("Missing V2 data page header"))?;
                // Missing 'is_compressed' should be interpreted as true
                // according to spec when using v2 pages.
                let is_compressed = header.is_compressed.unwrap_or(true);
                PageType::DataPageV2(DataPageHeaderV2 {
                    num_values: header.num_values.try_into()?,
                    encoding: Encoding::try_from(header.encoding)?,
                    num_nulls: header.num_nulls.try_into()?,
                    num_rows: header.num_rows.try_into()?,
                    def_levels_byte_len: header.definition_levels_byte_length.try_into()?,
                    rep_levels_byte_len: header.repetition_levels_byte_length.try_into()?,
                    is_compressed,
                    statistics: statistics::from_thrift(physical_type, header.statistics)?,
                })
            }
            _ => {
                not_implemented!("Decoding page header type: {:?}", header.type_);
            }
        };

        let metadata = PageMetadata {
            uncompressed_page_size: header.uncompressed_page_size,
            compressed_page_size: header.compressed_page_size,
            crc: header.crc,
        };

        Ok(PageHeader {
            metadata,
            page_type,
        })
    }
}
