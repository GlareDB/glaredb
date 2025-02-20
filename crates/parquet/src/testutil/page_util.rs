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

use std::collections::VecDeque;
use std::iter::Peekable;
use std::mem;

use bytes::Bytes;
use rayexec_execution::arrays::array::buffer_manager::BufferManager;

use crate::basic::Encoding;
use crate::column::page::{Page, PageMetadata, PageReader};
use crate::column_reader::ColumnData;
use crate::data_type::DataType;
use crate::encodings::encoding::{get_encoder, Encoder};
use crate::encodings::levels::LevelEncoder;
use crate::errors::ParquetResult;
use crate::schema::types::ColumnDescPtr;

pub trait DataPageBuilder {
    fn add_rep_levels(&mut self, max_level: i16, rep_levels: &[i16]);
    fn add_def_levels(&mut self, max_level: i16, def_levels: &[i16]);
    fn add_values<T: DataType>(&mut self, encoding: Encoding, values: &[T::T]);
    fn add_indices(&mut self, indices: Bytes);
    fn consume(self) -> Page;
}

/// A utility struct for building data pages (v1 or v2). Callers must call:
///   - add_rep_levels()
///   - add_def_levels()
///   - add_values() for normal data page / add_indices() for dictionary data page
///   - consume()
/// in order to populate and obtain a data page.
pub struct DataPageBuilderImpl {
    encoding: Option<Encoding>,
    num_values: u32,
    buffer: Vec<u8>,
    rep_levels_byte_len: u32,
    def_levels_byte_len: u32,
    datapage_v2: bool,
}

impl DataPageBuilderImpl {
    // `num_values` is the number of non-null values to put in the data page.
    // `datapage_v2` flag is used to indicate if the generated data page should use V2
    // format or not.
    pub fn new(_desc: ColumnDescPtr, num_values: u32, datapage_v2: bool) -> Self {
        DataPageBuilderImpl {
            encoding: None,
            num_values,
            buffer: vec![],
            rep_levels_byte_len: 0,
            def_levels_byte_len: 0,
            datapage_v2,
        }
    }

    // Adds levels to the buffer and return number of encoded bytes
    fn add_levels(&mut self, max_level: i16, levels: &[i16]) -> u32 {
        if max_level <= 0 {
            return 0;
        }
        let mut level_encoder = LevelEncoder::v1(Encoding::RLE, max_level, levels.len());
        level_encoder.put(levels);
        let encoded_levels = level_encoder.consume();
        // Actual encoded bytes (without length offset)
        let encoded_bytes = &encoded_levels[mem::size_of::<i32>()..];
        if self.datapage_v2 {
            // Level encoder always initializes with offset of i32, where it stores
            // length of encoded data; for data page v2 we explicitly
            // store length, therefore we should skip i32 bytes.
            self.buffer.extend_from_slice(encoded_bytes);
        } else {
            self.buffer.extend_from_slice(encoded_levels.as_slice());
        }
        encoded_bytes.len() as u32
    }
}

impl DataPageBuilder for DataPageBuilderImpl {
    fn add_rep_levels(&mut self, max_levels: i16, rep_levels: &[i16]) {
        self.num_values = rep_levels.len() as u32;
        self.rep_levels_byte_len = self.add_levels(max_levels, rep_levels);
    }

    fn add_def_levels(&mut self, max_levels: i16, def_levels: &[i16]) {
        self.num_values = def_levels.len() as u32;
        self.def_levels_byte_len = self.add_levels(max_levels, def_levels);
    }

    fn add_values<T: DataType>(&mut self, encoding: Encoding, values: &[T::T]) {
        assert!(
            self.num_values >= values.len() as u32,
            "num_values: {}, values.len(): {}",
            self.num_values,
            values.len()
        );
        self.encoding = Some(encoding);
        let mut encoder: Box<dyn Encoder<T>> =
            get_encoder::<T>(encoding).expect("get_encoder() should be OK");
        encoder.put(values).expect("put() should be OK");
        let encoded_values = encoder
            .flush_buffer()
            .expect("consume_buffer() should be OK");
        self.buffer.extend_from_slice(&encoded_values);
    }

    fn add_indices(&mut self, indices: Bytes) {
        self.encoding = Some(Encoding::RLE_DICTIONARY);
        self.buffer.extend_from_slice(&indices);
    }

    fn consume(self) -> Page {
        if self.datapage_v2 {
            Page::DataPageV2 {
                num_values: self.num_values,
                encoding: self.encoding.unwrap(),
                num_nulls: 0, /* set to dummy value - don't need this when reading
                               * data page */
                num_rows: self.num_values, /* num_rows only needs in skip_records, now we not support skip REPEATED field,
                                            * so we can assume num_values == num_rows */
                def_levels_byte_len: self.def_levels_byte_len,
                rep_levels_byte_len: self.rep_levels_byte_len,
                is_compressed: false,
                statistics: None, // set to None, we do not need statistics for tests
            }
        } else {
            Page::DataPage {
                num_values: self.num_values,
                encoding: self.encoding.unwrap(),
                def_level_encoding: Encoding::RLE,
                rep_level_encoding: Encoding::RLE,
                statistics: None, // set to None, we do not need statistics for tests
            }
        }
    }
}

// TODO: Remove
/// A utility page reader which stores pages in memory.
pub struct InMemoryPageReader {
    pages: VecDeque<Page>,
}

impl InMemoryPageReader {
    pub fn new(pages: impl IntoIterator<Item = Page>) -> Self {
        Self {
            pages: pages.into_iter().collect(),
        }
    }
}

impl PageReader for InMemoryPageReader {
    // TODO
    fn read_next_page<B>(&mut self, _column_data: &mut ColumnData<B>) -> ParquetResult<Option<Page>>
    where
        B: BufferManager,
    {
        Ok(self.pages.pop_front())
    }

    fn peek_next_page<B>(
        &mut self,
        column_data: &ColumnData<B>,
    ) -> ParquetResult<Option<PageMetadata>>
    where
        B: BufferManager,
    {
        if let Some(x) = self.pages.front() {
            match x {
                Page::DataPage { num_values, .. } => Ok(Some(PageMetadata {
                    num_rows: None,
                    num_levels: Some(*num_values as _),
                    is_dict: false,
                })),
                Page::DataPageV2 {
                    num_rows,
                    num_values,
                    ..
                } => Ok(Some(PageMetadata {
                    num_rows: Some(*num_rows as _),
                    num_levels: Some(*num_values as _),
                    is_dict: false,
                })),
                Page::DictionaryPage { .. } => Ok(Some(PageMetadata {
                    num_rows: None,
                    num_levels: None,
                    is_dict: true,
                })),
            }
        } else {
            Ok(None)
        }
    }

    fn skip_next_page<B>(&mut self, column_data: &ColumnData<B>) -> ParquetResult<()>
    where
        B: BufferManager,
    {
        let _ = self.pages.pop_front();
        Ok(())
    }
}
