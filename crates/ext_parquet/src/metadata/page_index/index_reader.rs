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

#![allow(unused)]

//! Support for reading [`Index`] and [`PageLocation`] from parquet metadata.

use std::ops::Range;

use glaredb_error::{Result, ResultExt};

use crate::basic::Type;
use crate::data_type::Int96;
use crate::format::{ColumnIndex, OffsetIndex, PageLocation};
use crate::metadata::page_index::index::{Index, NativeIndex};
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};

/// Computes the covering range of two optional ranges
///
/// For example `acc_range(Some(7..9), Some(1..3)) = Some(1..9)`
pub(crate) fn acc_range(a: Option<Range<usize>>, b: Option<Range<usize>>) -> Option<Range<usize>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.start.min(b.start)..a.end.max(b.end)),
        (None, x) | (x, None) => x,
    }
}

pub(crate) fn decode_offset_index(data: &[u8]) -> Result<Vec<PageLocation>> {
    let mut prot = TCompactSliceInputProtocol::new(data);
    let offset =
        OffsetIndex::read_from_in_protocol(&mut prot).context("failed read offset index")?;
    Ok(offset.page_locations)
}

pub(crate) fn decode_column_index(data: &[u8], column_type: Type) -> Result<Index> {
    let mut prot = TCompactSliceInputProtocol::new(data);

    let index =
        ColumnIndex::read_from_in_protocol(&mut prot).context("failed to read column index")?;

    let index = match column_type {
        Type::BOOLEAN => Index::BOOLEAN(NativeIndex::<bool>::try_new(index)?),
        Type::INT32 => Index::INT32(NativeIndex::<i32>::try_new(index)?),
        Type::INT64 => Index::INT64(NativeIndex::<i64>::try_new(index)?),
        Type::INT96 => Index::INT96(NativeIndex::<Int96>::try_new(index)?),
        Type::FLOAT => Index::FLOAT(NativeIndex::<f32>::try_new(index)?),
        Type::DOUBLE => Index::DOUBLE(NativeIndex::<f64>::try_new(index)?),
        Type::BYTE_ARRAY => Index::BYTE_ARRAY(NativeIndex::try_new(index)?),
        Type::FIXED_LEN_BYTE_ARRAY => Index::FIXED_LEN_BYTE_ARRAY(NativeIndex::try_new(index)?),
    };

    Ok(index)
}
