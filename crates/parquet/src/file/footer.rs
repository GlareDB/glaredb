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

use std::sync::Arc;

use super::PARQUET_MAGIC_ENC;
use crate::basic::ColumnOrder;
use crate::errors::{general_err, ParquetError, ParquetResult};
use crate::file::metadata::*;
use crate::file::{FOOTER_SIZE, PARQUET_MAGIC};
use crate::format::{ColumnOrder as TColumnOrder, FileMetaData as TFileMetaData};
use crate::schema::types::{self, SchemaDescriptor};
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};

/// Decodes [`ParquetMetaData`] from the provided bytes.
///
/// Typically this is used to decode the metadata from the end of a parquet
/// file. The format of `buf` is the Thift compact binary protocol, as specified
/// by the [Parquet Spec].
///
/// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
pub fn decode_metadata(buf: &[u8]) -> ParquetResult<ParquetMetaData> {
    // TODO: row group filtering
    let mut prot = TCompactSliceInputProtocol::new(buf);
    let t_file_metadata: TFileMetaData = TFileMetaData::read_from_in_protocol(&mut prot)
        .map_err(|e| ParquetError::General(format!("Could not parse metadata: {e}")))?;
    let schema = types::from_thrift(&t_file_metadata.schema)?;
    let schema_descr = Arc::new(SchemaDescriptor::new(schema));
    let mut row_groups = Vec::new();
    for rg in t_file_metadata.row_groups {
        row_groups.push(RowGroupMetaData::from_thrift(schema_descr.clone(), rg)?);
    }
    let column_orders = parse_column_orders(t_file_metadata.column_orders, &schema_descr);

    let file_metadata = FileMetaData::new(
        t_file_metadata.version,
        t_file_metadata.num_rows,
        t_file_metadata.created_by,
        t_file_metadata.key_value_metadata,
        schema_descr,
        column_orders,
    );
    Ok(ParquetMetaData::new(file_metadata, row_groups))
}

/// Decodes the Parquet footer returning the metadata length in bytes
///
/// A parquet footer is 8 bytes long and has the following layout:
/// * 4 bytes for the metadata length
/// * 4 bytes for the magic bytes 'PAR1'
///
/// ```text
/// +-----+--------+
/// | len | 'PAR1' |
/// +-----+--------+
/// ```
pub fn decode_footer(slice: &[u8; FOOTER_SIZE]) -> ParquetResult<usize> {
    // check this is indeed a parquet file
    if &slice[4..] == PARQUET_MAGIC_ENC {
        return Err(general_err!("Encrypted parquet files not yet supported"));
    }

    if &slice[4..] != PARQUET_MAGIC {
        return Err(general_err!("Invalid Parquet file. Corrupt footer"));
    }

    // get the metadata length from the footer
    let metadata_len = u32::from_le_bytes(slice[..4].try_into().unwrap());
    // u32 won't be larger than usize in most cases
    Ok(metadata_len as usize)
}

/// Parses column orders from Thrift definition.
/// If no column orders are defined, returns `None`.
fn parse_column_orders(
    t_column_orders: Option<Vec<TColumnOrder>>,
    schema_descr: &SchemaDescriptor,
) -> Option<Vec<ColumnOrder>> {
    match t_column_orders {
        Some(orders) => {
            // Should always be the case
            assert_eq!(
                orders.len(),
                schema_descr.num_columns(),
                "Column order length mismatch"
            );
            let mut res = Vec::new();
            for (i, column) in schema_descr.columns().iter().enumerate() {
                match orders[i] {
                    TColumnOrder::TYPEORDER(_) => {
                        let sort_order = ColumnOrder::get_sort_order(
                            column.logical_type(),
                            column.converted_type(),
                            column.physical_type(),
                        );
                        res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                    }
                }
            }
            Some(res)
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::basic::{SortOrder, Type};
    use crate::format::TypeDefinedOrder;
    use crate::schema::types::Type as SchemaType;

    #[test]
    fn test_metadata_column_orders_parse() {
        // Define simple schema, we do not need to provide logical types.
        let fields = vec![
            Arc::new(
                SchemaType::primitive_type_builder("col1", Type::INT32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                SchemaType::primitive_type_builder("col2", Type::FLOAT)
                    .build()
                    .unwrap(),
            ),
        ];
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();
        let schema_descr = SchemaDescriptor::new(Arc::new(schema));

        let t_column_orders = Some(vec![
            TColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
            TColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
        ]);

        assert_eq!(
            parse_column_orders(t_column_orders, &schema_descr),
            Some(vec![
                ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED),
                ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)
            ])
        );

        // Test when no column orders are defined.
        assert_eq!(parse_column_orders(None, &schema_descr), None);
    }

    #[test]
    #[should_panic(expected = "Column order length mismatch")]
    fn test_metadata_column_orders_len_mismatch() {
        let schema = SchemaType::group_type_builder("schema").build().unwrap();
        let schema_descr = SchemaDescriptor::new(Arc::new(schema));

        let t_column_orders = Some(vec![TColumnOrder::TYPEORDER(TypeDefinedOrder::new())]);

        parse_column_orders(t_column_orders, &schema_descr);
    }
}
