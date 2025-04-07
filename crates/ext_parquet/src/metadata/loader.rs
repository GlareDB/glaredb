use std::io;
use std::sync::Arc;

use glaredb_core::runtime::filesystem::AnyFile;
use glaredb_error::{DbError, Result, ResultExt};

use super::{FileMetaData, ParquetMetaData, RowGroupMetaData};
use crate::metadata::{FOOTER_SIZE, MIN_FILE_SIZE, PARQUET_MAGIC, PARQUET_MAGIC_ENC};
use crate::schema::types::{self, SchemaDescriptor};
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};
use crate::{basic, format};

#[derive(Debug)]
pub struct MetaDataLoader {
    // TODO: Caching and stuff.
}

impl MetaDataLoader {
    pub fn new() -> Self {
        MetaDataLoader {}
    }

    /// Loads parquet metadata from a file.
    pub async fn load_from_file(file: &mut AnyFile) -> Result<ParquetMetaData> {
        let size = file.call_size();
        if size < MIN_FILE_SIZE {
            return Err(DbError::new(format!(
                "'{}' it not a valid parquet file, too small",
                file.call_path()
            )));
        }

        let mut read_buf = vec![0; FOOTER_SIZE];
        // Read the footer.
        file.call_seek(io::SeekFrom::End(FOOTER_SIZE as i64))
            .await?;
        file.call_read_exact(&mut read_buf).await?;

        if &read_buf[0..4] == PARQUET_MAGIC_ENC {
            return Err(DbError::new("Encrypted parquet files not yet supported"));
        }
        if &read_buf[4..] != PARQUET_MAGIC {
            return Err(DbError::new("Invalid Parquet file. Corrupt footer"));
        }

        let metadata_len = u32::from_le_bytes(read_buf[..4].try_into().unwrap());
        let metadata_pos_from_end = (metadata_len as i64) + (FOOTER_SIZE as i64);

        // Read the metadata.
        read_buf.resize(metadata_len as usize, 0);
        file.call_seek(io::SeekFrom::End(metadata_pos_from_end))
            .await?;
        file.call_read_exact(&mut read_buf).await?;

        let metadata = decode_metadata(&read_buf)?;

        Ok(metadata)
    }
}

/// Decodes [`ParquetMetaData`] from the provided bytes.
///
/// Typically this is used to decode the metadata from the end of a parquet
/// file. The format of `buf` is the Thift compact binary protocol, as specified
/// by the [Parquet Spec].
///
/// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
pub fn decode_metadata(buf: &[u8]) -> Result<ParquetMetaData> {
    // TODO: row group filtering
    let mut prot = TCompactSliceInputProtocol::new(buf);
    let t_file_metadata: format::FileMetaData =
        format::FileMetaData::read_from_in_protocol(&mut prot)
            .context("Could not parse metadata")?;
    let schema = types::from_thrift(&t_file_metadata.schema)?;
    let schema_descr = Arc::new(SchemaDescriptor::new(schema));
    let mut row_groups = Vec::new();
    for rg in t_file_metadata.row_groups {
        row_groups.push(RowGroupMetaData::from_thrift(schema_descr.clone(), rg)?);
    }
    let column_orders = parse_column_orders(t_file_metadata.column_orders, &schema_descr)?;

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

/// Parses column orders from Thrift definition. If no column orders are
/// defined, returns `None`.
fn parse_column_orders(
    t_column_orders: Option<Vec<format::ColumnOrder>>,
    schema_descr: &SchemaDescriptor,
) -> Result<Option<Vec<basic::ColumnOrder>>> {
    match t_column_orders {
        Some(orders) => {
            if orders.len() != schema_descr.num_columns() {
                return Err(DbError::new("Column order length mismatch")
                    .with_field("expect", orders.len())
                    .with_field("got", schema_descr.num_columns()));
            }

            let mut res = Vec::new();
            for (i, column) in schema_descr.columns().iter().enumerate() {
                match orders[i] {
                    format::ColumnOrder::TYPEORDER(_) => {
                        let sort_order = basic::ColumnOrder::get_sort_order(
                            column.logical_type(),
                            column.converted_type(),
                            column.physical_type(),
                        );
                        res.push(basic::ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                    }
                }
            }
            Ok(Some(res))
        }
        None => Ok(None),
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
            format::ColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
            format::ColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
        ]);

        assert_eq!(
            parse_column_orders(t_column_orders, &schema_descr).unwrap(),
            Some(vec![
                basic::ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED),
                basic::ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)
            ])
        );

        // Test when no column orders are defined.
        assert_eq!(parse_column_orders(None, &schema_descr).unwrap(), None);
    }

    #[test]
    fn test_metadata_column_orders_len_mismatch() {
        let schema = SchemaType::group_type_builder("schema").build().unwrap();
        let schema_descr = SchemaDescriptor::new(Arc::new(schema));
        let t_column_orders = Some(vec![
            format::ColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
        ]);
        parse_column_orders(t_column_orders, &schema_descr).unwrap_err();
    }
}
