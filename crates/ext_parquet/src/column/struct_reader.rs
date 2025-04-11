use glaredb_core::arrays::datatype::{DataType, TimeUnit};
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::buffer::buffer_manager::AsRawBufferManager;
use glaredb_core::storage::projections::Projections;
use glaredb_error::{Result, not_implemented};

use super::column_reader::{ColumnReader, ValueColumnReader};
use super::value_reader::int96::Int96TsReader;
use super::value_reader::primitive::{
    PlainFloat32ValueReader,
    PlainFloat64ValueReader,
    PlainInt32ValueReader,
    PlainInt64ValueReader,
    PlainTsNsValueReader,
};
use super::value_reader::varlen::VarlenByteValueReader;
use crate::basic;
use crate::schema::types::{ColumnDescriptor, SchemaDescriptor};

#[derive(Debug)]
pub struct StructReader {
    pub(crate) readers: Vec<Box<dyn ColumnReader>>,
}

impl StructReader {
    /// Create a new reader for the root of the parquet data.
    pub fn try_new_root(
        manager: &impl AsRawBufferManager,
        projections: &Projections,
        column_schema: &ColumnSchema,
        parquet_schema: &SchemaDescriptor,
    ) -> Result<Self> {
        let readers = projections
            .data_indices()
            .iter()
            .map(|&col_idx| {
                // TODO: I'll fix this later, we're just assuming a flat schema
                // right now.
                let col_descr = parquet_schema.leaves[col_idx].clone();
                let datatype = column_schema.fields[col_idx].datatype.clone();
                new_column_reader(manager, datatype, col_descr)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(StructReader { readers })
    }
}

/// Create a new boxed column reader.
pub(crate) fn new_column_reader(
    manager: &impl AsRawBufferManager,
    datatype: DataType,
    descr: ColumnDescriptor,
) -> Result<Box<dyn ColumnReader>> {
    Ok(match &datatype {
        DataType::Int32 => Box::new(ValueColumnReader::<PlainInt32ValueReader>::try_new(
            manager, datatype, descr,
        )?),
        DataType::Int64 => Box::new(ValueColumnReader::<PlainInt64ValueReader>::try_new(
            manager, datatype, descr,
        )?),
        DataType::Float32 => Box::new(ValueColumnReader::<PlainFloat32ValueReader>::try_new(
            manager, datatype, descr,
        )?),
        DataType::Float64 => Box::new(ValueColumnReader::<PlainFloat64ValueReader>::try_new(
            manager, datatype, descr,
        )?),
        DataType::Timestamp(m) => match (m.unit, descr.physical_type()) {
            (TimeUnit::Nanosecond, basic::Type::INT64) => {
                Box::new(ValueColumnReader::<PlainTsNsValueReader>::try_new(
                    manager, datatype, descr,
                )?)
            }
            (TimeUnit::Nanosecond, basic::Type::INT96) => {
                Box::new(ValueColumnReader::<Int96TsReader>::try_new(
                    manager, datatype, descr,
                )?)
            }
            other => not_implemented!("timestamp reader for data type: {other:?}"),
        },
        DataType::Utf8 => Box::new(ValueColumnReader::<VarlenByteValueReader>::try_new(
            manager, datatype, descr,
        )?),
        other => not_implemented!("reader for data type: {other}"),
    })
}
