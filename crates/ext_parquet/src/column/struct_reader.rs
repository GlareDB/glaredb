use glaredb_core::arrays::datatype::{DataType, TimeUnit};
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::buffer::buffer_manager::AsRawBufferManager;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::{Result, not_implemented};

use super::column_reader::{ColumnReader, ValueColumnReader};
use super::row_group_pruner::NopRowGroupPruner;
use super::value_reader::bool::BoolValueReader;
use super::value_reader::int96::Int96TsReader;
use super::value_reader::primitive::{
    CastingInt32ToInt8Reader,
    CastingInt32ToInt16Reader,
    CastingInt32ToInt64Reader,
    CastingInt32ToUInt8Reader,
    CastingInt32ToUInt16Reader,
    CastingInt32ToUInt32Reader,
    CastingInt64ToUInt64Reader,
    PlainFloat16ValueReader,
    PlainFloat32ValueReader,
    PlainFloat64ValueReader,
    PlainInt32ValueReader,
    PlainInt64ValueReader,
    PlainTsNsValueReader,
};
use super::value_reader::varlen::{BinaryValueReader, Utf8ValueReader};
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
        filters: &[PhysicalScanFilter],
    ) -> Result<Self> {
        let readers = projections
            .data_indices()
            .iter()
            .map(|&col_idx| {
                // Get the filters to apply to just this column.
                let filters = filters.iter().filter(|filter| {
                    filter.columns.len() == 1
                        && filter.columns.contains(&ProjectedColumn::Data(col_idx))
                });

                // TODO: I'll fix this later, we're just assuming a flat schema
                // right now.
                let col_descr = parquet_schema.leaves[col_idx].clone();
                let datatype = column_schema.fields[col_idx].datatype.clone();
                new_column_reader(manager, datatype, col_descr, filters)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(StructReader { readers })
    }
}

/// Create a new boxed column reader.
pub(crate) fn new_column_reader<'a>(
    manager: &impl AsRawBufferManager,
    datatype: DataType,
    descr: ColumnDescriptor,
    filters: impl Iterator<Item = &'a PhysicalScanFilter>,
) -> Result<Box<dyn ColumnReader>> {
    Ok(match &datatype {
        DataType::Boolean => Box::new(ValueColumnReader::<BoolValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Int8 => Box::new(ValueColumnReader::<CastingInt32ToInt8Reader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Int16 => Box::new(ValueColumnReader::<CastingInt32ToInt16Reader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Int32 => Box::new(ValueColumnReader::<PlainInt32ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Int64 => Box::new(ValueColumnReader::<PlainInt64ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::UInt8 => Box::new(ValueColumnReader::<CastingInt32ToUInt8Reader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::UInt16 => Box::new(ValueColumnReader::<CastingInt32ToUInt16Reader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::UInt32 => Box::new(ValueColumnReader::<CastingInt32ToUInt32Reader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::UInt64 => Box::new(ValueColumnReader::<CastingInt64ToUInt64Reader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Float16 => Box::new(ValueColumnReader::<PlainFloat16ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Float32 => Box::new(ValueColumnReader::<PlainFloat32ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Float64 => Box::new(ValueColumnReader::<PlainFloat64ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Date32 => Box::new(ValueColumnReader::<PlainInt32ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Decimal64(_) => match descr.physical_type() {
            basic::Type::INT32 => {
                Box::new(ValueColumnReader::<CastingInt32ToInt64Reader, _>::try_new(
                    manager,
                    datatype,
                    descr,
                    NopRowGroupPruner::default(),
                )?)
            }
            basic::Type::INT64 => Box::new(ValueColumnReader::<PlainInt64ValueReader, _>::try_new(
                manager,
                datatype,
                descr,
                NopRowGroupPruner::default(),
            )?),
            other => not_implemented!("decimal64 reader for physical type: {other:?}"),
        },
        DataType::Timestamp(m) => match (m.unit, descr.physical_type()) {
            (TimeUnit::Nanosecond, basic::Type::INT64) => {
                Box::new(ValueColumnReader::<PlainTsNsValueReader, _>::try_new(
                    manager,
                    datatype,
                    descr,
                    NopRowGroupPruner::default(),
                )?)
            }
            (TimeUnit::Nanosecond, basic::Type::INT96) => {
                Box::new(ValueColumnReader::<Int96TsReader, _>::try_new(
                    manager,
                    datatype,
                    descr,
                    NopRowGroupPruner::default(),
                )?)
            }
            other => not_implemented!("timestamp reader for physical type: {other:?}"),
        },
        DataType::Utf8 => Box::new(ValueColumnReader::<Utf8ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataType::Binary => Box::new(ValueColumnReader::<BinaryValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        other => not_implemented!("create parquet column reader for data type: {other}"),
    })
}
