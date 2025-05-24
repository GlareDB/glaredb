use glaredb_core::arrays::datatype::{DataType, DataTypeId, TimeUnit};
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::arrays::scalar::unwrap::{
    UnwrapI8,
    UnwrapI16,
    UnwrapI32,
    UnwrapI64,
    UnwrapU8,
    UnwrapU16,
    UnwrapU32,
    UnwrapU64,
};
use glaredb_core::buffer::buffer_manager::AsRawBufferManager;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::{Result, not_implemented};

use super::column_reader::{ColumnReader, ValueColumnReader};
use super::row_group_pruner::{NopRowGroupPruner, PrimitiveRowGroupPruner};
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
    PlainTsMicrosValueReader,
    PlainTsNsValueReader,
};
use super::value_reader::varlen::{BinaryValueReader, Utf8ValueReader};
use crate::basic;
use crate::metadata::RowGroupMetaData;
use crate::schema::types::{ColumnDescriptor, SchemaDescriptor};

#[derive(Debug)]
pub struct StructReader {
    pub(crate) readers: Vec<Box<dyn ColumnReader>>,
}

impl StructReader {
    /// Create a new reader for the root of the parquet data.
    ///
    /// The struct reader will only look at the data indices in the projection.
    /// Metadata projections need to be handled at a higher level.
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

    pub fn prepare_scan_unit(
        &mut self,
        projections: &Projections,
        parquet_schema: &SchemaDescriptor,
    ) -> Result<()> {
        debug_assert_eq!(projections.data_indices().len(), self.readers.len());

        for (idx, reader) in self.readers.iter_mut().enumerate() {
            let col_idx = projections.data_indices()[idx];
            let col_descr = parquet_schema.leaves[col_idx].clone();
            reader.prepare_scan_unit(col_descr)?;
        }

        Ok(())
    }

    /// Checks to see if we can prune this row group by looking at the stats for
    /// each column.
    ///
    /// This ANDs all column filters, meaning we'll prune a row group if _any_
    /// column says we should prune it, and the predicate will never be
    /// satisfied.
    pub fn should_prune(
        &self,
        projections: &Projections,
        row_group: &RowGroupMetaData,
    ) -> Result<bool> {
        debug_assert_eq!(projections.data_indices().len(), self.readers.len());

        for (&proj, col_reader) in projections.data_indices().iter().zip(&self.readers) {
            let col_meta = &row_group.columns[proj];
            if let Some(stats) = &col_meta.statistics {
                if col_reader.should_prune(stats)? {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

/// Create a new boxed column reader.
pub(crate) fn new_column_reader<'a>(
    manager: &impl AsRawBufferManager,
    datatype: DataType,
    descr: ColumnDescriptor,
    filters: impl Iterator<Item = &'a PhysicalScanFilter>,
) -> Result<Box<dyn ColumnReader>> {
    Ok(match datatype.id() {
        DataTypeId::Boolean => Box::new(ValueColumnReader::<BoolValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataTypeId::Int8 => Box::new(ValueColumnReader::<CastingInt32ToInt8Reader, _>::try_new(
            manager,
            datatype,
            descr,
            PrimitiveRowGroupPruner::<_, UnwrapI8>::new(filters),
        )?),
        DataTypeId::Int16 => Box::new(ValueColumnReader::<CastingInt32ToInt16Reader, _>::try_new(
            manager,
            datatype,
            descr,
            PrimitiveRowGroupPruner::<_, UnwrapI16>::new(filters),
        )?),
        DataTypeId::Int32 => Box::new(ValueColumnReader::<PlainInt32ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            PrimitiveRowGroupPruner::<_, UnwrapI32>::new(filters),
        )?),
        DataTypeId::Int64 => Box::new(ValueColumnReader::<PlainInt64ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            PrimitiveRowGroupPruner::<_, UnwrapI64>::new(filters),
        )?),
        DataTypeId::UInt8 => Box::new(ValueColumnReader::<CastingInt32ToUInt8Reader, _>::try_new(
            manager,
            datatype,
            descr,
            PrimitiveRowGroupPruner::<_, UnwrapU8>::new(filters),
        )?),
        DataTypeId::UInt16 => {
            Box::new(ValueColumnReader::<CastingInt32ToUInt16Reader, _>::try_new(
                manager,
                datatype,
                descr,
                PrimitiveRowGroupPruner::<_, UnwrapU16>::new(filters),
            )?)
        }
        DataTypeId::UInt32 => {
            Box::new(ValueColumnReader::<CastingInt32ToUInt32Reader, _>::try_new(
                manager,
                datatype,
                descr,
                PrimitiveRowGroupPruner::<_, UnwrapU32>::new(filters),
            )?)
        }
        DataTypeId::UInt64 => {
            Box::new(ValueColumnReader::<CastingInt64ToUInt64Reader, _>::try_new(
                manager,
                datatype,
                descr,
                PrimitiveRowGroupPruner::<_, UnwrapU64>::new(filters),
            )?)
        }
        DataTypeId::Float16 => Box::new(ValueColumnReader::<PlainFloat16ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataTypeId::Float32 => Box::new(ValueColumnReader::<PlainFloat32ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataTypeId::Float64 => Box::new(ValueColumnReader::<PlainFloat64ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataTypeId::Date32 => Box::new(ValueColumnReader::<PlainInt32ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataTypeId::Decimal64 => match descr.physical_type() {
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
        DataTypeId::Timestamp => {
            let m = datatype.try_get_timestamp_type_meta()?;
            match (m.unit, descr.physical_type()) {
                (TimeUnit::Nanosecond, basic::Type::INT64) => {
                    Box::new(ValueColumnReader::<PlainTsNsValueReader, _>::try_new(
                        manager,
                        datatype,
                        descr,
                        NopRowGroupPruner::default(),
                    )?)
                }
                (TimeUnit::Microsecond, basic::Type::INT64) => {
                    Box::new(ValueColumnReader::<PlainTsMicrosValueReader, _>::try_new(
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
            }
        }
        DataTypeId::Utf8 => Box::new(ValueColumnReader::<Utf8ValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        DataTypeId::Binary => Box::new(ValueColumnReader::<BinaryValueReader, _>::try_new(
            manager,
            datatype,
            descr,
            NopRowGroupPruner::default(),
        )?),
        other => not_implemented!("create parquet column reader for data type: {other}"),
    })
}
