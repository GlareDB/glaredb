use glaredb_error::Result;
use glaredb_execution::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI32,
    PhysicalUtf8,
};
use glaredb_execution::arrays::batch::Batch;
use glaredb_execution::arrays::datatype::{DataType, DataTypeId};
use glaredb_execution::functions::Signature;
use glaredb_execution::functions::function_set::TableFunctionSet;
use glaredb_execution::functions::table::RawTableFunction;
use glaredb_execution::storage::projections::Projections;
use tpchgen::generators::{Region, RegionGenerator, RegionGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_REGION: TableFunctionSet = TableFunctionSet {
    name: "region",
    aliases: &[],
    doc: None,
    functions: &[
        // region(sf)
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Table),
            &TableGen::new(RegionTable),
        ),
        // region()
        RawTableFunction::new_scan(
            &Signature::new(&[], DataTypeId::Table),
            &TableGen::new(RegionTable),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct RegionTable;

impl TpchTable for RegionTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("r_regionkey", DataType::Int32),
        TpchColumn::new("r_name", DataType::Utf8),
        TpchColumn::new("r_comment", DataType::Utf8),
    ];

    type RowIter = RegionGeneratorIterator<'static>;
    type Row = Region<'static>;

    fn create_row_iter(_sf: Option<f64>) -> Result<Self::RowIter> {
        Ok(RegionGenerator::new().iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            0 => {
                let mut r_keys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, region) in rows.iter().enumerate() {
                    r_keys.put(idx, &(region.r_regionkey as i32));
                }
                Ok(())
            }
            1 => {
                let mut r_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, region) in rows.iter().enumerate() {
                    r_names.put(idx, region.r_name);
                }
                Ok(())
            }
            2 => {
                let mut r_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, region) in rows.iter().enumerate() {
                    r_comments.put(idx, region.r_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other}"),
        })
    }
}
