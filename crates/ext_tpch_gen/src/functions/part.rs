use glaredb_error::{OptionExt, Result};
use glaredb_execution::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalF64,
    PhysicalI32,
    PhysicalI64,
    PhysicalUtf8,
};
use glaredb_execution::arrays::batch::Batch;
use glaredb_execution::arrays::datatype::{DataType, DataTypeId};
use glaredb_execution::functions::Signature;
use glaredb_execution::functions::function_set::TableFunctionSet;
use glaredb_execution::functions::table::RawTableFunction;
use glaredb_execution::storage::projections::Projections;
use tpchgen::generators::{Part, PartGenerator, PartGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_PART: TableFunctionSet = TableFunctionSet {
    name: "part",
    aliases: &[],
    doc: None,
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Table),
        &TableGen::new(PartTable),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct PartTable;

impl TpchTable for PartTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("p_partkey", DataType::Int64),
        TpchColumn::new("p_name", DataType::Utf8),
        TpchColumn::new("p_mfgr", DataType::Utf8),
        TpchColumn::new("p_brand", DataType::Utf8),
        TpchColumn::new("p_type", DataType::Utf8),
        TpchColumn::new("p_size", DataType::Int32),
        TpchColumn::new("p_container", DataType::Utf8),
        TpchColumn::new("p_retailprice", DataType::Float64), // TODO: Decimal(15, 2)
        TpchColumn::new("p_comment", DataType::Utf8),
    ];

    type RowIter = PartGeneratorIterator<'static>;
    type Row = Part<'static>;

    fn create_row_iter(sf: Option<f64>) -> Result<Self::RowIter> {
        let sf = sf.required("sf")?;
        Ok(PartGenerator::new(sf, 1, 1).iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            0 => {
                let mut p_keys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_keys.put(idx, &(part.p_partkey));
                }
                Ok(())
            }
            1 => {
                let mut p_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_names.put(idx, &part.p_name);
                }
                Ok(())
            }
            2 => {
                let mut p_mfgrs = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_mfgrs.put(idx, &part.p_mfgr);
                }
                Ok(())
            }
            3 => {
                let mut p_brands = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_brands.put(idx, &part.p_brand);
                }
                Ok(())
            }
            4 => {
                let mut p_types = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_types.put(idx, part.p_type);
                }
                Ok(())
            }
            5 => {
                let mut p_sizes = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_sizes.put(idx, &part.p_size);
                }
                Ok(())
            }
            6 => {
                let mut p_containers = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_containers.put(idx, part.p_container);
                }
                Ok(())
            }
            7 => {
                let mut p_prices = PhysicalF64::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_prices.put(idx, &part.p_retailprice);
                }
                Ok(())
            }
            8 => {
                let mut p_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_comments.put(idx, part.p_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other}"),
        })
    }
}
