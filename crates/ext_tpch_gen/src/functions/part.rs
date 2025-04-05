use std::fmt::Write;

use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI32,
    PhysicalI64,
    PhysicalUtf8,
};
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use glaredb_core::functions::Signature;
use glaredb_core::functions::documentation::{Category, Documentation};
use glaredb_core::functions::function_set::TableFunctionSet;
use glaredb_core::functions::table::RawTableFunction;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_error::{OptionExt, Result};
use tpchgen::generators::{Part, PartGenerator, PartGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_PART: TableFunctionSet = TableFunctionSet {
    name: "part",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Generates TPC-H part data with the specified scale factor.",
        arguments: &["scale_factor"],
        example: None,
    }],
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
        TpchColumn::new(
            "p_retailprice",
            DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        ),
        TpchColumn::new("p_comment", DataType::Utf8),
    ];

    type RowIter = PartGeneratorIterator<'static>;
    type Row = Part<'static>;

    fn create_row_iter(sf: Option<f64>) -> Result<Self::RowIter> {
        let sf = sf.required("sf")?;
        Ok(PartGenerator::new(sf, 1, 1).iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        let mut s_buf = String::new();

        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            ProjectedColumn::Data(0) => {
                let mut p_keys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_keys.put(idx, &(part.p_partkey));
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut p_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", part.p_name)?;
                    p_names.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut p_mfgrs = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", part.p_mfgr)?;
                    p_mfgrs.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let mut p_brands = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", part.p_brand)?;
                    p_brands.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(4) => {
                let mut p_types = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_types.put(idx, part.p_type);
                }
                Ok(())
            }
            ProjectedColumn::Data(5) => {
                let mut p_sizes = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_sizes.put(idx, &part.p_size);
                }
                Ok(())
            }
            ProjectedColumn::Data(6) => {
                let mut p_containers = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_containers.put(idx, part.p_container);
                }
                Ok(())
            }
            ProjectedColumn::Data(7) => {
                let mut p_prices = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_prices.put(idx, &part.p_retailprice.0);
                }
                Ok(())
            }
            ProjectedColumn::Data(8) => {
                let mut p_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, part) in rows.iter().enumerate() {
                    p_comments.put(idx, part.p_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other:?}"),
        })
    }
}
