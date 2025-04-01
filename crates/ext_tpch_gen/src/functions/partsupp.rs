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
use glaredb_core::storage::projections::Projections;
use glaredb_error::{OptionExt, Result};
use tpchgen::generators::{PartSupp, PartSuppGenerator, PartSuppGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_PARTSUPP: TableFunctionSet = TableFunctionSet {
    name: "partsupp",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Generates TPC-H part supplier data with the specified scale factor.",
        arguments: &["scale_factor"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Table),
        &TableGen::new(PartSuppTable),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct PartSuppTable;

impl TpchTable for PartSuppTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("ps_partkey", DataType::Int64),
        TpchColumn::new("ps_suppkey", DataType::Int64),
        TpchColumn::new("ps_availqty", DataType::Int32),
        TpchColumn::new(
            "ps_supplycost",
            DataType::Decimal64(DecimalTypeMeta::new(15, 2)),
        ),
        TpchColumn::new("ps_comment", DataType::Utf8),
    ];

    type RowIter = PartSuppGeneratorIterator<'static>;
    type Row = PartSupp<'static>;

    fn create_row_iter(sf: Option<f64>) -> Result<Self::RowIter> {
        let sf = sf.required("sf")?;
        Ok(PartSuppGenerator::new(sf, 1, 1).iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            0 => {
                let mut ps_partkeys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, ps) in rows.iter().enumerate() {
                    ps_partkeys.put(idx, &(ps.ps_partkey));
                }
                Ok(())
            }
            1 => {
                let mut ps_suppkeys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, ps) in rows.iter().enumerate() {
                    ps_suppkeys.put(idx, &(ps.ps_suppkey));
                }
                Ok(())
            }
            2 => {
                let mut ps_availqtys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, ps) in rows.iter().enumerate() {
                    ps_availqtys.put(idx, &(ps.ps_availqty));
                }
                Ok(())
            }
            3 => {
                let mut ps_supplycosts = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, ps) in rows.iter().enumerate() {
                    ps_supplycosts.put(idx, &(ps.ps_supplycost.0));
                }
                Ok(())
            }
            4 => {
                let mut ps_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, ps) in rows.iter().enumerate() {
                    ps_comments.put(idx, ps.ps_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other}"),
        })
    }
}
