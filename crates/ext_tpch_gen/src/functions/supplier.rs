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
use tpchgen::generators::{Supplier, SupplierGenerator, SupplierGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_SUPPLIER: TableFunctionSet = TableFunctionSet {
    name: "supplier",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Generates TPC-H supplier data with the specified scale factor.",
        arguments: &["scale_factor"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Table),
        &TableGen::new(SupplierTable),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct SupplierTable;

impl TpchTable for SupplierTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("s_suppkey", DataType::int64()),
        TpchColumn::new("s_name", DataType::utf8()),
        TpchColumn::new("s_address", DataType::utf8()),
        TpchColumn::new("s_nationkey", DataType::int32()),
        TpchColumn::new("s_phone", DataType::utf8()),
        TpchColumn::new(
            "s_acctbal",
            DataType::decimal64(DecimalTypeMeta::new(15, 2)),
        ),
        TpchColumn::new("s_comment", DataType::utf8()),
    ];

    type RowIter = SupplierGeneratorIterator<'static>;
    type Row = Supplier;

    fn create_row_iter(sf: Option<f64>) -> Result<Self::RowIter> {
        let sf = sf.required("sf")?;
        Ok(SupplierGenerator::new(sf, 1, 1).iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        let mut s_buf = String::new();

        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            ProjectedColumn::Data(0) => {
                let mut s_keys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_keys.put(idx, &(supp.s_suppkey));
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut s_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", supp.s_name)?;
                    s_names.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut s_addresses = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", supp.s_address)?;
                    s_addresses.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let mut s_nationkeys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_nationkeys.put(idx, &(supp.s_nationkey as i32));
                }
                Ok(())
            }
            ProjectedColumn::Data(4) => {
                let mut s_phones = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", supp.s_phone)?;
                    s_phones.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(5) => {
                let mut s_balances = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_balances.put(idx, &supp.s_acctbal.0);
                }
                Ok(())
            }
            ProjectedColumn::Data(6) => {
                let mut s_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_comments.put(idx, &supp.s_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other:?}"),
        })
    }
}
