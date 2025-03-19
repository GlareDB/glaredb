use glaredb_error::Result;
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
use glaredb_execution::functions::function_set::TableFunctionSet;
use glaredb_execution::functions::table::RawTableFunction;
use glaredb_execution::functions::Signature;
use glaredb_execution::storage::projections::Projections;
use tpchgen::generators::{Supplier, SupplierGenerator, SupplierGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_SUPPLIER: TableFunctionSet = TableFunctionSet {
    name: "supplier",
    aliases: &[],
    doc: None,
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[], DataTypeId::Table),
        &TableGen::new(SupplierTable),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct SupplierTable;

impl TpchTable for SupplierTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("s_suppkey", DataType::Int64),
        TpchColumn::new("s_name", DataType::Utf8),
        TpchColumn::new("s_address", DataType::Utf8),
        TpchColumn::new("s_nationkey", DataType::Int32),
        TpchColumn::new("s_phone", DataType::Utf8),
        TpchColumn::new("s_acctbal", DataType::Float64), // TODO: Decimal(15, 2)
        TpchColumn::new("s_comment", DataType::Utf8),
    ];

    type RowIter = SupplierGeneratorIterator<'static>;
    type Row = Supplier;

    fn create_row_iter(sf: f64) -> Self::RowIter {
        SupplierGenerator::new(sf, 1, 1).iter()
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            0 => {
                let mut s_keys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_keys.put(idx, &(supp.s_suppkey));
                }
                Ok(())
            }
            1 => {
                let mut s_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_names.put(idx, &supp.s_name);
                }
                Ok(())
            }
            2 => {
                let mut s_addresses = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_addresses.put(idx, &supp.s_address);
                }
                Ok(())
            }
            3 => {
                let mut s_nationkeys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_nationkeys.put(idx, &(supp.s_nationkey as i32));
                }
                Ok(())
            }
            4 => {
                let mut s_phones = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_phones.put(idx, &supp.s_phone);
                }
                Ok(())
            }
            5 => {
                let mut s_balances = PhysicalF64::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_balances.put(idx, &supp.s_acctbal);
                }
                Ok(())
            }
            6 => {
                let mut s_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, supp) in rows.iter().enumerate() {
                    s_comments.put(idx, &supp.s_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other}"),
        })
    }
}
