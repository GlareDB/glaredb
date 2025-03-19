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
use tpchgen::generators::{Customer, CustomerGenerator, CustomerGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_CUSTOMER: TableFunctionSet = TableFunctionSet {
    name: "customer",
    aliases: &[],
    doc: None,
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[], DataTypeId::Table),
        &TableGen::new(CustomerTable),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct CustomerTable;

impl TpchTable for CustomerTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("c_custkey", DataType::Int64),
        TpchColumn::new("c_name", DataType::Utf8),
        TpchColumn::new("c_address", DataType::Utf8),
        TpchColumn::new("c_nationkey", DataType::Int32),
        TpchColumn::new("c_phone", DataType::Utf8),
        TpchColumn::new("c_acctbal", DataType::Float64), // TODO: Decimal(15, 2)
        TpchColumn::new("c_mktsegment", DataType::Utf8),
        TpchColumn::new("c_comment", DataType::Utf8),
    ];

    type RowIter = CustomerGeneratorIterator<'static>;
    type Row = Customer;

    fn create_row_iter(sf: f64) -> Self::RowIter {
        CustomerGenerator::new(sf, 1, 1).iter()
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            0 => {
                let mut c_keys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_keys.put(idx, &(cust.c_custkey));
                }
                Ok(())
            }
            1 => {
                let mut c_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_names.put(idx, &cust.c_name);
                }
                Ok(())
            }
            2 => {
                let mut c_addresses = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_addresses.put(idx, &cust.c_address);
                }
                Ok(())
            }
            3 => {
                let mut c_nationkeys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_nationkeys.put(idx, &(cust.c_nationkey as i32));
                }
                Ok(())
            }
            4 => {
                let mut c_phones = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_phones.put(idx, &cust.c_phone);
                }
                Ok(())
            }
            5 => {
                let mut c_balances = PhysicalF64::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_balances.put(idx, &cust.c_acctbal);
                }
                Ok(())
            }
            6 => {
                let mut c_segments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_segments.put(idx, &cust.c_mktsegment);
                }
                Ok(())
            }
            7 => {
                let mut c_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_comments.put(idx, &cust.c_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other}"),
        })
    }
}
