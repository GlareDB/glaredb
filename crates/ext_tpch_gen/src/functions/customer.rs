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
use tpchgen::generators::{Customer, CustomerGenerator, CustomerGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_CUSTOMER: TableFunctionSet = TableFunctionSet {
    name: "customer",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Generates TPC-H customer data with the specified scale factor.",
        arguments: &["scale_factor"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Table),
        &TableGen::new(CustomerTable),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct CustomerTable;

impl TpchTable for CustomerTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("c_custkey", DataType::int64()),
        TpchColumn::new("c_name", DataType::utf8()),
        TpchColumn::new("c_address", DataType::utf8()),
        TpchColumn::new("c_nationkey", DataType::int32()),
        TpchColumn::new("c_phone", DataType::utf8()),
        TpchColumn::new(
            "c_acctbal",
            DataType::decimal64(DecimalTypeMeta::new(15, 2)),
        ),
        TpchColumn::new("c_mktsegment", DataType::utf8()),
        TpchColumn::new("c_comment", DataType::utf8()),
    ];

    type RowIter = CustomerGeneratorIterator<'static>;
    type Row = Customer<'static>;

    fn create_row_iter(sf: Option<f64>) -> Result<Self::RowIter> {
        let sf = sf.required("sf")?;
        Ok(CustomerGenerator::new(sf, 1, 1).iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        let mut s_buf = String::new();

        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            ProjectedColumn::Data(0) => {
                let mut c_keys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_keys.put(idx, &(cust.c_custkey));
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut c_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", cust.c_name)?;
                    c_names.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut c_addresses = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", cust.c_address)?;
                    c_addresses.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let mut c_nationkeys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_nationkeys.put(idx, &(cust.c_nationkey as i32));
                }
                Ok(())
            }
            ProjectedColumn::Data(4) => {
                let mut c_phones = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", cust.c_phone)?;
                    c_phones.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(5) => {
                let mut c_balances = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_balances.put(idx, &cust.c_acctbal.0);
                }
                Ok(())
            }
            ProjectedColumn::Data(6) => {
                let mut c_segments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_segments.put(idx, cust.c_mktsegment);
                }
                Ok(())
            }
            ProjectedColumn::Data(7) => {
                let mut c_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, cust) in rows.iter().enumerate() {
                    c_comments.put(idx, cust.c_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other:?}"),
        })
    }
}
