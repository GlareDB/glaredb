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
use tpchgen::generators::{Order, OrderGenerator, OrderGeneratorIterator};

use super::convert;
use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_ORDERS: TableFunctionSet = TableFunctionSet {
    name: "orders",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Generates TPC-H orders data with the specified scale factor.",
        arguments: &["scale_factor"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Table),
        &TableGen::new(OrdersTable),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct OrdersTable;

impl TpchTable for OrdersTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("o_orderkey", DataType::int64()),
        TpchColumn::new("o_custkey", DataType::int64()),
        TpchColumn::new("o_orderstatus", DataType::utf8()),
        TpchColumn::new(
            "o_totalprice",
            DataType::decimal64(DecimalTypeMeta::new(15, 2)),
        ),
        TpchColumn::new("o_orderdate", DataType::date32()),
        TpchColumn::new("o_orderpriority", DataType::utf8()),
        TpchColumn::new("o_clerk", DataType::utf8()),
        TpchColumn::new("o_shippriority", DataType::int32()),
        TpchColumn::new("o_comment", DataType::utf8()),
    ];

    type RowIter = OrderGeneratorIterator<'static>;
    type Row = Order<'static>;

    fn create_row_iter(sf: Option<f64>) -> Result<Self::RowIter> {
        let sf = sf.required("sf")?;
        Ok(OrderGenerator::new(sf, 1, 1).iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            ProjectedColumn::Data(0) => {
                let mut o_keys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_keys.put(idx, &order.o_orderkey);
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut o_custkeys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_custkeys.put(idx, &order.o_custkey);
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut o_statuses = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_statuses.put(idx, convert::status_to_str(order.o_orderstatus));
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let mut o_totalprices = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_totalprices.put(idx, &order.o_totalprice.0);
                }
                Ok(())
            }
            ProjectedColumn::Data(4) => {
                let mut o_dates = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_dates.put(
                        idx,
                        &convert::tpch_date_to_days_after_epoch(order.o_orderdate),
                    );
                }
                Ok(())
            }
            ProjectedColumn::Data(5) => {
                let mut o_priorities = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_priorities.put(idx, order.o_orderpriority);
                }
                Ok(())
            }
            ProjectedColumn::Data(6) => {
                let mut s_buf = String::new();
                let mut o_clerks = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    s_buf.clear();
                    write!(s_buf, "{}", order.o_clerk)?;
                    o_clerks.put(idx, &s_buf);
                }
                Ok(())
            }
            ProjectedColumn::Data(7) => {
                let mut o_shipprios = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_shipprios.put(idx, &order.o_shippriority);
                }
                Ok(())
            }
            ProjectedColumn::Data(8) => {
                let mut o_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_comments.put(idx, order.o_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other:?}"),
        })
    }
}
