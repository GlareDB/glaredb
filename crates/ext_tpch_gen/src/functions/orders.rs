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
use tpchgen::generators::{Order, OrderGenerator, OrderGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_ORDERS: TableFunctionSet = TableFunctionSet {
    name: "orders",
    aliases: &[],
    doc: None,
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Float64], DataTypeId::Table),
        &TableGen::new(OrdersTable),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct OrdersTable;

impl TpchTable for OrdersTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("o_orderkey", DataType::Int64),
        TpchColumn::new("o_custkey", DataType::Int64),
        TpchColumn::new("o_orderstatus", DataType::Utf8),
        TpchColumn::new("o_totalprice", DataType::Float64), // TODO: Decimal(15, 2)
        TpchColumn::new("o_orderdate", DataType::Date32),
        TpchColumn::new("o_orderpriority", DataType::Utf8),
        TpchColumn::new("o_clerk", DataType::Utf8),
        TpchColumn::new("o_shippriority", DataType::Int32),
        TpchColumn::new("o_comment", DataType::Utf8),
    ];

    type RowIter = OrderGeneratorIterator<'static>;
    type Row = Order<'static>;

    fn create_row_iter(sf: Option<f64>) -> Result<Self::RowIter> {
        let sf = sf.required("sf")?;
        Ok(OrderGenerator::new(sf, 1, 1).iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            0 => {
                let mut o_keys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_keys.put(idx, &order.o_orderkey);
                }
                Ok(())
            }
            1 => {
                let mut o_custkeys = PhysicalI64::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_custkeys.put(idx, &order.o_custkey);
                }
                Ok(())
            }
            2 => {
                let mut o_statuses = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    // Single byte buffer is large enough here since the only
                    // values can be 'F', 'O', or 'P'.
                    let mut buf = [0];
                    let s = order.o_orderstatus.encode_utf8(&mut buf);
                    o_statuses.put(idx, s);
                }
                Ok(())
            }
            3 => {
                let mut o_totalprices = PhysicalF64::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_totalprices.put(idx, &order.o_totalprice);
                }
                Ok(())
            }
            4 => {
                // TODO
                // let mut o_dates = PhysicalI32::get_addressable_mut(output.data_mut())?;
                // for (idx, order) in rows.iter().enumerate() {
                // }
                Ok(())
            }
            5 => {
                let mut o_priorities = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_priorities.put(idx, &order.o_orderpriority);
                }
                Ok(())
            }
            6 => {
                let mut o_clerks = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_clerks.put(idx, &order.o_clerk);
                }
                Ok(())
            }
            7 => {
                let mut o_shipprios = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_shipprios.put(idx, &order.o_shippriority);
                }
                Ok(())
            }
            8 => {
                let mut o_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, order) in rows.iter().enumerate() {
                    o_comments.put(idx, order.o_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other}"),
        })
    }
}
