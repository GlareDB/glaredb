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
use tpchgen::generators::{Nation, NationGenerator, NationGeneratorIterator};

use super::table_gen::{TableGen, TpchColumn, TpchTable};

pub const FUNCTION_SET_NATION: TableFunctionSet = TableFunctionSet {
    name: "nation",
    aliases: &[],
    doc: None,
    functions: &[
        // nation(sf)
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Table),
            &TableGen::new(NationTable),
        ),
        // nation()
        RawTableFunction::new_scan(
            &Signature::new(&[], DataTypeId::Table),
            &TableGen::new(NationTable),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct NationTable;

impl TpchTable for NationTable {
    const COLUMNS: &[TpchColumn] = &[
        TpchColumn::new("n_nationkey", DataType::Int32),
        TpchColumn::new("n_name", DataType::Utf8),
        TpchColumn::new("n_regionkey", DataType::Int32),
        TpchColumn::new("n_comment", DataType::Utf8),
    ];

    type RowIter = NationGeneratorIterator<'static>;
    type Row = Nation<'static>;

    fn create_row_iter(_sf: Option<f64>) -> Result<Self::RowIter> {
        Ok(NationGenerator::new().iter())
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, output| match col_idx {
            0 => {
                let mut n_nationkeys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, nation) in rows.iter().enumerate() {
                    n_nationkeys.put(idx, &(nation.n_nationkey as i32));
                }
                Ok(())
            }
            1 => {
                let mut n_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, nation) in rows.iter().enumerate() {
                    n_names.put(idx, nation.n_name);
                }
                Ok(())
            }
            2 => {
                let mut n_regionkeys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                for (idx, nation) in rows.iter().enumerate() {
                    n_regionkeys.put(idx, &(nation.n_regionkey as i32));
                }
                Ok(())
            }
            3 => {
                let mut n_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                for (idx, nation) in rows.iter().enumerate() {
                    n_comments.put(idx, nation.n_comment);
                }
                Ok(())
            }
            other => panic!("invalid projection {other}"),
        })
    }
}
