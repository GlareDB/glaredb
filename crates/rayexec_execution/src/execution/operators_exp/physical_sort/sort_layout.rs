use crate::arrays::buffer::physical_type::PhysicalType;
use crate::arrays::datatype::DataType;
use crate::expr::physical::PhysicalSortExpression;

#[derive(Debug)]
pub struct SortLayout {
    pub input_types: Vec<DataType>,
    pub key_columns: Vec<usize>,
    pub key_sizes: Vec<usize>,
    pub key_nulls_first: Vec<bool>,
    pub key_desc: Vec<bool>,
}

impl SortLayout {
    pub fn new(input_types: Vec<DataType>, exprs: &[PhysicalSortExpression]) -> Self {
        let key_columns = exprs.iter().map(|expr| expr.column.idx).collect();
        let key_nulls_first = exprs.iter().map(|expr| expr.nulls_first).collect();
        let key_desc = exprs.iter().map(|expr| expr.desc).collect();

        let key_sizes = exprs
            .iter()
            .map(|sort_expr| {
                let key_type = &input_types[sort_expr.column.idx];

                let size = match key_type.physical_type() {
                    PhysicalType::Int8 => std::mem::size_of::<i8>(),
                    PhysicalType::Int32 => std::mem::size_of::<i32>(),
                    _ => unimplemented!(),
                };
                size + 1 // Account for validity byte. Currently we set it for everything.
            })
            .collect();

        SortLayout {
            input_types,
            key_desc,
            key_sizes,
            key_columns,
            key_nulls_first,
        }
    }
}
