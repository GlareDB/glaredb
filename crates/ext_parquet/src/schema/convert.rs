use glaredb_core::arrays::datatype::DataType;
use glaredb_error::Result;

use super::types::SchemaType;

fn convert_primitive(parquet_type: &SchemaType) -> Result<DataType> {
    unimplemented!()
    // match parquet_type {
    //     Type::PrimitiveType {
    //         basic_info,
    //         physical_type,
    //         type_length,
    //         scale,
    //         precision,
    //     } => {
    //         unimplemented!()
    //     }
    //     Type::GroupType { basic_info, fields } => unimplemented!(),
    // }
}
