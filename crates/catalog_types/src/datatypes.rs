use datafusion::arrow::datatypes::DataType;

pub type TypeId = u32;

/// Return the built in type id for an arrow data type.
///
/// Returns `None` if the data type is not supported.
pub fn type_id_for_arrow_type(typ: &DataType) -> Option<TypeId> {
    Some(match typ {
        DataType::Boolean => 0,
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 3,
        DataType::Int64 => 4,
        DataType::UInt8 => 5,
        DataType::UInt16 => 6,
        DataType::UInt32 => 7,
        DataType::UInt64 => 8,
        DataType::Float16 => 9,
        DataType::Float32 => 10,
        DataType::Float64 => 11,
        DataType::Utf8 => 12,
        _ => return None,
    })
}

/// Get the arrow data type with the given type id.
// TODO: Will need to pass in more info for more complex types.
pub fn arrow_type_for_type_id(id: TypeId) -> Option<DataType> {
    Some(match id {
        0 => DataType::Boolean,
        1 => DataType::Int8,
        2 => DataType::Int16,
        3 => DataType::Int32,
        4 => DataType::Int64,
        5 => DataType::UInt8,
        6 => DataType::UInt16,
        7 => DataType::UInt32,
        8 => DataType::UInt64,
        9 => DataType::Float16,
        10 => DataType::Float32,
        11 => DataType::Float64,
        12 => DataType::Utf8,
        _ => return None,
    })
}
