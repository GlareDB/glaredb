use crate::field::{DataType, Field};
use rayexec_arrow_ipc::Schema::{Field as IpcField, Precision as IpcPrecision, Type as IpcType};
use rayexec_error::{RayexecError, Result};

/// Convert an arrow ipc field to a rayexec field..
pub fn ipc_to_field(field: IpcField) -> Result<Field> {
    if field.custom_metadata().is_some() {
        // I don't think we'll ever want to support custom metadata, but maybe
        // we should just ignore it.
        return Err(RayexecError::new("metadata unsupported"));
    }

    if field.dictionary().is_some() {
        // TODO
        return Err(RayexecError::new("dictionaries unsupported"));
    }

    let datatype = match field.type_type() {
        IpcType::Null => DataType::Null,
        IpcType::Bool => DataType::Boolean,
        IpcType::Int => {
            let int_type = field.type__as_int().unwrap();
            if int_type.is_signed() {
                match int_type.bitWidth() {
                    8 => DataType::Int8,
                    16 => DataType::Int16,
                    32 => DataType::Int32,
                    64 => DataType::Int64,
                    other => {
                        return Err(RayexecError::new(format!("Unsupported int size: {other}")))
                    }
                }
            } else {
                match int_type.bitWidth() {
                    8 => DataType::UInt8,
                    16 => DataType::UInt16,
                    32 => DataType::UInt32,
                    64 => DataType::UInt64,
                    other => {
                        return Err(RayexecError::new(format!("Unsupported int size: {other}")))
                    }
                }
            }
        }
        IpcType::FloatingPoint => {
            let float_type = field.type__as_floating_point().unwrap();
            match float_type.precision() {
                IpcPrecision::SINGLE => DataType::Float32,
                IpcPrecision::DOUBLE => DataType::Float64,
                other => {
                    return Err(RayexecError::new(format!(
                        "Unsupported float precision: {:?}",
                        other.variant_name()
                    )))
                }
            }
        }
        IpcType::Utf8 => DataType::Utf8,
        IpcType::LargeUtf8 => DataType::LargeUtf8,
        IpcType::Binary => DataType::Binary,
        IpcType::LargeBinary => DataType::LargeBinary,
        other => {
            return Err(RayexecError::new(format!(
                "Unsupported ipc type: {:?}",
                other.variant_name(),
            )))
        }
    };

    Ok(Field {
        name: field.name().unwrap().to_string(),
        datatype,
        nullable: field.nullable(),
    })
}
