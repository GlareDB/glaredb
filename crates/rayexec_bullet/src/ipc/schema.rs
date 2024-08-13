//! Conversion to/from ipc schema.
use super::{
    gen::schema::{
        Field as IpcField, Precision as IpcPrecision, Schema as IpcSchema, Type as IpcType,
    },
    IpcConfig,
};
use crate::datatype::DecimalTypeMeta;
use crate::{
    datatype::DataType,
    field::{Field, Schema},
    ipc::gen::schema::{
        BoolBuilder, DecimalBuilder, FieldBuilder, IntBuilder, LargeUtf8Builder, NullBuilder,
        SchemaBuilder, Utf8Builder,
    },
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use rayexec_error::{not_implemented, RayexecError, Result};

pub fn ipc_to_schema(schema: IpcSchema, conf: &IpcConfig) -> Result<Schema> {
    let ipc_fields = schema.fields().unwrap();
    let fields = ipc_fields
        .into_iter()
        .map(|f| ipc_to_field(f, conf))
        .collect::<Result<Vec<_>>>()?;

    Ok(Schema::new(fields))
}

/// Convert an arrow ipc field to a rayexec field..
pub fn ipc_to_field(field: IpcField, _conf: &IpcConfig) -> Result<Field> {
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
        IpcType::Decimal => {
            let dec_type = field.type__as_decimal().unwrap();
            let meta = DecimalTypeMeta {
                precision: dec_type.precision() as u8,
                scale: dec_type.scale() as i8,
            };

            match dec_type.bitWidth() {
                64 => DataType::Decimal64(meta),
                128 => DataType::Decimal128(meta),
                other => {
                    return Err(RayexecError::new(format!(
                        "Unsupported decimal size: {other}"
                    )))
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

pub fn schema_to_ipc<'a>(
    schema: &Schema,
    builder: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<IpcSchema<'a>>> {
    let fields = schema
        .fields
        .iter()
        .map(|f| field_to_ipc(f, builder))
        .collect::<Result<Vec<_>>>()?;

    let fields = builder.create_vector(&fields);

    let mut schema_builder = SchemaBuilder::new(builder);
    schema_builder.add_fields(fields);

    Ok(schema_builder.finish())
}

pub fn field_to_ipc<'a>(
    field: &Field,
    builder: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<IpcField<'a>>> {
    let name = builder.create_string(&field.name);

    let empty_children: Vec<WIPOffset<IpcField>> = Vec::new();

    let (datatype, type_, children) = match &field.datatype {
        DataType::Null => (
            IpcType::Null,
            NullBuilder::new(builder).finish().as_union_value(),
            empty_children.clone(),
        ),
        DataType::Boolean => (
            IpcType::Bool,
            BoolBuilder::new(builder).finish().as_union_value(),
            empty_children.clone(),
        ),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let mut int_builder = IntBuilder::new(builder);
            int_builder.add_is_signed(true);
            match &field.datatype {
                DataType::Int8 => int_builder.add_bitWidth(8),
                DataType::Int16 => int_builder.add_bitWidth(16),
                DataType::Int32 => int_builder.add_bitWidth(32),
                DataType::Int64 => int_builder.add_bitWidth(64),
                _ => unreachable!(),
            }
            (
                IpcType::Int,
                int_builder.finish().as_union_value(),
                empty_children.clone(),
            )
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let mut int_builder = IntBuilder::new(builder);
            int_builder.add_is_signed(false);
            match &field.datatype {
                DataType::UInt8 => int_builder.add_bitWidth(8),
                DataType::UInt16 => int_builder.add_bitWidth(16),
                DataType::UInt32 => int_builder.add_bitWidth(32),
                DataType::UInt64 => int_builder.add_bitWidth(64),
                _ => unreachable!(),
            }
            (
                IpcType::Int,
                int_builder.finish().as_union_value(),
                empty_children.clone(),
            )
        }
        DataType::Decimal64(m) | DataType::Decimal128(m) => {
            let mut dec_builder = DecimalBuilder::new(builder);
            dec_builder.add_scale(m.scale as i32);
            dec_builder.add_precision(m.precision as i32);
            match &field.datatype {
                DataType::Decimal64(_) => dec_builder.add_bitWidth(64),
                DataType::Decimal128(_) => dec_builder.add_bitWidth(128),
                _ => unreachable!(),
            }
            (
                IpcType::Decimal,
                dec_builder.finish().as_union_value(),
                empty_children.clone(),
            )
        }
        DataType::Utf8 => (
            IpcType::Utf8,
            Utf8Builder::new(builder).finish().as_union_value(),
            empty_children.clone(),
        ),
        DataType::LargeUtf8 => (
            IpcType::LargeUtf8,
            LargeUtf8Builder::new(builder).finish().as_union_value(),
            empty_children.clone(),
        ),

        other => not_implemented!("write ipc datatype {other}"),
    };

    let children = builder.create_vector(&children);

    let mut field_builder = FieldBuilder::new(builder);
    field_builder.add_name(name);
    field_builder.add_type_type(datatype);
    field_builder.add_type_(type_);
    field_builder.add_children(children);
    field_builder.add_nullable(field.nullable);

    Ok(field_builder.finish())
}

#[cfg(test)]
mod tests {
    use crate::{datatype::DecimalTypeMeta, ipc::gen::schema::root_as_schema};

    use super::*;

    fn roundtrip(schema: Schema) {
        let mut builder = FlatBufferBuilder::new();

        let ipc = schema_to_ipc(&schema, &mut builder).unwrap();
        builder.finish(ipc, None);
        let buf = builder.finished_data();

        let ipc = root_as_schema(buf).unwrap();
        let got = ipc_to_schema(ipc, &IpcConfig::default()).unwrap();

        assert_eq!(schema, got);
    }

    #[test]
    fn simple_schema_roundtrip() {
        let schema = Schema::new([
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::Utf8, true),
        ]);

        roundtrip(schema);
    }

    #[test]
    fn decimal_roundtrip() {
        let schema = Schema::new([Field::new(
            "f1",
            DataType::Decimal64(DecimalTypeMeta {
                precision: 4,
                scale: 2,
            }),
            true,
        )]);

        roundtrip(schema);
    }
}
