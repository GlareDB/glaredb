use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType,
    Field as ArrowField,
    Schema as ArrowSchema,
    TimeUnit,
};
use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};

use crate::lake::iceberg::errors::Result;

fn primitive_type_to_arrow(value: &PrimitiveType) -> Result<DataType> {
    Ok(match value {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => {
            DataType::Decimal128(*precision as u8, *scale as i8)
        }
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Time => DataType::Timestamp(TimeUnit::Microsecond, None), // TODO: Possibly `Time32` instead?
        PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::String => DataType::Utf8,
        PrimitiveType::Uuid => DataType::Utf8,
        PrimitiveType::Fixed(l) => DataType::FixedSizeBinary(*l as i32),
        PrimitiveType::Binary => DataType::Binary,
    })
}

fn iceberg_type_to_arrow(value: &Type) -> Result<DataType> {
    match value {
        Type::Primitive(t) => primitive_type_to_arrow(t),
        Type::List(t) => list_type_to_arrow(t),
        Type::Struct(t) => struct_type_to_arrow(t),
        Type::Map(t) => map_type_to_arrow(t),
    }
}

fn list_type_to_arrow(value: &ListType) -> Result<DataType> {
    let field = iceberg_field_to_arrow(&value.element_field)?;
    Ok(DataType::List(Arc::new(field)))
}

fn map_type_to_arrow(value: &MapType) -> Result<DataType> {
    let key_field = iceberg_field_to_arrow(&value.key_field)?;
    let val_field = iceberg_field_to_arrow(&value.value_field)?;
    let field = ArrowField::new_struct("entryies", vec![key_field, val_field], false);
    let typ = DataType::Map(Arc::new(field), false);
    Ok(typ)
}

fn struct_type_to_arrow(value: &StructType) -> Result<DataType> {
    let fields = value
        .fields()
        .iter()
        .map(|f| iceberg_field_to_arrow(f))
        .collect::<Result<Vec<_>>>()?;

    let typ = DataType::Struct(fields.into());
    Ok(typ)
}

fn iceberg_field_to_arrow(f: &NestedField) -> Result<ArrowField> {
    let typ = iceberg_type_to_arrow(&f.field_type)?;
    Ok(ArrowField::new(&f.name, typ, !f.required))
}

pub fn iceberg_schema_to_arrow(s: &Schema) -> Result<ArrowSchema> {
    let fields = s
        .as_struct()
        .fields()
        .iter()
        .map(|f| iceberg_field_to_arrow(f))
        .collect::<Result<Vec<_>>>()?;
    Ok(ArrowSchema::new(fields))
}
