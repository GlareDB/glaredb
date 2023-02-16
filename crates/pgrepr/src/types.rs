use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use datafusion::arrow::array::Float16Array;
use datafusion::arrow::{array::Array, datatypes::DataType as ArrowType};
use datafusion::scalar::ScalarValue;
use tokio_postgres::types::Type as PgType;

use crate::error::{PgReprError, Result};
use crate::format::Format;
use crate::writer::{BinaryWriter, TextWriter, Writer};

/// Returns a compatible postgres type for the arrow datatype.
///
/// If the type hint is not-none, it returns the type type inside the option.
pub fn arrow_to_pg_type(df_type: &ArrowType, type_hint: Option<PgType>) -> PgType {
    type_hint.unwrap_or(match df_type {
        &ArrowType::Boolean => PgType::BOOL,
        &ArrowType::Int8 | &ArrowType::Int16 => PgType::INT2,
        &ArrowType::Int32 => PgType::INT4,
        &ArrowType::Int64 => PgType::INT8,
        &ArrowType::Float16 | &ArrowType::Float32 => PgType::FLOAT4,
        &ArrowType::Float64 => PgType::FLOAT8,
        // When there's a type we aren't really familiar with, we want to
        // return text in that case (literally!). We just want to send a
        // text representation of the datatype.
        _ => return PgType::TEXT,
    })
}

pub fn encode_as_pg_type(
    buf: &mut BytesMut,
    format: Format,
    array: &Arc<dyn Array>,
    row_idx: usize,
    pg_type: &PgType,
) -> Result<()> {
    let scalar = try_scalar_from_array(array, row_idx)?;

    if scalar.is_null() {
        buf.put_i32(-1);
        return Ok(());
    }

    let (from, to) = (array.data_type(), pg_type);

    match format {
        Format::Text => encode_as_pg_type_not_null::<TextWriter>(buf, scalar, from, to),
        Format::Binary => encode_as_pg_type_not_null::<BinaryWriter>(buf, scalar, from, to),
    }
}

fn encode_as_pg_type_not_null<W: Writer>(
    buf: &mut BytesMut,
    scalar: ScalarValue,
    from: &ArrowType,
    to: &PgType,
) -> Result<()> {
    match (from, to, scalar) {
        (&ArrowType::Boolean, &PgType::BOOL, ScalarValue::Boolean(Some(v))) => {
            W::write_bool(buf, v)
        }
        (&ArrowType::Int8, &PgType::INT2, ScalarValue::Int8(Some(v))) => {
            W::write_int2(buf, v as i16)
        }
        (&ArrowType::Int16, &PgType::INT2, ScalarValue::Int16(Some(v))) => W::write_int2(buf, v),
        (&ArrowType::Int32, &PgType::INT4, ScalarValue::Int32(Some(v))) => W::write_int4(buf, v),
        (&ArrowType::Int64, &PgType::INT8, ScalarValue::Int64(Some(v))) => W::write_int8(buf, v),
        (&ArrowType::Float16, &PgType::FLOAT4, ScalarValue::Float32(Some(v))) => {
            W::write_float4(buf, v)
        }
        (&ArrowType::Float32, &PgType::FLOAT4, ScalarValue::Float32(Some(v))) => {
            W::write_float4(buf, v)
        }
        (&ArrowType::Float64, &PgType::FLOAT8, ScalarValue::Float64(Some(v))) => {
            W::write_float8(buf, v)
        }
        (_, &PgType::TEXT, scalar) => {
            // Here we don't know how to process the arrow data-type. In these
            // cases it's recommended that we return the string representation
            // (along-with telling postgres that we're returning a TEXT).
            W::write_any(buf, scalar)
        }
        (from, to, _) => {
            // This should be unreachable. We definitely never want to convert
            // something that we can't. For the cases where we don't have an
            // appropriate conversion possible, the PG type should always be
            // TEXT in those cases.
            Err(PgReprError::InternalError(format!(
                "unable to convert ArrowType({}) to PostgresType({})",
                from, to,
            )))
        }
    }
}

fn try_scalar_from_array(array: &Arc<dyn Array>, row_idx: usize) -> Result<ScalarValue> {
    match ScalarValue::try_from_array(array, row_idx) {
        Ok(scalar) => Ok(scalar),
        Err(_) => {
            // This data-type is not supported by arrow. Try to find a suitable
            // conversion if possible, else error!
            match array.data_type() {
                &ArrowType::Float16 => {
                    // To ScalarValue::Float32
                    let array = array.as_any().downcast_ref::<Float16Array>().unwrap();
                    Ok(ScalarValue::Float32(match array.is_null(row_idx) {
                        true => None,
                        false => Some(array.value(row_idx).to_f32()),
                    }))
                }
                _ => Err(PgReprError::UnsupportedArrowType(
                    array.data_type().to_owned(),
                )),
            }
        }
    }
}
