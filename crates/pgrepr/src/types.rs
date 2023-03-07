use datafusion::arrow::datatypes::DataType as ArrowType;
use tokio_postgres::types::Type as PgType;

/// Returns a compatible postgres type for the arrow datatype. If the type hint
/// is not-none, it returns the type inside the option.
pub fn arrow_to_pg_type(df_type: &ArrowType, type_hint: Option<PgType>) -> PgType {
    // TODO: Create pseudo types for unsigned integers and use them.
    type_hint.unwrap_or(match df_type {
        &ArrowType::Boolean => PgType::BOOL,
        &ArrowType::Int8 | &ArrowType::Int16 => PgType::INT2,
        &ArrowType::Int32 => PgType::INT4,
        &ArrowType::Int64 => PgType::INT8,
        &ArrowType::Float16 | &ArrowType::Float32 => PgType::FLOAT4,
        &ArrowType::Float64 => PgType::FLOAT8,
        &ArrowType::Utf8 => PgType::TEXT,
        &ArrowType::Binary => PgType::BYTEA,
        &ArrowType::Timestamp(_, None) => PgType::TIMESTAMP,
        &ArrowType::Timestamp(_, Some(_)) => PgType::TIMESTAMPTZ,
        &ArrowType::Time64(_) => PgType::TIME,
        &ArrowType::Date32 => PgType::DATE,
        // TODO: Intervals, numerics: They are a little complicated since not
        // directly supported by the tokio-postgres library, need to implement
        // them explicitly. We might be better having our own `ScalarValue`
        // enum so as to move away from Arrow restrictions. Shouldn't be much
        // work since all we use these values for is for encoding/decoding.

        // When there's a type we aren't really familiar with, we want to
        // return text in that case (literally!). We just want to send a
        // text representation of the datatype.
        _ => return PgType::TEXT,
    })
}
