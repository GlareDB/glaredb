use std::{fmt::Write, sync::Arc};

use chrono::{Duration, TimeZone, Utc};
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, UInt64Array},
        compute::{cast_with_options, CastOptions},
        datatypes::{DataType, Field, Schema, TimeUnit},
        error::ArrowError,
        record_batch::RecordBatch,
        util::display::FormatOptions,
    },
    scalar::ScalarValue,
};
use decimal::Decimal128;
use once_cell::sync::Lazy;
use repr::str::encode::{encode_binary, encode_binary_mysql, encode_binary_snowflake, encode_date, encode_decimal, encode_float, encode_int, encode_string, encode_time, encode_utc_timestamp};

use super::errors::{DatasourceCommonError, Result};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Datasource {
    Postgres,
    MySql,
    BigQuery,
    Snowflake,
    Clickhouse,
    SqlServer,
}

/// Returns true if the literal expression encoding should be wrapped inside
/// quotes.
fn is_literal_quotable(datasource: Datasource, lit: &ScalarValue) -> bool {
    match lit {
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::Float32(_)
        | ScalarValue::Float64(_)
        | ScalarValue::Decimal128(..) => false,
        ScalarValue::Binary(_) if datasource == Datasource::MySql => false,
        _ => true,
    }
}

/// Encodes the literal expression as a string in the buffer. This is used to
/// translate the query's where clause.
pub fn encode_literal_to_text(
    datasource: Datasource,
    buf: &mut String,
    lit: &ScalarValue,
) -> Result<()> {
    if lit.is_null() {
        buf.write_str("NULL")?;
        return Ok(());
    }

    if is_literal_quotable(datasource, lit) {
        buf.write_str("'")?;
    }
    match lit {
        ScalarValue::Boolean(Some(v)) => {
            if *v {
                buf.write_str("TRUE")?;
            } else {
                buf.write_str("FALSE")?;
            }
        }
        ScalarValue::Int8(Some(v)) => encode_int(buf, *v)?,
        ScalarValue::Int16(Some(v)) => encode_int(buf, *v)?,
        ScalarValue::Int32(Some(v)) => encode_int(buf, *v)?,
        ScalarValue::Int64(Some(v)) => encode_int(buf, *v)?,
        ScalarValue::Float32(Some(v)) => encode_float(buf, *v)?,
        ScalarValue::Float64(Some(v)) => encode_float(buf, *v)?,
        ScalarValue::Utf8(Some(v)) => encode_string(buf, v)?,
        ScalarValue::Binary(Some(v)) if datasource == Datasource::MySql => {
            encode_binary_mysql(buf, v)?
        }
        ScalarValue::Binary(Some(v)) if datasource == Datasource::Snowflake => {
            encode_binary_snowflake(buf, v)?
        }
        ScalarValue::Binary(Some(v)) => encode_binary(buf, v)?,
        ScalarValue::TimestampNanosecond(Some(v), tz) => {
            let naive = Utc.timestamp_nanos(*v).naive_utc();
            encode_utc_timestamp(buf, &naive, tz.is_some())?;
        }
        ScalarValue::TimestampMicrosecond(Some(v), tz) => {
            let naive = Utc.timestamp_micros(*v).unwrap().naive_utc();
            encode_utc_timestamp(buf, &naive, tz.is_some())?;
        }
        ScalarValue::TimestampMillisecond(Some(v), tz) => {
            let naive = Utc.timestamp_millis_opt(*v).unwrap().naive_utc();
            encode_utc_timestamp(buf, &naive, tz.is_some())?;
        }
        ScalarValue::TimestampSecond(Some(v), tz) => {
            let naive = Utc.timestamp_opt(*v, 0).unwrap().naive_utc();
            encode_utc_timestamp(buf, &naive, tz.is_some())?;
        }
        ScalarValue::Time64Nanosecond(Some(v)) => {
            let naive = Utc.timestamp_nanos(*v).naive_utc().time();
            encode_time(buf, &naive, /* tz = */ false)?;
        }
        ScalarValue::Time64Microsecond(Some(v)) => {
            let naive = Utc.timestamp_micros(*v).unwrap().naive_utc().time();
            encode_time(buf, &naive, /* tz = */ false)?;
        }
        ScalarValue::Date32(Some(v)) => {
            let epoch = Utc.timestamp_nanos(0).naive_utc().date();
            let naive = epoch
                .checked_add_signed(Duration::days(*v as i64))
                .expect("scalar value should be a valid date");
            encode_date(buf, &naive)?;
        }
        ScalarValue::Decimal128(Some(v), _precision, scale) => {
            let decimal = Decimal128::new(*v, *scale).expect("value should be a valid decimal128");
            encode_decimal(buf, &decimal)?;
        }
        s => {
            return Err(DatasourceCommonError::UnsupportedDatafusionScalar(
                s.data_type(),
            ))
        }
    };
    if is_literal_quotable(datasource, lit) {
        buf.write_str("'")?;
    }
    Ok(())
}

static DEFAULT_CAST_OPTIONS: Lazy<CastOptions> = Lazy::new(|| CastOptions {
    // If a cast fails we should rather report the error and fix it instead
    // of returning NULLs. This is a programming error.
    safe: false,
    format_options: FormatOptions::default(),
});

fn normalize_column(column: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let dt = match column.data_type() {
        DataType::Timestamp(TimeUnit::Second, tz)
        | DataType::Timestamp(TimeUnit::Millisecond, tz)
        | DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())
        }
        DataType::Time64(TimeUnit::Second)
        | DataType::Time64(TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond) => DataType::Time64(TimeUnit::Nanosecond),
        _ => return Ok(Arc::clone(column)), // No need of any conversion
    };

    let array = cast_with_options(column, &dt, &DEFAULT_CAST_OPTIONS)?;
    Ok(array)
}

/// Creates a new batch of records from the current batch by casting some
/// unsupported types to the ones we support.
///
/// For conversion mapping look at `normalize_column` function.
pub fn normalize_batch(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(batch.num_columns());
    for (field, col) in batch.schema().fields().iter().zip(batch.columns()) {
        let col = normalize_column(col)?;
        let field = Field::new(
            // Convert field name to lowercase since everything's case
            // insensitive for us.
            field.name().to_lowercase(),
            col.data_type().clone(),
            field.is_nullable(),
        );
        columns.push(col);
        fields.push(field);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}

pub static COUNT_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
});

pub fn create_count_record_batch(count: u64) -> RecordBatch {
    RecordBatch::try_new(
        COUNT_SCHEMA.clone(),
        vec![Arc::new(UInt64Array::from_value(
            count, /* rows = */ 1,
        ))],
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::{
        array::{
            Int32Builder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
            TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
        },
        datatypes::Schema,
    };

    use super::*;

    #[test]
    fn test_literal_encode() {
        struct TestCase {
            datasource: Datasource,
            literal: ScalarValue,
            expected: Option<&'static str>,
        }

        use Datasource::*;

        let cases = vec![
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Int8(Some(12)),
                expected: Some("12"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Int16(Some(123)),
                expected: Some("123"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Int32(Some(1234)),
                expected: Some("1234"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Int64(Some(12345)),
                expected: Some("12345"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Float32(Some(123.45)),
                expected: Some("123.45"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Float64(Some(12345.6789)),
                expected: Some("12345.6789"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Utf8(Some("abc".to_string())),
                expected: Some("'abc'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Binary(Some(b"abc".to_vec())),
                expected: Some("'\\x616263'"),
            },
            TestCase {
                datasource: MySql,
                literal: ScalarValue::Binary(Some(b"abc".to_vec())),
                expected: Some("0x616263"),
            },
            TestCase {
                datasource: Snowflake,
                literal: ScalarValue::Binary(Some(b"abc".to_vec())),
                expected: Some("'616263'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::TimestampNanosecond(Some(938709124 * 1_000_000_000), None),
                expected: Some("'1999-09-30 16:32:04'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::TimestampNanosecond(
                    Some(938709124 * 1_000_000_000),
                    Some("UTC".into()),
                ),
                expected: Some("'1999-09-30 16:32:04+00'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::TimestampMicrosecond(Some(938709124 * 1_000_000), None),
                expected: Some("'1999-09-30 16:32:04'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::TimestampMicrosecond(
                    Some(938709124 * 1_000_000),
                    Some("UTC".into()),
                ),
                expected: Some("'1999-09-30 16:32:04+00'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Time64Nanosecond(Some(59524 * 1_000_000_000)),
                expected: Some("'16:32:04'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Time64Microsecond(Some(59524 * 1_000_000)),
                expected: Some("'16:32:04'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Date32(Some(10_864)),
                expected: Some("'1999-09-30'"),
            },
            TestCase {
                datasource: Postgres,
                literal: ScalarValue::Decimal128(Some(123456), 38, 3),
                expected: Some("123.456"),
            },
        ];

        cases.into_iter().for_each(|case| {
            let mut buf = String::new();
            let res = encode_literal_to_text(case.datasource, &mut buf, &case.literal);
            match (res, case.expected) {
                (Ok(_), Some(s)) => assert_eq!(&buf, s),
                (Ok(_), None) => panic!("expected error, got result: {}", buf),
                (Err(e1), None) => {
                    let dt = case.literal.data_type();
                    assert!(matches!(e1, DatasourceCommonError::UnsupportedDatafusionScalar(ty) if ty == dt));
                }
                (Err(e), Some(s)) => panic!("expected result: {}, got error: {}", s, e),
            };
        });
    }

    #[test]
    fn test_batch_normalization() {
        let orig_fields = vec![
            Field::new("c1", DataType::Int32, true),
            Field::new(
                "c2",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "c3",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("c4", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("c5", DataType::Time64(TimeUnit::Nanosecond), true),
        ];

        let expected_fields = vec![
            Field::new("c1", DataType::Int32, true),
            Field::new(
                "c2",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "c3",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("c4", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new("c5", DataType::Time64(TimeUnit::Nanosecond), true),
        ];

        let orig_arrays: Vec<ArrayRef> = vec![
            {
                let mut c1 = Int32Builder::new();
                c1.append_value(1);
                Arc::new(c1.finish())
            },
            {
                let mut c2 = TimestampMicrosecondBuilder::new().with_data_type(
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                );
                c2.append_value(1);
                Arc::new(c2.finish())
            },
            {
                let mut c3 = TimestampNanosecondBuilder::new().with_data_type(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some("UTC".into()),
                ));
                c3.append_value(1);
                Arc::new(c3.finish())
            },
            {
                let mut c4 = Time64MicrosecondBuilder::new();
                c4.append_value(1);
                Arc::new(c4.finish())
            },
            {
                let mut c5 = Time64NanosecondBuilder::new();
                c5.append_value(1);
                Arc::new(c5.finish())
            },
        ];

        let expected_arrays: Vec<ArrayRef> =
            vec![
                {
                    let mut c1 = Int32Builder::new();
                    c1.append_value(1);
                    Arc::new(c1.finish())
                },
                {
                    let mut c2 = TimestampNanosecondBuilder::new().with_data_type(
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    );
                    c2.append_value(1_000);
                    Arc::new(c2.finish())
                },
                {
                    let mut c3 = TimestampNanosecondBuilder::new().with_data_type(
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    );
                    c3.append_value(1);
                    Arc::new(c3.finish())
                },
                {
                    let mut c4 = Time64NanosecondBuilder::new();
                    c4.append_value(1_000);
                    Arc::new(c4.finish())
                },
                {
                    let mut c5 = Time64NanosecondBuilder::new();
                    c5.append_value(1);
                    Arc::new(c5.finish())
                },
            ];

        let orig_schema = Schema::new(orig_fields);
        let orig_batch = RecordBatch::try_new(Arc::new(orig_schema), orig_arrays).unwrap();

        let res_batch = normalize_batch(&orig_batch).unwrap();

        assert_eq!(res_batch.schema().fields(), &expected_fields.into());
        assert_eq!(res_batch.columns(), &expected_arrays);
    }
}
