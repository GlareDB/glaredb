use rayexec_bullet::datatype::{DataType, DataTypeId};

/// Return the score for casting from `have` to `want`.
///
/// A higher score indicates a more preferred cast.
///
/// This is a best-effort attempt to determine if casting from one type to
/// another is valid and won't lose precision.
pub const fn implicit_cast_score(have: &DataType, want: DataTypeId) -> i32 {
    // Cast NULL to anything.
    if have.is_null() {
        return target_score(want);
    }

    match have {
        // Simple integer casts.
        DataType::Int8 => return int8_cast_score(want),
        DataType::Int16 => return int16_cast_score(want),
        DataType::Int32 => return int32_cast_score(want),
        DataType::Int64 => return int64_cast_score(want),
        DataType::UInt8 => return uint8_cast_score(want),
        DataType::UInt16 => return uint16_cast_score(want),
        DataType::UInt32 => return uint32_cast_score(want),
        DataType::UInt64 => return uint64_cast_score(want),

        // Float casts
        DataType::Float32 => return float32_cast_score(want),
        DataType::Float64 => return float64_cast_score(want),

        // String casts
        DataType::Utf8 | DataType::LargeUtf8 => match want {
            DataTypeId::Int8
            | DataTypeId::Int16
            | DataTypeId::Int32
            | DataTypeId::Int64
            | DataTypeId::UInt8
            | DataTypeId::UInt16
            | DataTypeId::UInt32
            | DataTypeId::UInt64
            | DataTypeId::Decimal64
            | DataTypeId::Decimal128
            | DataTypeId::Interval
            | DataTypeId::Timestamp => return target_score(want),

            // Non-zero since it's a valid cast, just we would prefer something
            // else.
            DataTypeId::Utf8 | DataTypeId::LargeUtf8 => return 1,
            _ => (),
        },
        _ => (),
    }

    // No valid cast found.
    -1
}

/// Determine the score for the target type we can cast to.
///
/// More "specific" types will have a higher target score.
const fn target_score(target: DataTypeId) -> i32 {
    match target {
        DataTypeId::Utf8 => 1,
        DataTypeId::Int64 => 101,
        DataTypeId::UInt64 => 102,
        DataTypeId::Int32 => 111,
        DataTypeId::UInt32 => 112,
        DataTypeId::Int16 => 121,
        DataTypeId::UInt16 => 122,
        DataTypeId::Int8 => 131,
        DataTypeId::UInt8 => 132,
        DataTypeId::Float32 => 141,
        DataTypeId::Float64 => 142,
        DataTypeId::Decimal64 => 151,
        DataTypeId::Decimal128 => 152,
        _ => 100,
    }
}

const fn int8_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::Int8
        | DataTypeId::Int16
        | DataTypeId::Int32
        | DataTypeId::Int64
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn int16_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::Int16
        | DataTypeId::Int32
        | DataTypeId::Int64
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn int32_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::Int32
        | DataTypeId::Int64
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn int64_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::Int64
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn uint8_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::UInt8
        | DataTypeId::UInt16
        | DataTypeId::Int16
        | DataTypeId::UInt32
        | DataTypeId::Int32
        | DataTypeId::UInt64
        | DataTypeId::Int64
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn uint16_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::UInt16
        | DataTypeId::UInt32
        | DataTypeId::Int32
        | DataTypeId::UInt64
        | DataTypeId::Int64
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn uint32_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::UInt32
        | DataTypeId::UInt64
        | DataTypeId::Int64
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn uint64_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::UInt64
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn float32_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::Float64 | DataTypeId::Decimal64 | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

const fn float64_cast_score(want: DataTypeId) -> i32 {
    match want {
        DataTypeId::Decimal64 | DataTypeId::Decimal128 => target_score(want),
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::{TimeUnit, TimestampTypeMeta};

    use super::*;

    #[test]
    fn implicit_cast_from_utf8() {
        assert!(implicit_cast_score(&DataType::Utf8, DataTypeId::Int32) > 0);
        assert!(implicit_cast_score(&DataType::Utf8, DataTypeId::Timestamp) > 0);
        assert!(implicit_cast_score(&DataType::Utf8, DataTypeId::Interval) > 0);

        assert!(implicit_cast_score(&DataType::LargeUtf8, DataTypeId::Int32) > 0);
        assert!(implicit_cast_score(&DataType::LargeUtf8, DataTypeId::Timestamp) > 0);
        assert!(implicit_cast_score(&DataType::LargeUtf8, DataTypeId::Interval) > 0);
    }

    #[test]
    fn never_implicit_to_utf8() {
        // ...except when we're casting from utf8 utf8
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::Utf8) < 0);
        assert!(
            implicit_cast_score(
                &DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Millisecond)),
                DataTypeId::Utf8
            ) < 0
        );
    }

    #[test]
    fn integer_casts() {
        // Valid
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::Int64) > 0);
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::Decimal64) > 0);
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::Float32) > 0);

        // Not valid
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::UInt64) < 0);
    }

    #[test]
    fn float_casts() {
        // Valid
        assert!(implicit_cast_score(&DataType::Float64, DataTypeId::Decimal64) > 0);

        // Not valid
        assert!(implicit_cast_score(&DataType::Float64, DataTypeId::Int64) < 0);
    }
}
