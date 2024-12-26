use crate::arrays::datatype::{DataType, DataTypeId};

/// Score that should be used if no cast is needed.
pub const NO_CAST_SCORE: u32 = 400;

/// Return the score for casting from `have` to `want`.
///
/// Returns None if there's not a valid implicit cast.
///
/// A higher score indicates a more preferred cast.
///
/// This is a best-effort attempt to determine if casting from one type to
/// another is valid and won't lose precision.
pub const fn implicit_cast_score(have: &DataType, want: DataTypeId) -> Option<u32> {
    // Cast NULL to anything.
    if have.is_null() {
        return Some(target_score(want));
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
        DataType::Float16 => return float16_cast_score(want),
        DataType::Float32 => return float32_cast_score(want),
        DataType::Float64 => return float64_cast_score(want),

        // String casts
        DataType::Utf8 => match want {
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
            | DataTypeId::Timestamp => return Some(target_score(want)),

            // Non-zero since it's a valid cast, just we would prefer something
            // else.
            DataTypeId::Utf8 => return Some(1),
            _ => (),
        },
        _ => (),
    }

    // No valid cast found.
    None
}

/// Determine the score for the target type we can cast to.
///
/// More "specific" types will have a higher target score.
const fn target_score(target: DataTypeId) -> u32 {
    match target {
        DataTypeId::Int8 => 191,
        DataTypeId::UInt8 => 190,
        DataTypeId::Int16 => 181,
        DataTypeId::UInt16 => 180,
        DataTypeId::Int32 => 171,
        DataTypeId::UInt32 => 170,
        DataTypeId::Int64 => 161,
        DataTypeId::UInt64 => 160,
        DataTypeId::Float16 => 152,
        DataTypeId::Float32 => 151,
        DataTypeId::Float64 => 141,
        DataTypeId::Decimal64 => 131,
        DataTypeId::Decimal128 => 121,
        DataTypeId::Utf8 => 1,
        _ => 100,
    }
}

const fn int8_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::Int8
        | DataTypeId::Int16
        | DataTypeId::Int32
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn int16_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::Int16
        | DataTypeId::Int32
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn int32_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::Int32
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn int64_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn uint8_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::UInt8
        | DataTypeId::UInt16
        | DataTypeId::Int16
        | DataTypeId::UInt32
        | DataTypeId::Int32
        | DataTypeId::UInt64
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn uint16_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::UInt16
        | DataTypeId::UInt32
        | DataTypeId::Int32
        | DataTypeId::UInt64
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn uint32_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::UInt32
        | DataTypeId::UInt64
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn uint64_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::UInt64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn float16_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn float32_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::Float64 | DataTypeId::Decimal64 | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

const fn float64_cast_score(want: DataTypeId) -> Option<u32> {
    Some(match want {
        DataTypeId::Decimal64 | DataTypeId::Decimal128 => target_score(want),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use crate::arrays::datatype::{TimeUnit, TimestampTypeMeta};

    use super::*;

    #[test]
    fn implicit_cast_from_utf8() {
        assert!(implicit_cast_score(&DataType::Utf8, DataTypeId::Int32).is_some());
        assert!(implicit_cast_score(&DataType::Utf8, DataTypeId::Timestamp).is_some());
        assert!(implicit_cast_score(&DataType::Utf8, DataTypeId::Interval).is_some());
    }

    #[test]
    fn never_implicit_to_utf8() {
        // ...except when we're casting from utf8 utf8
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::Utf8).is_none());
        assert!(implicit_cast_score(
            &DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Millisecond)),
            DataTypeId::Utf8
        )
        .is_none());
    }

    #[test]
    fn integer_casts() {
        // Valid
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::Int64).is_some());
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::Decimal64).is_some());
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::Float32).is_some());

        // Not valid
        assert!(implicit_cast_score(&DataType::Int16, DataTypeId::UInt64).is_none());
    }

    #[test]
    fn float_casts() {
        // Valid
        assert!(implicit_cast_score(&DataType::Float64, DataTypeId::Decimal64).is_some());

        // Not valid
        assert!(implicit_cast_score(&DataType::Float64, DataTypeId::Int64).is_none());
    }

    #[test]
    fn prefer_cast_int32_to_int64() {
        // https://github.com/GlareDB/rayexec/issues/229

        let to_int64_score = implicit_cast_score(&DataType::Int32, DataTypeId::Int64).unwrap();
        let to_float32_score = implicit_cast_score(&DataType::Int32, DataTypeId::Float32).unwrap();

        assert!(
            to_int64_score > to_float32_score,
            "int64: {to_int64_score}, float32: {to_float32_score}"
        );
    }
}
