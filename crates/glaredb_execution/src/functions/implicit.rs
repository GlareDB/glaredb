use crate::arrays::datatype::DataTypeId;

/// Score that should be used if no cast is needed.
pub const NO_CAST_SCORE: u32 = 800;

const FROM_STRING_CAST_SCORE: u32 = 200;

// TODO:
// String literal => allow from utf8 cast
// String var => disallow from cast
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImplicitCastConfig {
    /// Allow implicit casting from strings.
    pub allow_from_utf8: bool,
    /// Allow implicit casting to strings.
    pub allow_to_utf8: bool,
}

impl ImplicitCastConfig {
    /// Cast config to use for unions (and other set ops).
    ///
    /// Types for unions must fit into well-known domains.
    pub const UNION: Self = Self {
        allow_to_utf8: false,
        allow_from_utf8: false,
    };
}

/// Return the score for casting from `have` to `want`.
///
/// Returns None if there's not a valid implicit cast.
///
/// A higher score indicates a more preferred cast.
///
/// This is a best-effort attempt to determine if casting from one type to
/// another is valid and won't lose precision.
///
/// `allow_from_utf8` determines if we should allow implicit casting from strings.
pub const fn implicit_cast_score(
    have: DataTypeId,
    want: DataTypeId,
    conf: ImplicitCastConfig,
) -> Option<u32> {
    match have {
        // Cast NULL to anything.
        DataTypeId::Null => return Some(target_score(want)),
        // Simple integer casts.
        DataTypeId::Int8 => return int8_cast_score(want, conf),
        DataTypeId::Int16 => return int16_cast_score(want, conf),
        DataTypeId::Int32 => return int32_cast_score(want, conf),
        DataTypeId::Int64 => return int64_cast_score(want, conf),
        DataTypeId::UInt8 => return uint8_cast_score(want, conf),
        DataTypeId::UInt16 => return uint16_cast_score(want, conf),
        DataTypeId::UInt32 => return uint32_cast_score(want, conf),
        DataTypeId::UInt64 => return uint64_cast_score(want, conf),

        // Float casts
        DataTypeId::Float16 => return float16_cast_score(want, conf),
        DataTypeId::Float32 => return float32_cast_score(want, conf),
        DataTypeId::Float64 => return float64_cast_score(want, conf),

        // Decimal casts
        DataTypeId::Decimal64 => return decimal64_cast_score(want, conf),
        DataTypeId::Decimal128 => return decimal128_cast_score(want, conf),

        // String casts
        DataTypeId::Utf8 if conf.allow_from_utf8 => match want {
            DataTypeId::Int8
            | DataTypeId::Int16
            | DataTypeId::Int32
            | DataTypeId::Int64
            | DataTypeId::UInt8
            | DataTypeId::UInt16
            | DataTypeId::UInt32
            | DataTypeId::UInt64
            // | DataTypeId::Float16 // Exluded because this shouldn't be common. Unsure if this will change though.
            | DataTypeId::Float32
            | DataTypeId::Float64
            | DataTypeId::Decimal64
            | DataTypeId::Decimal128
            | DataTypeId::Interval
            | DataTypeId::Timestamp => return Some(FROM_STRING_CAST_SCORE),

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

const fn int8_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
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
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn int16_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::Int16
        | DataTypeId::Int32
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn int32_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::Int32
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn int64_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    // Note we don't allow implicit casting to Decimal64 (max precision
    // overflow).
    Some(match want {
        DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn uint8_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
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
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn uint16_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
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
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn uint32_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::UInt32
        | DataTypeId::UInt64
        | DataTypeId::Int64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn uint64_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::UInt64
        | DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn float16_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::Float16
        | DataTypeId::Float32
        | DataTypeId::Float64
        | DataTypeId::Decimal64
        | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn float32_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::Float64 | DataTypeId::Decimal64 | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn float64_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::Decimal64 | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn decimal64_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::Float32 | DataTypeId::Float64 | DataTypeId::Decimal128 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

const fn decimal128_cast_score(want: DataTypeId, conf: ImplicitCastConfig) -> Option<u32> {
    Some(match want {
        DataTypeId::Float32 | DataTypeId::Float64 => target_score(want),
        DataTypeId::Utf8 if conf.allow_to_utf8 => target_score(want),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implicit_cast_from_utf8() {
        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: true,
        };

        assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Int32, conf).is_some());
        assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Timestamp, conf).is_some());
        assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Interval, conf).is_some());
    }

    #[test]
    fn disallow_implicit_cast_from_utf8() {
        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: false,
        };

        assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Int32, conf).is_none());
        assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Timestamp, conf).is_none());
        assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Interval, conf).is_none());
    }

    #[test]
    fn disallow_implicit_to_utf8() {
        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: false,
        };

        // ...except when we're casting from utf8 utf8
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Utf8, conf).is_none());
        assert!(implicit_cast_score(DataTypeId::Timestamp, DataTypeId::Utf8, conf).is_none());
    }

    #[test]
    fn allow_implicit_to_utf8() {
        let conf = ImplicitCastConfig {
            allow_to_utf8: true,
            allow_from_utf8: false,
        };

        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Utf8, conf).is_some());
        assert!(implicit_cast_score(DataTypeId::Float64, DataTypeId::Utf8, conf).is_some());
    }

    #[test]
    fn integer_casts() {
        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: false,
        };

        // Valid
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Int64, conf).is_some());
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Decimal64, conf).is_some());
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Float32, conf).is_some());

        // Not valid
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::UInt64, conf).is_none());

        // Max precision for i64 too big.
        assert!(implicit_cast_score(DataTypeId::Int64, DataTypeId::Decimal64, conf).is_none());
    }

    #[test]
    fn float_casts() {
        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: false,
        };

        // Valid
        assert!(implicit_cast_score(DataTypeId::Float64, DataTypeId::Decimal64, conf).is_some());

        // Not valid
        assert!(implicit_cast_score(DataTypeId::Float64, DataTypeId::Int64, conf).is_none());
    }

    #[test]
    fn decimal_to_float_scores_higher_than_float_to_decimal() {
        // Mostly when it comes to arith. We want to prefer casting decimals to
        // floats instead of the other way around.
        //
        // This shouldn't impact decimal/decimal arith.

        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: false,
        };

        let d_to_f_score =
            implicit_cast_score(DataTypeId::Decimal64, DataTypeId::Float32, conf).unwrap();
        let f_to_d_score =
            implicit_cast_score(DataTypeId::Float32, DataTypeId::Decimal64, conf).unwrap();
        assert!(d_to_f_score > f_to_d_score);

        let d_to_f_score =
            implicit_cast_score(DataTypeId::Decimal64, DataTypeId::Float64, conf).unwrap();
        let f_to_d_score =
            implicit_cast_score(DataTypeId::Float64, DataTypeId::Decimal64, conf).unwrap();
        assert!(d_to_f_score > f_to_d_score);
    }

    #[test]
    fn prefer_cast_int32_to_int64() {
        // https://github.com/GlareDB/rayexec/issues/229
        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: false,
        };

        let to_int64_score =
            implicit_cast_score(DataTypeId::Int32, DataTypeId::Int64, conf).unwrap();
        let to_float32_score =
            implicit_cast_score(DataTypeId::Int32, DataTypeId::Float32, conf).unwrap();

        assert!(
            to_int64_score > to_float32_score,
            "int64: {to_int64_score}, float32: {to_float32_score}"
        );
    }
}
