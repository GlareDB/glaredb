use super::cast::CastRule;
use super::cast::builtin::BUILTIN_CAST_FUNCTION_SETS;
use crate::arrays::datatype::DataTypeId;

/// Score that should be used if no cast is needed.
pub const NO_CAST_SCORE: u32 = 800;

/// Cast score to use when casting to any.
// TODO: Probably remove this.
const TO_ANY_SCORE: u32 = 10;

// TODO: Definitely remove this
const NULL_TO_LIST_SCORE: u32 = 10;

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
pub fn implicit_cast_score(have: DataTypeId, want: DataTypeId) -> Option<u32> {
    if want == DataTypeId::Any {
        return Some(TO_ANY_SCORE);
    }

    if have == DataTypeId::Null && matches!(want, DataTypeId::List(_)) {
        return Some(NULL_TO_LIST_SCORE);
    }

    println!("HAVE: {have} WANT {want}");

    // TODO: Pass in the function sets. This should be in the catalog.
    for cast_set in BUILTIN_CAST_FUNCTION_SETS {
        if cast_set.target == want {
            for cast_fn in cast_set.functions {
                if cast_fn.src == have {
                    if let CastRule::Implicit(score) = cast_fn.rule {
                        return Some(score);
                    }
                }
            }
        }
    }

    // No valid cast found.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implicit_cast_from_utf8() {
        assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Int32).is_some());
        // assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Timestamp, conf).is_some()); TODO
        // assert!(implicit_cast_score(DataTypeId::Utf8, DataTypeId::Interval, conf).is_some()); TODO
    }

    #[test]
    fn allow_implicit_to_utf8() {
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Utf8).is_some());
        assert!(implicit_cast_score(DataTypeId::Float64, DataTypeId::Utf8).is_some());
    }

    #[test]
    fn integer_casts() {
        // Valid
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Int64).is_some());
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Decimal64).is_some());
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::Float32).is_some());

        // Not valid
        assert!(implicit_cast_score(DataTypeId::Int16, DataTypeId::UInt64).is_none());

        // Max precision for i64 too big.
        assert!(implicit_cast_score(DataTypeId::Int64, DataTypeId::Decimal64).is_none());
    }

    #[test]
    fn float_casts() {
        // Valid
        assert!(implicit_cast_score(DataTypeId::Float64, DataTypeId::Decimal64).is_some());

        // Not valid
        assert!(implicit_cast_score(DataTypeId::Float64, DataTypeId::Int64).is_none());
    }

    #[test]
    fn decimal_to_float_scores_higher_than_float_to_decimal() {
        // Mostly when it comes to arith. We want to prefer casting decimals to
        // floats instead of the other way around.
        //
        // This shouldn't impact decimal/decimal arith.

        let d_to_f_score = implicit_cast_score(DataTypeId::Decimal64, DataTypeId::Float32).unwrap();
        let f_to_d_score = implicit_cast_score(DataTypeId::Float32, DataTypeId::Decimal64).unwrap();
        assert!(d_to_f_score > f_to_d_score);

        let d_to_f_score = implicit_cast_score(DataTypeId::Decimal64, DataTypeId::Float64).unwrap();
        let f_to_d_score = implicit_cast_score(DataTypeId::Float64, DataTypeId::Decimal64).unwrap();
        assert!(d_to_f_score > f_to_d_score);
    }

    #[test]
    fn prefer_cast_int32_to_int64() {
        // https://github.com/GlareDB/rayexec/issues/229

        let to_int64_score = implicit_cast_score(DataTypeId::Int32, DataTypeId::Int64).unwrap();
        let to_float32_score = implicit_cast_score(DataTypeId::Int32, DataTypeId::Float32).unwrap();

        assert!(
            to_int64_score > to_float32_score,
            "int64: {to_int64_score}, float32: {to_float32_score}"
        );
    }
}
