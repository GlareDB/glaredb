use std::fmt;

use glaredb_error::Result;

use super::Signature;
use super::cast::DEFAULT_IMPLICIT_CAST_SCORES;
use super::implicit::{NO_CAST_SCORE, implicit_cast_score};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::scalar::ScalarValue;
use crate::expr::Expression;
use crate::expr::literal_expr::LiteralExpr;

#[derive(Debug, Clone, PartialEq)]
pub enum CastType {
    /// Need to cast the type to this one.
    Cast { to: DataTypeId, score: u32 },
    /// Casting isn't needed, the original data type works.
    NoCastNeeded,
    /// The literal value was refined, we can create a literal expression
    /// directly from the refined value.
    RefinedLiteral { refined: RefinedLiteral, score: u32 },
}

impl CastType {
    fn score(&self) -> u32 {
        match self {
            Self::Cast { score, .. } => *score,
            Self::NoCastNeeded => NO_CAST_SCORE,
            Self::RefinedLiteral { score, .. } => *score,
        }
    }
}

/// Bonus applied to the cast score when we successfully refine a literal.
pub const REFINED_LITERAL_SCORE_BONUS: u32 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefinedLiteral {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
}

/// Extract the literal value from an expression.
///
/// This is used since we will only read integer literals in queries as i32 or
/// i64, but sometimes that literal may cause unwanted casts if the expression
/// contains smaller integer sizes.
///
/// By extracting the literals, we can check to see if the literal can actually
/// fit in the smaller size, which may reduce the number of casts in a query.
#[derive(Debug, Clone, Copy)]
pub enum InputLiteral {
    Int32(i32),
    Int64(i64),
    /// Either not a literal, or a literal we don't care to handle.
    Unhandled,
}

/// A datatype with some additional metadata around if we're dealing with
/// literals.
#[derive(Debug)]
pub struct InputDataType {
    pub datatype: DataType,
    pub literal: InputLiteral,
}

impl InputDataType {
    pub fn try_from_expr(expr: &Expression) -> Result<Self> {
        let (datatype, literal) = match expr {
            Expression::Literal(LiteralExpr(ScalarValue::Int32(v))) => {
                (DataType::int32(), InputLiteral::Int32(*v))
            }
            Expression::Literal(LiteralExpr(ScalarValue::Int64(v))) => {
                (DataType::int64(), InputLiteral::Int64(*v))
            }
            other => (other.datatype()?, InputLiteral::Unhandled),
        };

        Ok(InputDataType { datatype, literal })
    }
}

impl From<DataType> for InputDataType {
    fn from(value: DataType) -> Self {
        InputDataType {
            datatype: value,
            literal: InputLiteral::Unhandled,
        }
    }
}

// Since it's used in the "no function found" error.
impl fmt::Display for InputDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.datatype)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateSignature {
    /// Index of the signature
    pub signature_idx: usize,
    /// Casts that would need to be applied in order to satisfy the signature.
    pub casts: Vec<CastType>,
}

impl CandidateSignature {
    /// Find candidate signatures for the given dataypes.
    ///
    /// This will return a sorted vec where the first element is the candidate
    /// with the highest score.
    pub fn find_candidates<'a>(
        inputs: &[InputDataType],
        sigs: impl IntoIterator<Item = &'a Signature>,
    ) -> Vec<Self> {
        // TODO: Track only top 'k' candidates to reduce some work being done
        // here. Whatever 'k' is should match the constant in
        // 'NoFunctionMatches'.

        let mut candidates = Vec::new();

        let mut buf = Vec::new();
        for (idx, sig) in sigs.into_iter().enumerate() {
            if !Self::compare_and_fill_types(
                inputs,
                sig.positional_args,
                sig.variadic_arg,
                &mut buf,
            ) {
                continue;
            }

            candidates.push(CandidateSignature {
                signature_idx: idx,
                casts: std::mem::take(&mut buf),
            })
        }

        candidates.sort_unstable_by(|a, b| {
            let a_score: u32 = a.casts.iter().map(|c| c.score()).sum();
            let b_score: u32 = b.casts.iter().map(|c| c.score()).sum();

            a_score.cmp(&b_score).reverse() // Higher score should be first.
        });

        candidates
    }

    /// Compare the types we have with the types we want, filling the provided
    /// buffer with the cast type.
    ///
    /// Returns true if everything is able to be implicitly cast, false otherwise.
    fn compare_and_fill_types(
        have: &[InputDataType],
        want: &[DataTypeId],
        variadic: Option<DataTypeId>,
        buf: &mut Vec<CastType>,
    ) -> bool {
        // TODO: This logic is duplicated with candidate exact match.
        if variadic.is_some() {
            if have.len() < want.len() {
                return false;
            }
        } else if have.len() != want.len() {
            return false;
        }

        buf.clear();

        for (have, &want) in have.iter().zip(want.iter()) {
            if Self::no_cast_needed(&have.datatype, want) {
                buf.push(CastType::NoCastNeeded);
                continue;
            }

            if let Some((refined, score)) = Self::try_refine_literal(have, want) {
                buf.push(CastType::RefinedLiteral { refined, score });
                continue;
            }

            let score = implicit_cast_score(have.datatype.id, want);
            if let Some(score) = score {
                buf.push(CastType::Cast { to: want, score });
                continue;
            }

            return false;
        }

        // Check variadic.
        let remaining = &have[want.len()..];
        match variadic {
            Some(expected) if !remaining.is_empty() => {
                let expected = if expected == DataTypeId::Any {
                    // Find a common data type to use in place of Any.
                    match Self::best_datatype_for_variadic_any(remaining) {
                        Some(typ) => typ,
                        None => return false, // No common data type for all remaining args.
                    }
                } else {
                    expected
                };

                for have in remaining {
                    if Self::no_cast_needed(&have.datatype, expected) {
                        buf.push(CastType::NoCastNeeded);
                        continue;
                    }

                    if let Some((refined, score)) = Self::try_refine_literal(have, expected) {
                        buf.push(CastType::RefinedLiteral { refined, score });
                        continue;
                    }

                    let score = implicit_cast_score(have.datatype.id, expected);
                    if let Some(score) = score {
                        buf.push(CastType::Cast {
                            to: expected,
                            score,
                        });
                        continue;
                    }

                    return false;
                }

                // Everything's valid, casts have been pushed to the buffer.
                true
            }
            _ => {
                // No variadics to check. If we got this far, everything's valid
                // for this signature.
                true
            }
        }
    }

    /// Try to refine an integer literal based on the type we want.
    ///
    /// This will only return a refined literal if the we can safely fit the
    /// literal in the new type, as well as the score for the cast.
    fn try_refine_literal(have: &InputDataType, want: DataTypeId) -> Option<(RefinedLiteral, u32)> {
        match (have.literal, want) {
            // Int32
            (InputLiteral::Int32(v), DataTypeId::Int8) => match i8::try_from(v) {
                Ok(v) => Some((
                    RefinedLiteral::Int8(v),
                    DEFAULT_IMPLICIT_CAST_SCORES.i8 + REFINED_LITERAL_SCORE_BONUS,
                )),
                Err(_) => None,
            },
            (InputLiteral::Int32(v), DataTypeId::Int16) => match i16::try_from(v) {
                Ok(v) => Some((
                    RefinedLiteral::Int16(v),
                    DEFAULT_IMPLICIT_CAST_SCORES.i16 + REFINED_LITERAL_SCORE_BONUS,
                )),
                Err(_) => None,
            },
            (InputLiteral::Int32(v), DataTypeId::Int64) => Some((
                RefinedLiteral::Int64(v as i64),
                DEFAULT_IMPLICIT_CAST_SCORES.i64 + REFINED_LITERAL_SCORE_BONUS,
            )),
            // Int64
            (InputLiteral::Int64(v), DataTypeId::Int8) => match i8::try_from(v) {
                Ok(v) => Some((
                    RefinedLiteral::Int8(v),
                    DEFAULT_IMPLICIT_CAST_SCORES.i8 + REFINED_LITERAL_SCORE_BONUS,
                )),
                Err(_) => None,
            },
            (InputLiteral::Int64(v), DataTypeId::Int16) => match i16::try_from(v) {
                Ok(v) => Some((
                    RefinedLiteral::Int16(v),
                    DEFAULT_IMPLICIT_CAST_SCORES.i16 + REFINED_LITERAL_SCORE_BONUS,
                )),
                Err(_) => None,
            },
            (InputLiteral::Int64(v), DataTypeId::Int32) => match i32::try_from(v) {
                Ok(v) => Some((
                    RefinedLiteral::Int32(v),
                    DEFAULT_IMPLICIT_CAST_SCORES.i32 + REFINED_LITERAL_SCORE_BONUS,
                )),
                Err(_) => None,
            },
            _ => None,
        }
    }

    fn no_cast_needed(have: &DataType, want: DataTypeId) -> bool {
        match (have.id, want) {
            (_, DataTypeId::Any) => true,
            (a, b) => a == b,
        }
    }

    /// Get the best common data type that we can cast to for the given inputs. Returns None
    /// if there isn't a common data type.
    fn best_datatype_for_variadic_any(inputs: &[InputDataType]) -> Option<DataTypeId> {
        let mut best_type = None;
        let mut best_total_score = 0;

        for input in inputs {
            let test_type = input.datatype.id;
            let mut total_score = 0;
            let mut valid = true;

            for input in inputs {
                if input.datatype.id == test_type {
                    // Arbitrary.
                    total_score += 200;
                    continue;
                }

                let score = implicit_cast_score(input.datatype.id, test_type);
                match score {
                    Some(score) => total_score += score,
                    None => {
                        // Test type is not a valid cast for this input.
                        valid = false;
                    }
                }
            }

            if total_score > best_total_score && valid {
                best_type = Some(test_type);
                best_total_score = total_score;
            }
        }

        best_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_candidate_no_match() {
        let inputs = &[DataType::int64().into()];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        let expected: Vec<CandidateSignature> = Vec::new();
        assert_eq!(expected, candidates);
    }

    #[test]
    fn find_candidate_simple_no_variadic() {
        let inputs = &[DataType::int64().into()];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::Int64],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        let expected = vec![CandidateSignature {
            signature_idx: 0,
            casts: vec![CastType::NoCastNeeded],
        }];

        assert_eq!(expected, candidates);
    }

    #[test]
    fn find_candidate_match_list_i64() {
        let inputs = &[DataType::list(DataType::int64()).into()];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        let expected = vec![CandidateSignature {
            signature_idx: 0,
            casts: vec![CastType::NoCastNeeded],
        }];

        assert_eq!(expected, candidates);
    }

    #[test]
    fn find_candidate_match_list_any() {
        let inputs = &[DataType::list(DataType::int64()).into()];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List],
            variadic_arg: None,
            return_type: DataTypeId::Any,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        let expected = vec![CandidateSignature {
            signature_idx: 0,
            casts: vec![CastType::NoCastNeeded],
        }];

        assert_eq!(expected, candidates);
    }

    // #[test]
    // fn find_candidate_no_match_list_inner_types_different() {
    //     let inputs = &[DataType::list(DataType::int64())];
    //     let sigs = &[Signature {
    //         positional_args: &[DataTypeId::List],
    //         variadic_arg: None,
    //         return_type: DataTypeId::Float64,
    //     }];

    //     let candidates = CandidateSignature::find_candidates(inputs, sigs);
    //     assert!(candidates.is_empty());
    // }

    #[test]
    fn find_candidate_match_list_with_null_input() {
        // SELECT * FROM unnest(NULL);
        //
        // NULL should be castable to a list.
        //
        // TDB if this is desired behavior.
        //
        // Postgres with null without types:
        // ```
        // select * from unnest(null);
        // ERROR:  function unnest(unknown) is not unique
        // ```
        //
        // However postgres executes it fine when provided `null::int[]` (and
        // produces zero rows).
        let inputs = &[DataType::null().into()];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List],
            variadic_arg: None,
            return_type: DataTypeId::Table,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        assert_eq!(1, candidates.len(), "candidates: {candidates:?}");
    }

    #[test]
    fn find_candidate_simple_with_variadic() {
        let inputs = &[
            DataType::int64().into(),
            DataType::int64().into(),
            DataType::int64().into(),
        ];
        let sigs = &[Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        let expected = vec![CandidateSignature {
            signature_idx: 0,
            casts: vec![
                CastType::NoCastNeeded,
                CastType::NoCastNeeded,
                CastType::NoCastNeeded,
            ],
        }];

        assert_eq!(expected, candidates);
    }

    #[test]
    fn find_candidate_utf8_positional_and_variadic() {
        let inputs = &[DataType::utf8().into(), DataType::utf8().into()];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: Some(DataTypeId::Utf8),
            return_type: DataTypeId::Utf8,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        let expected = vec![CandidateSignature {
            signature_idx: 0,
            casts: vec![CastType::NoCastNeeded, CastType::NoCastNeeded],
        }];

        assert_eq!(expected, candidates);
    }

    #[test]
    fn find_candidate_utf8_positional_and_variadic_cast_needed() {
        // SELECT concat('a', 1)
        //
        // Should want to cast Int32 to Utf8.

        let inputs = &[DataType::utf8().into(), DataType::int32().into()];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: Some(DataTypeId::Utf8),
            return_type: DataTypeId::Utf8,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        assert_eq!(1, candidates.len());

        assert!(matches!(candidates[0].casts[0], CastType::NoCastNeeded));
        assert!(matches!(
            candidates[0].casts[1],
            CastType::Cast {
                to: DataTypeId::Utf8,
                ..
            }
        ));
    }

    #[test]
    fn find_candidate_utf8_positional_and_variadic_no_args_no_match() {
        let inputs = &[];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: Some(DataTypeId::Utf8),
            return_type: DataTypeId::Utf8,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        assert!(candidates.is_empty());
    }

    #[test]
    fn find_candidate_float32_and_string() {
        // SELECT f32_col + '5.0'
        let inputs = &[DataType::float32().into(), DataType::utf8().into()];

        let sigs = &[Signature {
            positional_args: &[DataTypeId::Float32, DataTypeId::Float32],
            variadic_arg: None,
            return_type: DataTypeId::Float32,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        assert_eq!(1, candidates.len());
    }

    #[test]
    fn best_datatype_for_ints_and_floats() {
        let inputs = &[
            DataType::int64().into(),
            DataType::float64().into(),
            DataType::int64().into(),
        ];
        let best = CandidateSignature::best_datatype_for_variadic_any(inputs);
        assert_eq!(Some(DataTypeId::Float64), best);
    }

    #[test]
    fn best_datatype_for_floats() {
        let inputs = &[
            DataType::float64().into(),
            DataType::float64().into(),
            DataType::float64().into(),
        ];
        let best = CandidateSignature::best_datatype_for_variadic_any(inputs);
        assert_eq!(Some(DataTypeId::Float64), best);
    }
}
