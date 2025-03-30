use super::Signature;
use super::implicit::{ImplicitCastConfig, NO_CAST_SCORE, implicit_cast_score};
use crate::arrays::datatype::{DataType, DataTypeId};

#[derive(Debug, Clone, PartialEq)]
pub enum CastType {
    /// Need to cast the type to this one.
    Cast { to: DataTypeId, score: u32 },
    /// Casting isn't needed, the original data type works.
    NoCastNeeded,
}

impl CastType {
    fn score(&self) -> u32 {
        match self {
            Self::Cast { score, .. } => *score,
            Self::NoCastNeeded => NO_CAST_SCORE,
        }
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
        inputs: &[DataType],
        sigs: impl IntoIterator<Item = &'a Signature>,
    ) -> Vec<Self> {
        let mut candidates = Vec::new();
        let datatype_ids: Vec<_> = inputs.iter().map(|d| d.datatype_id()).collect();

        let mut buf = Vec::new();
        for (idx, sig) in sigs.into_iter().enumerate() {
            if !Self::compare_and_fill_types(
                &datatype_ids,
                sig.positional_args,
                sig.variadic_arg,
                &mut buf,
                ImplicitCastConfig {
                    allow_from_utf8: true,
                    allow_to_utf8: true,
                },
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
        have: &[DataTypeId],
        want: &[DataTypeId],
        variadic: Option<DataTypeId>,
        buf: &mut Vec<CastType>,
        conf: ImplicitCastConfig,
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

        for (&have, &want) in have.iter().zip(want.iter()) {
            if Self::no_cast_needed(&have, &want) {
                buf.push(CastType::NoCastNeeded);
                continue;
            }

            let score = implicit_cast_score(have, want, conf);
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
                    match Self::best_datatype_for_variadic_any(remaining, conf) {
                        Some(typ) => typ,
                        None => return false, // No common data type for all remaining args.
                    }
                } else {
                    expected
                };

                for have in remaining {
                    if Self::no_cast_needed(have, &expected) {
                        buf.push(CastType::NoCastNeeded);
                        continue;
                    }

                    let score = implicit_cast_score(*have, expected, conf);
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

    fn no_cast_needed(have: &DataTypeId, want: &DataTypeId) -> bool {
        match (have, want) {
            (&DataTypeId::List(_), &DataTypeId::List(&DataTypeId::Any)) => true,
            (_, &DataTypeId::Any) => true,
            (a, b) => a == b,
        }
    }

    /// Get the best common data type that we can cast to for the given inputs. Returns None
    /// if there isn't a common data type.
    fn best_datatype_for_variadic_any(
        inputs: &[DataTypeId],
        conf: ImplicitCastConfig,
    ) -> Option<DataTypeId> {
        let mut best_type = None;
        let mut best_total_score = 0;

        for input in inputs {
            let test_type = input;
            let mut total_score = 0;
            let mut valid = true;

            for input in inputs {
                if input == test_type {
                    // Arbitrary.
                    total_score += 200;
                    continue;
                }

                let score = implicit_cast_score(*input, *test_type, conf);
                match score {
                    Some(score) => total_score += score,
                    None => {
                        // Test type is not a valid cast for this input.
                        valid = false;
                    }
                }
            }

            if total_score > best_total_score && valid {
                best_type = Some(*test_type);
                best_total_score = total_score;
            }
        }

        best_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::ListTypeMeta;

    #[test]
    fn find_candidate_no_match() {
        let inputs = &[DataType::Int64];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List(&DataTypeId::Any)],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        let expected: Vec<CandidateSignature> = Vec::new();
        assert_eq!(expected, candidates);
    }

    #[test]
    fn find_candidate_simple_no_variadic() {
        let inputs = &[DataType::Int64];
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
        let inputs = &[DataType::List(ListTypeMeta::new(DataType::Int64))];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List(&DataTypeId::Int64)],
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
        let inputs = &[DataType::List(ListTypeMeta::new(DataType::Int64))];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List(&DataTypeId::Any)],
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

    #[test]
    fn find_candidate_no_match_list_inner_types_different() {
        let inputs = &[DataType::List(ListTypeMeta::new(DataType::Int64))];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List(&DataTypeId::Float64)],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        assert!(candidates.is_empty());
    }

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
        let inputs = &[DataType::Null];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List(&DataTypeId::Any)],
            variadic_arg: None,
            return_type: DataTypeId::Table,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        assert_eq!(1, candidates.len(), "candidates: {candidates:?}");
    }

    #[test]
    fn find_candidate_simple_with_variadic() {
        let inputs = &[DataType::Int64, DataType::Int64, DataType::Int64];
        let sigs = &[Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::List(&DataTypeId::Any),
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
        let inputs = &[DataType::Utf8, DataType::Utf8];
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

        let inputs = &[DataType::Utf8, DataType::Int32];
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
        let inputs = &[DataType::Float32, DataType::Utf8];

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
        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: false,
        };

        let inputs = &[DataTypeId::Int64, DataTypeId::Float64, DataTypeId::Int64];
        let best = CandidateSignature::best_datatype_for_variadic_any(inputs, conf);
        assert_eq!(Some(DataTypeId::Float64), best);
    }

    #[test]
    fn best_datatype_for_floats() {
        let conf = ImplicitCastConfig {
            allow_to_utf8: false,
            allow_from_utf8: false,
        };

        let inputs = &[
            DataTypeId::Float64,
            DataTypeId::Float64,
            DataTypeId::Float64,
        ];
        let best = CandidateSignature::best_datatype_for_variadic_any(inputs, conf);
        assert_eq!(Some(DataTypeId::Float64), best);
    }
}
