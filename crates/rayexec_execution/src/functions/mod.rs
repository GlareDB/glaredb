pub mod aggregate;
pub mod copy;
pub mod documentation;
pub mod implicit;
pub mod proto;
pub mod scalar;
pub mod table;

use std::borrow::Borrow;
use std::fmt::Display;

use documentation::Documentation;
use fmtutil::IntoDisplayableSlice;
use implicit::{implicit_cast_score, NO_CAST_SCORE};
use crate::arrays::datatype::{DataType, DataTypeId};
use rayexec_error::{RayexecError, Result};

/// Function signature.
// TODO: Include named args. Also make sure to update PartialEq too.
#[derive(Debug, Clone, Copy)]
pub struct Signature {
    /// Expected positional input argument types for this signature.
    pub positional_args: &'static [DataTypeId],

    /// Type of the variadic args if this function is variadic.
    ///
    /// If None, the function is not considered variadic.
    ///
    /// If the variadic type is `DataTypeId::Any`, and the user provides 1 or
    /// more variadic arguments, the signature will never be considered an exact
    /// match, and instead a candidate signature search will be triggered. This
    /// allows us to determine a single data type that all variadic args can be
    /// cast to, which simplifies planning and function implementation.
    pub variadic_arg: Option<DataTypeId>,

    /// The expected return type.
    ///
    /// This is purely informational (and could be used for documentation). The
    /// concrete data type is determined by the planned function, which is what
    /// gets used during planning.
    // TODO: Remove?
    pub return_type: DataTypeId,

    /// Optional documentation for this function.
    pub doc: Option<&'static Documentation>,
}

/// Represents a named argument in the signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamedArgument {
    pub name: &'static str,
    pub arg: DataTypeId,
}

impl Signature {
    pub const fn new_positional(input: &'static [DataTypeId], return_type: DataTypeId) -> Self {
        Signature {
            positional_args: input,
            variadic_arg: None,
            return_type,
            doc: None,
        }
    }

    /// Check if this signature is a variadic signature.
    pub const fn is_variadic(&self) -> bool {
        self.variadic_arg.is_some()
    }

    /// Return if inputs given data types exactly satisfy the signature.
    fn exact_match(&self, inputs: &[DataType]) -> bool {
        if self.positional_args.len() != inputs.len() && !self.is_variadic() {
            return false;
        }

        for (&expected, have) in self.positional_args.iter().zip(inputs.iter()) {
            if expected == DataTypeId::Any {
                continue;
            }

            if have.datatype_id() != expected {
                return false;
            }
        }

        // Check variadic.
        if let Some(expected) = self.variadic_arg {
            let remaining = &inputs[self.positional_args.len()..];
            for have in remaining {
                if expected == DataTypeId::Any {
                    // If we're matching against any, we're never an exact match.
                    return false;
                }

                if have.datatype_id() != expected {
                    return false;
                }
            }
        }

        true
    }
}

impl PartialEq for Signature {
    fn eq(&self, other: &Self) -> bool {
        self.positional_args == other.positional_args
            && self.variadic_arg == other.variadic_arg
            && self.return_type == other.return_type
    }
}

impl Eq for Signature {}

/// Trait for defining informating about functions.
pub trait FunctionInfo {
    /// Name of the function.
    fn name(&self) -> &'static str;

    /// Aliases for the function.
    ///
    /// When the system catalog is initialized, the function will be placed into
    /// the catalog using both its name and all of its aliases.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// A description for the function.
    ///
    /// Defaults to an empty string.
    fn description(&self) -> &'static str {
        ""
    }

    /// Signature for the function.
    ///
    /// This is used during binding/planning to determine the return type for a
    /// function given some inputs, and how we should handle implicit casting.
    fn signatures(&self) -> &[Signature];

    /// Get the signature for a function if it's an exact match for the inputs.
    ///
    /// If there are no exact signatures for these types, None will be retuned.
    fn exact_signature(&self, inputs: &[DataType]) -> Option<&Signature> {
        self.signatures().iter().find(|sig| sig.exact_match(inputs))
    }

    /// Get candidate signatures for this function given the input datatypes.
    ///
    /// The returned candidates will have info on which arguments need to be
    /// casted and which are fine to state as-is.
    ///
    /// Candidates are returned in sorted order with the highest cast score
    /// being first.
    fn candidate(&self, inputs: &[DataType]) -> Vec<CandidateSignature> {
        CandidateSignature::find_candidates(inputs, self.signatures())
    }
}

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
    fn find_candidates(inputs: &[DataType], sigs: &[Signature]) -> Vec<Self> {
        let mut candidates = Vec::new();

        let mut buf = Vec::new();
        for (idx, sig) in sigs.iter().enumerate() {
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
        have: &[DataType],
        want: &[DataTypeId],
        variadic: Option<DataTypeId>,
        buf: &mut Vec<CastType>,
    ) -> bool {
        if have.len() != want.len() && variadic.is_none() {
            return false;
        }
        buf.clear();

        for (have, &want) in have.iter().zip(want.iter()) {
            if have.datatype_id() == want {
                buf.push(CastType::NoCastNeeded);
                continue;
            }

            let score = implicit_cast_score(have, want);
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
                    if have.datatype_id() == expected {
                        buf.push(CastType::NoCastNeeded);
                        continue;
                    }

                    let score = implicit_cast_score(have, expected);
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

    /// Get the best common data type that we can cast to for the given inputs. Returns None
    /// if there isn't a common data type.
    fn best_datatype_for_variadic_any(inputs: &[DataType]) -> Option<DataTypeId> {
        let mut best_type = None;
        let mut best_total_score = 0;

        for input in inputs {
            let test_type = input.datatype_id();
            let mut total_score = 0;
            let mut valid = true;

            for input in inputs {
                if input.datatype_id() == test_type {
                    // Arbitrary.
                    total_score += 200;
                    continue;
                }

                let score = implicit_cast_score(input, test_type);
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

/// Check the number of arguments provided, erroring if it doesn't match the
/// expected number of arguments.
pub fn plan_check_num_args<T>(
    func: &impl FunctionInfo,
    inputs: &[T],
    expected: usize,
) -> Result<()> {
    if inputs.len() != expected {
        return Err(RayexecError::new(format!(
            "Expected {} {} for '{}', received {}",
            expected,
            if expected == 1 { "input" } else { "inputs" },
            func.name(),
            inputs.len(),
        )));
    }
    Ok(())
}

pub fn plan_check_num_args_one_of<T, const N: usize>(
    func: &impl FunctionInfo,
    inputs: &[T],
    one_of: [usize; N],
) -> Result<()> {
    if !one_of.contains(&inputs.len()) {
        return Err(RayexecError::new(format!(
            "Expected {} inputs for '{}', received {}",
            one_of.display_with_brackets(),
            func.name(),
            inputs.len(),
        )));
    }
    Ok(())
}

/// Return an error indicating the input types we got are not ones we can
/// handle.
// TODO: Include valid signatures in the error
pub fn invalid_input_types_error<T>(func: &impl FunctionInfo, got: &[T]) -> RayexecError
where
    T: Borrow<DataType> + Display,
{
    // TODO: Include relevant valid signatures. What "relevant" means and how we
    // determine that is stil tbd.
    RayexecError::new(format!(
        "Got invalid type(s) '{}' for '{}'",
        got.display_with_brackets(),
        func.name()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_candidate_no_match() {
        let inputs = &[DataType::Int64];
        let sigs = &[Signature {
            positional_args: &[DataTypeId::List],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
            doc: None,
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
            doc: None,
        }];

        let candidates = CandidateSignature::find_candidates(inputs, sigs);
        let expected = vec![CandidateSignature {
            signature_idx: 0,
            casts: vec![CastType::NoCastNeeded],
        }];

        assert_eq!(expected, candidates);
    }

    #[test]
    fn find_candidate_simple_with_variadic() {
        let inputs = &[DataType::Int64, DataType::Int64, DataType::Int64];
        let sigs = &[Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
            doc: None,
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
    fn best_datatype_for_ints_and_floats() {
        let inputs = &[DataType::Int64, DataType::Float64, DataType::Int64];
        let best = CandidateSignature::best_datatype_for_variadic_any(inputs);
        assert_eq!(Some(DataTypeId::Float64), best);
    }

    #[test]
    fn best_datatype_for_floats() {
        let inputs = &[DataType::Float64, DataType::Float64, DataType::Float64];
        let best = CandidateSignature::best_datatype_for_variadic_any(inputs);
        assert_eq!(Some(DataTypeId::Float64), best);
    }
}
