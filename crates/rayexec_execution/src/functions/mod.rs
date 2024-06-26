pub mod aggregate;
pub mod implicit;
pub mod scalar;
pub mod table;

use fmtutil::IntoDisplayableSlice;
use implicit::implicit_cast_score;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::{RayexecError, Result};

/// Function signature.
#[derive(Debug, Clone, PartialEq)]
pub struct Signature {
    /// Expected input types for this signature.
    ///
    /// If the last data type is a list, this signature will be considered
    /// variadic.
    pub input: &'static [DataTypeId],

    /// The expected return type.
    ///
    /// Note that for some functions, this might return a compound type
    /// `DataType::Struct(TypeMeta::None)` which might require further
    /// refinement.
    pub return_type: DataTypeId,
}

impl Signature {
    /// Check if this signature is a variadic signature.
    pub const fn is_variadic(&self) -> bool {
        match self.input.last() {
            Some(id) => matches!(id, DataTypeId::List),
            None => false,
        }
    }

    /// Return if inputs given data types exactly satisfy the signature.
    fn exact_match(&self, inputs: &[DataType]) -> bool {
        if self.is_variadic() {
            unimplemented!()
        }

        if self.input.len() != inputs.len() {
            return false;
        }

        for (&expected, have) in self.input.iter().zip(inputs.iter()) {
            if expected == DataTypeId::Any {
                continue;
            }

            if have.datatype_id() != expected {
                return false;
            }
        }

        true
    }
}

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

    /// Signature for the function.
    ///
    /// This is used during binding/planning to determine the return type for a
    /// function given some inputs, and how we should handle implicit casting.
    fn signatures(&self) -> &[Signature];

    /// Get the return type for this function if the inputs have an exact
    /// signature match.
    ///
    /// If there are no exact signatures for these types, None will be retuned.
    ///
    /// This can be overridden to allow for working with more complex types
    /// (like extracting a field from a struct).
    fn return_type_for_inputs(&self, inputs: &[DataType]) -> Option<DataType> {
        let sig = self
            .signatures()
            .iter()
            .find(|sig| sig.exact_match(inputs))?;
        let datatype = DataType::try_default_datatype(sig.return_type).ok()?;

        Some(datatype)
    }

    /// Get candidate signatures for this function given the input datatypes.
    ///
    /// The returned candidates will have info on which arguments need to be
    /// casted and which are fine to state as-is.
    fn candidate(&self, inputs: &[DataType]) -> Vec<CandidateSignature> {
        CandidateSignature::find_candidates(inputs, self.signatures())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CastType {
    /// Need to cast the type to this one.
    Cast { to: DataTypeId, score: i32 },

    /// Casting isn't needed, the original data type works.
    NoCastNeeded,
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
    fn find_candidates(inputs: &[DataType], sigs: &[Signature]) -> Vec<Self> {
        let mut candidates = Vec::new();

        let mut buf = Vec::new();
        for (idx, sig) in sigs.iter().enumerate() {
            if sig.is_variadic() {
                unimplemented!()
            }

            if !Self::compare_and_fill_types(inputs, sig.input, &mut buf) {
                continue;
            }

            candidates.push(CandidateSignature {
                signature_idx: idx,
                casts: std::mem::take(&mut buf),
            })
        }

        candidates
    }

    /// Compare the types we have with the types we want, filling the provided
    /// buffer with the cast type.
    ///
    /// Returns true if everything is able to be implicitly cast, false otherwise.
    fn compare_and_fill_types(
        have: &[DataType],
        want: &[DataTypeId],
        buf: &mut Vec<CastType>,
    ) -> bool {
        if have.len() != want.len() {
            return false;
        }
        buf.clear();

        for (have, &want) in have.iter().zip(want.iter()) {
            if have.datatype_id() == want {
                buf.push(CastType::NoCastNeeded);
                continue;
            }

            let score = implicit_cast_score(have, want);
            if score > 0 {
                buf.push(CastType::Cast { to: want, score });
                continue;
            }

            return false;
        }

        true
    }
}

/// Check the number of arguments provided, erroring if it doesn't match the
/// expected number of arguments.
pub fn specialize_check_num_args(
    func: &impl FunctionInfo,
    inputs: &[DataType],
    expected: usize,
) -> Result<()> {
    if inputs.len() != expected {
        return Err(RayexecError::new(format!(
            "Expected {} input for '{}', received {}",
            expected,
            func.name(),
            inputs.len(),
        )));
    }
    Ok(())
}

/// Return an error indicating the input types we got are not ones we can
/// handle.
// TODO: Include valid signatures in the error
pub fn invalid_input_types_error(func: &impl FunctionInfo, got: &[&DataType]) -> RayexecError {
    RayexecError::new(format!(
        "Got invalid type(s) '{}' for '{}'",
        got.displayable(),
        func.name()
    ))
}
