// Allow `new` constructors for functions without an associated Default
// implementations.
//
// Functions must be able to be created via a constant context, and some
// functions defualt a `const fn new` to accomplish this. Functions are never
// created outside of a const context, so the Default implementation is useless.
#![allow(clippy::new_without_default)]

pub mod aggregate;
pub mod bind_state;
pub mod candidate;
pub mod documentation;
pub mod function_set;
pub mod implicit;
pub mod scalar;
pub mod table;

use std::borrow::Borrow;
use std::fmt::Display;

use candidate::CandidateSignature;
use glaredb_error::{DbError, Result};

use crate::arrays::datatype::{DataType, DataTypeId};
use crate::util::fmt::displayable::IntoDisplayableSlice;

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
}

/// Represents a named argument in the signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamedArgument {
    pub name: &'static str,
    pub arg: DataTypeId,
}

impl Signature {
    pub const fn new(inputs: &'static [DataTypeId], return_type: DataTypeId) -> Self {
        Signature {
            positional_args: inputs,
            variadic_arg: None,
            return_type,
        }
    }

    /// Check if this signature is a variadic signature.
    pub const fn is_variadic(&self) -> bool {
        self.variadic_arg.is_some()
    }

    /// Return if inputs given data types exactly satisfy the signature.
    fn exact_match(&self, inputs: &[DataType]) -> bool {
        if self.is_variadic() {
            // If function is variadic, we need at least the defined number of
            // positional arguments.
            if inputs.len() < self.positional_args.len() {
                return false;
            }
        } else {
            // If the function is not variadic, then positional args needs to be
            // exact.
            if inputs.len() != self.positional_args.len() {
                return false;
            }
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

/// Check the number of arguments provided, erroring if it doesn't match the
/// expected number of arguments.
pub fn plan_check_num_args<T>(
    func: &impl FunctionInfo,
    inputs: &[T],
    expected: usize,
) -> Result<()> {
    if inputs.len() != expected {
        return Err(DbError::new(format!(
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
        return Err(DbError::new(format!(
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
pub fn invalid_input_types_error<T>(func: &impl FunctionInfo, got: &[T]) -> DbError
where
    T: Borrow<DataType> + Display,
{
    // TODO: Include relevant valid signatures. What "relevant" means and how we
    // determine that is stil tbd.
    DbError::new(format!(
        "Got invalid type(s) '{}' for '{}'",
        got.display_with_brackets(),
        func.name()
    ))
}
