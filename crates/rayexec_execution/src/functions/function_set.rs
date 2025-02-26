use super::aggregate::RawAggregateFunction;
use super::documentation::Documentation;
use super::scalar::RawScalarFunction;
use super::{CandidateSignature, Signature};
use crate::arrays::datatype::DataType;

pub type ScalarFunctionSet = FunctionSet<RawScalarFunction>;
pub type AggregateFunctionSet = FunctionSet<RawAggregateFunction>;

#[derive(Debug, Clone, Copy)]
pub struct FunctionSet<T: 'static> {
    /// Name of the function.
    pub name: &'static str,
    /// Set of aliases for this function.
    pub aliases: &'static [&'static str],
    /// Optional documentation for the function.
    pub doc: Option<&'static Documentation>,
    /// The function implementations.
    pub functions: &'static [T],
}

impl<T> FunctionSet<T>
where
    T: FunctionInfo,
{
    /// Get a reference to a function that has an exact signature match for the
    /// given positional inputs.
    ///
    /// If no signatures match (e.g. incorrect number of args, or args need to
    /// be casted), None will be returned.
    pub fn find_exact(&self, inputs: &[DataType]) -> Option<&T> {
        self.functions
            .iter()
            .find(|func| func.signature().exact_match(inputs))
    }

    /// Get candidate signatures for this function given the input datatypes.
    ///
    /// The returned candidates will have info on which arguments need to be
    /// casted and which are fine to state as-is.
    ///
    /// Candidates are returned in sorted order with the highest cast score
    /// being first.
    pub fn candidates(&self, inputs: &[DataType]) -> Vec<CandidateSignature> {
        CandidateSignature::find_candidates(
            inputs,
            self.functions.iter().map(|func| func.signature()),
        )
    }

    /// Get the function at the given index.
    pub fn get(&self, idx: usize) -> Option<&T> {
        self.functions.get(idx)
    }
}

pub trait FunctionInfo: Sized + Copy {
    fn signature(&self) -> &Signature;
}

impl FunctionInfo for RawScalarFunction {
    fn signature(&self) -> &Signature {
        RawScalarFunction::signature(self)
    }
}

impl FunctionInfo for RawAggregateFunction {
    fn signature(&self) -> &Signature {
        RawAggregateFunction::signature(self)
    }
}
