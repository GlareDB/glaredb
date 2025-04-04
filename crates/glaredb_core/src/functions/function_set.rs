use super::aggregate::RawAggregateFunction;
use super::documentation::Documentation;
use super::scalar::RawScalarFunction;
use super::table::{RawTableFunction, TableFunctionType};
use super::{CandidateSignature, Signature};
use crate::arrays::datatype::DataType;

pub type ScalarFunctionSet = FunctionSet<RawScalarFunction>;
pub type AggregateFunctionSet = FunctionSet<RawAggregateFunction>;
pub type TableFunctionSet = FunctionSet<RawTableFunction>;

#[derive(Debug, Clone, Copy)]
pub struct FunctionSet<T: 'static> {
    /// Name of the function.
    pub name: &'static str,
    /// Set of aliases for this function.
    pub aliases: &'static [&'static str],
    /// Documentation for the function.
    ///
    /// If a function accepts different arities, then there should (ideally) be
    /// a documentation object for each.
    pub doc: &'static [&'static Documentation],
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

impl TableFunctionSet {
    /// Checks if any function in the function set is a scan function.
    ///
    /// This is used when resolving table functions, since for scans, we allow
    /// async binding. However async can only happen during the resolve step.
    ///
    /// This currently returns true if _any_ function is scan. We might need to
    /// tighten up semantics around this.
    pub fn is_scan_function(&self) -> bool {
        self.functions
            .iter()
            .any(|func| func.function_type() == TableFunctionType::Scan)
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

impl FunctionInfo for RawTableFunction {
    fn signature(&self) -> &Signature {
        RawTableFunction::signature(self)
    }
}
