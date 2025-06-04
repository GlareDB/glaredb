use std::fmt::{self, Display};

use super::aggregate::RawAggregateFunction;
use super::candidate::InputDataType;
use super::documentation::Documentation;
use super::scalar::RawScalarFunction;
use super::table::{RawTableFunction, TableFunctionType};
use super::{CandidateSignature, Signature};
use crate::arrays::datatype::DataTypeId;
use crate::util::fmt::displayable::IntoDisplayableSlice;

pub type ScalarFunctionSet = FunctionSet<RawScalarFunction>;
pub type AggregateFunctionSet = FunctionSet<RawAggregateFunction>;
pub type TableFunctionSet = FunctionSet<RawTableFunction>;

#[derive(Debug, Clone, Copy)]
pub struct FunctionSet<T: 'static> {
    /// Name of the function.
    pub name: FnName,
    /// Set of aliases for this function.
    pub aliases: &'static [FnName],
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
    pub fn find_exact(&self, inputs: &[DataTypeId]) -> Option<&T> {
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
    pub fn candidates(&self, inputs: &[InputDataType]) -> Vec<CandidateSignature> {
        CandidateSignature::find_candidates(
            inputs,
            self.functions.iter().map(|func| func.signature()),
        )
    }

    /// Get the function at the given index.
    pub fn get(&self, idx: usize) -> Option<&T> {
        self.functions.get(idx)
    }

    pub fn no_function_matches<'a>(
        &'a self,
        datatypes: &'a [InputDataType],
    ) -> NoFunctionMatches<'a, T> {
        NoFunctionMatches {
            datatypes,
            function: self,
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FnName {
    Default {
        name: &'static str,
    },
    Namespaced {
        schema: &'static str,
        name: &'static str,
    },
}

impl FnName {
    pub const fn default(name: &'static str) -> Self {
        FnName::Default { name }
    }

    pub const fn namespaced(schema: &'static str, name: &'static str) -> Self {
        FnName::Namespaced { schema, name }
    }
}

impl fmt::Display for FnName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default { name } => write!(f, "{}", name),
            Self::Namespaced { schema, name } => write!(f, "{}.{}", schema, name),
        }
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

#[derive(Debug)]
pub struct NoFunctionMatches<'a, T: 'static> {
    datatypes: &'a [InputDataType],
    function: &'a FunctionSet<T>,
}

impl<T> Display for NoFunctionMatches<'_, T>
where
    T: FunctionInfo + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "No function matches '{}({})'. You may need to add explicit type casts.",
            self.function.name,
            self.datatypes.display_as_list()
        )?;

        // Some functions have a lot of sigs (arith functions). Limit the number
        // we display to not be overwhelming.
        const MAX_NUM_SIGS: usize = 8;

        let count = usize::min(MAX_NUM_SIGS, self.function.functions.len());
        if count > 0 {
            write!(f, "\nCandidate functions:")?;
        }

        for func in self.function.functions.iter().take(count) {
            let sig = FunctionInfo::signature(func);
            write!(
                f,
                "\n    {}({}) -> {}",
                self.function.name,
                sig.positional_args.display_as_list(),
                sig.return_type,
            )?;
        }

        if count < self.function.functions.len() {
            let not_shown = self.function.functions.len() - count;
            write!(f, "\n    ...")?;
            write!(
                f,
                "\n   {not_shown} not shown. View the full list of functions in the catalog."
            )?;
        }

        Ok(())
    }
}
