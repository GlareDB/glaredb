use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet};

/// "Builtin" functions that require special handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialBuiltinFunction {
    /// UNNEST function for unnesting lists and structs.
    Unnest,
    /// GROUPING function for reporting the group of an expression in a grouping
    /// set.
    Grouping,
}

impl SpecialBuiltinFunction {
    pub fn name(&self) -> &str {
        match self {
            Self::Unnest => "unnest",
            Self::Grouping => "grouping",
        }
    }

    pub fn try_from_name(func_name: &str) -> Option<Self> {
        match func_name {
            "unnest" => Some(Self::Unnest),
            "grouping" => Some(Self::Grouping),
            _ => None,
        }
    }
}

/// A resolved aggregate or scalar function.
#[derive(Debug, Clone)]
pub enum ResolvedFunction {
    Scalar(ScalarFunctionSet),
    Aggregate(AggregateFunctionSet),
    Special(SpecialBuiltinFunction),
}

impl ResolvedFunction {
    pub fn name(&self) -> &str {
        match self {
            Self::Scalar(f) => f.name,
            Self::Aggregate(f) => f.name,
            Self::Special(f) => f.name(),
        }
    }

    pub fn is_aggregate(&self) -> bool {
        matches!(self, ResolvedFunction::Aggregate(_))
    }
}
