use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet};

/// "Builtin" functions that require special handling.
// TODO: This should be genericized into "rewrite rules" which ideally can be
// stored in the catalog (similar to what's in mind for CastFunctionSet).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialBuiltinFunction {
    /// UNNEST function for unnesting lists and structs.
    // TODO: This shouldn't even be a special function.
    //
    // When used as a table function (`SELECT * FROM unnest([1,2,3])`) this
    // isn't used, the proper table function is looked up.
    //
    // When used in a scalar context (e.g. select list), we should instead allow
    // _any_ table function to be used in a scalar context, and genericize the
    // current scalar unnest planning to cover that. E.g. `SELECT
    // generate_series(1, 5)` should just work without needing to specialize
    // anything.
    Unnest,
    /// GROUPING function for reporting the group of an expression in a grouping
    /// set.
    Grouping,
    /// COALESCE function for returning the first non-NULL argument.
    Coalesce,
}

impl SpecialBuiltinFunction {
    pub fn name(&self) -> &str {
        match self {
            Self::Unnest => "unnest",
            Self::Grouping => "grouping",
            Self::Coalesce => "coalesce",
        }
    }

    pub fn try_from_name(func_name: &str) -> Option<Self> {
        match func_name {
            "unnest" => Some(Self::Unnest),
            "grouping" => Some(Self::Grouping),
            "coalesce" => Some(Self::Coalesce),
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
