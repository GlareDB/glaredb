/// Function categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    /// Functions that are used for implementing SQL operators.
    Operator(OperatorCategory),
    Aggregate(AggregateCategory),
    Numeric,
    DateTime,
    List,
    String,
    Regexp,
    Binary,
    Table,
    System,
    Debug,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorCategory {
    Numeric,
    Comparison,
    Logical,
    Struct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateCategory {
    General,
    Statistics, // I don't know where you're getting your statistics
}

impl Category {
    pub const GENERAL_PURPOSE_AGGREGATE: Self = Category::Aggregate(AggregateCategory::General);
    pub const STATISTICS_AGGREGATE: Self = Category::Aggregate(AggregateCategory::Statistics);

    pub const NUMERIC_OPERATOR: Self = Category::Operator(OperatorCategory::Numeric);
    pub const COMPARISON_OPERATOR: Self = Category::Operator(OperatorCategory::Comparison);
    pub const LOGICAL_OPERATOR: Self = Category::Operator(OperatorCategory::Logical);
    pub const STRUCT_OPERATOR: Self = Category::Operator(OperatorCategory::Struct);

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Operator(OperatorCategory::Numeric) => "numeric_operator",
            Self::Operator(OperatorCategory::Comparison) => "comparison_operator",
            Self::Operator(OperatorCategory::Logical) => "logical_operator",
            Self::Operator(OperatorCategory::Struct) => "struct_operator",
            Self::Aggregate(AggregateCategory::General) => "general_purpose_aggregate",
            Self::Aggregate(AggregateCategory::Statistics) => "statistics_aggregate",
            Self::Numeric => "numeric",
            Self::DateTime => "datetime",
            Self::List => "list",
            Self::String => "string",
            Self::Regexp => "regexp",
            Self::Binary => "binary",
            Self::Table => "table",
            Self::System => "system",
            Self::Debug => "debug",
        }
    }
}

/// Documentation for a single function variant.
#[derive(Debug, Clone, Copy)]
pub struct Documentation {
    /// Category this function belongs in.
    pub category: Category,
    /// Short description of the function.
    pub description: &'static str,
    /// Argument names for this variant.
    ///
    /// If this doesn't match the length of the positional arguments in the
    /// signature, generic names will be used.
    pub arguments: &'static [&'static str],
    /// An optional example for the function.
    pub example: Option<Example>,
}

/// A simple example.
#[derive(Debug, Clone, Copy)]
pub struct Example {
    /// Example usage of the function.
    ///
    /// This should just be the function call itself and not an entire query.
    pub example: &'static str,
    /// The output for the above example.
    pub output: &'static str,
}
