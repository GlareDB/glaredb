/// Function categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    General,
    Aggregate,
    Numeric,
    Date,
    Time,
    Interval,
    List,
    String,
    Regexp,
    Binary,
    Table,
    System,
}

/// Documentation for a single function variant.
// TODO: Allow multiple arguments+example pairs for different arity functions.
//
// Previously had docs on signatures, but that's unecessary.
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
