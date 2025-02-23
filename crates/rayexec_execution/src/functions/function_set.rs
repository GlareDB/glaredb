use super::documentation::Documentation;
use super::scalar::RawScalarFunction;

pub type ScalarFunctionSet = FunctionSet<RawScalarFunction>;

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
