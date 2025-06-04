//! Simple example extension adding in some spark functions.
pub mod functions;

use functions::{FUNCTION_SET_CSC, FUNCTION_SET_EXPM1};
use glaredb_core::extension::{Extension, ExtensionFunctions};
use glaredb_core::functions::function_set::ScalarFunctionSet;

#[derive(Debug, Clone, Copy)]
pub struct SparkExtension;

impl Extension for SparkExtension {
    const NAME: &str = "spark";
    const FUNCTIONS: Option<&'static ExtensionFunctions> = Some(&ExtensionFunctions {
        namespace: "spark",
        scalar: &[&FUNCTION_SET_CSC, &FUNCTION_SET_EXPM1],
        aggregate: &[],
        table: &[],
    });
}
