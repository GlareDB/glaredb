//! Simple example extension adding in some spark functions.
pub mod functions;

use functions::{FUNCTION_SET_CSC, FUNCTION_SET_EXPM1};
use glaredb_execution::extension::Extension;
use glaredb_execution::functions::function_set::ScalarFunctionSet;

pub struct SparkExtension;

impl Extension for SparkExtension {
    const NAME: &str = "spark";
    const FUNCTION_NAMESPACE: Option<&str> = Some("spark");

    fn scalar_functions(&self) -> &[ScalarFunctionSet] {
        &[FUNCTION_SET_CSC, FUNCTION_SET_EXPM1]
    }
}
