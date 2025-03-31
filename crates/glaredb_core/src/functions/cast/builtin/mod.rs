pub mod to_primitive;
pub mod to_string;

use to_string::FUNCTION_SET_TO_STRING;

use super::CastFunctionSet;

pub const BUILTIN_CAST_FUNCTION_SETS: &[CastFunctionSet] = &[FUNCTION_SET_TO_STRING];
