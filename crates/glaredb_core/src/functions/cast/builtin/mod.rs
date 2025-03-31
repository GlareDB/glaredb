use to_string::FUNCTION_SET_TO_STRING;

use super::CastFunctionSet;

pub mod to_string;

pub const BUILTIN_CAST_FUNCTION_SETS: &[CastFunctionSet] = &[FUNCTION_SET_TO_STRING];
