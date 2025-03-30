pub mod avg;
pub mod bit_and;
pub mod bit_or;
pub mod bool_and;
pub mod bool_or;
pub mod corr;
pub mod count;
pub mod covar;
pub mod first;
pub mod minmax;
pub mod regr_avg;
pub mod regr_count;
pub mod regr_r2;
pub mod regr_slope;
pub mod stddev;
pub mod string_agg;
pub mod sum;

use avg::FUNCTION_SET_AVG;
use bit_and::FUNCTION_SET_BIT_AND;
use bit_or::FUNCTION_SET_BIT_OR;
use bool_and::FUNCTION_SET_BOOL_AND;
use bool_or::FUNCTION_SET_BOOL_OR;
use corr::FUNCTION_SET_CORR;
use count::FUNCTION_SET_COUNT;
use covar::{FUNCTION_SET_COVAR_POP, FUNCTION_SET_COVAR_SAMP};
use first::FUNCTION_SET_FIRST;
use minmax::{FUNCTION_SET_MAX, FUNCTION_SET_MIN};
use regr_avg::{FUNCTION_SET_REGR_AVG_X, FUNCTION_SET_REGR_AVG_Y};
use regr_count::FUNCTION_SET_REGR_COUNT;
use regr_r2::FUNCTION_SET_REGR_R2;
use regr_slope::FUNCTION_SET_REGR_SLOPE;
use stddev::{
    FUNCTION_SET_STDDEV_POP,
    FUNCTION_SET_STDDEV_SAMP,
    FUNCTION_SET_VAR_POP,
    FUNCTION_SET_VAR_SAMP,
};
use string_agg::FUNCTION_SET_STRING_AGG;
use sum::FUNCTION_SET_SUM;

use crate::functions::function_set::AggregateFunctionSet;

pub const BUILTIN_AGGREGATE_FUNCTION_SETS: &[AggregateFunctionSet] = &[
    FUNCTION_SET_SUM,
    FUNCTION_SET_AVG,
    FUNCTION_SET_COUNT,
    FUNCTION_SET_MIN,
    FUNCTION_SET_MAX,
    FUNCTION_SET_FIRST,
    FUNCTION_SET_STDDEV_POP,
    FUNCTION_SET_STDDEV_SAMP,
    FUNCTION_SET_VAR_POP,
    FUNCTION_SET_VAR_SAMP,
    FUNCTION_SET_COVAR_POP,
    FUNCTION_SET_COVAR_SAMP,
    FUNCTION_SET_CORR,
    FUNCTION_SET_REGR_COUNT,
    FUNCTION_SET_REGR_AVG_Y,
    FUNCTION_SET_REGR_AVG_X,
    FUNCTION_SET_REGR_R2,
    FUNCTION_SET_REGR_SLOPE,
    FUNCTION_SET_STRING_AGG,
    FUNCTION_SET_BOOL_AND,
    FUNCTION_SET_BOOL_OR,
    FUNCTION_SET_BIT_AND,
    FUNCTION_SET_BIT_OR,
];
