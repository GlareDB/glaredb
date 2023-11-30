// we make use of the document! macro to generate the documentation for the builtin functions.
// specifically the `stringify!` macro is used to get the name of the function.
// `Abs` would otherwise be `Abs` instead of `abs`. and so on.
#![allow(non_camel_case_types)]

use crate::{builtins::BuiltinFunction, document};
use datafusion::logical_expr::AggregateFunction;
use protogen::metastore::types::catalog::FunctionType;

document! {
    "Gives the approximate count of distinct elements using HyperLogLog.",
    "approx_distinct(a)",
    approx_distinct
}

document! {
    "Gives the approximate median of a column.",
    "approx_median(a)",
    approx_median
}

document! {
    "Gives the approximate percentile of a column",
    "approx_percentile_cont(a)",
    approx_percentile_cont
}

document! {
    "Gives the approximate percentile of a column with a weight column",
    "approx_percentile_cont_with_weight(a)",
    approx_percentile_cont_with_weight
}

document! {
    "Returns a list containing all the values of a column",
    "array_agg(a)",
    array_agg
}
document! {
    "Returns the average of a column",
    "avg(a)",
    avg
}
document! {
    "Returns the bitwise AND of a column",
    "bit_and(a)",
    bit_and
}
document! {
    "Returns the bitwise OR of a column",
    "bit_or(a)",
    bit_or
}
document! {
    "Returns the bitwise XOR of a column",
    "bit_xor(a)",
    bit_xor
}
document!(
    "Returns the boolean AND of a column",
    "bool_and(a)",
    bool_and
);
document! {
    "Returns the boolean OR of a column",
    "bool_or(a)",
    bool_or
}

document! {
    "Returns the correlation coefficient of two columns",
    "correlation(x, y)",
    correlation
}
document! {
    "Returns the number of rows in a column",
    "count(a)",
    count
}
document! {
    "Returns the covariance of two columns",
    "covariance(x, y)",
    covariance
}
document! {
    "Returns the population covariance of two columns",
    "covariance_pop(x, y)",
    covariance_pop
}
document! {
    "Returns the first value in a column",
    "first_value(a)",
    first_value
}
document! {
    "Returns 1 if a column is aggregated, 0 otherwise",
    "grouping(a)",
    grouping
}
document! {
    "Returns the last value in a column",
    "last_value(a)",
    last_value
}
document! {
    "Returns the maximum value in a column",
    "max(a)",
    max
}
document! {
    "Returns the median value in a column",
    "median(a)",
    median
}
document! {
    "Returns the minimum value in a column",
    "min(a)",
    min
}
document! {
    "Returns the average of the independent variable for non-null pairs in a group, where x is the independent variable and y is the dependent variable.",
    "regr_avgx(y, x)",
    regr_avgx
}

document! {
    "Returns the average of the dependent variable for non-null pairs in a group, where x is the independent variable and y is the dependent variable.",
    "regr_avgy(y, x)",
    regr_avgy
}

document! {
    "Returns the number of non-null number pairs in a group.",
    "regr_count(y, x)",
    regr_count
}

document! {
    "Returns the intercept of the univariate linear regression line for non-null pairs in a group.",
    "regr_intercept(y, x)",
    regr_intercept
}

document! {
    "Returns the coefficient of determination (R-squared) for non-null pairs in a group.",
    "regr_r2(y, x)",
    regr_r2
}

document! {
    "Returns the slope of the linear regression line for non-null pairs in a group.",
    "regr_slope(y, x)",
    regr_slope
}

document! {
    "Returns the sum of squares of the independent variable for non-null pairs in a group.",
    "regr_sxx(y, x)",
    regr_sxx
}

document! {
    "Returns the sum of products of independent times dependent variable for non-null pairs in a group.",
    "regr_sxy(y, x)",
    regr_sxy
}

document! {
    "Returns the sum of squares of the dependent variable for non-null pairs in a group.",
    "regr_syy(y, x)",
    regr_syy
}

document! {
    "Returns the sample standard deviation of a column",
    "stddev(a)",
    stddev
}

document! {
    "Returns the population standard deviation of a column",
    "stddev_pop(a)",
    stddev_pop
}

document! {
    "Returns the sum of a column",
    "sum(a)",
    sum
}
document! {
    "Returns the sample variance of a column",
    "variance(a)",
    variance
}

document! {
    "Returns the population variance of a column",
    "variance_pop(a)",
    variance_pop
}

impl BuiltinFunction for AggregateFunction {
    fn function_type(&self) -> FunctionType {
        FunctionType::Aggregate
    }
    fn name(&self) -> &'static str {
        use AggregateFunction::*;
        match self {
            ApproxDistinct => approx_distinct::NAME,
            ApproxMedian => approx_median::NAME,
            ApproxPercentileCont => approx_percentile_cont::NAME,
            ApproxPercentileContWithWeight => approx_percentile_cont_with_weight::NAME,
            ArrayAgg => array_agg::NAME,
            Avg => avg::NAME,
            BitAnd => bit_and::NAME,
            BitOr => bit_or::NAME,
            BitXor => bit_xor::NAME,
            BoolAnd => bool_and::NAME,
            BoolOr => bool_or::NAME,
            Correlation => correlation::NAME,
            Count => count::NAME,
            Covariance => covariance::NAME,
            CovariancePop => covariance_pop::NAME,
            FirstValue => first_value::NAME,
            Grouping => grouping::NAME,
            LastValue => last_value::NAME,
            Max => max::NAME,
            Median => median::NAME,
            Min => min::NAME,
            RegrAvgx => regr_avgx::NAME,
            RegrAvgy => regr_avgy::NAME,
            RegrCount => regr_count::NAME,
            RegrIntercept => regr_intercept::NAME,
            RegrR2 => regr_r2::NAME,
            RegrSlope => regr_slope::NAME,
            RegrSXX => regr_sxx::NAME,
            RegrSXY => regr_sxy::NAME,
            RegrSYY => regr_syy::NAME,
            Stddev => stddev::NAME,
            StddevPop => stddev_pop::NAME,
            Sum => sum::NAME,
            Variance => variance::NAME,
            VariancePop => variance_pop::NAME,
        }
    }

    fn signature(&self) -> Option<datafusion::logical_expr::Signature> {
        Some(AggregateFunction::signature(self))
    }
    fn sql_example(&self) -> Option<String> {
        use AggregateFunction::*;
        Some(
            match self {
                ApproxDistinct => approx_distinct::EXAMPLE,
                ApproxMedian => approx_median::EXAMPLE,
                ApproxPercentileCont => approx_percentile_cont::EXAMPLE,
                ApproxPercentileContWithWeight => approx_percentile_cont_with_weight::EXAMPLE,
                ArrayAgg => array_agg::EXAMPLE,
                Avg => avg::EXAMPLE,
                BitAnd => bit_and::EXAMPLE,
                BitOr => bit_or::EXAMPLE,
                BitXor => bit_xor::EXAMPLE,
                BoolAnd => bool_and::EXAMPLE,
                BoolOr => bool_or::EXAMPLE,
                Correlation => correlation::EXAMPLE,
                Count => count::EXAMPLE,
                Covariance => covariance::EXAMPLE,
                CovariancePop => covariance_pop::EXAMPLE,
                FirstValue => first_value::EXAMPLE,
                Grouping => grouping::EXAMPLE,
                LastValue => last_value::EXAMPLE,
                Max => max::EXAMPLE,
                Median => median::EXAMPLE,
                Min => min::EXAMPLE,
                RegrAvgx => regr_avgx::EXAMPLE,
                RegrAvgy => regr_avgy::EXAMPLE,
                RegrCount => regr_count::EXAMPLE,
                RegrIntercept => regr_intercept::EXAMPLE,
                RegrR2 => regr_r2::EXAMPLE,
                RegrSlope => regr_slope::EXAMPLE,
                RegrSXX => regr_sxx::EXAMPLE,
                RegrSXY => regr_sxy::EXAMPLE,
                RegrSYY => regr_syy::EXAMPLE,
                Stddev => stddev::EXAMPLE,
                StddevPop => stddev_pop::EXAMPLE,
                Sum => sum::EXAMPLE,
                Variance => variance::EXAMPLE,
                VariancePop => variance_pop::EXAMPLE,
            }
            .to_string(),
        )
    }
    fn description(&self) -> Option<String> {
        use AggregateFunction::*;
        Some(
            match self {
                ApproxDistinct => approx_distinct::DESCRIPTION,
                ApproxMedian => approx_median::DESCRIPTION,
                ApproxPercentileCont => approx_percentile_cont::DESCRIPTION,
                ApproxPercentileContWithWeight => approx_percentile_cont_with_weight::DESCRIPTION,
                ArrayAgg => array_agg::DESCRIPTION,
                Avg => avg::DESCRIPTION,
                BitAnd => bit_and::DESCRIPTION,
                BitOr => bit_or::DESCRIPTION,
                BitXor => bit_xor::DESCRIPTION,
                BoolAnd => bool_and::DESCRIPTION,
                BoolOr => bool_or::DESCRIPTION,
                Correlation => correlation::DESCRIPTION,
                Count => count::DESCRIPTION,
                Covariance => covariance::DESCRIPTION,
                CovariancePop => covariance_pop::DESCRIPTION,
                FirstValue => first_value::DESCRIPTION,
                Grouping => grouping::DESCRIPTION,
                LastValue => last_value::DESCRIPTION,
                Max => max::DESCRIPTION,
                Median => median::DESCRIPTION,
                Min => min::DESCRIPTION,
                RegrAvgx => regr_avgx::DESCRIPTION,
                RegrAvgy => regr_avgy::DESCRIPTION,
                RegrCount => regr_count::DESCRIPTION,
                RegrIntercept => regr_intercept::DESCRIPTION,
                RegrR2 => regr_r2::DESCRIPTION,
                RegrSlope => regr_slope::DESCRIPTION,
                RegrSXX => regr_sxx::DESCRIPTION,
                RegrSXY => regr_sxy::DESCRIPTION,
                RegrSYY => regr_syy::DESCRIPTION,
                Stddev => stddev::DESCRIPTION,
                StddevPop => stddev_pop::DESCRIPTION,
                Sum => sum::DESCRIPTION,
                Variance => variance::DESCRIPTION,
                VariancePop => variance_pop::DESCRIPTION,
            }
            .to_string(),
        )
    }
}
