// we make use of the document! macro to generate the documentation for the builtin functions.
// specifically the `stringify!` macro is used to get the name of the function.
// `Abs` would otherwise be `Abs` instead of `abs`. and so on.
#![allow(non_camel_case_types)]

use crate::{builtins::BuiltinFunction, document};
use datafusion::logical_expr::AggregateFunction;
use protogen::metastore::types::catalog::FunctionType;

document! {
    doc => "Gives the approximate count of distinct elements using HyperLogLog",
    example => "approx_distinct(a)",
    name => approx_distinct
}

document! {
    doc => "Gives the approximate median of a column",
    example => "approx_median(a)",
    name => approx_median
}

document! {
    doc => "Gives the approximate percentile of a column",
    example => "approx_percentile_cont(a)",
    name => approx_percentile_cont
}

document! {
    doc => "Gives the approximate percentile of a column with a weight column",
    example => "approx_percentile_cont_with_weight(a)",
    name => approx_percentile_cont_with_weight
}

document! {
    doc => "Returns a list containing all the values of a column",
    example => "array_agg(a)",
    name => array_agg
}
document! {
    doc => "Returns the average of a column",
    example => "avg(a)",
    name => avg
}
document! {
    doc => "Returns the bitwise AND of a column",
    example => "bit_and(a)",
    name => bit_and
}
document! {
    doc => "Returns the bitwise OR of a column",
    example => "bit_or(a)",
    name => bit_or
}
document! {
    doc => "Returns the bitwise XOR of a column",
    example => "bit_xor(a)",
    name => bit_xor
}
document!(
    doc => "Returns the boolean AND of a column",
    example => "bool_and(a)",
    name => bool_and
);
document! {
    doc => "Returns the boolean OR of a column",
    example => "bool_or(a)",
    name => bool_or
}

document! {
    doc => "Returns the correlation coefficient of two columns",
    example => "correlation(x, y)",
    name => correlation
}
document! {
    doc => "Returns the number of rows in a column",
    example => "count(a)",
    name => count
}
document! {
    doc => "Returns the covariance of two columns",
    example => "covariance(x, y)",
    name => covariance
}
document! {
    doc => "Returns the population covariance of two columns",
    example => "covariance_pop(x, y)",
    name => covariance_pop
}
document! {
    doc => "Returns the first value in a column",
    example => "first_value(a)",
    name => first_value
}
document! {
    doc => "Returns 1 if a column is aggregated, 0 otherwise",
    example => "grouping(a)",
    name => grouping
}
document! {
    doc => "Returns the last value in a column",
    example => "last_value(a)",
    name => last_value
}
document! {
    doc => "Returns the maximum value in a column",
    example => "max(a)",
    name => max
}
document! {
    doc => "Returns the median value in a column",
    example => "median(a)",
    name => median
}
document! {
    doc => "Returns the minimum value in a column",
    example => "min(a)",
    name => min
}
document! {
    doc => "Returns the average of the independent variable for non-null pairs in a group, where x is the independent variable and y is the dependent variable",
    example => "regr_avgx(y, x)",
    name => regr_avgx
}

document! {
    doc => "Returns the average of the dependent variable for non-null pairs in a group, where x is the independent variable and y is the dependent variable",
    example => "regr_avgy(y, x)",
    name => regr_avgy
}

document! {
    doc => "Returns the number of non-null number pairs in a group",
    example => "regr_count(y, x)",
    name => regr_count
}

document! {
    doc => "Returns the intercept of the univariate linear regression line for non-null pairs in a group",
    example => "regr_intercept(y, x)",
    name => regr_intercept
}

document! {
    doc => "Returns the coefficient of determination (R-squared) for non-null pairs in a group",
    example => "regr_r2(y, x)",
    name => regr_r2
}

document! {
    doc => "Returns the slope of the linear regression line for non-null pairs in a group",
    example => "regr_slope(y, x)",
    name => regr_slope
}

document! {
    doc => "Returns the sum of squares of the independent variable for non-null pairs in a group",
    example => "regr_sxx(y, x)",
    name => regr_sxx
}

document! {
    doc => "Returns the sum of products of independent times dependent variable for non-null pairs in a group",
    example => "regr_sxy(y, x)",
    name => regr_sxy
}

document! {
    doc => "Returns the sum of squares of the dependent variable for non-null pairs in a group",
    example => "regr_syy(y, x)",
    name => regr_syy
}

document! {
    doc => "Returns the sample standard deviation of a column",
    example => "stddev(a)",
    name => stddev
}

document! {
    doc => "Returns the population standard deviation of a column",
    example => "stddev_pop(a)",
    name => stddev_pop
}

document! {
    doc => "Returns the sum of a column",
    example => "sum(a)",
    name => sum
}
document! {
    doc => "Returns the sample variance of a column",
    example => "variance(a)",
    name => variance
}

document! {
    doc => "Returns the population variance of a column",
    example => "variance_pop(a)",
    name => variance_pop
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
