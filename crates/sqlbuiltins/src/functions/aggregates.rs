use datafusion::logical_expr::AggregateFunction;

use crate::builtins::BuiltinFunction;

// mostly using a macro here to preserve the formatting.
// rustfmt will otherwise compact the lines.
macro_rules! make_const {
    (
        $var_name:ident,
        name => $name:expr,
        example => $example:expr,
        description => $description:expr
    ) => {
        const $var_name: (&str, &str, &str) = ($name, $example, $description);
    };
}
make_const! {
    APPROX_DISTINCT,
    name => "approx_distinct",
    example => "approx_distinct(a)",
    description => "Gives the approximate count of distinct elements using HyperLogLog."
}

make_const!(
    APPROX_MEDIAN,
    name => "approx_median",
    example => "approx_median(a)",
    description => "Gives the approximate median of a column."
);

make_const!(
    APPROX_PERCENTILE_CONT,
    name => "approx_percentile_cont",
    example => "approx_percentile_cont(a)",
    description => "Gives the approximate percentile of a column"
);

make_const!(
    APPROX_PERCENTILE_CONT_WITH_WEIGHT,
    name => "approx_percentile_cont_with_weight",
    example => "approx_percentile_cont_with_weight(a)",
    description => "Gives the approximate percentile of a column with a weight column"
);

make_const!(
    ARRAY_AGG,
    name => "array_agg",
    example => "array_agg(a)",
    description => "Returns a list containing all the values of a column"
);
make_const!(
    AVG,
    name => "avg",
    example => "avg(a)",
    description => "Returns the average of a column"
);
make_const!(
    BIT_AND,
    name => "bit_and",
    example => "bit_and(a)",
    description => "Returns the bitwise AND of a column"
);
make_const!(
    BIT_OR,
    name => "bit_or",
    example => "bit_or(a)",
    description => "Returns the bitwise OR of a column"
);
make_const!(
    BIT_XOR,
    name => "bit_xor",
    example => "bit_xor(a)",
    description => "Returns the bitwise XOR of a column"
);
make_const!(
    BOOL_AND,
    name => "bool_and",
    example => "bool_and(a)",
    description => "Returns the boolean AND of a column"
);
make_const!(
    BOOL_OR,
    name => "bool_or",
    example => "bool_or(a)",
    description => "Returns the boolean OR of a column"
);
make_const!(
    CORRELATION,
    name => "correlation",
    example => "correlation(x, y)",
    description => "Returns the correlation coefficient of two columns"
);
make_const!(
    COUNT,
    name => "count",
    example => "count(a)",
    description => "Returns the number of rows in a column"
);
make_const!(
    COVARIANCE,
    name => "covariance",
    example => "covariance(x, y)",
    description => "Returns the covariance of two columns"
);
make_const!(
    COVARIANCE_POP,
    name => "covariance_pop",
    example => "covariance_pop(x, y)",
    description => "Returns the population covariance of two columns"
);
make_const!(
    FIRST_VALUE,
    name => "first_value",
    example => "first_value(a)",
    description => "Returns the first value in a column"
);
make_const!(
    GROUPING,
    name => "grouping",
    example => "grouping(a)",
    description => "Returns 1 if a column is aggregated, 0 otherwise"
);
make_const!(
    LAST_VALUE,
    name => "last_value",
    example => "last_value(a)",
    description => "Returns the last value in a column"
);
make_const!(
    MAX,
    name => "max",
    example => "max(a)",
    description => "Returns the maximum value in a column"
);
make_const!(
    MEDIAN,
    name => "median",
    example => "median(a)",
    description => "Returns the median value in a column"
);
make_const!(
    MIN,
    name => "min",
    example => "min(a)",
    description => "Returns the minimum value in a column"
);
make_const!(
    REGR_AVGX,
    name => "regr_avgx",
    example => "regr_avgx(y, x)",
    description => "Returns the average of the independent variable for non-null pairs in a group, where x is the independent variable and y is the dependent variable."
);
make_const!(
    REGR_AVGY,
    name => "regr_avgy",
    example => "regr_avgy(y, x)",
    description => "Returns the average of the dependent variable for non-null pairs in a group, where x is the independent variable and y is the dependent variable."
);
make_const!(
    REGR_COUNT,
    name => "regr_count",
    example => "regr_count(y, x)",
    description => "Returns the number of non-null number pairs in a group."
);
make_const!(
    REGR_INTERCEPT,
    name => "regr_intercept",
    example => "regr_intercept(y, x)",
    description => "Returns the intercept of the univariate linear regression line for non-null pairs in a group."
);
make_const!(
    REGR_R2,
    name => "regr_r2",
    example => "regr_r2(y, x)",
    description => "Returns the coefficient of determination (R-squared) for non-null pairs in a group."
);
make_const!(
    REGR_SLOPE,
    name => "regr_slope",
    example => "regr_slope(y, x)",
    description => "Returns the slope of the linear regression line for non-null pairs in a group."
);
make_const!(
    REGR_SXX,
    name => "regr_sxx",
    example => "regr_sxx(y, x)",
    description => "Returns the sum of squares of the independent variable for non-null pairs in a group."
);
make_const!(
    REGR_SXY,
    name => "regr_sxy",
    example => "regr_sxy(y, x)",
    description => "Returns the sum of products of independent times dependent variable for non-null pairs in a group."
);
make_const!(
    REGR_SYY,
    name => "regr_syy",
    example => "regr_syy(y, x)",
    description => "Returns the sum of squares of the dependent variable for non-null pairs in a group."
);
make_const!(
    STDDEV,
    name => "stddev",
    example => "stddev(a)",
    description => "Returns the sample standard deviation of a column"
);
make_const!(
    STDDEV_POP,
    name => "stddev_pop",
    example => "stddev_pop(a)",
    description => "Returns the population standard deviation of a column"
);
make_const!(
    SUM,
    name => "sum",
    example => "sum(a)",
    description => "Returns the sum of a column"
);
make_const!(
    VARIANCE,
    name => "variance",
    example => "variance(a)",
    description => "Returns the sample variance of a column"
);
make_const!(
    VARIANCE_POP,
    name => "variance_pop",
    example => "variance_pop(a)",
    description => "Returns the population variance of a column"
);

impl BuiltinFunction for AggregateFunction {
    fn name(&self) -> &str {
        use AggregateFunction::*;
        match self {
            ApproxDistinct => APPROX_DISTINCT.0,
            ApproxMedian => APPROX_MEDIAN.0,
            ApproxPercentileCont => APPROX_PERCENTILE_CONT.0,
            ApproxPercentileContWithWeight => APPROX_PERCENTILE_CONT_WITH_WEIGHT.0,
            ArrayAgg => ARRAY_AGG.0,
            Avg => AVG.0,
            BitAnd => BIT_AND.0,
            BitOr => BIT_OR.0,
            BitXor => BIT_XOR.0,
            BoolAnd => BOOL_AND.0,
            BoolOr => BOOL_OR.0,
            Correlation => CORRELATION.0,
            Count => COUNT.0,
            Covariance => COVARIANCE.0,
            CovariancePop => COVARIANCE_POP.0,
            FirstValue => FIRST_VALUE.0,
            Grouping => GROUPING.0,
            LastValue => LAST_VALUE.0,
            Max => MAX.0,
            Median => MEDIAN.0,
            Min => MIN.0,
            RegrAvgx => REGR_AVGX.0,
            RegrAvgy => REGR_AVGY.0,
            RegrCount => REGR_COUNT.0,
            RegrIntercept => REGR_INTERCEPT.0,
            RegrR2 => REGR_R2.0,
            RegrSlope => REGR_SLOPE.0,
            RegrSXX => REGR_SXX.0,
            RegrSXY => REGR_SXY.0,
            RegrSYY => REGR_SYY.0,
            Stddev => STDDEV.0,
            StddevPop => STDDEV_POP.0,
            Sum => SUM.0,
            Variance => VARIANCE.0,
            VariancePop => VARIANCE_POP.0,
        }
    }

    fn signature(&self) -> Option<datafusion::logical_expr::Signature> {
        Some(AggregateFunction::signature(self))
    }
    fn sql_example(&self) -> Option<String> {
        use AggregateFunction::*;
        Some(
            match self {
                ApproxDistinct => APPROX_DISTINCT.1,
                ApproxMedian => APPROX_MEDIAN.1,
                ApproxPercentileCont => APPROX_PERCENTILE_CONT.1,
                ApproxPercentileContWithWeight => APPROX_PERCENTILE_CONT_WITH_WEIGHT.1,
                ArrayAgg => ARRAY_AGG.1,
                Avg => AVG.1,
                BitAnd => BIT_AND.1,
                BitOr => BIT_OR.1,
                BitXor => BIT_XOR.1,
                BoolAnd => BOOL_AND.1,
                BoolOr => BOOL_OR.1,
                Correlation => CORRELATION.1,
                Count => COUNT.1,
                Covariance => COVARIANCE.1,
                CovariancePop => COVARIANCE_POP.1,
                FirstValue => FIRST_VALUE.1,
                Grouping => GROUPING.1,
                LastValue => LAST_VALUE.1,
                Max => MAX.1,
                Median => MEDIAN.1,
                Min => MIN.1,
                RegrAvgx => REGR_AVGX.1,
                RegrAvgy => REGR_AVGY.1,
                RegrCount => REGR_COUNT.1,
                RegrIntercept => REGR_INTERCEPT.1,
                RegrR2 => REGR_R2.1,
                RegrSlope => REGR_SLOPE.1,
                RegrSXX => REGR_SXX.1,
                RegrSXY => REGR_SXY.1,
                RegrSYY => REGR_SYY.1,
                Stddev => STDDEV.1,
                StddevPop => STDDEV_POP.1,
                Sum => SUM.1,
                Variance => VARIANCE.1,
                VariancePop => VARIANCE_POP.1,
            }
            .to_string(),
        )
    }
    fn description(&self) -> Option<String> {
        use AggregateFunction::*;
        Some(
            match self {
                ApproxDistinct => APPROX_DISTINCT.2,
                ApproxMedian => APPROX_MEDIAN.2,
                ApproxPercentileCont => APPROX_PERCENTILE_CONT.2,
                ApproxPercentileContWithWeight => APPROX_PERCENTILE_CONT_WITH_WEIGHT.2,
                ArrayAgg => ARRAY_AGG.2,
                Avg => AVG.2,
                BitAnd => BIT_AND.2,
                BitOr => BIT_OR.2,
                BitXor => BIT_XOR.2,
                BoolAnd => BOOL_AND.2,
                BoolOr => BOOL_OR.2,
                Correlation => CORRELATION.2,
                Count => COUNT.2,
                Covariance => COVARIANCE.2,
                CovariancePop => COVARIANCE_POP.2,
                FirstValue => FIRST_VALUE.2,
                Grouping => GROUPING.2,
                LastValue => LAST_VALUE.2,
                Max => MAX.2,
                Median => MEDIAN.2,
                Min => MIN.2,
                RegrAvgx => REGR_AVGX.2,
                RegrAvgy => REGR_AVGY.2,
                RegrCount => REGR_COUNT.2,
                RegrIntercept => REGR_INTERCEPT.2,
                RegrR2 => REGR_R2.2,
                RegrSlope => REGR_SLOPE.2,
                RegrSXX => REGR_SXX.2,
                RegrSXY => REGR_SXY.2,
                RegrSYY => REGR_SYY.2,
                Stddev => STDDEV.2,
                StddevPop => STDDEV_POP.2,
                Sum => SUM.2,
                Variance => VARIANCE.2,
                VariancePop => VARIANCE_POP.2,
            }
            .to_string(),
        )
    }
}
