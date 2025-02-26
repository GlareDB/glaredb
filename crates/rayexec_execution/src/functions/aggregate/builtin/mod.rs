pub mod avg;
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

use std::sync::LazyLock;

use super::AggregateFunction2;

pub static BUILTIN_AGGREGATE_FUNCTIONS: LazyLock<Vec<Box<dyn AggregateFunction2>>> =
    LazyLock::new(|| {
        vec![
            Box::new(sum::Sum),
            Box::new(avg::Avg),
            Box::new(count::Count),
            Box::new(minmax::Min),
            Box::new(minmax::Max),
            Box::new(first::First),
            Box::new(stddev::StddevPop),
            Box::new(stddev::StddevSamp),
            Box::new(stddev::VarPop),
            Box::new(stddev::VarSamp),
            Box::new(covar::CovarPop),
            Box::new(covar::CovarSamp),
            Box::new(corr::Corr),
            Box::new(regr_count::RegrCount),
            Box::new(regr_avg::RegrAvgY),
            Box::new(regr_avg::RegrAvgX),
            Box::new(regr_r2::RegrR2),
            Box::new(regr_slope::RegrSlope),
            Box::new(string_agg::StringAgg),
        ]
    });
