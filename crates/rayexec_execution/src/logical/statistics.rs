pub mod assumptions {
    //! Assumptions when we don't have complete statistics available to us.

    pub const EQUALITY_SELECTIVITY: f64 = 0.1;
    pub const RANGE_SELECTIVITY: f64 = 0.3;
    pub const BOOLEAN_SELECTIVITY: f64 = 0.5;

    pub const DEFAULT_SELECTIVITY: f64 = 0.3;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsCount {
    Exact(usize),
    Estimated(usize),
    Unknown,
}

impl StatisticsCount {
    pub fn value(self) -> Option<usize> {
        match self {
            Self::Exact(v) | Self::Estimated(v) => Some(v),
            Self::Unknown => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statistics {
    /// Cardinality of the operator.
    pub cardinality: StatisticsCount,
    /// Statistics for each column emitted by an operator.
    ///
    /// May be None if no column statistics are available.
    pub column_stats: Option<Vec<ColumnStatistics>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnStatistics {
    /// Number of distinct values in the column.
    pub num_distinct: StatisticsCount,
}

impl Statistics {
    pub const fn unknown() -> Self {
        Statistics {
            cardinality: StatisticsCount::Unknown,
            column_stats: None,
        }
    }
}
