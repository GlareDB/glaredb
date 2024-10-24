use std::cmp::Ordering;
use std::fmt;

pub mod assumptions {
    //! Assumptions when we don't have complete statistics available to us.

    /// Selectivity with '='.
    pub const EQUALITY_SELECTIVITY: f64 = 0.1;
    /// Selectivity with other comparison operators like '<', '>', '!=' etc.
    pub const INEQUALITY_SELECTIVITY: f64 = 0.3;
    /// Default selectivity to use if neither of the above apply.
    pub const DEFAULT_SELECTIVITY: f64 = 0.2;
}

#[derive(Debug, Clone, Default, Copy, PartialEq, Eq)]
pub enum StatisticsValue<T> {
    /// Exact value is known.
    Exact(T),
    /// Value is estimated.
    Estimated(T),
    /// Value is unknown.
    #[default]
    Unknown,
}

impl<T> StatisticsValue<T> {
    /// Try to get a value if available.
    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Exact(v) | Self::Estimated(v) => Some(v),
            Self::Unknown => None,
        }
    }
}

impl<T> PartialOrd for StatisticsValue<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.value(), other.value()) {
            (Some(a), Some(b)) => a.partial_cmp(b),
            (Some(_), None) => Some(Ordering::Greater),
            (None, Some(_)) => Some(Ordering::Less),
            (None, None) => Some(Ordering::Equal),
        }
    }
}

impl<T> Ord for StatisticsValue<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<T> fmt::Display for StatisticsValue<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(v) => write!(f, "{v}"),
            Self::Estimated(v) => write!(f, "{v} [estimated]"),
            Self::Unknown => write!(f, "[unknown]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statistics {
    /// Cardinality of the operator.
    pub cardinality: StatisticsValue<usize>,
    /// Statistics for each column emitted by an operator.
    ///
    /// May be None if no column statistics are available.
    pub column_stats: Option<Vec<ColumnStatistics>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnStatistics {
    /// Number of distinct values in the column.
    pub num_distinct: StatisticsValue<usize>,
}

impl Statistics {
    pub const fn unknown() -> Self {
        Statistics {
            cardinality: StatisticsValue::Unknown,
            column_stats: None,
        }
    }
}
