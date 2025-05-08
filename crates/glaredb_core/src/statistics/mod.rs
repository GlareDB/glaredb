pub mod hll;
pub mod value;

pub mod assumptions {
    //! Assumptions when we don't have complete statistics available to us.

    /// Selectivity with '='.
    pub const EQUALITY_SELECTIVITY: f64 = 0.1;
    /// Selectivity with other comparison operators like '<', '>', '!=' etc.
    pub const INEQUALITY_SELECTIVITY: f64 = 0.3;
    /// Default selectivity to use if neither of the above apply.
    pub const DEFAULT_SELECTIVITY: f64 = 0.2;
}
