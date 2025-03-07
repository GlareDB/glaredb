/// Scan projections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projections {
    /// Column indices to project out of the scan.
    ///
    /// If None, project all columns.
    pub column_indices: Option<Vec<usize>>,
}

impl Projections {
    pub const fn all() -> Self {
        Projections {
            column_indices: None,
        }
    }
}
