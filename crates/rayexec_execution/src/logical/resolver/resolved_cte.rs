// TODO: This might need some scoping information.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCte {
    /// Normalized name for the CTE.
    pub name: String,
    /// Depth this CTE was found at.
    pub depth: usize,
}

// TODO: Proto conv
