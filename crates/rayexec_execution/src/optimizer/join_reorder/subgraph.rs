use super::edge::NeighborEdge;
use crate::expr::comparison_expr::ComparisonOperator;

#[derive(Debug, Clone, Copy)]
pub struct Subgraph {
    /// Computed numerator thus far.
    ///
    /// Product of all base relation cardinalities in this subgraph.
    pub numerator: f64,
    /// The computed denominator thus far.
    ///
    /// Computed by multiplying the min NDV for all edges involved in the join.
    pub selectivity_denom: f64,
}

impl Subgraph {
    pub fn estimated_cardinality(&self) -> f64 {
        self.numerator / self.selectivity_denom
    }

    pub fn update_numerator(&mut self, other: &Subgraph) {
        self.numerator *= other.numerator
    }

    /// Updates this subgraph's selectivity denominator by an implied join from
    /// `other` subgraph.
    pub fn update_denom(&mut self, other: &Subgraph, edge: &NeighborEdge) {
        // Only update the denominator (selectivity) if we have a join
        // condition. If we don't, we should assume cross join and not make this
        // join more selective (and thus higher cost).
        let op = match edge.edge_op {
            Some(op) => op,
            None => return,
        };

        let mut denom = self.selectivity_denom * other.selectivity_denom;

        match op {
            ComparisonOperator::Eq => {
                // =
                denom *= edge.min_ndv
            }
            ComparisonOperator::NotEq => {
                denom *= 0.1 // Assuming 10% selectivity for !=
            }
            ComparisonOperator::Lt
            | ComparisonOperator::Gt
            | ComparisonOperator::LtEq
            | ComparisonOperator::GtEq => {
                // For range joins, adjust selectivity. Assuming 1/3rd of
                // the data falls into the range.
                denom *= 3.0
            }
        }

        self.selectivity_denom = denom;
    }
}
