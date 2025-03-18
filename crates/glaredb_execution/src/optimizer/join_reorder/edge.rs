use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::ControlFlow;

use glaredb_error::{DbError, Result};

use super::graph::{BaseRelation, RelId, RelationSet};
use super::ReorderableCondition;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode};
use crate::expr::column_expr::ColumnReference;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::logical::binder::table_list::TableRef;

pub type HyperEdgeId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId {
    pub hyper_edge_id: HyperEdgeId,
    pub edge_id: usize,
}

/// All hyper edges for the graph.
#[derive(Debug)]
pub struct HyperEdges(pub Vec<HyperEdge>);

/// Hyper edge connecting two or more relations in the graph.
#[derive(Debug)]
pub struct HyperEdge {
    pub id: HyperEdgeId,
    /// All distinct edges making up this hyper edge.
    pub edges: HashMap<EdgeId, Edge>,
    /// Minimum num distinct values across all relations connected by this hyper
    /// edge.
    ///
    /// This is the basis for our cardinality estimate.
    pub min_ndv: f64,
    /// All column expressions within this hyper edge.
    pub columns: HashSet<ColumnReference>,
}

/// Edge connecting extactly two relations in the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    /// The expression joining two nodes in the graph.
    ///
    /// If None, this indicates a cross join between the two relations.
    pub filter: Option<ReorderableCondition>,
    /// Refs on the left side of the comparison.
    pub left_refs: HashSet<TableRef>,
    /// Refs on the right side of the comparison.
    pub right_refs: HashSet<TableRef>,
    /// Base relation the left side is pointing to.
    pub left_rel: RelId,
    /// Base relation the right side is pointing to.
    pub right_rel: RelId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeType {
    Cross,
    Inner { op: ComparisonOperator },
    Semi,
}

#[derive(Debug)]
pub struct NeighborEdge {
    /// Operator that's used in the condition. Affects the computed denominator
    /// for the subgraph.
    pub edge_op: EdgeType,
    /// Id for the hyper edge this edge was in.
    pub _hyper_edge_id: HyperEdgeId,
    /// Id of the edge.
    pub edge_id: EdgeId,
    /// Min NDV for the hyper edge. Affects the computed denominator.
    pub min_ndv: f64,
}

impl HyperEdges {
    /// Create a new set of hyper edges from join conditions.
    ///
    /// Hyper edge NDV will be initialized from base relation cardinalities.
    pub fn new(
        conditions: impl IntoIterator<Item = ReorderableCondition>,
        base_relations: &HashMap<RelId, BaseRelation>,
    ) -> Result<Self> {
        let mut hyper_edges = HyperEdges(Vec::new());

        for condition in conditions {
            hyper_edges.insert_condition_as_edge(condition, base_relations)?;
        }

        // TODO: Round of combining hyper edges.

        Ok(hyper_edges)
    }

    pub fn find_neighbors(&self, set: &RelationSet, exclude: &HashSet<RelId>) -> Vec<RelId> {
        let mut neighbors = HashSet::new();

        self.for_each_edge(&mut |_hyp, _edge_id, edge| {
            if set.relation_indices.contains(&edge.left_rel) && !exclude.contains(&edge.right_rel) {
                neighbors.insert(edge.right_rel);
            }

            if set.relation_indices.contains(&edge.right_rel) && !exclude.contains(&edge.left_rel) {
                neighbors.insert(edge.left_rel);
            }

            ControlFlow::Continue(())
        });

        neighbors.into_iter().collect()
    }

    pub fn find_edges(&self, p1: &RelationSet, p2: &RelationSet) -> Vec<NeighborEdge> {
        let mut found = Vec::new();

        self.for_each_edge(&mut |hyp, edge_id, edge| {
            let typ = match &edge.filter {
                Some(ReorderableCondition::Semi { .. }) => EdgeType::Semi,
                Some(ReorderableCondition::Inner { condition }) => {
                    EdgeType::Inner { op: condition.op }
                }
                None => EdgeType::Cross,
            };

            if p1.relation_indices.contains(&edge.left_rel)
                && p2.relation_indices.contains(&edge.right_rel)
            {
                found.push(NeighborEdge {
                    edge_op: typ,
                    _hyper_edge_id: hyp.id,
                    edge_id,
                    min_ndv: hyp.min_ndv,
                })
            }

            if p1.relation_indices.contains(&edge.right_rel)
                && p2.relation_indices.contains(&edge.left_rel)
            {
                found.push(NeighborEdge {
                    edge_op: typ,
                    _hyper_edge_id: hyp.id,
                    edge_id,
                    min_ndv: hyp.min_ndv,
                })
            }

            ControlFlow::Continue(())
        });

        found
    }

    fn for_each_edge<F>(&self, f: &mut F)
    where
        F: FnMut(&HyperEdge, EdgeId, &Edge) -> ControlFlow<()>,
    {
        for hyper_edge in &self.0 {
            for (edge_id, edge) in &hyper_edge.edges {
                match f(hyper_edge, *edge_id, edge) {
                    ControlFlow::Continue(_) => (),
                    ControlFlow::Break(_) => return,
                }
            }
        }
    }

    pub fn remove_edge(&mut self, id: EdgeId) -> Option<Edge> {
        let hyper_edge = self.0.get_mut(id.hyper_edge_id)?;
        hyper_edge.edges.remove(&id)
    }

    pub fn get_edge(&self, id: EdgeId) -> Option<&Edge> {
        let hyper_edge = self.0.get(id.hyper_edge_id)?;
        hyper_edge.edges.get(&id)
    }

    /// Checks if all edges containing a join condition have been removed from
    /// the hyper graph.
    pub fn all_non_empty_edges_removed(&self) -> bool {
        for hyper_edge in &self.0 {
            for edge in hyper_edge.edges.values() {
                if edge.filter.is_some() {
                    return false;
                }
            }
        }
        true
    }

    /// Insert an edge representing a cross product between left and right.
    pub fn insert_cross_product(&mut self, left: &BaseRelation, right: &BaseRelation) {
        // We can just create new hyper edges for this.
        let id = self.0.len();
        let hyp = HyperEdge {
            id,
            edges: [(
                EdgeId {
                    hyper_edge_id: id,
                    edge_id: 0,
                },
                Edge {
                    filter: None,
                    left_refs: left.output_refs.clone(),
                    right_refs: right.output_refs.clone(),
                    left_rel: left.rel_id,
                    right_rel: right.rel_id,
                },
            )]
            .into_iter()
            .collect(),
            min_ndv: f64::min(left.cardinality, right.cardinality),
            columns: HashSet::new(), // No columns involved.
        };

        self.0.push(hyp);
    }

    fn insert_condition_as_edge(
        &mut self,
        condition: ReorderableCondition,
        base_relations: &HashMap<RelId, BaseRelation>,
    ) -> Result<()> {
        let mut min_ndv = f64::MAX;

        let [left_refs, right_refs] = condition.get_left_right_table_refs();

        let mut left_rel = None;
        let mut right_rel = None;

        for (&rel_id, rel) in base_relations {
            if left_refs.is_subset(&rel.output_refs) {
                left_rel = Some(rel_id);

                // Note we initialize NDV to relation cardinality which will
                // typically overestimate NDV, but by taking the min of all
                // cardinalities involved in the condition, we can
                // significantly reduce it.
                min_ndv = f64::min(min_ndv, rel.cardinality);
            }

            if right_refs.is_subset(&rel.output_refs) {
                right_rel = Some(rel_id);

                // See above.
                min_ndv = f64::min(min_ndv, rel.cardinality);
            }
        }

        // We have the "local" min_ndv, check existing hyper edges to see if
        // it can be added to one.

        let cols = condition.get_column_refs();

        let left_rel = left_rel.ok_or_else(|| DbError::new("Missing left rel id"))?;
        let right_rel = right_rel.ok_or_else(|| DbError::new("Missing right rel id"))?;

        let edge = Edge {
            filter: Some(condition),
            left_refs,
            right_refs,
            left_rel,
            right_rel,
        };

        for hyper_edge in &mut self.0 {
            if !hyper_edge.columns.is_disjoint(&cols) {
                // Hyper edge is connected. Add this edge to the hyper edge,
                // and update min_ndv if needed.
                let edge_id = EdgeId {
                    hyper_edge_id: hyper_edge.id,
                    edge_id: hyper_edge.edges.len(),
                };
                hyper_edge.edges.insert(edge_id, edge);

                // Add new columns.
                hyper_edge.columns.extend(cols);

                hyper_edge.min_ndv = f64::min(hyper_edge.min_ndv, min_ndv);

                // We're done, edge is now in the hyper graph.
                return Ok(());
            }
        }

        // No overlap with any existing edges. Initialize new one.
        let hyper_edge_id = self.0.len();
        let hyper_edge = HyperEdge {
            id: hyper_edge_id,
            edges: [(
                EdgeId {
                    hyper_edge_id,
                    edge_id: 0,
                },
                edge,
            )]
            .into_iter()
            .collect(),
            min_ndv,
            columns: cols,
        };

        self.0.push(hyper_edge);

        Ok(())
    }
}

impl ContextDisplay for HyperEdges {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        for hyp in &self.0 {
            writeln!(f, "Hyperedge: {}", hyp.id)?;
            writeln!(f, "  min_ndv: {}", hyp.min_ndv)?;
            writeln!(f, "  columns:")?;
            for col in &hyp.columns {
                write!(f, "    - ")?;
                col.fmt_using_context(mode, f)?;
                writeln!(f)?;
            }
        }
        Ok(())
    }
}
