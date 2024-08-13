use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::logical::{
    context::QueryContext,
    expr::{LogicalExpression, Subquery},
    operator::{CrossJoin, EqualityJoin, JoinType, LogicalNode, LogicalOperator},
};
use rayexec_error::{RayexecError, Result};

use super::scope::ColumnRef;

#[derive(Debug, Default)]
pub struct SubqueryDecorrelator {}

impl SubqueryDecorrelator {
    pub fn plan_correlated(
        &mut self,
        context: &mut QueryContext,
        subquery: &mut Subquery,
        input: &mut LogicalOperator,
        input_cols: usize,
        _subquery_cols: usize,
    ) -> Result<LogicalExpression> {
        let mut root = *subquery.take_root();
        match subquery {
            Subquery::Scalar { .. } => {
                // Dependent join on input and subquery root.

                // Materialize the original input. It'll be fed into the pushed
                // down dependent join, as well as the output of subquery.
                // TODO: Eliminate duplicates in original
                let orig = input.take();
                let idx = context.push_plan_for_materialization(orig);
                let scan = context.generate_scan_for_idx(idx, &[])?; // wtf goes in outer?

                let mut dep_push_down = DependentJoinPushDown::new();
                // Figure out which columns we'll be decorrelating.
                dep_push_down.find_correlated_columns(&mut root)?;

                // "Push" down the dependent join
                //
                // Note we may end up generating more than one materialized scan
                // in cases where we're pushing down through joins or set
                // operations where both sides have correlations.
                dep_push_down.push_down(context, idx, &mut root)?;

                // TODO: Build join columns. Left: materialized scan, right:
                // materialized scan offset + generated column indices in push
                // down.

                // TODO: NULL == NULL when available
                *input = LogicalOperator::EqualityJoin(LogicalNode::new(EqualityJoin {
                    left: Box::new(LogicalOperator::MaterializedScan(LogicalNode::new(scan))),
                    right: Box::new(root),
                    join_type: JoinType::Inner,
                    left_on: vec![0],  // TODO
                    right_on: vec![0], // TODO
                }));

                println!("PLAN:\n{}", input.debug_explain(Some(context)));

                let expr = LogicalExpression::new_column(input_cols);

                Ok(expr)
            }
            Subquery::Exists { .. } => {
                //
                unimplemented!()
            }
            Subquery::Any { .. } => unimplemented!(),
        }
    }
}

// TODO: Determine if this is fine.
#[derive(Debug, Default)]
struct LogicalOperatorRefMap<V>(HashMap<*const LogicalOperator, V>);

impl<V: Copy> LogicalOperatorRefMap<V> {
    fn insert(&mut self, operator: &LogicalOperator, value: V) {
        self.0.insert(operator as _, value);
    }

    fn get(&self, operator: &LogicalOperator) -> Option<V> {
        let ptr = operator as *const LogicalOperator;
        self.0.get(&ptr).copied()
    }
}

#[derive(Debug)]
struct DependentJoinPushDown {
    /// Correlated columns at each lateral level.
    ///
    /// (lateral_level -> column_ref)
    ///
    /// The column refs are btree since we want to maintain those in order.
    correlated: BTreeMap<usize, BTreeSet<ColumnRef>>,

    /// Computed offsets for new column references at each lateral level.
    ///
    /// Essentially this means for each lateral level, we'll be appending
    /// additional columns to the right by way of a dependent join. This lets us
    /// compute the new decorrelated column references quickly.
    lateral_offsets: BTreeMap<usize, usize>,

    /// Hash map for tracking if a logical operator contains any correlated
    /// columns, or if any of its children does.
    has_correlations: LogicalOperatorRefMap<bool>,
}

impl DependentJoinPushDown {
    fn new() -> Self {
        DependentJoinPushDown {
            correlated: BTreeMap::new(),
            lateral_offsets: BTreeMap::new(),
            has_correlations: LogicalOperatorRefMap::default(),
        }
    }

    /// Finds all correlated columns in this plan, placing those columns in the
    /// `correlated` map.
    ///
    /// This is done prior to pushing down the dependent join so that we know
    /// how many joins we're working with, which let's us replace the correlated
    /// columns during the push down.
    fn find_correlated_columns(&mut self, root: &mut LogicalOperator) -> Result<()> {
        // Walk post to determine correlations in children first.
        root.walk_mut_post(&mut |plan| {
            let has_correlation = match plan {
                LogicalOperator::Projection(node) => {
                    let has_correlation =
                        self.add_any_correlated_columns(&mut node.as_mut().exprs)?;
                    has_correlation
                        || self
                            .has_correlations
                            .get(&node.as_ref().input)
                            .unwrap_or(false)
                }
                LogicalOperator::Filter(node) => {
                    let filter = node.as_mut();
                    let has_correlation =
                        self.add_any_correlated_columns([&mut filter.predicate])?;
                    has_correlation || self.has_correlations.get(&filter.input).unwrap_or(false)
                }
                LogicalOperator::Aggregate(node) => {
                    let agg = node.as_mut();
                    let mut has_correlation =
                        self.add_any_correlated_columns(&mut agg.aggregates)?;
                    has_correlation |= self.add_any_correlated_columns(&mut agg.group_exprs)?;
                    has_correlation || self.has_correlations.get(&agg.input).unwrap_or(false)
                }
                LogicalOperator::MaterializedScan(_)
                | LogicalOperator::Empty(_)
                | LogicalOperator::Scan(_)
                | LogicalOperator::TableFunction(_) => false,
                _ => true, // TODO: More
            };

            self.has_correlations.insert(plan, has_correlation);

            Ok(())
        })?;

        let mut curr_offset = 0;
        for (lateral, cols) in &self.correlated {
            self.lateral_offsets.insert(*lateral, curr_offset);
            // Subsequent laterals get added to the right.
            curr_offset += cols.len();
        }

        Ok(())
    }

    /// For an existing column ref, return the new column ref that would point
    /// to the resulting column in the join.
    fn decorrelated_ref(&self, col: ColumnRef) -> ColumnRef {
        // All unwraps indicate programmer bug. We should have already seen all
        // possible lateral levels and column references.

        let lateral_offset = self.lateral_offsets.get(&col.scope_level).unwrap();
        let lateral_cols = self.correlated.get(&col.scope_level).unwrap();

        let pos = lateral_cols
            .iter()
            .position(|correlated_col| correlated_col == &col)
            .unwrap();

        // New column ref is the offset of this lateral level + the offset of
        // the column _within_ this lateral level.
        ColumnRef {
            scope_level: 0,
            item_idx: lateral_offset + pos,
        }
    }

    /// Iterate and walk all the given expression, inserting correlated columns
    /// into the `correlated` map as they're encountered.
    ///
    /// Returns true if there was a correlation.
    fn add_any_correlated_columns<'a>(
        &mut self,
        exprs: impl IntoIterator<Item = &'a mut LogicalExpression>,
    ) -> Result<bool> {
        use std::collections::btree_map::Entry;

        let mut has_correlation = false;
        LogicalExpression::walk_mut_many(
            exprs,
            &mut |expr| match expr {
                LogicalExpression::ColumnRef(col) if col.scope_level > 0 => {
                    has_correlation = true;
                    match self.correlated.entry(col.scope_level) {
                        Entry::Vacant(ent) => {
                            let mut cols = BTreeSet::new();
                            cols.insert(*col);
                            ent.insert(cols);
                        }
                        Entry::Occupied(mut ent) => {
                            ent.get_mut().insert(*col);
                        }
                    }
                    Ok(())
                }
                _ => Ok(()),
            },
            &mut |_| Ok(()),
        )?;

        Ok(has_correlation)
    }

    /// Rewrites correlated columns in the given expressions, returning the
    /// number of expressions that were rewritten.
    fn rewrite_correlated_columns<'a>(
        &self,
        exprs: impl IntoIterator<Item = &'a mut LogicalExpression>,
    ) -> Result<usize> {
        let mut num_rewritten = 0;
        LogicalExpression::walk_mut_many(
            exprs,
            &mut |expr| match expr {
                LogicalExpression::ColumnRef(col) if col.scope_level > 0 => {
                    *expr = LogicalExpression::ColumnRef(self.decorrelated_ref(*col));
                    num_rewritten += 1;
                    Ok(())
                }
                _ => Ok(()),
            },
            &mut |_| Ok(()),
        )?;
        Ok(num_rewritten)
    }

    /// Push down dependent joins.
    fn push_down(
        &self,
        context: &mut QueryContext,
        materialized_idx: usize,
        plan: &mut LogicalOperator,
    ) -> Result<()> {
        // Note we're not using the `walk_` methods since the logic in this is a
        // bit more involved.

        if !self.has_correlations.get(plan).expect("correlation bool") {
            // Operator (and children) don't have correlations. Cross join it
            // with the materialized outer plan.
            let scan = context.generate_scan_for_idx(materialized_idx, &[])?;
            let orig = plan.take();
            *plan = LogicalOperator::CrossJoin(LogicalNode::new(CrossJoin {
                left: Box::new(orig),
                right: Box::new(LogicalOperator::MaterializedScan(LogicalNode::new(scan))),
            }));

            return Ok(());
        }

        match plan {
            LogicalOperator::Filter(node) => {
                let filter = node.as_mut();
                self.push_down(context, materialized_idx, &mut filter.input)?;
                // Filter is simple, don't need to do anything special.
                let _ = self.rewrite_correlated_columns([&mut filter.predicate])?;
            }
            LogicalOperator::Projection(node) => {
                self.push_down(context, materialized_idx, &mut node.as_mut().input)?;
                // yolo
                let _ = self.rewrite_correlated_columns(&mut node.as_mut().exprs)?;
            }
            other => {
                // TODO: More operators
                return Err(RayexecError::new(format!(
                    "Unimplemented dependent join push down for operator: {other:?}"
                )));
            }
        }

        Ok(())
    }
}
