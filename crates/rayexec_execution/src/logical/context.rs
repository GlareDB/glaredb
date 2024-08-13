use std::collections::HashMap;

use rayexec_bullet::field::TypeSchema;
use rayexec_error::{RayexecError, Result};

use super::{
    binder::bound_table::CteIndex,
    operator::{LogicalOperator, MaterializedScan},
    planner::scope::Scope,
};

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedPlan {
    /// Index within the query context.
    pub idx: usize,

    /// Number of operators that will be scanning the result of materialization.
    pub num_scans: usize,

    /// The root of the plan that will be materialized.
    pub root: LogicalOperator,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedCteReference {
    /// Index of the materialized plan.
    pub materialized_idx: usize,

    /// The scope that was generated during planning of the CTE body.
    pub scope: Scope,
}

/// Additional query context to allow for more complex query graphs.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryContext {
    /// Plans that will be materialized during execution.
    ///
    /// This is used to allow for graph-like query plans to allow multiple
    /// operators be able read from the same plan.
    ///
    /// This holds materializations for correlated subqueries (cannot be
    /// referenced) as well as materializations for materialized CTEs (can be
    /// referenced)
    ///
    /// Materializations may depend on materializations that come before it.
    pub materialized: Vec<MaterializedPlan>,

    /// Mapping between a materialized CTE and a materialized plan.
    ///
    /// During planning, the first time we come across a materialized CTE, we'll
    /// generate a plan, put it in `materialized`, and put the reference in this
    /// map. The next time we see the materialized CTE, we'll just create a
    /// MaterializeScan operator which will scan from the same plan.
    pub materialized_cte_refs: HashMap<CteIndex, MaterializedCteReference>,
}

impl QueryContext {
    pub fn new() -> Self {
        QueryContext {
            materialized: Vec::new(),
            materialized_cte_refs: HashMap::new(),
        }
    }

    /// If this context has any materializations.
    pub fn has_materializations(&self) -> bool {
        !self.materialized.is_empty()
    }

    /// Push a plan for materialization.
    ///
    /// The index of the plan within the query context will be returned.
    pub fn push_plan_for_materialization(&mut self, root: LogicalOperator) -> usize {
        let idx = self.materialized.len();
        self.materialized.push(MaterializedPlan {
            idx,
            num_scans: 0,
            root,
        });
        idx
    }

    pub fn push_materialized_cte(
        &mut self,
        bound: CteIndex,
        root: LogicalOperator,
        scope: Scope,
    ) -> usize {
        let idx = self.push_plan_for_materialization(root);
        let reference = MaterializedCteReference {
            materialized_idx: idx,
            scope,
        };
        self.materialized_cte_refs.insert(bound, reference);
        idx
    }

    pub fn get_materialized_cte_reference(
        &self,
        bound: CteIndex,
    ) -> Option<&MaterializedCteReference> {
        self.materialized_cte_refs.get(&bound)
    }

    /// Generates a logical materialized scan for a plan at the given index.
    pub fn generate_scan_for_idx(
        &mut self,
        idx: usize,
        outer: &[TypeSchema],
    ) -> Result<MaterializedScan> {
        let plan = self.materialized.get_mut(idx).ok_or_else(|| {
            RayexecError::new(format!("Missing materialized plan at index {idx}"))
        })?;

        let schema = plan.root.output_schema(outer)?;
        plan.num_scans += 1;

        Ok(MaterializedScan { idx, schema })
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}
