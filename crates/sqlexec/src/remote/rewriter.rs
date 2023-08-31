use crate::planner::logical_plan::CopyTo;

use super::client::RemoteSessionClient;
use super::local_side::{ClientSendExecsRef, LocalSideTableProvider};
use datafusion::common::tree_node::RewriteRecursion;
use datafusion::common::tree_node::TreeNodeRewriter;
use datafusion::datasource::{source_as_provider, DefaultTableSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion_ext::local_hint::is_local_table_hint;
use protogen::metastore::types::options::{CopyToDestinationOptions, CopyToFormatOptions};
use std::sync::Arc;

/// A logical plan rewriter that will inspect all table scans rewriting all
/// `LocalTableHint` table providers into `LocalSideTableProvider` table
/// providers.
#[derive(Clone)]
pub struct LocalSideTableRewriter {
    // TODO: Once this is an extension, there would be no need to pass it in
    // here.
    pub client: RemoteSessionClient,
    pub exec_refs: Vec<ClientSendExecsRef>,
}

impl LocalSideTableRewriter {
    pub fn new(client: RemoteSessionClient) -> Self {
        LocalSideTableRewriter {
            client,
            exec_refs: Vec::new(),
        }
    }
}

impl TreeNodeRewriter for LocalSideTableRewriter {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> Result<RewriteRecursion> {
        match node {
            LogicalPlan::Explain(_) => Ok(RewriteRecursion::Stop),
            LogicalPlan::TableScan(_) => Ok(RewriteRecursion::Mutate),
            _ => Ok(RewriteRecursion::Continue),
        }
    }

    fn mutate(&mut self, node: Self::N) -> Result<Self::N> {
        match node {
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }) => {
                // Downcast to provider.
                let provider = source_as_provider(&source)?;

                // Only attempt to change out the provider if we've properly
                // hinted that it's a "local" table.
                if is_local_table_hint(&provider) {
                    let new_provider = LocalSideTableProvider::new(provider, self.client.clone());
                    let exec_ref = new_provider.get_exec_ref();
                    self.exec_refs.push(exec_ref);

                    // TODO: Use a custom node rather than changing the name.
                    //
                    // Datafusion implements Eq for logical plan nodes that
                    // don't check the source. The `TreeNode` impl for logical
                    // plan will only actually replace the node if it's not
                    // equal to the old node.
                    //
                    // To get around this, we can change the name used. I'm not
                    // totally comfortable with that, but it gets this working,
                    // and table names aren't used when creating the physical
                    // plan.
                    Ok(LogicalPlan::TableScan(TableScan {
                        table_name: "local_table_rewrite".into(),
                        source: Arc::new(DefaultTableSource::new(Arc::new(new_provider))),
                        projection,
                        projected_schema,
                        fetch,
                        filters,
                    }))
                } else {
                    Ok(LogicalPlan::TableScan(TableScan {
                        table_name,
                        source,
                        projection,
                        projected_schema,
                        fetch,
                        filters,
                    }))
                }
            }
            other => Ok(other),
        }
    }
}

/// A logical plan rewriter that replaces a remote copy to with `CopyToQuery`
/// so the remote server is only responsible for executing the query and we run
/// the actual copy to on local.
///
/// NOTE: We should only have one copy remote to local in a plan.
#[derive(Debug, Default)]
pub struct CopyRemoteToLocalRewriter(pub Option<(CopyToFormatOptions, CopyToDestinationOptions)>);

impl TreeNodeRewriter for CopyRemoteToLocalRewriter {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> Result<RewriteRecursion> {
        if let LogicalPlan::Extension(_) = node {
            Ok(RewriteRecursion::Mutate)
        } else {
            Ok(RewriteRecursion::Continue)
        }
    }

    fn mutate(&mut self, node: Self::N) -> Result<Self::N> {
        if let LogicalPlan::Extension(ext) = node {
            if let Some(copy_to) = ext.node.as_any().downcast_ref::<CopyTo>() {
                // Rewrite if the destination is local.
                if matches!(&copy_to.dest, CopyToDestinationOptions::Local(_)) {
                    if self.0.is_some() {
                        return Err(DataFusionError::Internal(
                            "a logical plan should have only 1 copy to".to_string(),
                        ));
                    }
                    self.0 = Some((copy_to.format.clone(), copy_to.dest.clone()));
                    Ok(copy_to.source.clone())
                } else {
                    Ok(LogicalPlan::Extension(ext))
                }
            } else {
                Ok(LogicalPlan::Extension(ext))
            }
        } else {
            Ok(node)
        }
    }
}
