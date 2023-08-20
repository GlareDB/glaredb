use super::client::RemoteSessionClient;
use super::local_side::{ClientSendExecsRef, LocalSideTableProvider};
use datafusion::common::tree_node::RewriteRecursion;
use datafusion::common::tree_node::TreeNodeRewriter;
use datafusion::datasource::{source_as_provider, DefaultTableSource};
use datafusion::error::Result;
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion_ext::local_hint::is_local_table_hint;
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
        if matches!(node, LogicalPlan::TableScan(..)) {
            Ok(RewriteRecursion::Mutate)
        } else {
            Ok(RewriteRecursion::Continue)
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

                    println!("replacing?");

                    // TODO: Use a custom node rather than changing the name.
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
