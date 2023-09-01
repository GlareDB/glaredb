use crate::planner::extension::ExtensionType;
use crate::planner::logical_plan::{CopyTo, CreateTempTable, Insert};

use super::client::RemoteSessionClient;
use super::local_side::{ClientSendExecsRef, LocalSideTableProvider};
use datafusion::common::tree_node::RewriteRecursion;
use datafusion::common::tree_node::TreeNodeRewriter;
use datafusion::datasource::{source_as_provider, DefaultTableSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, TableScan};
use datafusion_ext::local_hint::is_local_table_hint;
use protogen::metastore::types::catalog::TableEntry;
use protogen::metastore::types::options::{
    CopyToDestinationOptions, CopyToDestinationOptionsLocal, CopyToFormatOptions,
};
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

/// A logical plan rewriter that replaces a DDL actions with just the source
/// plan so the remote server is just responsible for executing the query and we
/// can execute the DDL actions locally.
///
/// NOTE: We should only have one DDL in a plan.
#[derive(Debug)]
pub enum DDLRewriter {
    None,
    InsertIntoTempTable(TableEntry),
    CreateTempTable(CreateTempTable),
    DropTempTables {
        temp_tables: Vec<TableEntry>,
        if_exists: bool,
        all_local: bool,
    },
    CopyRemoteToLocal {
        format: CopyToFormatOptions,
        dest: CopyToDestinationOptionsLocal,
    },
}

impl Default for DDLRewriter {
    fn default() -> Self {
        Self::None
    }
}

impl DDLRewriter {
    fn set(&mut self, val: Self) -> Result<()> {
        if matches!(self, Self::None) {
            *self = val;
            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "a logical plan should have only 1 DDL action".to_string(),
            ))
        }
    }
}

impl TreeNodeRewriter for DDLRewriter {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> Result<RewriteRecursion> {
        if let LogicalPlan::Extension(_) = node {
            Ok(RewriteRecursion::Mutate)
        } else {
            Ok(RewriteRecursion::Continue)
        }
    }

    fn mutate(&mut self, node: Self::N) -> Result<Self::N> {
        let logical_plan = if let LogicalPlan::Extension(ext) = node {
            let ext_type = ext
                .node
                .name()
                .parse::<ExtensionType>()
                .expect("unknown extension type");

            match ext_type {
                ExtensionType::Insert => {
                    let insert_into = ext.node.as_any().downcast_ref::<Insert>().unwrap();
                    if insert_into.table.meta.is_temp {
                        self.set(Self::InsertIntoTempTable(insert_into.table.clone()))?;
                        insert_into.source.clone()
                    } else {
                        LogicalPlan::Extension(ext)
                    }
                }
                ExtensionType::CreateTempTable => {
                    let create_temp_table =
                        ext.node.as_any().downcast_ref::<CreateTempTable>().unwrap();
                    self.set(Self::CreateTempTable(create_temp_table.clone()))?;
                    create_temp_table
                        .source
                        .clone()
                        .unwrap_or(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: create_temp_table.schema.clone(),
                        }))
                }
                ExtensionType::DropTables => {
                    // TODO: Figure out how to seperate temp tables and others.
                    // Send the other tables to remote and keep the remote
                    // tables locally. Create a "combined" plan where both
                    // remote and local tables are deleted (if required).
                    LogicalPlan::Extension(ext)
                }
                ExtensionType::CopyTo => {
                    let copy_to = ext.node.as_any().downcast_ref::<CopyTo>().unwrap();
                    if let CopyToDestinationOptions::Local(dest) = &copy_to.dest {
                        self.set(Self::CopyRemoteToLocal {
                            format: copy_to.format.clone(),
                            dest: dest.clone(),
                        })?;
                        copy_to.source.clone()
                    } else {
                        LogicalPlan::Extension(ext)
                    }
                }
                _ => LogicalPlan::Extension(ext),
            }
        } else {
            node
        };

        Ok(logical_plan)
    }
}
