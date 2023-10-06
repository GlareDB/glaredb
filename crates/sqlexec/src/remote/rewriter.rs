use std::sync::Arc;

use crate::planner::extension::ExtensionType;
use crate::planner::logical_plan::{CopyTo, CreateTempTable, Insert};
use crate::planner::physical_plan::remote_scan::ProviderReference;

use datafusion::common::tree_node::RewriteRecursion;
use datafusion::common::tree_node::TreeNodeRewriter;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use protogen::metastore::types::catalog::{RuntimePreference, TableEntry};
use protogen::metastore::types::options::{
    CopyToDestinationOptions, CopyToDestinationOptionsLocal, CopyToFormatOptions,
};

/// A logical plan rewriter that replaces a DDL actions with just the source
/// plan so the remote server is just responsible for executing the query and we
/// can execute the DDL actions locally.
///
/// NOTE: We should only have one DDL in a plan.
pub enum DDLRewriter {
    None,
    InsertIntoLocalTable(Arc<dyn TableProvider>),
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
                    if insert_into.runtime_preference == RuntimePreference::Local {
                        let provider = match &insert_into.provider {
                            ProviderReference::Provider(provider) => provider.clone(),
                            ProviderReference::RemoteReference(_) => unreachable!(
                                "required local table, found remote reference to table"
                            ),
                        };
                        self.set(Self::InsertIntoLocalTable(provider))?;
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
