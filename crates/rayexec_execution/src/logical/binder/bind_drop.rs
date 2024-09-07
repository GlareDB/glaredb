use rayexec_error::{not_implemented, Result};
use rayexec_parser::ast;

use crate::{
    database::drop::{DropInfo, DropObject},
    logical::{
        logical_drop::LogicalDrop,
        operator::{LocationRequirement, Node},
        resolver::ResolvedMeta,
    },
};

use super::bind_context::{BindContext, BindScopeRef};

#[derive(Debug)]
pub struct DropBinder {
    pub current: BindScopeRef,
}

impl DropBinder {
    pub fn new(current: BindScopeRef) -> Self {
        DropBinder { current }
    }

    pub fn bind_drop(
        &self,
        _bind_context: &mut BindContext,
        mut drop: ast::DropStatement<ResolvedMeta>,
    ) -> Result<Node<LogicalDrop>> {
        match drop.drop_type {
            ast::DropType::Schema => {
                let [catalog, schema] = drop.name.pop_2()?;

                // Dropping defaults to restricting (erroring) on dependencies.
                let deps = drop.deps.unwrap_or(ast::DropDependents::Restrict);

                Ok(Node {
                    node: LogicalDrop {
                        catalog,
                        info: DropInfo {
                            schema,
                            object: DropObject::Schema,
                            cascade: ast::DropDependents::Cascade == deps,
                            if_exists: drop.if_exists,
                        },
                    },
                    location: LocationRequirement::ClientLocal,
                    children: Vec::new(),
                })
            }
            other => not_implemented!("drop {other:?}"),
        }
    }
}
