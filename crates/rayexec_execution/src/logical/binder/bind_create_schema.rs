use rayexec_error::Result;
use rayexec_parser::ast;

use crate::{
    database::create::OnConflict,
    logical::{
        logical_create::LogicalCreateSchema,
        operator::{LocationRequirement, Node},
        resolver::ResolvedMeta,
    },
};

use super::bind_context::{BindContext, BindScopeRef};

#[derive(Debug)]
pub struct CreateSchemaBinder {
    pub current: BindScopeRef,
}

impl CreateSchemaBinder {
    pub fn new(current: BindScopeRef) -> Self {
        CreateSchemaBinder { current }
    }

    pub fn bind_create_schema(
        &self,
        _bind_context: &mut BindContext,
        mut create: ast::CreateSchema<ResolvedMeta>,
    ) -> Result<Node<LogicalCreateSchema>> {
        let on_conflict = if create.if_not_exists {
            OnConflict::Ignore
        } else {
            OnConflict::Error
        };

        let [catalog, schema] = create.name.pop_2()?;

        Ok(Node {
            node: LogicalCreateSchema {
                catalog,
                name: schema,
                on_conflict,
            },
            location: LocationRequirement::ClientLocal,
            children: Vec::new(),
        })
    }
}
