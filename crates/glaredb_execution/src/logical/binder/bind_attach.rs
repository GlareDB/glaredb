use std::collections::HashMap;

use glaredb_error::{RayexecError, Result};
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use super::column_binder::ErroringColumnBinder;
use super::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::logical_attach::{LogicalAttachDatabase, LogicalDetachDatabase};
use crate::logical::operator::{LocationRequirement, Node};
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;
use crate::logical::statistics::StatisticsValue;

#[derive(Debug)]
pub enum BoundAttach {
    Database(Node<LogicalAttachDatabase>),
}

#[derive(Debug)]
pub enum BoundDetach {
    Database(Node<LogicalDetachDatabase>),
}

#[derive(Debug)]
pub struct AttachBinder {
    pub current: BindScopeRef,
}

impl AttachBinder {
    pub fn new(current: BindScopeRef) -> Self {
        AttachBinder { current }
    }

    pub fn bind_attach(
        &self,
        bind_context: &mut BindContext,
        mut attach: ast::Attach<ResolvedMeta>,
    ) -> Result<BoundAttach> {
        match attach.attach_type {
            ast::AttachType::Database => {
                let mut options = HashMap::new();

                for (k, v) in attach.options {
                    let k = k.into_normalized_string();
                    let expr = BaseExpressionBinder::new(self.current, &ResolveContext::empty())
                        .bind_expression(
                            bind_context,
                            &v,
                            &mut ErroringColumnBinder,
                            RecursionContext {
                                allow_windows: false,
                                allow_aggregates: false,
                                is_root: true,
                            },
                        )?;
                    let v = expr.try_into_scalar()?;

                    if options.contains_key(&k) {
                        return Err(RayexecError::new(format!(
                            "Option '{k}' provided more than once"
                        )));
                    }
                    options.insert(k, v);
                }

                if attach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        attach.alias
                    )));
                }
                let name = attach.alias.pop()?;
                let datasource = attach.datasource_name;

                // Currently this always has a "client local" requirement. This
                // essentially means catalog management happens on the client
                // (currently). For hybrid exec, the client-local catalog acts
                // as a stub.
                //
                // The semantics for this may change when we have a real
                // cloud-based catalog. Even then, this logic can still make
                // sense where the client calls a remote endpoint for persisting
                // catalog changes.
                Ok(BoundAttach::Database(Node {
                    node: LogicalAttachDatabase {
                        datasource: datasource.into_normalized_string(),
                        name,
                        options,
                    },
                    location: LocationRequirement::ClientLocal,
                    children: Vec::new(),
                    estimated_cardinality: StatisticsValue::Unknown,
                }))
            }
            ast::AttachType::Table => Err(RayexecError::new("Attach tables not yet supported")),
        }
    }

    pub fn bind_detach(
        &self,
        _bind_context: &mut BindContext,
        mut detach: ast::Detach<ResolvedMeta>,
    ) -> Result<BoundDetach> {
        match detach.attach_type {
            ast::AttachType::Database => {
                if detach.alias.0.len() != 1 {
                    return Err(RayexecError::new(format!(
                        "Expected a single identifier, got '{}'",
                        detach.alias
                    )));
                }
                let name = detach.alias.pop()?;

                Ok(BoundDetach::Database(Node {
                    node: LogicalDetachDatabase { name },
                    location: LocationRequirement::ClientLocal,
                    children: Vec::new(),
                    estimated_cardinality: StatisticsValue::Unknown,
                }))
            }
            ast::AttachType::Table => Err(RayexecError::new("Detach tables not yet supported")),
        }
    }
}
