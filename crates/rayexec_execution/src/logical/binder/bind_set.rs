use rayexec_bullet::datatype::DataTypeOld;
use rayexec_error::Result;
use rayexec_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use super::column_binder::ErroringColumnBinder;
use crate::config::session::SessionConfig;
use crate::logical::binder::expr_binder::{BaseExpressionBinder, RecursionContext};
use crate::logical::logical_set::{LogicalResetVar, LogicalSetVar, LogicalShowVar, VariableOrAll};
use crate::logical::operator::{LocationRequirement, Node};
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;
use crate::logical::statistics::StatisticsValue;

#[derive(Debug)]
pub struct SetVarBinder<'a> {
    pub current: BindScopeRef,
    pub config: &'a SessionConfig,
}

impl<'a> SetVarBinder<'a> {
    pub fn new(current: BindScopeRef, config: &'a SessionConfig) -> Self {
        SetVarBinder { current, config }
    }

    pub fn bind_set(
        &self,
        bind_context: &mut BindContext,
        mut set: ast::SetVariable<ResolvedMeta>,
    ) -> Result<Node<LogicalSetVar>> {
        let expr = BaseExpressionBinder::new(self.current, &ResolveContext::empty())
            .bind_expression(
                bind_context,
                &set.value,
                &mut ErroringColumnBinder,
                RecursionContext {
                    allow_windows: false,
                    allow_aggregates: false,
                    is_root: true,
                },
            )?;

        let name = set.reference.pop()?; // TODO: Allow compound references?
        let value = expr.try_into_scalar()?;

        // Verify exists.
        let _ = self.config.get_as_scalar(&name)?;

        Ok(Node {
            node: LogicalSetVar { name, value },
            location: LocationRequirement::ClientLocal,
            children: Vec::new(),
            estimated_cardinality: StatisticsValue::Unknown,
        })
    }

    pub fn bind_reset(
        &self,
        _bind_context: &mut BindContext,
        reset: ast::ResetVariable<ResolvedMeta>,
    ) -> Result<Node<LogicalResetVar>> {
        let var = match reset.var {
            ast::VariableOrAll::Variable(mut v) => {
                let name = v.pop()?; // TODO: Allow compound references?
                let _ = self.config.get_as_scalar(&name)?; // Verify exists
                VariableOrAll::Variable(name)
            }
            ast::VariableOrAll::All => VariableOrAll::All,
        };

        Ok(Node {
            node: LogicalResetVar { var },
            location: LocationRequirement::ClientLocal,
            children: Vec::new(),
            estimated_cardinality: StatisticsValue::Unknown,
        })
    }

    pub fn bind_show(
        &self,
        bind_context: &mut BindContext,
        mut show: ast::Show<ResolvedMeta>,
    ) -> Result<Node<LogicalShowVar>> {
        let name = show.reference.pop()?; // TODO: Allow compound references?
        let var = self.config.get_as_scalar(&name)?;

        bind_context.push_table(
            self.current,
            None,
            vec![DataTypeOld::Utf8],
            vec![name.clone()],
        )?;

        Ok(Node {
            node: LogicalShowVar { name, value: var },
            location: LocationRequirement::ClientLocal, // Technically could be any since the variable is copied.
            children: Vec::new(),
            estimated_cardinality: StatisticsValue::Unknown,
        })
    }
}
