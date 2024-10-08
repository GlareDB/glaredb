use rayexec_error::Result;
use rayexec_parser::statement::Statement;

use super::bind_attach::{AttachBinder, BoundAttach, BoundDetach};
use super::bind_context::BindContext;
use super::bind_copy::{BoundCopyTo, CopyBinder};
use super::bind_create_schema::CreateSchemaBinder;
use super::bind_create_table::{BoundCreateTable, CreateTableBinder};
use super::bind_create_view::CreateViewBinder;
use super::bind_describe::DescribeBinder;
use super::bind_drop::DropBinder;
use super::bind_explain::{BoundExplain, ExplainBinder};
use super::bind_insert::{BoundInsert, InsertBinder};
use super::bind_query::BoundQuery;
use super::bind_set::SetVarBinder;
use crate::config::vars::SessionVars;
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::logical_create::{LogicalCreateSchema, LogicalCreateView};
use crate::logical::logical_describe::LogicalDescribe;
use crate::logical::logical_drop::LogicalDrop;
use crate::logical::logical_set::{LogicalResetVar, LogicalSetVar, LogicalShowVar};
use crate::logical::operator::Node;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;

/// "Bound" variants for SQL statements that we support.
///
/// Many of these just produce logical plans, so the followup plan step should
/// just use the logical plan directly.
///
/// The real benefit of this is for statements that contain queries (SELECT).
/// The followup plan step takes bound queries to produce reasonable logical
/// plans.
#[derive(Debug)]
pub enum BoundStatement {
    Query(BoundQuery),
    SetVar(Node<LogicalSetVar>),
    ResetVar(Node<LogicalResetVar>),
    ShowVar(Node<LogicalShowVar>),
    Attach(BoundAttach),
    Detach(BoundDetach),
    Drop(Node<LogicalDrop>),
    Insert(BoundInsert),
    CreateSchema(Node<LogicalCreateSchema>),
    CreateTable(BoundCreateTable),
    CreateView(Node<LogicalCreateView>),
    Describe(Node<LogicalDescribe>),
    Explain(BoundExplain),
    CopyTo(BoundCopyTo),
}

#[derive(Debug)]
pub struct StatementBinder<'a> {
    pub session_vars: &'a SessionVars,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> StatementBinder<'a> {
    pub fn bind(
        &self,
        statement: Statement<ResolvedMeta>,
    ) -> Result<(BoundStatement, BindContext)> {
        let mut context = BindContext::new();
        let root_scope = context.root_scope_ref();

        let statement = match statement {
            Statement::Query(query) => {
                let binder = QueryBinder {
                    current: root_scope,
                    resolve_context: self.resolve_context,
                };
                BoundStatement::Query(binder.bind(&mut context, query)?)
            }
            Statement::SetVariable(set) => BoundStatement::SetVar(
                SetVarBinder::new(root_scope, self.session_vars).bind_set(&mut context, set)?,
            ),
            Statement::ShowVariable(set) => BoundStatement::ShowVar(
                SetVarBinder::new(root_scope, self.session_vars).bind_show(&mut context, set)?,
            ),
            Statement::ResetVariable(set) => BoundStatement::ResetVar(
                SetVarBinder::new(root_scope, self.session_vars).bind_reset(&mut context, set)?,
            ),
            Statement::Attach(attach) => BoundStatement::Attach(
                AttachBinder::new(root_scope).bind_attach(&mut context, attach)?,
            ),
            Statement::Detach(detach) => BoundStatement::Detach(
                AttachBinder::new(root_scope).bind_detach(&mut context, detach)?,
            ),
            Statement::Drop(drop) => {
                BoundStatement::Drop(DropBinder::new(root_scope).bind_drop(&mut context, drop)?)
            }
            Statement::Insert(insert) => BoundStatement::Insert(
                InsertBinder::new(root_scope, self.resolve_context)
                    .bind_insert(&mut context, insert)?,
            ),
            Statement::CreateSchema(create) => BoundStatement::CreateSchema(
                CreateSchemaBinder::new(root_scope).bind_create_schema(&mut context, create)?,
            ),
            Statement::CreateTable(create) => BoundStatement::CreateTable(
                CreateTableBinder::new(root_scope, self.resolve_context)
                    .bind_create_table(&mut context, create)?,
            ),
            Statement::CreateView(create) => BoundStatement::CreateView(
                CreateViewBinder::new(root_scope, self.resolve_context)
                    .bind_create_view(&mut context, create)?,
            ),
            Statement::Describe(describe) => BoundStatement::Describe(
                DescribeBinder::new(root_scope, self.resolve_context)
                    .bind_describe(&mut context, describe)?,
            ),
            Statement::Explain(explain) => BoundStatement::Explain(
                ExplainBinder::new(root_scope, self.resolve_context)
                    .bind_explain(&mut context, explain)?,
            ),
            Statement::CopyTo(copy_to) => BoundStatement::CopyTo(
                CopyBinder::new(root_scope, self.resolve_context)
                    .bind_copy_to(&mut context, copy_to)?,
            ),
        };

        Ok((statement, context))
    }
}
