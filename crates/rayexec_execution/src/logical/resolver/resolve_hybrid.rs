use std::sync::Arc;

use crate::{
    database::{catalog::CatalogTx, memory_catalog::MemoryCatalog, Database, DatabaseContext},
    datasource::{DataSourceRegistry, FileHandlers},
    logical::{operator::LocationRequirement, resolver::ResolveMode},
};
use rayexec_error::{RayexecError, Result};
use tracing::debug;

use super::{
    resolve_context::MaybeResolved, resolve_normal::NormalResolver,
    resolved_table_function::ResolvedTableFunctionReference, ResolveContext, Resolver,
};

/// Extends a context by attaching additional databases using information
/// provided by partially bound objects supplied by the client.
///
/// This allows us to selectively attach databases that are needed for a query.
#[derive(Debug)]
pub struct HybridContextExtender<'a> {
    pub context: &'a mut DatabaseContext,
    pub registry: &'a DataSourceRegistry,
}

impl<'a> HybridContextExtender<'a> {
    pub fn new(context: &'a mut DatabaseContext, registry: &'a DataSourceRegistry) -> Self {
        HybridContextExtender { context, registry }
    }

    /// Iterates the provided bind data and attaches databases to the context as
    /// necessary.
    pub async fn attach_unknown_databases(
        &mut self,
        resolve_context: &ResolveContext,
    ) -> Result<()> {
        for item in resolve_context.tables.inner.iter() {
            if let MaybeResolved::Unresolved(unbound) = item {
                // We might have already attached a database. E.g. by already
                // iterating over a table that comes from the same catalog.
                if self.context.database_exists(&unbound.catalog) {
                    // TODO: Probably need to check more than just the name.
                    continue;
                }

                match &unbound.attach_info {
                    Some(info) => {
                        // TODO: Some of this repeated with session.

                        let datasource = self
                            .registry
                            .get_datasource(&info.datasource)
                            .ok_or_else(|| {
                                RayexecError::new(format!(
                                    "Unknown data source: '{}'",
                                    info.datasource
                                ))
                            })?;

                        let connection = datasource.connect(info.options.clone()).await?;
                        let catalog = Arc::new(MemoryCatalog::default());
                        if let Some(catalog_storage) = connection.catalog_storage.as_ref() {
                            // TODO: Not sure if we actaully want to do this
                            // here, especially if the context is query-scoped.
                            catalog_storage.initial_load(&catalog).await?;
                        }

                        let database = Database {
                            catalog,
                            catalog_storage: connection.catalog_storage,
                            table_storage: Some(connection.table_storage),
                            attach_info: Some(info.clone()),
                        };

                        self.context.attach_database(&unbound.catalog, database)?;
                    }
                    None => {
                        return Err(RayexecError::new(format!(
                            "Unable to attach database for '{}', missing attach info",
                            unbound.catalog
                        )))
                    }
                }
            }
        }

        Ok(())
    }
}

/// Resolver for resolving partially bound statements.
///
/// The database context provided on this does not need to match the database
/// context that was used during the intial binding. The use case is to allow
/// the "local" session to partially bind a query, serialize the query, then
/// have the remote side complete the binding.
///
/// This allows for two instances with differently registered data source to
/// both work on query planning.
///
/// For example, the following query would typically fail in when running in
/// wasm:
///
/// SELECT * FROM read_postgres(...) INNER JOIN 'myfile.csv' ON ...
///
/// This is because we don't register the postgres data source in the wasm
/// bindings because we can't actually connect to postgres in the browser.
/// However with hyrbid execution (and this resolver), the wasm session is able
/// to resolve everything _but_ the `read_postgres` call, then send the serialized
/// plan to remote node, which then uses this resolver to appropriately bind the
/// `read_postgres` function (assuming the remote node has the postgres data
/// source registered).
///
/// Once resolved, the remote node can continue with planning the statement,
/// sending back parts of the pipeline that the "local" side should execute.
// TODO: Somehow do search path.
#[derive(Debug)]
pub struct HybridResolver<'a> {
    pub resolver: Resolver<'a>,
}

impl<'a> HybridResolver<'a> {
    pub fn new(tx: &'a CatalogTx, context: &'a DatabaseContext) -> Self {
        // Currently just use an empty file handler, all files should have been
        // resolved appropriately on the "local" side.
        //
        // This may change if:
        // - We have "remote"-only (or cloud-only) file handlers.
        // - We want to handle object store files remotely always, enabling
        //   things like automatically using credentials and stuff.
        //
        // The second point will likely be handled in a way where we replace the
        // file with the proper function on the "local" side anyways, so this
        // would still be fine being empty.
        const EMPTY_FILE_HANDLER_REF: &FileHandlers = &FileHandlers::empty();

        // Note we're using bindmode normal here since everything we attempt to
        // bind in this resolver should succeed.
        HybridResolver {
            resolver: Resolver::new(ResolveMode::Normal, tx, context, EMPTY_FILE_HANDLER_REF),
        }
    }

    /// Resolve all remaining unresolved references in the resolve context,
    /// erroring if anything fails to resolve.
    ///
    /// Already resolved items should not be checked.
    pub async fn resolve_remaining(
        &self,
        mut resolve_context: ResolveContext,
    ) -> Result<ResolveContext> {
        self.resolve_unresolved_table_fns(&mut resolve_context)
            .await?;
        self.resolve_unresolved_tables(&mut resolve_context).await?;
        // TODO: Might be worth doing these in parallel since we have the
        // complete context of the query.
        Ok(resolve_context)
    }

    async fn resolve_unresolved_tables(&self, resolve_context: &mut ResolveContext) -> Result<()> {
        for item in resolve_context.tables.inner.iter_mut() {
            if let MaybeResolved::Unresolved(unresolved) = item {
                debug!(%unresolved.reference, "(hybrid) resolving unresolved table");

                // Pass in empty bind data to resolver since it's only used for
                // CTE lookup, which shouldn't be possible here.
                let empty = ResolveContext::default();

                let table = NormalResolver::new(self.resolver.tx, self.resolver.context)
                    .require_resolve_table_or_cte(&unresolved.reference, &empty)
                    .await?;

                debug!(%unresolved.reference, "(hybrid) resolved unbound table");

                *item = MaybeResolved::Resolved(table, LocationRequirement::Remote)
            }
        }

        Ok(())
    }

    async fn resolve_unresolved_table_fns(
        &self,
        resolve_context: &mut ResolveContext,
    ) -> Result<()> {
        for item in resolve_context.table_functions.inner.iter_mut() {
            if let MaybeResolved::Unresolved(unresolved) = item {
                let table_fn = NormalResolver::new(self.resolver.tx, self.resolver.context)
                    .require_resolve_table_function(&unresolved.reference)?;

                let name = table_fn.name().to_string();
                let func = table_fn
                    .plan_and_initialize(self.resolver.context, unresolved.args.clone())
                    .await?;

                // TODO: Marker indicating this needs to be executing remotely.
                *item = MaybeResolved::Resolved(
                    ResolvedTableFunctionReference { name, func },
                    LocationRequirement::Remote,
                )
            }
        }

        Ok(())
    }
}
