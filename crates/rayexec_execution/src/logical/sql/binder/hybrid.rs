use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    datasource::FileHandlers,
    logical::{operator::LocationRequirement, sql::binder::BindMode},
};
use rayexec_error::Result;

use super::{
    bind_data::{BoundTableFunctionReference, MaybeBound},
    resolver::Resolver,
    BindData, Binder,
};

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
/// to bind everything _but_ the `read_postgres` call, then send the serialized
/// plan to remote node, which then uses this resolver to appropriately bind the
/// `read_postgres` function (assuming the remote node has the postgres data
/// source registered).
///
/// Once resolved, the remote node can continue with planning the statement,
/// sending back parts of the pipeline that the "local" side should execute.
// TODO: Somehow do search path.
#[derive(Debug)]
pub struct HybridResolver<'a> {
    pub binder: Binder<'a>,
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
            binder: Binder::new(BindMode::Normal, tx, context, EMPTY_FILE_HANDLER_REF),
        }
    }

    /// Resolve all unbound references in the bind data, erroring if anything
    /// fails to resolve.
    ///
    /// Bound items should not be checked.
    pub async fn resolve_all_unbound(&self, mut bind_data: BindData) -> Result<BindData> {
        self.resolve_unbound_table_fns(&mut bind_data).await?;
        self.resolve_unbound_tables(&mut bind_data).await?;
        // TODO: Might be worth doing these in parallel since we have the
        // complete context of the query.
        Ok(bind_data)
    }

    async fn resolve_unbound_tables(&self, bind_data: &mut BindData) -> Result<()> {
        for item in bind_data.tables.inner.iter_mut() {
            if let MaybeBound::Unbound(unbound) = item {
                // Pass in empty bind data to resolver since it's only used for
                // CTE lookup, which shouldn't be possible here.
                let empty = BindData::default();

                let table = Resolver::new(self.binder.tx, self.binder.context)
                    .require_resolve_table_or_cte(unbound, &empty)
                    .await?;

                *item = MaybeBound::Bound(table, LocationRequirement::Remote)
            }
        }

        Ok(())
    }

    async fn resolve_unbound_table_fns(&self, bind_data: &mut BindData) -> Result<()> {
        for item in bind_data.table_functions.inner.iter_mut() {
            if let MaybeBound::Unbound(unbound) = item {
                let table_fn = Resolver::new(self.binder.tx, self.binder.context)
                    .require_resolve_table_function(&unbound.reference)?;

                let name = table_fn.name().to_string();
                let func = table_fn.plan_and_initialize(unbound.args.clone()).await?;

                // TODO: Marker indicating this needs to be executing remotely.
                *item = MaybeBound::Bound(
                    BoundTableFunctionReference { name, func },
                    LocationRequirement::Remote,
                )
            }
        }

        Ok(())
    }
}
