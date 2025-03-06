use rayexec_error::Result;

use super::builtin_views::BUILTIN_VIEWS;
use super::create::{CreateCopyToFunctionInfo, CreateViewInfo};
use super::memory_catalog::MemoryCatalog;
use crate::database::catalog::CatalogTx;
use crate::database::create::{
    CreateAggregateFunctionInfo,
    CreateScalarFunctionInfo,
    CreateSchemaInfo,
    CreateTableFunctionInfo,
    OnConflict,
};
use crate::datasource::DataSourceRegistry;
use crate::functions::aggregate::builtin::BUILTIN_AGGREGATE_FUNCTION_SETS;
use crate::functions::scalar::builtin::BUILTIN_SCALAR_FUNCTION_SETS;
use crate::functions::table::builtin::BUILTIN_TABLE_FUNCTION_SETS;

/// Create a new system catalog with builtin functions.
///
/// The provided data source reigstry is used to initialize table functions for
/// reading external sources.
pub fn new_system_catalog(registry: &DataSourceRegistry) -> Result<MemoryCatalog> {
    let catalog = MemoryCatalog::default();

    let tx = &CatalogTx {};

    let builtin = catalog.create_schema(
        tx,
        &CreateSchemaInfo {
            name: "glare_catalog".to_string(),
            on_conflict: OnConflict::Error,
        },
    )?;

    let _pg_catalog = catalog.create_schema(
        tx,
        &CreateSchemaInfo {
            name: "pg_catalog".to_string(),
            on_conflict: OnConflict::Error,
        },
    )?;

    let _pg_catalog = catalog.create_schema(
        tx,
        &CreateSchemaInfo {
            name: "information_schema".to_string(),
            on_conflict: OnConflict::Error,
        },
    )?;

    // Add builtin scalars.
    for func in BUILTIN_SCALAR_FUNCTION_SETS.iter() {
        builtin.create_scalar_function(
            tx,
            &CreateScalarFunctionInfo {
                name: func.name.to_string(),
                implementation: func.clone(),
                on_conflict: OnConflict::Error,
            },
        )?;

        for alias in func.aliases {
            builtin.create_scalar_function(
                tx,
                &CreateScalarFunctionInfo {
                    name: alias.to_string(),
                    implementation: func.clone(),
                    on_conflict: OnConflict::Error,
                },
            )?;
        }
    }

    // Add builtin aggregates.
    for func in BUILTIN_AGGREGATE_FUNCTION_SETS.iter() {
        builtin.create_aggregate_function(
            tx,
            &CreateAggregateFunctionInfo {
                name: func.name.to_string(),
                implementation: func.clone(),
                on_conflict: OnConflict::Error,
            },
        )?;

        for alias in func.aliases {
            builtin.create_aggregate_function(
                tx,
                &CreateAggregateFunctionInfo {
                    name: alias.to_string(),
                    implementation: func.clone(),
                    on_conflict: OnConflict::Error,
                },
            )?;
        }
    }

    // Add builtin table functions.
    for func in BUILTIN_TABLE_FUNCTION_SETS.iter() {
        builtin.create_table_function(
            tx,
            &CreateTableFunctionInfo {
                name: func.name.to_string(),
                implementation: func.clone(),
                on_conflict: OnConflict::Error,
            },
        )?;

        for alias in func.aliases {
            builtin.create_table_function(
                tx,
                &CreateTableFunctionInfo {
                    name: alias.to_string(),
                    implementation: func.clone(),
                    on_conflict: OnConflict::Error,
                },
            )?;
        }
    }

    // Add builtin views.
    for view in BUILTIN_VIEWS {
        builtin.create_view(
            tx,
            &CreateViewInfo {
                name: view.name.to_string(),
                column_aliases: None,
                on_conflict: OnConflict::Error,
                query_string: view.view.to_string(),
            },
        )?;
    }

    // // Add data source functions.
    // for datasource in registry.iter() {
    //     let table_funcs = datasource.initialize_table_functions();

    //     for func in table_funcs {
    //         builtin.create_table_function(
    //             tx,
    //             &CreateTableFunctionInfo {
    //                 name: func.name().to_string(),
    //                 implementation: func.clone(),
    //                 on_conflict: OnConflict::Error,
    //             },
    //         )?;

    //         for alias in func.aliases() {
    //             builtin.create_table_function(
    //                 tx,
    //                 &CreateTableFunctionInfo {
    //                     name: alias.to_string(),
    //                     implementation: func.clone(),
    //                     on_conflict: OnConflict::Error,
    //                 },
    //             )?;
    //         }
    //     }

    //     let copy_to_funcs = datasource.initialize_copy_to_functions();

    //     for func in copy_to_funcs {
    //         builtin.create_copy_to_function(
    //             tx,
    //             &CreateCopyToFunctionInfo {
    //                 name: func.copy_to.name().to_string(),
    //                 format: func.format,
    //                 implementation: func.copy_to,
    //                 on_conflict: OnConflict::Error,
    //             },
    //         )?;
    //     }
    // }

    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_without_error() {
        // This will ensure we don't have duplicate names for functions.
        new_system_catalog(&DataSourceRegistry::default()).unwrap();
    }
}
