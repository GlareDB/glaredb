use std::sync::Arc;

use datafusion::arrow::array::{BooleanBuilder, ListBuilder, StringBuilder, UInt32Builder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::logical_expr::TypeSignature;
use datasources::common::ssh::key::SshKey;
use datasources::common::ssh::SshConnectionParameters;
use protogen::metastore::types::catalog::{CatalogEntry, EntryType, SourceAccessMode, TableEntry};
use protogen::metastore::types::options::TunnelOptions;
use sqlbuiltins::builtins::{
    DATABASE_DEFAULT, GLARE_COLUMNS, GLARE_CREDENTIALS, GLARE_DATABASES, GLARE_DEPLOYMENT_METADATA,
    GLARE_FUNCTIONS, GLARE_SCHEMAS, GLARE_SSH_KEYS, GLARE_TABLES, GLARE_TUNNELS, GLARE_VIEWS,
    SCHEMA_CURRENT_SESSION,
};

use crate::metastore::catalog::{SessionCatalog, TempCatalog};

use super::{DispatchError, Result};

/// Dispatch to builtin system tables.
pub struct SystemTableDispatcher<'a> {
    catalog: &'a SessionCatalog,
    temp_objects: &'a TempCatalog,
}

impl<'a> SystemTableDispatcher<'a> {
    pub fn new(catalog: &'a SessionCatalog, temp_objects: &'a TempCatalog) -> Self {
        SystemTableDispatcher {
            catalog,
            temp_objects,
        }
    }

    pub fn dispatch(&self, ent: &TableEntry) -> Result<Arc<dyn TableProvider>> {
        let schema_ent = self
            .catalog
            .get_by_oid(ent.meta.parent)
            .ok_or_else(|| DispatchError::MissingObjectWithOid(ent.meta.parent))?;
        let name = &ent.meta.name;
        let schema = &schema_ent.get_meta().name;
        Ok(if GLARE_DATABASES.matches(schema, name) {
            Arc::new(self.build_glare_databases())
        } else if GLARE_TUNNELS.matches(schema, name) {
            Arc::new(self.build_glare_tunnels())
        } else if GLARE_CREDENTIALS.matches(schema, name) {
            Arc::new(self.build_glare_credentials())
        } else if GLARE_TABLES.matches(schema, name) {
            Arc::new(self.build_glare_tables())
        } else if GLARE_COLUMNS.matches(schema, name) {
            Arc::new(self.build_glare_columns())
        } else if GLARE_VIEWS.matches(schema, name) {
            Arc::new(self.build_glare_views())
        } else if GLARE_SCHEMAS.matches(schema, name) {
            Arc::new(self.build_glare_schemas())
        } else if GLARE_FUNCTIONS.matches(schema, name) {
            Arc::new(self.build_glare_functions())
        } else if GLARE_SSH_KEYS.matches(schema, name) {
            Arc::new(self.build_ssh_keys()?)
        } else if GLARE_DEPLOYMENT_METADATA.matches(schema, name) {
            Arc::new(self.build_glare_deployment_metadata()?)
        } else {
            return Err(DispatchError::MissingBuiltinTable {
                schema: schema.to_string(),
                name: name.to_string(),
            });
        })
    }

    fn build_glare_databases(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_DATABASES.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut external = BooleanBuilder::new();
        let mut datasource = StringBuilder::new();
        let mut access_mode = StringBuilder::new();

        for db in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Database)
        {
            oid.append_value(db.oid);
            database_name.append_value(&db.entry.get_meta().name);
            builtin.append_value(db.builtin);
            external.append_value(db.entry.get_meta().external);

            let db = match db.entry {
                CatalogEntry::Database(db) => db,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            datasource.append_value(db.options.as_str());
            access_mode.append_value(db.access_mode.as_str());
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(external.finish()),
                Arc::new(datasource.finish()),
                Arc::new(access_mode.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_tunnels(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_TUNNELS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut tunnel_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut tunnel_type = StringBuilder::new();

        for tunnel in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Tunnel)
        {
            oid.append_value(tunnel.oid);
            tunnel_name.append_value(&tunnel.entry.get_meta().name);
            builtin.append_value(tunnel.builtin);

            let tunnel = match tunnel.entry {
                CatalogEntry::Tunnel(tunnel) => tunnel,
                other => unreachable!("unexpected entry type: {other:?}"),
            };

            tunnel_type.append_value(tunnel.options.as_str());
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(tunnel_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(tunnel_type.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_credentials(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_CREDENTIALS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut credentials_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut provider = StringBuilder::new();
        let mut comment = StringBuilder::new();

        for creds in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Credentials)
        {
            oid.append_value(creds.oid);
            credentials_name.append_value(&creds.entry.get_meta().name);
            builtin.append_value(creds.builtin);

            let creds = match creds.entry {
                CatalogEntry::Credentials(creds) => creds,
                other => unreachable!("unexpected entry type: {other:?}"),
            };

            provider.append_value(creds.options.as_str());
            comment.append_value(&creds.comment);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(credentials_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(provider.finish()),
                Arc::new(comment.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_schemas(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_SCHEMAS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut database_name = StringBuilder::new();
        let mut schema_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();

        for schema in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Schema)
        {
            oid.append_value(schema.oid);
            database_oid.append_value(schema.entry.get_meta().parent);
            database_name.append_value(
                schema
                    .parent_entry
                    .map(|db| db.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            schema_name.append_value(&schema.entry.get_meta().name);
            builtin.append_value(schema.builtin);
        }
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_oid.finish()),
                Arc::new(database_name.finish()),
                Arc::new(schema_name.finish()),
                Arc::new(builtin.finish()),
            ],
        )
        .unwrap();
        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_tables(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_TABLES.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut schema_name = StringBuilder::new();
        let mut table_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut external = BooleanBuilder::new();
        let mut datasource = StringBuilder::new();
        let mut access_mode = StringBuilder::new();

        for table in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Table)
        {
            oid.append_value(table.oid);
            schema_oid.append_value(table.entry.get_meta().parent);
            database_oid.append_value(
                table
                    .parent_entry
                    .map(|schema| schema.get_meta().parent)
                    .unwrap_or_default(),
            );
            schema_name.append_value(
                table
                    .parent_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            table_name.append_value(&table.entry.get_meta().name);
            builtin.append_value(table.builtin);
            external.append_value(table.entry.get_meta().external);

            let table = match table.entry {
                CatalogEntry::Table(table) => table,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            datasource.append_value(table.options.as_str());
            access_mode.append_value(table.access_mode.as_str());
        }

        // Append temporary tables.
        for table in self.temp_objects.get_table_entries() {
            // TODO: Assign OID to temporary tables
            oid.append_value(table.meta.id);
            schema_oid.append_value(table.meta.parent);
            database_oid.append_value(DATABASE_DEFAULT.oid);
            schema_name.append_value(SCHEMA_CURRENT_SESSION.name);
            table_name.append_value(table.meta.name);
            builtin.append_value(table.meta.builtin);
            external.append_value(table.meta.external);
            datasource.append_value(table.options.as_str());
            access_mode.append_value(SourceAccessMode::ReadWrite.as_str());
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_oid.finish()),
                Arc::new(schema_oid.finish()),
                Arc::new(schema_name.finish()),
                Arc::new(table_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(external.finish()),
                Arc::new(datasource.finish()),
                Arc::new(access_mode.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_columns(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_COLUMNS.arrow_schema());

        let mut schema_oid = UInt32Builder::new();
        let mut table_oid = UInt32Builder::new();
        let mut table_name = StringBuilder::new();
        let mut column_name = StringBuilder::new();
        let mut column_ordinal = UInt32Builder::new();
        let mut data_type = StringBuilder::new();
        let mut is_nullable = BooleanBuilder::new();

        for table in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Table)
        {
            let ent = match table.entry {
                CatalogEntry::Table(ent) => ent,
                other => panic!("unexpected entry type: {:?}", other), // Bug
            };

            let cols = match ent.get_internal_columns() {
                Some(cols) => cols,
                None => continue,
            };

            for (i, col) in cols.iter().enumerate() {
                schema_oid.append_value(
                    table
                        .parent_entry
                        .map(|ent| ent.get_meta().id)
                        .unwrap_or_default(),
                );
                table_oid.append_value(table.oid);
                table_name.append_value(&table.entry.get_meta().name);
                column_name.append_value(&col.name);
                column_ordinal.append_value(i as u32);
                data_type.append_value(col.arrow_type.to_string());
                is_nullable.append_value(col.nullable);
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(schema_oid.finish()),
                Arc::new(table_oid.finish()),
                Arc::new(table_name.finish()),
                Arc::new(column_name.finish()),
                Arc::new(column_ordinal.finish()),
                Arc::new(data_type.finish()),
                Arc::new(is_nullable.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_views(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_VIEWS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut schema_name = StringBuilder::new();
        let mut view_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut sql = StringBuilder::new();

        for view in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::View)
        {
            let ent = match view.entry {
                CatalogEntry::View(ent) => ent,
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };

            oid.append_value(view.oid);
            database_oid.append_value(
                view.parent_entry
                    .map(|schema| schema.get_meta().parent)
                    .unwrap_or_default(),
            );
            schema_oid.append_value(view.entry.get_meta().parent);
            schema_name.append_value(
                view.parent_entry
                    .map(|schema| schema.get_meta().name.as_str())
                    .unwrap_or("<invalid>"),
            );
            view_name.append_value(&view.entry.get_meta().name);
            builtin.append_value(view.builtin);
            sql.append_value(&ent.sql);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(database_oid.finish()),
                Arc::new(schema_oid.finish()),
                Arc::new(schema_name.finish()),
                Arc::new(view_name.finish()),
                Arc::new(builtin.finish()),
                Arc::new(sql.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_glare_functions(&self) -> MemTable {
        let arrow_schema = Arc::new(GLARE_FUNCTIONS.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut function_name = StringBuilder::new();
        let mut function_type = StringBuilder::new();
        let mut parameters = ListBuilder::new(StringBuilder::new());
        let mut builtin = BooleanBuilder::new();

        for func in self
            .catalog
            .iter_entries()
            .filter(|ent| ent.entry_type() == EntryType::Function)
        {
            let ent = match func.entry {
                CatalogEntry::Function(ent) => ent,
                other => panic!("unexpected catalog entry: {:?}", other), // Bug
            };

            oid.append_value(func.oid);
            schema_oid.append_value(ent.meta.parent);
            function_name.append_value(&ent.meta.name);
            function_type.append_value(ent.func_type.as_str());

            const EMPTY: [Option<&'static str>; 0] = [];
            if let Some(sig) = &ent.signature {
                let sigs = sig_to_string_repr(&sig.type_signature)
                    .into_iter()
                    .map(Some)
                    .collect::<Vec<_>>();
                parameters.append_value(sigs);
            } else {
                parameters.append_value(EMPTY);
            }

            builtin.append_value(func.builtin);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(oid.finish()),
                Arc::new(schema_oid.finish()),
                Arc::new(function_name.finish()),
                Arc::new(function_type.finish()),
                Arc::new(parameters.finish()),
                Arc::new(builtin.finish()),
            ],
        )
        .unwrap();

        MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap()
    }

    fn build_ssh_keys(&self) -> Result<MemTable> {
        let arrow_schema = Arc::new(GLARE_SSH_KEYS.arrow_schema());

        let mut ssh_tunnel_oid = UInt32Builder::new();
        let mut ssh_tunnel_name = StringBuilder::new();
        let mut public_key = StringBuilder::new();

        for t in self.catalog.iter_entries().filter(|ent| match ent.entry {
            CatalogEntry::Tunnel(tunnel_entry) => {
                matches!(tunnel_entry.options, TunnelOptions::Ssh(_))
            }
            _ => false,
        }) {
            ssh_tunnel_oid.append_value(t.oid);
            ssh_tunnel_name.append_value(&t.entry.get_meta().name);

            match t.entry {
                CatalogEntry::Tunnel(tunnel_entry) => match &tunnel_entry.options {
                    TunnelOptions::Ssh(ssh_options) => {
                        let key = SshKey::from_bytes(&ssh_options.ssh_key)?;
                        let key = key.public_key()?;
                        let conn_params: SshConnectionParameters =
                            ssh_options.connection_string.parse()?;
                        let key = format!("{} {}", key, conn_params.user);
                        public_key.append_value(key);
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(ssh_tunnel_oid.finish()),
                Arc::new(ssh_tunnel_name.finish()),
                Arc::new(public_key.finish()),
            ],
        )
        .unwrap();

        Ok(MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap())
    }

    fn build_glare_deployment_metadata(&self) -> Result<MemTable> {
        let arrow_schema = Arc::new(GLARE_DEPLOYMENT_METADATA.arrow_schema());
        let deployment = self.catalog.deployment_metadata();

        let (mut key, mut value): (StringBuilder, StringBuilder) = [(
            Some("storage_size"),
            Some(deployment.storage_size.to_string()),
        )]
        .into_iter()
        .unzip();

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(key.finish()), Arc::new(value.finish())],
        )
        .unwrap();

        Ok(MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap())
    }
}
fn sig_to_string_repr(sig: &TypeSignature) -> Vec<String> {
    match sig {
        TypeSignature::Variadic(types) => {
            let types = types.iter().map(arrow_util::pretty::fmt_dtype);
            vec![format!("{}, ..", join_types(types, "/"))]
        }
        TypeSignature::Uniform(arg_count, valid_types) => {
            let types = valid_types.iter().map(arrow_util::pretty::fmt_dtype);
            vec![std::iter::repeat(join_types(types, "/"))
                .take(*arg_count)
                .collect::<Vec<String>>()
                .join(", ")]
        }
        TypeSignature::Exact(types) => {
            let types = types.iter().map(arrow_util::pretty::fmt_dtype);
            vec![join_types(types, ", ")]
        }
        TypeSignature::Any(arg_count) => {
            let types = std::iter::repeat("Any").take(*arg_count);
            vec![join_types(types, ",")]
        }
        TypeSignature::VariadicEqual => vec!["T, .., T".to_string()],
        TypeSignature::VariadicAny => vec!["Any, .., Any".to_string()],
        TypeSignature::OneOf(sigs) => sigs.iter().flat_map(sig_to_string_repr).collect(),
    }
}

/// Helper function to join types with specified delimiter.
pub(crate) fn join_types<T: Iterator<Item = U>, U: std::fmt::Display>(
    types: T,
    delimiter: &str,
) -> String {
    types
        .map(|t| t.to_string())
        .collect::<Vec<String>>()
        .join(delimiter)
}
