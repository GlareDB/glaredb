use std::sync::Arc;

use async_trait::async_trait;
use catalog::session_catalog::SessionCatalog;
use datafusion::{
    arrow::{
        array::{BooleanBuilder, ListBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field as ArrowField},
        record_batch::RecordBatch,
    },
    datasource::{MemTable, TableProvider},
    logical_expr::TypeSignature,
};
use protogen::metastore::types::{
    catalog::{CatalogEntry, EntryType, SourceAccessMode},
    options::{InternalColumnDefinition, TunnelOptions},
};

use crate::errors::Result;
use crate::{
    builtins::{DATABASE_DEFAULT, INTERNAL_SCHEMA, SCHEMA_CURRENT_SESSION},
    functions::FUNCTION_REGISTRY,
};
use datasources::common::ssh::key::SshKey;
use datasources::common::ssh::SshConnectionParameters;
use datasources::native::access::NativeTableStorage;

use super::BuiltinTable;

pub struct GlareDatabases;

#[async_trait]
impl BuiltinTable for GlareDatabases {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "databases"
    }
    fn oid(&self) -> u32 {
        16401
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("oid", DataType::UInt32, false),
            ("database_name", DataType::Utf8, false),
            ("builtin", DataType::Boolean, false),
            ("external", DataType::Boolean, false),
            ("datasource", DataType::Utf8, false),
            ("access_mode", DataType::Utf8, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut external = BooleanBuilder::new();
        let mut datasource = StringBuilder::new();
        let mut access_mode = StringBuilder::new();

        for db in catalog
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
        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareTunnels;

#[async_trait]
impl BuiltinTable for GlareTunnels {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "tunnels"
    }
    fn oid(&self) -> u32 {
        16402
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("oid", DataType::UInt32, false),
            ("tunnel_name", DataType::Utf8, false),
            ("builtin", DataType::Boolean, false),
            ("tunnel_type", DataType::Utf8, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut tunnel_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut tunnel_type = StringBuilder::new();

        for tunnel in catalog
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
        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareCredentials;

#[async_trait]
impl BuiltinTable for GlareCredentials {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "credentials"
    }
    fn oid(&self) -> u32 {
        16403
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("oid", DataType::UInt32, false),
            ("credentials_name", DataType::Utf8, false),
            ("builtin", DataType::Boolean, false),
            ("provider", DataType::Utf8, false),
            ("comment", DataType::Utf8, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut credentials_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut provider = StringBuilder::new();
        let mut comment = StringBuilder::new();

        for creds in catalog
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
        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareSchemas;

#[async_trait]
impl BuiltinTable for GlareSchemas {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "schemas"
    }
    fn oid(&self) -> u32 {
        16404
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("oid", DataType::UInt32, false),
            ("database_oid", DataType::UInt32, false),
            ("database_name", DataType::Utf8, false),
            ("schema_name", DataType::Utf8, false),
            ("builtin", DataType::Boolean, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut database_name = StringBuilder::new();
        let mut schema_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();

        for schema in catalog
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
        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareTables;

#[async_trait]
impl BuiltinTable for GlareTables {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "tables"
    }
    fn oid(&self) -> u32 {
        16405
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("oid", DataType::UInt32, false),
            ("database_oid", DataType::UInt32, false),
            ("schema_oid", DataType::UInt32, false),
            ("schema_name", DataType::Utf8, false),
            ("table_name", DataType::Utf8, false),
            ("builtin", DataType::Boolean, false),
            ("external", DataType::Boolean, false),
            ("datasource", DataType::Utf8, false),
            ("access_mode", DataType::Utf8, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut schema_name = StringBuilder::new();
        let mut table_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut external = BooleanBuilder::new();
        let mut datasource = StringBuilder::new();
        let mut access_mode = StringBuilder::new();

        for table in catalog
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
        for table in catalog.get_temp_catalog().get_table_entries() {
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

        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareViews;

#[async_trait]
impl BuiltinTable for GlareViews {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "views"
    }
    fn oid(&self) -> u32 {
        16406
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("oid", DataType::UInt32, false),
            ("database_oid", DataType::UInt32, false),
            ("schema_oid", DataType::UInt32, false),
            ("schema_name", DataType::Utf8, false),
            ("view_name", DataType::Utf8, false),
            ("builtin", DataType::Boolean, false),
            ("sql", DataType::Utf8, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut database_oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut schema_name = StringBuilder::new();
        let mut view_name = StringBuilder::new();
        let mut builtin = BooleanBuilder::new();
        let mut sql = StringBuilder::new();

        for view in catalog
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

        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareColumns;

#[async_trait]
impl BuiltinTable for GlareColumns {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "columns"
    }
    fn oid(&self) -> u32 {
        16407
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("schema_oid", DataType::UInt32, false),
            ("table_oid", DataType::UInt32, false),
            ("table_name", DataType::Utf8, false),
            ("column_name", DataType::Utf8, false),
            ("column_ordinal", DataType::UInt32, false),
            ("data_type", DataType::Utf8, false),
            ("is_nullable", DataType::Boolean, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut schema_oid = UInt32Builder::new();
        let mut table_oid = UInt32Builder::new();
        let mut table_name = StringBuilder::new();
        let mut column_name = StringBuilder::new();
        let mut column_ordinal = UInt32Builder::new();
        let mut data_type = StringBuilder::new();
        let mut is_nullable = BooleanBuilder::new();

        for table in catalog
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

        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareFunctions;

#[async_trait]
impl BuiltinTable for GlareFunctions {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "functions"
    }
    fn oid(&self) -> u32 {
        16408
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("oid", DataType::UInt32, false),
            ("schema_oid", DataType::UInt32, false),
            ("function_name", DataType::Utf8, false),
            ("function_type", DataType::Utf8, false), // table, scalar, aggregate
            (
                "parameters",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
                false,
            ),
            ("builtin", DataType::Boolean, false),
            ("example", DataType::Utf8, true),
            ("description", DataType::Utf8, true),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut oid = UInt32Builder::new();
        let mut schema_oid = UInt32Builder::new();
        let mut function_name = StringBuilder::new();
        let mut function_type = StringBuilder::new();
        let mut parameters = ListBuilder::new(StringBuilder::new());
        let mut builtin = BooleanBuilder::new();
        let mut sql_examples = StringBuilder::new();
        let mut descriptions = StringBuilder::new();

        for func in catalog
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
            sql_examples.append_option(FUNCTION_REGISTRY.get_function_example(&ent.meta.name));
            descriptions.append_option(FUNCTION_REGISTRY.get_function_description(&ent.meta.name));

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
                Arc::new(sql_examples.finish()),
                Arc::new(descriptions.finish()),
            ],
        )
        .unwrap();

        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareSShKeys;

#[async_trait]
impl BuiltinTable for GlareSShKeys {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "ssh_keys"
    }
    fn oid(&self) -> u32 {
        16409
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("ssh_tunnel_oid", DataType::UInt32, false),
            ("ssh_tunnel_name", DataType::Utf8, false),
            ("public_key", DataType::Utf8, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());

        let mut ssh_tunnel_oid = UInt32Builder::new();
        let mut ssh_tunnel_name = StringBuilder::new();
        let mut public_key = StringBuilder::new();

        for t in catalog.iter_entries().filter(|ent| match ent.entry {
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
                        let key = SshKey::from_bytes(&ssh_options.ssh_key)
                            .expect("keys should be checked on entry");
                        let key = key.public_key().expect("keys should be checked on entry");
                        let conn_params: SshConnectionParameters = ssh_options
                            .connection_string
                            .parse()
                            .expect("connection params should be checked on entry");
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

        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareDeploymentMetadata;

#[async_trait]
impl BuiltinTable for GlareDeploymentMetadata {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "deployment_metadata"
    }
    fn oid(&self) -> u32 {
        16410
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            ("key", DataType::Utf8, false),
            ("value", DataType::Utf8, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        _: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let arrow_schema = Arc::new(self.arrow_schema());
        let deployment = catalog.deployment_metadata();

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

        Ok(Arc::new(
            MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap(),
        ))
    }
}

pub struct GlareCachedExternalDatabaseTables;

#[async_trait]
impl BuiltinTable for GlareCachedExternalDatabaseTables {
    fn schema(&self) -> &'static str {
        INTERNAL_SCHEMA
    }
    fn name(&self) -> &'static str {
        "cached_external_database_tables"
    }
    fn oid(&self) -> u32 {
        16411
    }

    fn columns(&self) -> Vec<InternalColumnDefinition> {
        InternalColumnDefinition::from_tuples([
            // External database this entry is for.
            ("database_oid", DataType::UInt32, false),
            // Schema name (in external database).
            ("schema_name", DataType::Utf8, false),
            // Table name (in external database).
            ("table_name", DataType::Utf8, false),
            // Column name (in external database).
            ("column_name", DataType::Utf8, false),
            ("data_type", DataType::Utf8, false),
        ])
    }

    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        tables: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let ent = catalog
            .get_by_oid(self.oid())
            .expect("entry should be in catalog");
        let ent = match ent {
            CatalogEntry::Table(ent) => ent,
            other => panic!("unexpected entry type: {other:?}"),
        };

        match tables.load_table(ent).await {
            Ok(table) => Ok(Arc::new(table)),
            Err(_e) => {
                // TODO: I don't know if delta provides a guarantee about the
                // error returned if the table doesn't exist. We might want to
                // have a dedicated 'exists' method or something.
                let arrow_schema = Arc::new(self.arrow_schema());
                let empty = RecordBatch::new_empty(arrow_schema.clone());
                Ok(Arc::new(
                    MemTable::try_new(arrow_schema, vec![vec![empty]]).unwrap(),
                ))
            }
        }
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
