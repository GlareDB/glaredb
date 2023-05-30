//! MongoDB as a data source.
pub mod errors;

mod builder;
mod exec;
mod infer;

use errors::{MongoError, Result};
use exec::MongoBsonExec;
use infer::TableSampler;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datasource_common::errors::DatasourceCommonError;
use datasource_common::listing::{VirtualLister, VirtualTable};
use mongodb::bson::{Bson, Document, RawDocumentBuf};
use mongodb::Collection;
use mongodb::{options::ClientOptions, Client};
use std::any::Any;
use std::fmt::{Display, Write};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug)]
pub enum MongoProtocol {
    MongoDb,
    MongoDbSrv,
}

impl Default for MongoProtocol {
    fn default() -> Self {
        Self::MongoDb
    }
}

impl MongoProtocol {
    const MONGODB: &'static str = "mongodb";
    const MONGODB_SRV: &'static str = "mongodb+srv";
}

impl FromStr for MongoProtocol {
    type Err = MongoError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let proto = match s {
            Self::MONGODB => Self::MongoDb,
            Self::MONGODB_SRV => Self::MongoDbSrv,
            s => return Err(MongoError::InvalidProtocol(s.to_owned())),
        };
        Ok(proto)
    }
}

impl Display for MongoProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::MongoDb => Self::MONGODB,
            Self::MongoDbSrv => Self::MONGODB_SRV,
        };
        f.write_str(s)
    }
}

#[derive(Debug)]
pub enum MongoDbConnection {
    ConnectionString(String),
    Parameters {
        protocol: MongoProtocol,
        host: String,
        port: Option<u16>,
        user: String,
        password: Option<String>,
    },
}

impl MongoDbConnection {
    pub fn connection_string(&self) -> String {
        match self {
            Self::ConnectionString(s) => s.to_owned(),
            Self::Parameters {
                protocol,
                host,
                port,
                user,
                password,
            } => {
                let mut conn_str = String::new();
                // Protocol
                write!(&mut conn_str, "{protocol}://").unwrap();
                // Credentials
                write!(&mut conn_str, "{user}").unwrap();
                if let Some(password) = password {
                    write!(&mut conn_str, ":{password}").unwrap();
                }
                // Address
                write!(&mut conn_str, "@{host}").unwrap();
                if matches!(protocol, MongoProtocol::MongoDb) {
                    // Only attempt to write port if the protocol is "mongodb"
                    if let Some(port) = port {
                        write!(&mut conn_str, ":{port}").unwrap();
                    }
                }
                conn_str
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MongoAccessor {
    client: Client,
}

impl MongoAccessor {
    pub async fn connect(connection_string: &str) -> Result<MongoAccessor> {
        let mut opts = ClientOptions::parse(connection_string).await?;
        opts.app_name = Some("GlareDB (MongoDB Data source)".to_string());
        let client = Client::with_options(opts)?;

        Ok(MongoAccessor { client })
    }

    pub async fn validate_external_database(connection_string: &str) -> Result<()> {
        let accessor = Self::connect(connection_string).await?;
        let mut filter = Document::new();
        filter.insert("name".to_string(), Bson::String("glaredb".to_string()));
        let _ = accessor
            .client
            .list_database_names(Some(filter), None)
            .await?;
        Ok(())
    }

    pub fn into_table_accessor(self, info: MongoTableAccessInfo) -> MongoTableAccessor {
        MongoTableAccessor {
            info,
            client: self.client,
        }
    }
}

#[async_trait]
impl VirtualLister for MongoAccessor {
    async fn list_schemas(&self) -> Result<Vec<String>, DatasourceCommonError> {
        use DatasourceCommonError::ListingErrBoxed;

        let databases = self
            .client
            .list_database_names(/* filter: */ None, /* options: */ None)
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?;

        Ok(databases)
    }

    async fn list_tables(
        &self,
        schema: Option<&str>,
    ) -> Result<Vec<VirtualTable>, DatasourceCommonError> {
        use DatasourceCommonError::ListingErrBoxed;

        let database = if let Some(schema) = schema {
            self.client.database(schema)
        } else {
            self.client
                .default_database()
                .ok_or(ListingErrBoxed(Box::new(
                    MongoError::MissingSchemaForVirtualLister,
                )))?
        };

        let schema = database.name();

        let collections = database
            .list_collection_names(/* filter: */ None)
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?
            .into_iter()
            .map(|name| VirtualTable {
                schema: schema.to_owned(),
                table: name,
            })
            .collect();

        Ok(collections)
    }
}

#[derive(Debug, Clone)]
pub struct MongoTableAccessInfo {
    pub database: String, // "Schema"
    pub collection: String,
}

#[derive(Debug, Clone)]
pub struct MongoTableAccessor {
    info: MongoTableAccessInfo,
    client: Client,
}

impl MongoTableAccessor {
    /// Validate that we can access the table.
    pub async fn validate(&self) -> Result<()> {
        let _ = self
            .client
            .database(&self.info.database)
            .collection::<Document>(&self.info.collection)
            .estimated_document_count(None)
            .await?;

        Ok(())
    }

    pub async fn into_table_provider(self) -> Result<MongoTableProvider> {
        let collection = self
            .client
            .database(&self.info.database)
            .collection(&self.info.collection);
        let sampler = TableSampler::new(collection);

        let schema = sampler.infer_schema_from_sample().await?;

        Ok(MongoTableProvider {
            schema: Arc::new(schema),
            collection: self
                .client
                .database(&self.info.database)
                .collection(&self.info.collection),
        })
    }
}

pub struct MongoTableProvider {
    schema: Arc<ArrowSchema>,
    collection: Collection<RawDocumentBuf>,
}

#[async_trait]
impl TableProvider for MongoTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DatafusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        // Projection.
        //
        // Note that this projection will only project top-level fields. There
        // is not a way to project nested documents (at least when modelling
        // nested docs as a struct).
        let projected_schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => self.schema.clone(),
        };

        // TODO: Filters.

        Ok(Arc::new(MongoBsonExec::new(
            projected_schema,
            self.collection.clone(),
            limit,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_string() {
        let conn_str = MongoDbConnection::ConnectionString(
            "mongodb://prod:password123@127.0.0.1:5432".to_string(),
        )
        .connection_string();
        assert_eq!(&conn_str, "mongodb://prod:password123@127.0.0.1:5432");

        let conn_str = MongoDbConnection::Parameters {
            protocol: MongoProtocol::MongoDb,
            host: "127.0.0.1".to_string(),
            port: Some(5432),
            user: "prod".to_string(),
            password: Some("password123".to_string()),
        };
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "mongodb://prod:password123@127.0.0.1:5432");

        let conn_str = MongoDbConnection::Parameters {
            protocol: MongoProtocol::MongoDbSrv,
            host: "127.0.0.1".to_string(),
            port: Some(5432),
            user: "prod".to_string(),
            password: Some("password123".to_string()),
        };
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "mongodb+srv://prod:password123@127.0.0.1");

        // Missing password.
        let conn_str = MongoDbConnection::Parameters {
            protocol: Default::default(),
            host: "127.0.0.1".to_string(),
            port: Some(5432),
            user: "prod".to_string(),
            password: None,
        };
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "mongodb://prod@127.0.0.1:5432");

        // Missing port.
        let conn_str = MongoDbConnection::Parameters {
            protocol: Default::default(),
            host: "127.0.0.1".to_string(),
            port: None,
            user: "prod".to_string(),
            password: Some("password123".to_string()),
        };
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "mongodb://prod:password123@127.0.0.1");
    }
}
