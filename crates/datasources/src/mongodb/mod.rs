//! MongoDB as a data source.
pub mod errors;

mod exec;
mod infer;

use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::VirtualLister;
use errors::{MongoDbError, Result};
use exec::MongoBsonExec;
use infer::TableSampler;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    FieldRef, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{bson, Binary, Bson, Document, RawDocumentBuf};
use mongodb::options::{ClientOptions, FindOptions};
use mongodb::Client;
use mongodb::Collection;
use std::any::Any;
use std::fmt::{Display, Write};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tracing::debug;

/// Field name in mongo for uniquely identifying a record. Some special handling
/// needs to be done with the field when projecting.
const ID_FIELD_NAME: &str = "_id";

#[derive(Debug)]
pub enum MongoDbProtocol {
    MongoDb,
    MongoDbSrv,
}

impl Default for MongoDbProtocol {
    fn default() -> Self {
        Self::MongoDb
    }
}

impl MongoDbProtocol {
    const MONGODB: &'static str = "mongodb";
    const MONGODB_SRV: &'static str = "mongodb+srv";
}

impl FromStr for MongoDbProtocol {
    type Err = MongoDbError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let proto = match s {
            Self::MONGODB => Self::MongoDb,
            Self::MONGODB_SRV => Self::MongoDbSrv,
            s => return Err(MongoDbError::InvalidProtocol(s.to_owned())),
        };
        Ok(proto)
    }
}

impl Display for MongoDbProtocol {
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
        protocol: MongoDbProtocol,
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
                if matches!(protocol, MongoDbProtocol::MongoDb) {
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
pub struct MongoDbAccessor {
    client: Client,
}

impl MongoDbAccessor {
    pub async fn connect(connection_string: &str) -> Result<MongoDbAccessor> {
        let mut opts = ClientOptions::parse(connection_string).await?;
        opts.app_name = Some("GlareDB (MongoDB Data source)".to_string());
        let client = Client::with_options(opts)?;

        Ok(MongoDbAccessor { client })
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

    pub fn into_table_accessor(self, info: MongoDbTableAccessInfo) -> MongoDbTableAccessor {
        MongoDbTableAccessor {
            info,
            client: self.client,
        }
    }
}

#[async_trait]
impl VirtualLister for MongoDbAccessor {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        use ExtensionError::ListingErrBoxed;

        let databases = self
            .client
            .list_database_names(/* filter: */ None, /* options: */ None)
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?;

        Ok(databases)
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, ExtensionError> {
        use ExtensionError::ListingErrBoxed;

        let database = self.client.database(database);
        let collections = database
            .list_collection_names(/* filter: */ None)
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?;

        Ok(collections)
    }

    async fn list_columns(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Fields, ExtensionError> {
        use ExtensionError::ListingErrBoxed;

        let collection = self.client.database(database).collection(collection);
        let sampler = TableSampler::new(collection);

        let schema = sampler
            .infer_schema_from_sample()
            .await
            .map_err(|e| ListingErrBoxed(Box::new(e)))?;

        Ok(schema.fields)
    }
}

#[derive(Debug, Clone)]
pub struct MongoDbTableAccessInfo {
    pub database: String, // "Schema"
    pub collection: String,
    pub fields: Option<Vec<FieldRef>>, // filter
}

#[derive(Debug, Clone)]
pub struct MongoDbTableAccessor {
    info: MongoDbTableAccessInfo,
    client: Client,
}

impl MongoDbTableAccessor {
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

    pub async fn into_table_provider(self) -> Result<MongoDbTableProvider> {
        let schema = if self.info.fields.is_some() {
            ArrowSchema::new(self.info.fields.unwrap())
        } else {
            let collection = self
                .client
                .database(&self.info.database)
                .collection(&self.info.collection);

            TableSampler::new(collection)
                .infer_schema_from_sample()
                .await?
        };

        Ok(MongoDbTableProvider {
            schema: Arc::new(schema),
            collection: self
                .client
                .database(&self.info.database)
                .collection(&self.info.collection),
        })
    }
}

pub struct MongoDbTableProvider {
    schema: Arc<ArrowSchema>,
    collection: Collection<RawDocumentBuf>,
}

#[async_trait]
impl TableProvider for MongoDbTableProvider {
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
        let schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => self.schema.clone(),
        };

        // Projection document. Project everything that's in the schema.
        //
        // The `_id` field is special and needs to be manually suppressed if not
        // included in the schema.
        let mut proj_doc = Document::new();
        let mut has_id_field = false;
        for field in &schema.fields {
            proj_doc.insert(field.name(), 1);
            has_id_field = has_id_field || field.name().as_str() == ID_FIELD_NAME;
        }

        if !has_id_field {
            proj_doc.insert(ID_FIELD_NAME, 0);
        }

        let mut find_opts = FindOptions::default();
        find_opts.limit = limit.map(|v| v as i64);
        find_opts.projection = Some(proj_doc);

        let filter = match exprs_to_mdb_query(_filters) {
            Ok(query) => query,
            Err(err) => {
                debug!("mdb pushdown query err: {}", err.to_string());
                Document::new()
            }
        };

        let cursor = Mutex::new(Some(
            self.collection
                .find(Some(filter), Some(find_opts))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ));
        Ok(Arc::new(MongoBsonExec::new(cursor, schema, limit)))
    }
}

fn exprs_to_mdb_query(exprs: &[Expr]) -> Result<Document, ExtensionError> {
    let mut doc = Document::new();
    for e in exprs {
        let expr = &e;
        match expr {
            Expr::BinaryExpr(val) => {
                match val.left.as_ref() {
                    Expr::Column(key) => match val.right.as_ref() {
                        Expr::Literal(v) => doc.insert(
                            key.to_string(),
                            bson!({operator_to_mdbq(val.op)?: df_to_bson(v.clone())?}),
                        ),
                        _ => {
                            continue;
                        }
                    },
                    Expr::Literal(v) => match val.right.as_ref() {
                        Expr::Column(key) => doc.insert(
                            key.to_string(),
                            bson!({operator_to_mdbq(val.op)?: df_to_bson(v.clone())?}),
                        ),
                        _ => {
                            continue;
                        }
                    },
                    _ => {
                        continue;
                    }
                };
            }
            _ => {
                continue;
            }
        };
    }

    Ok(doc.to_owned())
}

fn operator_to_mdbq(op: Operator) -> Result<String, ExtensionError> {
    match op {
        Operator::Eq => Ok("$eq".to_string()),
        Operator::Gt => Ok("$gt".to_string()),
        Operator::Lt => Ok("$lt".to_string()),
        Operator::NotEq => Ok("$ne".to_string()),
        Operator::GtEq => Ok("$gte".to_string()),
        Operator::LtEq => Ok("$lte".to_string()),
        Operator::And => Ok("$and".to_string()),
        Operator::Or => Ok("$or".to_string()),
        Operator::Modulo => Ok("$mod".to_string()),
        Operator::RegexMatch => Ok("$regex".to_string()),
        _ => Err(ExtensionError::String(format!(
            "{} operator is not translated",
            op
        ))),
    }
}

fn df_to_bson(val: ScalarValue) -> Result<Bson, ExtensionError> {
    match val {
        ScalarValue::Binary(v) => Ok(Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v.unwrap_or_default(),
        })),
        ScalarValue::LargeBinary(v) => Ok(Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v.unwrap_or_default(),
        })),
        ScalarValue::FixedSizeBinary(_, v) => Ok(Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v.unwrap_or_default(),
        })),
        ScalarValue::Utf8(v) => Ok(Bson::String(v.unwrap_or_default())),
        ScalarValue::LargeUtf8(v) => Ok(Bson::String(v.unwrap_or_default())),
        ScalarValue::Boolean(v) => Ok(Bson::Boolean(v.unwrap_or_default())),
        ScalarValue::Int8(v) => Ok(Bson::Int32(i32::from(v.unwrap_or_default()))),
        ScalarValue::Int16(v) => Ok(Bson::Int32(i32::from(v.unwrap_or_default()))),
        ScalarValue::Int32(v) => Ok(Bson::Int32(v.unwrap_or_default())),
        ScalarValue::Int64(v) => Ok(Bson::Int64(v.unwrap_or_default())),
        ScalarValue::UInt16(v) => Ok(Bson::Int32(i32::from(v.unwrap_or_default()))),
        ScalarValue::UInt8(v) => Ok(Bson::Int32(i32::from(v.unwrap_or_default()))),
        ScalarValue::UInt32(v) => Ok(Bson::Int64(i64::from(v.unwrap_or_default()))),
        ScalarValue::UInt64(v) => Ok(Bson::Int64(i64::try_from(v.unwrap_or_default()).unwrap())),
        ScalarValue::Float32(v) => Ok(Bson::Double(f64::from(v.unwrap_or_default()))),
        ScalarValue::Float64(v) => Ok(Bson::Double(v.unwrap_or_default())),
        ScalarValue::Null => Ok(Bson::Null),
        _ => Err(ExtensionError::String(format!(
            "{} conversion undefined",
            val
        ))),
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
            protocol: MongoDbProtocol::MongoDb,
            host: "127.0.0.1".to_string(),
            port: Some(5432),
            user: "prod".to_string(),
            password: Some("password123".to_string()),
        };
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "mongodb://prod:password123@127.0.0.1:5432");

        let conn_str = MongoDbConnection::Parameters {
            protocol: MongoDbProtocol::MongoDbSrv,
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
