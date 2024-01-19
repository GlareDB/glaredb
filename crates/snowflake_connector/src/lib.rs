use crate::auth::{AuthOptions, Authenticator, DefaultAuthenticator, Session};
use crate::errors::{Result, SnowflakeError};
use crate::query::Query;
pub use crate::query::{
    snowflake_to_arrow_datatype,
    QueryBindParameter,
    QueryResult,
    QueryResultChunk,
    QueryResultChunkMeta,
};
use crate::req::SnowflakeClient;

mod auth;
mod query;
mod req;

pub mod datatype;
pub mod errors;

#[derive(Debug)]
pub struct ConnectionBuilder {
    account_name: String,
    login_name: String,

    password: Option<String>,
    database_name: Option<String>,
    schema_name: Option<String>,
    warehouse: Option<String>,
    role_name: Option<String>,
}

macro_rules! builder_fn {
    ($name:ident, $ty:ty) => {
        pub fn $name(mut self, $name: $ty) -> Self {
            self.$name = Some($name);
            self
        }
    };
}

impl ConnectionBuilder {
    builder_fn! {password, String}

    builder_fn! {database_name, String}

    builder_fn! {schema_name, String}

    builder_fn! {warehouse, String}

    builder_fn! {role_name, String}

    pub fn new(account_name: String, login_name: String) -> Self {
        Self {
            account_name,
            login_name,

            password: None,
            database_name: None,
            schema_name: None,
            warehouse: None,
            role_name: None,
        }
    }

    pub async fn build(self) -> Result<Connection> {
        if self.account_name.is_empty() || self.login_name.is_empty() {
            return Err(SnowflakeError::InvalidConnectionParameters(
                "account_name and login_name cannot be empty".to_string(),
            ));
        }

        let url = format!("https://{}.snowflakecomputing.com:443", self.account_name);
        let client = SnowflakeClient::builder().build(url)?;

        let password = self
            .password
            .ok_or(SnowflakeError::InvalidConnectionParameters(
                "password is required for default authentication".to_string(),
            ))?;

        let auth = DefaultAuthenticator {
            account_name: self.account_name,
            login_name: self.login_name,
            password,
        };
        let auth = Authenticator::Default(auth);

        let auth_options = AuthOptions {
            database_name: self.database_name,
            schema_name: self.schema_name,
            warehouse: self.warehouse,
            role_name: self.role_name,
        };

        let session = auth.authenticate(&client, auth_options).await?;

        Ok(Connection { client, session })
    }
}

pub struct Connection {
    client: SnowflakeClient,
    session: Session,
}

impl Connection {
    pub fn builder(account_name: String, login_name: String) -> ConnectionBuilder {
        ConnectionBuilder::new(account_name, login_name)
    }

    pub async fn close(self) -> Result<()> {
        self.session.close(&self.client).await
    }

    pub async fn exec_sync(&self, sql: String, bindings: Vec<QueryBindParameter>) -> Result<()> {
        let q = Query { sql, bindings };
        q.exec_sync(&self.client, &self.session).await
    }

    pub async fn query_sync(
        &self,
        sql: String,
        bindings: Vec<QueryBindParameter>,
    ) -> Result<QueryResult> {
        let q = Query { sql, bindings };
        q.query_sync(&self.client, &self.session).await
    }
}
