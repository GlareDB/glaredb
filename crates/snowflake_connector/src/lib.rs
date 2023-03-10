use crate::auth::{AuthOptions, Authenticator, DefaultAuthenticator, Session};
use crate::errors::{Result, SnowflakeError};
use crate::query::Query;
use crate::req::SnowflakeClient;

use datafusion::arrow::record_batch::RecordBatch;

mod auth;
mod query;
mod req;

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

    builder_fn! {password, String}
    builder_fn! {database_name, String}
    builder_fn! {schema_name, String}
    builder_fn! {warehouse, String}
    builder_fn! {role_name, String}

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

    pub async fn query(&self, sql: String) -> Result<RecordBatch> {
        let query = Query { sql };
        query.exec(&self.client, &self.session).await
    }
}
