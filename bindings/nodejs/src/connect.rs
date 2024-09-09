//! Main entry point for the js bindings.
//!
//! User's will call `connect` which returns a session for executing sql
//! queries.

use std::collections::HashMap;
use std::sync::Arc;

use crate::connection::Connection;
use crate::error::JsDatabaseError;

#[napi(object)]
#[derive(Default)]
pub struct ConnectOptions {
    pub spill_path: Option<String>,
    pub disable_tls: Option<bool>,
    pub cloud_addr: Option<String>,
    pub location: Option<String>,
    pub storage_options: Option<HashMap<String, String>>,
}

impl TryFrom<ConnectOptions> for glaredb::ConnectOptions {
    type Error = glaredb::ConnectOptionsBuilderError;

    fn try_from(
        val: ConnectOptions,
    ) -> Result<glaredb::ConnectOptions, glaredb::ConnectOptionsBuilderError> {
        glaredb::ConnectOptionsBuilder::default()
            .spill_path(val.spill_path)
            .disable_tls_opt(val.disable_tls)
            .cloud_addr_opt(val.cloud_addr)
            .location(val.location)
            .storage_options_opt(val.storage_options)
            .client_type(glaredb::ClientType::Node)
            .build()
    }
}

/// Connect to a GlareDB database.
#[napi(catch_unwind)]
pub async fn connect(
    data_dir_or_cloud_url: Option<String>,
    options: Option<ConnectOptions>,
) -> napi::Result<Connection> {
    let mut options: glaredb::ConnectOptions = options
        .unwrap_or_default()
        .try_into()
        .map_err(glaredb::DatabaseError::from)
        .map_err(JsDatabaseError::from)?;

    options.connection_target = data_dir_or_cloud_url;

    Ok(Connection {
        inner: Arc::new(options.connect().await.map_err(JsDatabaseError::from)?),
    })
}
