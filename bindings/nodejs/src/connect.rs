//! Main entry point for the js bindings.
//!
//! User's will call `connect` which returns a session for executing sql
//! queries.

use crate::connection::Connection;

use std::collections::HashMap;

#[napi(object)]
#[derive(Default)]
pub struct ConnectOptions {
    pub spill_path: Option<String>,
    pub disable_tls: Option<bool>,
    pub cloud_addr: Option<String>,
    pub location: Option<String>,
    pub storage_options: Option<HashMap<String, String>>,
}

impl ConnectOptions {
    fn spill_path(&mut self) -> Option<String> {
        self.spill_path.take()
    }

    fn disable_tls(&self) -> bool {
        self.disable_tls.unwrap_or(false)
    }

    fn cloud_addr(&self) -> String {
        self.cloud_addr
            .clone()
            .unwrap_or(String::from("https://console.glaredb.com"))
    }

    fn location(&mut self) -> Option<String> {
        self.location.take()
    }

    fn storage_options(&mut self) -> Option<HashMap<String, String>> {
        self.storage_options.take()
    }
}

/// Connect to a GlareDB database.
#[napi]
pub async fn connect(
    data_dir_or_cloud_url: Option<String>,
    options: Option<ConnectOptions>,
) -> napi::Result<Connection> {
    let mut options = options.unwrap_or_default();
    Connection::connect(
        data_dir_or_cloud_url,
        options.spill_path(),
        options.disable_tls(),
        options.cloud_addr(),
        options.location(),
        options.storage_options(),
    )
    .await
}
