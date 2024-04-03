//! Main entry point for the python bindings.
//!
//! User's will call `connect` which returns a session for executing sql
//! queries.

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use sqlexec::remote::client::RemoteClientType;

use crate::connection::Connection;
use crate::environment::PyEnvironmentReader;
use crate::error::PyGlareDbError;
use crate::runtime::wait_for_future;

/// Connect to a GlareDB database.
///
/// # Examples
///
/// Connect to an in-memory database.
///
/// ```python
/// import glaredb
///
/// con = glaredb.connect();
/// ```
///
/// Connect to a database that's persisted to disk.
///
/// ```python
/// import glaredb
///
/// con = glaredb.connect('./my/db');
/// ```
///
/// Connect to a GlareDB Cloud deployment.
///
/// ```python
/// import glaredb
///
/// con = glaredb.connect("glaredb://<user>:<password>@<org>.remote.glaredb.com:6443/<deployment>")
/// ```
#[pyfunction]
#[pyo3(signature = (data_dir_or_cloud_url = None, /, *, spill_path = None, disable_tls = false, cloud_addr = String::from("https://console.glaredb.com"), location = None, storage_options = None))]
pub fn connect(
    py: Python,
    data_dir_or_cloud_url: Option<String>,
    spill_path: Option<String>,
    disable_tls: bool,
    cloud_addr: String,
    location: Option<String>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Connection> {
    wait_for_future(py, async move {
        Ok(Connection {
            inner: Arc::new(
                glaredb::ConnectOptionsBuilder::default()
                    .connection_target(data_dir_or_cloud_url.clone())
                    .set_storage_options(storage_options)
                    .location(location)
                    .spill_path(spill_path)
                    .cloud_addr(cloud_addr)
                    .disable_tls(disable_tls)
                    .client_type(RemoteClientType::Python)
                    .environment_reader(Arc::new(Box::new(PyEnvironmentReader)))
                    .build()
                    .map_err(PyGlareDbError::from)?
                    .connect()
                    .await
                    .map_err(PyGlareDbError::from)?,
            ),
        })
    })
}
