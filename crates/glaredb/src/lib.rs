use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use derive_builder::Builder;
use sqlexec::engine::{Engine, EngineBackend, TrackedSession};
use sqlexec::errors::ExecError;
use sqlexec::remote::client::RemoteClientType;
use sqlexec::session::ExecutionResult;
use sqlexec::OperationInfo;
use url::Url;


#[derive(Default, Builder)]
pub struct ConnectOptions {
    #[builder(setter(into, strip_option))]
    pub connection_target: Option<String>,
    #[builder(setter(into, strip_option))]
    pub location: Option<String>,
    #[builder(setter(into, strip_option))]
    pub spill_path: Option<String>,
    #[builder(setter(strip_option))]
    pub storage_options: HashMap<String, String>,

    #[builder]
    pub disable_tls: Option<bool>,
    #[builder(default = "Some(\"https://console.glaredb.com\".to_string())")]
    #[builder(setter(into, strip_option))]
    pub cloud_addr: Option<String>,
    #[builder(default = "Some(RemoteClientType::Cli)")]
    #[builder(setter(strip_option))]
    pub client_type: Option<RemoteClientType>,
}

impl ConnectOptions {
    pub async fn connect(&self) -> Result<Connection, ExecError> {
        let mut engine = Engine::from_backend(self.backend()).await?;

        engine = engine.with_spill_path(self.spill_path.clone().map(|p| p.into()))?;

        let mut session = engine.default_local_session_context().await?;

        session
            .create_client_session(
                self.cloud_url(),
                self.cloud_addr.clone().unwrap_or_default(),
                self.disable_tls.unwrap_or_default(),
                self.client_type.clone().unwrap(),
                None,
            )
            .await?;

        Ok(Connection {
            _session: Arc::new(Mutex::new(session)),
            _engine: Arc::new(engine),
        })
    }

    fn backend(&self) -> EngineBackend {
        if let Some(location) = self.location.clone() {
            EngineBackend::Remote {
                location,
                options: self.storage_options.clone(),
            }
        } else if let Some(data_dir) = self.data_dir() {
            EngineBackend::Local(data_dir)
        } else {
            EngineBackend::Memory
        }
    }

    fn data_dir(&self) -> Option<PathBuf> {
        match self.connection_target.clone() {
            Some(s) => match Url::parse(&s) {
                Ok(_) => None,
                Err(_) => Some(PathBuf::from(s)),
            },
            None => None,
        }
    }

    fn cloud_url(&self) -> Option<Url> {
        self.connection_target
            .clone()
            .and_then(|v| Url::parse(&v).ok())
    }
}

impl ConnectOptionsBuilder {
    pub fn set_storage_option(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> &mut Self {
        let mut opts = match self.storage_options.to_owned() {
            Some(opts) => opts,
            None => HashMap::new(),
        };
        opts.insert(key.into(), value.into());
        self.storage_options(opts)
    }
}


pub struct Connection {
    _session: Arc<Mutex<TrackedSession>>,
    _engine: Arc<Engine>,
}

// TODO (create blocking and non-blocking variants)
impl Connection {
    pub async fn sql(&mut self, query: &str) -> Result<ExecutionResult, ExecError> {
        let mut ses = self._session.lock().unwrap();

        let plan = ses.create_logical_plan(query).await?;
        let op = OperationInfo::new().with_query_text(query);

        let (ep, execres) = ses.execute_logical_plan(plan, &op).await?;


        Ok(stream)
    }
}
