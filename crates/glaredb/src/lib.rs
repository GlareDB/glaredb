use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
// public re-export so downstream users of this package don't have to
// directly depend on DF (and our version no-less) to use our interfaces.
pub use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
pub use datafusion::physical_plan::SendableRecordBatchStream;
use derive_builder::Builder;
use futures::lock::Mutex;
use futures::stream::{Stream, StreamExt};
use sqlexec::engine::{Engine, EngineBackend, TrackedSession};
use sqlexec::environment::EnvironmentReader;
use sqlexec::errors::ExecError;
use sqlexec::remote::client::RemoteClientType;
use sqlexec::session::ExecutionResult;
use sqlexec::OperationInfo;
use url::Url;


#[derive(Default, Builder)]
pub struct ConnectOptions {
    #[builder(setter(into))]
    pub connection_target: Option<String>,
    #[builder(setter(into))]
    pub location: Option<String>,
    #[builder(setter(into))]
    pub spill_path: Option<String>,
    #[builder(setter(strip_option))]
    pub storage_options: HashMap<String, String>,

    #[builder(setter(strip_option))]
    pub disable_tls: Option<bool>,
    #[builder(default = "Some(\"https://console.glaredb.com\".to_string())")]
    #[builder(setter(into, strip_option))]
    pub cloud_addr: Option<String>,
    #[builder(default = "Some(RemoteClientType::Cli)")]
    #[builder(setter(strip_option))]
    pub client_type: Option<RemoteClientType>,
    #[builder(setter(strip_option))]
    pub environment_reader: Option<Arc<Box<dyn EnvironmentReader>>>,
}

impl ConnectOptionsBuilder {
    pub fn storage_option(
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

    pub fn set_storage_options(&mut self, opts: Option<HashMap<String, String>>) -> &mut Self {
        self.storage_options = opts;
        self
    }
}

impl ConnectOptions {
    pub fn new_in_memory() -> Self {
        Self {
            location: None,
            connection_target: None,
            ..Default::default()
        }
    }

    pub fn with_env_reader(&mut self, reader: Arc<Box<dyn EnvironmentReader>>) -> &mut Self {
        self.environment_reader.replace(reader);
        self
    }

    pub async fn connect(&mut self) -> Result<Connection, ExecError> {
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

        if let Some(env_reader) = self.environment_reader.take() {
            session.register_env_reader(env_reader)
        }

        Ok(Connection {
            session: Arc::new(Mutex::new(session)),
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

pub struct Connection {
    session: Arc<Mutex<TrackedSession>>,
    _engine: Arc<Engine>,
}


impl Connection {
    pub async fn execute(
        &self,
        query: impl Into<String>,
    ) -> Result<SendableRecordBatchStream, ExecError> {
        let mut ses = self.session.lock().await;
        let query = query.into();
        let plan = ses.create_logical_plan(&query).await?;
        let op = OperationInfo::new().with_query_text(query);

        Ok(Self::process_result(
            ses.execute_logical_plan(plan, &op).await?.1,
        ))
    }

    pub fn call(&self, query: impl Into<String>) -> RecordStream {
        let ses = self.session.clone();
        let query = query.into();

        RecordStream(Box::pin(
            futures::stream::once(async move {
                let mut ses = ses.lock().await;
                let plan = match ses.create_logical_plan(&query).await {
                    Ok(p) => p,
                    Err(e) => return Self::handle_error(e),
                };
                let op = OperationInfo::new().with_query_text(query);

                match ses.execute_logical_plan(plan, &op).await {
                    Ok((_, stream)) => Self::process_result(stream),
                    Err(err) => Self::handle_error(err),
                }
            })
            .flatten(),
        ))
    }

    pub async fn query(
        &self,
        query: impl Into<String>,
    ) -> Result<SendableRecordBatchStream, ExecError> {
        let mut ses = self.session.lock().await;
        let query = query.into();
        let plan = ses.create_logical_plan(&query).await?;
        let op = OperationInfo::new().with_query_text(query);

        match plan.to_owned().try_into_datafusion_plan()? {
            LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Extension(_) => Ok(Self::process_result(
                ses.execute_logical_plan(plan, &op).await?.1,
            )),
            _ => {
                let ses_clone = self.session.clone();

                Ok(Self::process_result(ExecutionResult::Query {
                    stream: Box::pin(RecordBatchStreamAdapter::new(
                        Arc::new(plan.output_schema().unwrap_or_else(Schema::empty)),
                        futures::stream::once(async move {
                            let mut ses = ses_clone.lock().await;
                            match ses.execute_logical_plan(plan, &op).await {
                                Ok((_, res)) => Self::process_result(res),
                                Err(e) => Self::handle_error(e),
                            }
                        })
                        .flatten(),
                    )),
                }))
            }
        }
    }

    pub async fn prql_query(
        &self,
        query: impl Into<String>,
    ) -> Result<SendableRecordBatchStream, ExecError> {
        let mut ses = self.session.lock().await;
        let query = query.into();
        let plan = ses.prql_to_lp(&query).await?;
        let op = OperationInfo::new().with_query_text(query);

        let ses_clone = self.session.clone();
        Ok(Self::process_result(ExecutionResult::Query {
            stream: Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(plan.output_schema().unwrap_or_else(Schema::empty)),
                futures::stream::once(async move {
                    let mut ses = ses_clone.lock().await;
                    match ses.execute_logical_plan(plan, &op).await {
                        Ok((_, res)) => Self::process_result(res),
                        Err(e) => Self::handle_error(e),
                    }
                })
                .flatten(),
            )),
        }))
    }

    pub fn prql_call(&self, query: impl Into<String>) -> RecordStream {
        let ses = self.session.clone();
        let query = query.into();

        RecordStream(Box::pin(
            futures::stream::once(async move {
                let mut ses = ses.lock().await;
                let plan = match ses.prql_to_lp(&query).await {
                    Ok(p) => p,
                    Err(e) => return Self::handle_error(e),
                };
                let op = OperationInfo::new().with_query_text(query);

                match ses.execute_logical_plan(plan, &op).await {
                    Ok((_, stream)) => Self::process_result(stream),
                    Err(err) => Self::handle_error(err),
                }
            })
            .flatten(),
        ))
    }


    fn handle_error(err: impl Into<DataFusionError>) -> SendableRecordBatchStream {
        Self::process_result(ExecutionResult::Error(err.into()))
    }

    fn process_result(res: ExecutionResult) -> SendableRecordBatchStream {
        match res {
            ExecutionResult::Query { stream } => stream,
            ExecutionResult::Error(e) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                futures::stream::once(async move { Err(e) }),
            )),
            ExecutionResult::InsertSuccess { rows_inserted } => {
                Self::numeric_result("count", rows_inserted as u64)
            }
            ExecutionResult::DeleteSuccess { deleted_rows } => {
                Self::numeric_result("count", deleted_rows as u64)
            }
            ExecutionResult::UpdateSuccess { updated_rows } => {
                Self::numeric_result("count", updated_rows as u64)
            }
            _ => Self::operation_result("result", res.to_string()),
        }
    }

    fn numeric_result(field_name: impl Into<String>, num: u64) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_name,
            DataType::UInt64,
            false,
        )]));

        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(UInt64Array::from_value(num, 1))],
                )
                .map_err(DataFusionError::from)
            }),
        ))
    }

    fn operation_result(
        field_name: impl Into<String>,
        op: impl Into<String>,
    ) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_name,
            DataType::Utf8,
            false,
        )]));
        let op = op.into();

        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async move {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from_iter_values(
                        vec![op].into_iter(),
                    ))],
                )
                .map_err(DataFusionError::from)
            }),
        ))
    }
}

pub struct RecordStream(Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>);

impl From<SendableRecordBatchStream> for RecordStream {
    fn from(val: SendableRecordBatchStream) -> RecordStream {
        RecordStream(val.boxed())
    }
}

impl From<Result<SendableRecordBatchStream, DataFusionError>> for RecordStream {
    fn from(val: Result<SendableRecordBatchStream, DataFusionError>) -> RecordStream {
        match val {
            Ok(stream) => stream.into(),
            Err(err) => RecordStream(Connection::handle_error(err).boxed()),
        }
    }
}

impl RecordStream {
    pub fn all(&mut self) -> Result<Vec<RecordBatch>, ExecError> {
        futures::executor::block_on(async move {
            let mut out = Vec::new();

            while let Some(batch) = self.0.next().await {
                out.push(batch?)
            }
            Ok(out)
        })
    }

    pub fn iter(&mut self) -> impl Iterator<Item = Result<RecordBatch, ExecError>> + '_ {
        RecordIter { inner: Some(self) }
    }
}

struct RecordIter<'a> {
    inner: Option<&'a mut RecordStream>,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<RecordBatch, ExecError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            Some(stream) => match futures::executor::block_on(async { stream.0.next().await }) {
                Some(val) => match val {
                    Ok(v) => Some(Ok(v)),
                    Err(err) => {
                        self.inner = None;
                        Some(Err(err.into()))
                    }
                },
                None => None,
            },
            None => None,
        }
    }
}
