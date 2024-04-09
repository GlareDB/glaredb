use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
// public re-export so downstream users of this package don't have to
// directly depend on DF (and our version no-less) to use our interfaces.
pub use datafusion::arrow::record_batch::RecordBatch;
pub use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
pub use datafusion::physical_plan::SendableRecordBatchStream;
use derive_builder::Builder;
use futures::lock::Mutex;
use futures::stream::{Stream, StreamExt};
use sqlexec::engine::{Engine, EngineStorage, TrackedSession};
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
    #[builder(setter(into), default = "HashMap::new()")]
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

    pub fn new_in_memory() -> Self {
        Self::default()
            .connection_target(None)
            .location(None)
            .spill_path(None)
            .disable_tls(true)
            .to_owned()
    }
}

impl ConnectOptions {
    pub async fn connect(self) -> Result<Connection, ExecError> {
        let mut engine = Engine::from_storage(self.backend()).await?;

        engine = engine.with_spill_path(self.spill_path.clone().map(|p| p.into()))?;

        let mut session = engine.default_local_session_context().await?;

        session
            .create_client_session(
                self.cloud_url(),
                self.cloud_addr.clone().unwrap_or_default(),
                self.disable_tls.unwrap_or_default(),
                self.client_type.unwrap_or_default(),
                None,
            )
            .await?;

        session.register_env_reader(self.environment_reader.clone());

        Ok(Connection {
            session: Arc::new(Mutex::new(session)),
            _engine: Arc::new(engine),
        })
    }

    fn backend(&self) -> EngineStorage {
        if let Some(location) = self.location.clone() {
            EngineStorage::Remote {
                location,
                options: self.storage_options.clone(),
            }
        } else if let Some(data_dir) = self.data_dir() {
            EngineStorage::Local(data_dir)
        } else {
            EngineStorage::Memory
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

#[derive(Debug, Clone)]
pub struct Connection {
    session: Arc<Mutex<TrackedSession>>,
    _engine: Arc<Engine>,
}

impl Connection {
    pub fn execute(&self, query: impl Into<String>) -> Operation {
        Operation {
            op: OperationType::Execute,
            query: query.into(),
            conn: Arc::new(self.clone()),
            schema: None,
        }
    }

    pub fn sql(&self, query: impl Into<String>) -> Operation {
        Operation {
            op: OperationType::Sql,
            query: query.into(),
            conn: Arc::new(self.clone()),
            schema: None,
        }
    }

    pub fn prql(&self, query: impl Into<String>) -> Operation {
        Operation {
            op: OperationType::Prql,
            query: query.into(),
            conn: Arc::new(self.clone()),
            schema: None,
        }
    }
}

pub struct RecordStream(Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>);

impl Stream for RecordStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

impl From<SendableRecordBatchStream> for RecordStream {
    fn from(val: SendableRecordBatchStream) -> Self {
        RecordStream(val.boxed())
    }
}

impl From<Result<SendableRecordBatchStream, DataFusionError>> for RecordStream {
    fn from(val: Result<SendableRecordBatchStream, DataFusionError>) -> Self {
        match val {
            Ok(stream) => stream.into(),
            Err(err) => RecordStream(Operation::handle_error(err).boxed()),
        }
    }
}

impl From<Result<SendableRecordBatchStream, ExecError>> for RecordStream {
    fn from(val: Result<SendableRecordBatchStream, ExecError>) -> Self {
        match val {
            Ok(stream) => stream.into(),
            Err(err) => RecordStream(Operation::handle_error(err).boxed()),
        }
    }
}

#[derive(Debug, Clone)]
enum OperationType {
    Sql,
    Prql,
    Execute,
}

#[derive(Debug, Clone)]
#[must_use = "operations do nothing unless call() or execute() run"]
pub struct Operation {
    op: OperationType,
    query: String,
    conn: Arc<Connection>,
    schema: Option<Arc<Schema>>,
}

impl ToString for Operation {
    fn to_string(&self) -> String {
        self.query.clone()
    }
}

impl Operation {
    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.schema.clone()
    }

    pub async fn execute(&mut self) -> Result<SendableRecordBatchStream, ExecError> {
        match self.op {
            OperationType::Sql => {
                let mut ses = self.conn.session.lock().await;
                let plan = ses.create_logical_plan(&self.query).await?;
                let op = OperationInfo::new().with_query_text(self.query.clone());
                let schema = Arc::new(plan.output_schema().unwrap_or_else(Schema::empty));
                self.schema.replace(schema.clone());

                match plan.to_owned().try_into_datafusion_plan()? {
                    LogicalPlan::Dml(_)
                    | LogicalPlan::Ddl(_)
                    | LogicalPlan::Copy(_)
                    | LogicalPlan::Extension(_) => Ok(Self::process_result(
                        ses.execute_logical_plan(plan, &op).await?.1,
                    )),
                    _ => {
                        let ses_clone = self.conn.session.clone();

                        Ok(Self::process_result(ExecutionResult::Query {
                            stream: Box::pin(RecordBatchStreamAdapter::new(
                                schema.clone(),
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
            OperationType::Prql => {
                let mut ses = self.conn.session.lock().await;
                let plan = ses.prql_to_lp(&self.query).await?;
                let op = OperationInfo::new().with_query_text(self.query.clone());
                let schema = Arc::new(plan.output_schema().unwrap_or_else(Schema::empty));

                let ses_clone = self.conn.session.clone();
                Ok(Self::process_result(ExecutionResult::Query {
                    stream: Box::pin(RecordBatchStreamAdapter::new(
                        schema.clone(),
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
            OperationType::Execute => {
                let mut ses = self.conn.session.lock().await;
                let plan = ses.create_logical_plan(&self.query).await?;
                let op = OperationInfo::new().with_query_text(self.query.clone());

                Ok(Self::process_result(
                    ses.execute_logical_plan(plan, &op).await?.1,
                ))
            }
        }
    }

    pub fn call(&mut self) -> RecordStream {
        let mut op = self.clone();
        RecordStream(Box::pin(
            futures::stream::once(async move {
                match op.execute().await {
                    Err(err) => Self::handle_error(err),
                    Ok(stream) => stream,
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
