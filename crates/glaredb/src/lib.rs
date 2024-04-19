//! GlareDB is a database engine designed to provide the user
//! experience and ergonomic embedded databases with the compute power
//! and flexibility of large scale distributed serverless compute engines.
//!
//! The GlareDB Rust SDK is a set of high-level wrappers for a GlareDB
//! instance as either a client or an embedded database. The
//! implementation primarily underpins the implementations of the
//! Python and Node.js bindings, but may be used/useful directly for
//! testing GlareDB from within Rust tests, and even inside of Rust
//! applications or to produce other bindings.

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
pub use datafusion::scalar::ScalarValue;
use derive_builder::Builder;
use futures::lock::Mutex;
use futures::stream::{Stream, StreamExt};
use futures::TryStreamExt;
use sqlexec::engine::{Engine, EngineStorage, TrackedSession};
pub use sqlexec::environment::EnvironmentReader;
use sqlexec::errors::ExecError;
use sqlexec::remote::client::RemoteClientType;
use sqlexec::session::ExecutionResult;
use sqlexec::OperationInfo;
use url::Url;

/// ConnectOptions are the set of options to configure a GlareDB
/// instance, and are an analogue to the commandline arguments to
/// produce a "running database". The ConnectOptionsBuilder provides a
/// chainable interface to construct these values and produce a
/// structure. You can construct `ConnectOptions` fully without the
/// builder interface, depending on your preference.
///
/// The `.connect()` method on `ConnectionOptions` is the main way to
/// produce a GlareDB instance. All instances use an in-process
/// metastore (catalog).
///
/// The `connection_target`, `location`, `spill_path` and
/// `storage_options` all control the behavior of a local, single node
/// instance, while the remaining options configure a GlareDB instance
/// for hybrid-execution.
#[derive(Default, Builder)]
pub struct ConnectOptions {
    /// The connection_target specifies where the GlareDB instances
    /// storage is. This is either, in memory (unspecified or
    /// `memory://`), a path to something on the local file-system for
    /// persistent local databases, or an object-store URL for
    /// databases that are backed up onto cloud storage.
    #[builder(setter(into))]
    pub connection_target: Option<String>,
    /// Location is that path **within** the `connection_target` where
    /// the database's files will be stored. This is required for all
    /// object-store backed GlareDB instances and ignored in all other
    /// cases.
    #[builder(setter(into))]
    pub location: Option<String>,
    /// Specifies the location on the local file system where this
    /// process will write files so that it can spill data for
    /// operations to local disk (sorts, large joins, etc.)
    #[builder(setter(into))]
    pub spill_path: Option<String>,
    /// Defines the options used to configure the object store
    /// (credentials, etc.)
    #[builder(setter(into), default = "HashMap::new()")]
    pub storage_options: HashMap<String, String>,

    /// By default, the client will connect to the GlareDB service
    /// using TLS. When this option is specified (and true), then this
    /// GlareDB instance will establish an insecure connection. Use for
    /// testing and development.
    #[builder(setter(strip_option))]
    pub disable_tls: Option<bool>,
    /// Location of the GlareDB cloud instance used by GlareDB
    /// to negotiate out-of-band certificate provisioning.
    #[builder(default = "Some(\"https://console.glaredb.com\".to_string())")]
    #[builder(setter(into, strip_option))]
    pub cloud_addr: Option<String>,
    /// Client type distinguishes what kind of remote client this is,
    /// and is used for logging and introspection.
    #[builder(default = "Some(RemoteClientType::Cli)")]
    #[builder(setter(strip_option))]
    pub client_type: Option<RemoteClientType>,
    /// Specify an optional environment reader, which GlareDB uses in
    /// embedded cases so that queries can bindings to extract tables
    /// from variables in the binding's scope that data frames, or the
    /// output of a query.
    #[builder(setter(strip_option))]
    pub environment_reader: Option<Arc<dyn EnvironmentReader>>,
}

impl ConnectOptionsBuilder {
    /// Adds a single option (key/value pair) to the builder for the
    /// storage options map. All keys must be unique, and setting the
    /// same option more than once.
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

    /// Overrides the storage option map in the Builder. Passing an
    /// empty map or None to this method removes the existing data and
    /// resets the state of the storage options in the builder.
    pub fn set_storage_options(&mut self, opts: Option<HashMap<String, String>>) -> &mut Self {
        self.storage_options = opts;
        self
    }

    pub fn cloud_addr_opt(&mut self, v: Option<String>) -> &mut Self {
        self.cloud_addr = Some(v);
        self
    }

    pub fn disable_tls_opt(&mut self, v: Option<bool>) -> &mut Self {
        self.disable_tls = Some(v);
        self
    }

    pub fn storage_options_opt(&mut self, v: Option<HashMap<String, String>>) -> &mut Self {
        self.storage_options = v;
        self
    }

    /// Constructs an in-memory connection configuration, which can be
    /// used for default operations and tests without impacting the
    /// file system. All state (tables, catalog, etc,) are local, but
    /// these instances can write data to files and process data in
    /// other data sources.
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
    /// Creates a Connection object according to the options
    /// specified.
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

        session.register_env_reader(self.environment_reader);

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

/// Connections hold the state of a GlareDB object. These connections
/// are not always backed by network connections, and in all cases
/// include the full capabilities of a local GlareDB instance. When
/// connected to a remote GlareDB instance, all execution is hybrid
/// wherein queries are parsed, planed and orchestrated locally, but
/// executation can occur locally or on the remote instance according
/// to capacity.
///
/// All of the connection's operations are lazy, and return
/// `Operation` objects that must be executed in order for the query
/// to run. `Operation` objects can be executed more than once to
/// rerun the query.
#[derive(Debug, Clone)]
pub struct Connection {
    session: Arc<Mutex<TrackedSession>>,
    _engine: Arc<Engine>,
}

impl Connection {
    /// Execute creates a query that is parsed and then evaluates and
    /// runs immediately when the Operation is invoked regardless of
    /// the content.
    pub fn execute(&self, query: impl Into<String>) -> Operation {
        Operation {
            op: OperationType::Execute,
            query: query.into(),
            conn: Arc::new(self.clone()),
            schema: None,
            plan: None,
        }
    }

    /// Creates a query that is parsed when the Operation is invoked;
    /// however, the query is only executed when the results are
    /// iterated _unless_ the operation is a write operation or a DDL
    /// operation, which are executed when the operation is invoked.
    pub fn sql(&self, query: impl Into<String>) -> Operation {
        Operation {
            op: OperationType::Sql,
            query: query.into(),
            conn: Arc::new(self.clone()),
            schema: None,
            plan: None,
        }
    }

    /// PRQL queries have the same semantics as SQL queries; however,
    /// because PRQL does not include syntax for DML or DDL
    /// operations, these queries only run when the result stream are
    /// invoked.
    pub fn prql(&self, query: impl Into<String>) -> Operation {
        Operation {
            op: OperationType::Prql,
            query: query.into(),
            conn: Arc::new(self.clone()),
            schema: None,
            plan: None,
        }
    }
}

/// RecordStream is like DataFusion's `SendableRecordBatchStream`,
/// except it does not provide access to the schema except via the
/// results.
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

/// RowMap represents a single record in an ordered map.
type RowMap = indexmap::IndexMap<Arc<String>, ScalarValue>;

/// RowMapBatch is equivalent to a row-based view of a record
/// batch. Use this type sparingly, and/or in tests when you know the
/// result size is small.
#[derive(Default, Debug)]
pub struct RowMapBatch(Vec<RowMap>);

impl TryFrom<RecordBatch> for RowMapBatch {
    type Error = DataFusionError;

    fn try_from(batch: RecordBatch) -> Result<Self, Self::Error> {
        let schema = batch.schema();
        let mut out = Vec::with_capacity(batch.num_rows());

        let mut fields = Vec::with_capacity(schema.fields.len());
        for field in schema.fields().into_iter() {
            fields.push(Arc::new(field.name().to_owned()))
        }

        for row in 0..batch.num_rows() {
            let mut record = RowMap::with_capacity(batch.num_columns());
            for (idx, field) in fields.clone().into_iter().enumerate() {
                record.insert(field, ScalarValue::try_from_array(batch.column(idx), row)?);
            }
            out.push(record);
        }

        Ok(RowMapBatch(out))
    }
}

impl TryFrom<Result<RecordBatch, DataFusionError>> for RowMapBatch {
    type Error = DataFusionError;

    fn try_from(value: Result<RecordBatch, DataFusionError>) -> Result<Self, Self::Error> {
        RowMapBatch::try_from(value?)
    }
}

impl Extend<RowMap> for RowMapBatch {
    fn extend<T: IntoIterator<Item = RowMap>>(&mut self, iter: T) {
        for elem in iter {
            self.0.push(elem)
        }
    }
}

impl RowMapBatch {
    pub fn iter(&self) -> impl Iterator<Item = RowMap> {
        self.0.clone().into_iter()
    }

    /// Returns the row at the specific index, or None if the index is
    /// out of bounds.
    pub fn row(&self, idx: usize) -> Option<RowMap> {
        self.0.get(idx).cloned()
    }

    /// The number of rows in the RowMapBatch.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl RecordStream {
    // Collects all of the record batches in a stream, aborting if
    // there are any errors.
    pub async fn to_vec(&mut self) -> Result<Vec<RecordBatch>, DataFusionError> {
        let stream = &mut self.0;
        stream.try_collect().await
    }

    /// Collects all of the record batches and rotates the results for
    /// a map-based row-oriented format.
    pub async fn to_rows(&mut self) -> Result<Vec<RowMapBatch>, DataFusionError> {
        let stream = &mut self.0;
        stream.map(RowMapBatch::try_from).try_collect().await
    }

    // Iterates through the stream, ensuring propagating any errors,
    // but discarding all of the data.
    pub async fn check(&mut self) -> Result<(), DataFusionError> {
        let stream = &mut self.0;

        while let Some(b) = stream.next().await {
            b?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
enum OperationType {
    /// SQL operations create a operation that runs DDL/DML operations
    /// directly, and executes other queries lazily when the results
    /// are iterated.
    Sql,
    /// PRQL, which does not support DDL/DML in our implementation,
    /// creates a lazy query object that only runs when the results
    /// are iterated.
    Prql,
    /// Execute Operations run a SQL operation directly when the
    /// `Operation`'s `evalutate()` or `resolve()` methods run.
    Execute,
}

#[derive(Debug, Clone)]
#[must_use = "operations do nothing unless evaluate() or resolve() run"]
pub struct Operation {
    op: OperationType,
    query: String,
    conn: Arc<Connection>,
    schema: Option<Arc<Schema>>,
    plan: Option<sqlexec::LogicalPlan>,
}

impl ToString for Operation {
    fn to_string(&self) -> String {
        self.query.clone()
    }
}

impl Operation {
    /// Schema returns the schema of the results. This method returns
    /// `None` before the query executes.
    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.schema.clone()
    }

    /// Evaluate constructs a plan for the query, and in the case of
    /// all `OperationType::Execute` operations and
    /// `OperationType::Sql` operations that write data, the operation
    /// run immediately. All other operations run when `.resolve()` is
    /// called.
    pub async fn evaluate(&mut self) -> Result<Self, ExecError> {
        match self.op {
            OperationType::Sql => {
                let mut ses = self.conn.session.lock().await;

                let plan = ses.create_logical_plan(&self.query).await?;

                self.schema
                    .replace(Arc::new(plan.output_schema().unwrap_or_else(Schema::empty)));

                match plan.to_owned().try_into_datafusion_plan()? {
                    LogicalPlan::Dml(_)
                    | LogicalPlan::Ddl(_)
                    | LogicalPlan::Copy(_)
                    | LogicalPlan::Extension(_) => {
                        RecordStream::from(Self::process_result(
                            ses.execute_logical_plan(
                                plan,
                                &OperationInfo::new().with_query_text(self.query.clone()),
                            )
                            .await?
                            .1,
                        ))
                        .check()
                        .await?;
                    }
                    _ => {
                        self.plan.replace(plan);
                    }
                };

                Ok(self.clone())
            }
            OperationType::Prql => {
                let plan = self
                    .conn
                    .session
                    .lock()
                    .await
                    .prql_to_lp(&self.query)
                    .await?;

                self.schema
                    .replace(Arc::new(plan.output_schema().unwrap_or_else(Schema::empty)));

                self.plan.replace(plan);

                Ok(self.clone())
            }
            OperationType::Execute => {
                let mut ses = self.conn.session.lock().await;
                let plan = ses.create_logical_plan(&self.query).await?;

                self.schema
                    .replace(Arc::new(plan.output_schema().unwrap_or_else(Schema::empty)));

                RecordStream::from(Self::process_result(
                    ses.execute_logical_plan(
                        plan,
                        &OperationInfo::new().with_query_text(self.query.clone()),
                    )
                    .await?
                    .1,
                ))
                .check()
                .await?;

                Ok(self.clone())
            }
        }
    }

    /// Resolves the results of the query, according to the semantics
    /// of the operation's type. Uses the plan built during
    /// `evaluate()` if populated, but will re-plan on subsequent
    /// calls or when evaluate isn't called first. Returns an error if
    /// there is problem parsing the query or creating a
    /// stream. Operations created with `execute()` run when the
    /// `resolve()` method runs. For operations with the `sql()`
    /// method, write operations and DDL operations run before
    /// `resolve()` returns. All other operations are lazy and only
    /// execute as the results are processed.
    pub async fn resolve(&mut self) -> Result<SendableRecordBatchStream, ExecError> {
        match self.op {
            OperationType::Sql => {
                let mut ses = self.conn.session.lock().await;

                let plan = if self.plan.is_some() {
                    self.plan.take().unwrap()
                } else {
                    self.schema = None;
                    ses.create_logical_plan(&self.query).await?
                };

                let schema = if self.schema.is_some() {
                    self.schema.clone().unwrap()
                } else {
                    self.schema
                        .insert(Arc::new(plan.output_schema().unwrap_or_else(Schema::empty)))
                        .to_owned()
                };
                self.schema.replace(schema.clone());

                let op = OperationInfo::new().with_query_text(self.query.clone());
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

                let plan = if self.plan.is_some() {
                    self.plan.take().unwrap()
                } else {
                    self.schema = None;
                    ses.prql_to_lp(&self.query).await?
                };

                let schema = if self.schema.is_some() {
                    self.schema.clone().unwrap()
                } else {
                    self.schema
                        .insert(Arc::new(plan.output_schema().unwrap_or_else(Schema::empty)))
                        .to_owned()
                };

                let ses_clone = self.conn.session.clone();
                let op = OperationInfo::new().with_query_text(self.query.clone());
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

    /// Call returns the results of the query as a stream. No
    /// processing happens until the stream is processed, and errors
    /// parsing the query are returned as the the first result.
    pub fn call(&mut self) -> RecordStream {
        let mut op = self.clone();
        RecordStream(Box::pin(
            futures::stream::once(async move {
                match op.resolve().await {
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
