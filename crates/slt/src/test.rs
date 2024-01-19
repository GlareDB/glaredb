use std::ops::Deref;
use std::sync::Arc;
use std::{
    collections::HashMap,
    fmt::Debug,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, Result};
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use async_trait::async_trait;
use clap::builder::PossibleValue;
use clap::ValueEnum;
use datafusion::arrow::datatypes::Schema;
use futures::StreamExt;
use glob::Pattern;
use tonic::transport::{Channel, Endpoint};

use datafusion_ext::vars::SessionVars;
use metastore::util::MetastoreClientMode;
use pgrepr::format::Format;
use pgrepr::scalar::Scalar;
use pgrepr::types::arrow_to_pg_type;
use regex::{Captures, Regex};
use rpcsrv::flight::handler::FLIGHTSQL_DATABASE_HEADER;
use sqlexec::engine::{Engine, EngineStorageConfig, SessionStorageConfig, TrackedSession};
use sqlexec::errors::ExecError;
use sqlexec::remote::client::RemoteClient;
use sqlexec::session::ExecutionResult;

use sqllogictest::{
    parse_with_name, AsyncDB, ColumnType, DBOutput, DefaultColumnType, Injected, Record, Runner,
};

use telemetry::Tracker;
use tokio::sync::{oneshot, Mutex};
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};
use uuid::Uuid;

#[async_trait]
pub trait Hook: Send + Sync {
    async fn pre(
        &self,
        _config: &Config,
        _client: TestClient,
        _vars: &mut HashMap<String, String>,
    ) -> Result<()> {
        Ok(())
    }

    async fn post(
        &self,
        _config: &Config,
        _client: TestClient,
        _vars: &HashMap<String, String>,
    ) -> Result<()> {
        Ok(())
    }
}

pub type TestHook = Arc<dyn Hook>;

/// List of hooks that should be ran for tests that match a pattern.
///
/// For example, a pattern "*" will run a hook against all tests, while
/// "*/tunnels/ssh" would only run hooks for the ssh tunnels tests.
pub type TestHooks = Vec<(Pattern, TestHook)>;

#[async_trait]
pub trait FnTest: Send + Sync {
    async fn run(
        &self,
        config: &Config,
        client: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<()>;
}

const ENV_REGEX: &str = r"\$\{\s*(\w+)\s*\}";

pub enum Test {
    File(PathBuf),
    FnTest(Box<dyn FnTest>),
}

impl Debug for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(path) => write!(f, "File({path:?})"),
            Self::FnTest(_) => write!(f, "FnTest"),
        }
    }
}

impl Test {
    pub async fn execute(
        self,
        config: &Config,
        client: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<()> {
        match self {
            Self::File(path) => {
                let regx = Regex::new(ENV_REGEX).unwrap();
                let records = parse_file(&regx, &path, vars)?;

                let mut runner = Runner::new(|| {
                    let client = client.clone();
                    async { Ok(client) }
                });

                runner
                    .run_multi_async(records)
                    .await
                    .map_err(|e| anyhow!("test fail: {}", e))
            }
            Self::FnTest(fn_test) => fn_test.run(config, client, vars).await,
        }
    }
}

fn parse_file<T: ColumnType>(
    regx: &Regex,
    path: &Path,
    vars: &HashMap<String, String>,
) -> Result<Vec<Record<T>>> {
    let script = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("Error while opening `{}`: {}", path.to_string_lossy(), e))?;

    // Replace all occurances of ${some_env_var} with actual values
    // from the environment.
    let mut err = None;
    let script = regx.replace_all(&script, |caps: &Captures| {
        let env_var = &caps[1];
        // Try if there's a local var with the key. Fallback to environment
        // variable.
        if let Some(var) = vars.get(env_var) {
            return var.to_string();
        }
        match std::env::var(env_var) {
            Ok(v) => v,
            Err(error) => {
                let error = anyhow!("Error fetching environment variable `{env_var}`: {error}");
                let err_msg = error.to_string();
                err = Some(error);
                err_msg
            }
        }
    });
    if let Some(err) = err {
        return Err(err);
    }

    let mut records = vec![];

    let script_name = path.to_str().unwrap();
    let parsed_records = parse_with_name(&script, script_name).map_err(|e| {
        anyhow!(
            "Error while parsing `{}`: {}",
            path.to_string_lossy(),
            e.kind()
        )
    })?;

    for rec in parsed_records {
        records.push(rec);

        // What we just pushed
        let rec = records.last().unwrap();

        // Includes are not actually processed by the runner. It's more of a
        // pre-processor, so we process them during the parse stage.
        //
        // This code was borrowed from `parse_file` function since the inner
        // function is private.

        if let Record::Include { filename, .. } = rec {
            let complete_filename = {
                let mut path_buf = path.to_path_buf();
                path_buf.pop();
                path_buf.push(filename.clone());
                path_buf.as_os_str().to_string_lossy().to_string()
            };

            for included_file in glob::glob(&complete_filename)
                .map_err(|e| anyhow!("Invalid include file at {}: {}", path.to_string_lossy(), e))?
                .filter_map(Result::ok)
            {
                let included_file = included_file.as_os_str().to_string_lossy().to_string();

                records.push(Record::Injected(Injected::BeginInclude(
                    included_file.clone(),
                )));
                records.extend(parse_file(regx, &PathBuf::from(&included_file), vars)?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}

#[derive(Clone)]
pub struct PgTestClient {
    client: Arc<Client>,
    conn_err_rx: Arc<Mutex<oneshot::Receiver<Result<(), tokio_postgres::Error>>>>,
}

impl Deref for PgTestClient {
    type Target = Client;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl PgTestClient {
    pub async fn new(client_config: &Config) -> Result<Self> {
        let (client, conn) = client_config.connect(NoTls).await?;
        let (conn_err_tx, conn_err_rx) = oneshot::channel();
        tokio::spawn(async move { conn_err_tx.send(conn.await) });
        Ok(Self {
            client: Arc::new(client),
            conn_err_rx: Arc::new(Mutex::new(conn_err_rx)),
        })
    }

    async fn close(&self) -> Result<()> {
        let PgTestClient { conn_err_rx, .. } = self;
        let mut conn_err_rx = conn_err_rx.lock().await;

        if let Ok(result) = conn_err_rx.try_recv() {
            // Handle connection error
            match result {
                Ok(()) => Err(anyhow!("Client connection unexpectedly closed")),
                Err(err) => Err(anyhow!("Client connection errored: {err}")),
            }
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct RpcTestClient {
    session: Arc<Mutex<TrackedSession>>,
    _engine: Arc<Engine>,
}

impl RpcTestClient {
    pub async fn new(data_dir: PathBuf, config: &Config) -> Result<Self> {
        let metastore = MetastoreClientMode::LocalInMemory.into_client().await?;
        let storage = EngineStorageConfig::try_from_path_buf(&data_dir)?;
        let engine = Engine::new(metastore, storage, Arc::new(Tracker::Nop), None).await?;
        let port = config.get_ports().first().unwrap();

        let addr = format!("http://0.0.0.0:{port}");
        let remote_client = RemoteClient::connect(addr.parse().unwrap()).await?;
        let mut session = engine
            .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
            .await?;
        let test_id = Uuid::new_v4();
        session
            .attach_remote_session(remote_client, Some(test_id))
            .await?;
        Ok(RpcTestClient {
            session: Arc::new(Mutex::new(session)),
            _engine: Arc::new(engine),
        })
    }
}
#[derive(Clone)]
pub struct FlightSqlTestClient {
    client: FlightSqlServiceClient<Channel>,
}
impl FlightSqlTestClient {
    pub async fn new(config: &Config) -> Result<Self> {
        let port = config.get_ports().first().unwrap();
        let addr = format!("http://0.0.0.0:{port}");
        let conn = Endpoint::new(addr)?.connect().await?;
        let dbid: Uuid = config.get_dbname().unwrap().parse().unwrap();

        let mut client = FlightSqlServiceClient::new(conn);
        client.set_header(FLIGHTSQL_DATABASE_HEADER, dbid.to_string());
        Ok(FlightSqlTestClient { client })
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum ClientProtocol {
    // Connect over a local postgres instance
    #[default]
    Postgres,
    // Connect over a local RPC instance
    Rpc,
    // Connect over a local FlightSql instance
    FlightSql,
}

impl ValueEnum for ClientProtocol {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Postgres, Self::Rpc, Self::FlightSql]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(match self {
            ClientProtocol::Postgres => PossibleValue::new("postgres"),
            ClientProtocol::Rpc => PossibleValue::new("rpc"),
            ClientProtocol::FlightSql => PossibleValue::new("flightsql").alias("flight-sql"),
        })
    }
}

#[derive(Clone)]
pub enum TestClient {
    Pg(PgTestClient),
    Rpc(RpcTestClient),
    FlightSql(FlightSqlTestClient),
}

impl TestClient {
    pub async fn close(self) -> Result<()> {
        match self {
            Self::Pg(pg_client) => pg_client.close().await,
            Self::Rpc(_) => Ok(()),
            Self::FlightSql(_) => Ok(()),
        }
    }
}

#[async_trait]
impl AsyncDB for PgTestClient {
    type Error = sqlexec::errors::ExecError;
    type ColumnType = DefaultColumnType;
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut output = Vec::new();
        let mut num_columns = 0;

        let rows = self
            .simple_query(sql)
            .await
            .map_err(|e| ExecError::Internal(format!("cannot execute simple query: {e}")))?;
        for row in rows {
            match row {
                SimpleQueryMessage::Row(row) => {
                    num_columns = row.len();
                    let mut row_output = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    row_output.push("(empty)".to_string());
                                } else {
                                    row_output.push(v.to_string().trim().to_owned());
                                }
                            }
                            None => row_output.push("NULL".to_string()),
                        }
                    }
                    output.push(row_output);
                }
                SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
        }
        if output.is_empty() && num_columns == 0 {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text; num_columns],
                rows: output,
            })
        }
    }
}

#[async_trait]
impl AsyncDB for RpcTestClient {
    type Error = sqlexec::errors::ExecError;
    type ColumnType = DefaultColumnType;
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut output = Vec::new();
        let mut num_columns = 0;
        let RpcTestClient { session, .. } = self;

        let mut session = session.lock().await;
        const UNNAMED: String = String::new();
        let statements = session.parse_query(sql)?;

        for stmt in statements {
            session.prepare_statement(UNNAMED, stmt, Vec::new()).await?;
            let prepared = session.get_prepared_statement(&UNNAMED)?;
            let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
            session.bind_statement(
                UNNAMED,
                &UNNAMED,
                Vec::new(),
                vec![Format::Text; num_fields],
            )?;
            let stream = session.execute_portal(&UNNAMED, 0).await?;

            match stream {
                ExecutionResult::Query { stream, .. } => {
                    let batches = stream
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;

                    for batch in batches {
                        if num_columns == 0 {
                            num_columns = batch.num_columns();
                        }

                        for row_idx in 0..batch.num_rows() {
                            let mut row_output = Vec::with_capacity(num_columns);

                            for col in batch.columns() {
                                let pg_type = arrow_to_pg_type(col.data_type(), None);
                                let scalar = Scalar::try_from_array(col, row_idx, &pg_type)?;

                                if scalar.is_null() {
                                    row_output.push("NULL".to_string());
                                } else {
                                    let mut buf = BytesMut::new();
                                    scalar.encode_with_format(Format::Text, &mut buf)?;

                                    if buf.is_empty() {
                                        row_output.push("(empty)".to_string())
                                    } else {
                                        let scalar =
                                            String::from_utf8(buf.to_vec()).map_err(|e| {
                                                ExecError::Internal(format!(
                                                "invalid text formatted result from pg encoder: {e}"
                                            ))
                                            })?;
                                        row_output.push(scalar.trim().to_owned());
                                    }
                                }
                            }
                            output.push(row_output);
                        }
                    }
                }
                ExecutionResult::Error(e) => return Err(e.into()),
                _ => (),
            }
        }

        if output.is_empty() && num_columns == 0 {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text; num_columns],
                rows: output,
            })
        }
    }
}

#[async_trait]
impl AsyncDB for FlightSqlTestClient {
    type Error = sqlexec::errors::ExecError;
    type ColumnType = DefaultColumnType;
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut output = Vec::new();
        let mut num_columns = 0;

        let mut client = self.client.clone();
        let ticket = client.execute(sql.to_string(), None).await?;
        let ticket = ticket
            .endpoint
            .first()
            .ok_or_else(|| ExecError::String("The server should support this".to_string()))?
            .clone();
        let ticket = ticket.ticket.unwrap();
        let mut stream = client.do_get(ticket).await?;
        // the schema should be the first message returned, else client should error
        let flight_data = stream.message().await?.unwrap();

        // convert FlightData to a stream
        let schema = Arc::new(Schema::try_from(&flight_data)?);

        // all the remaining stream messages should be dictionary and record batches
        let dictionaries_by_field = HashMap::new();
        while let Some(flight_data) = stream.message().await? {
            let batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_field)?;
            if num_columns == 0 {
                num_columns = batch.num_columns();
            }

            for row_idx in 0..batch.num_rows() {
                let mut row_output = Vec::with_capacity(num_columns);

                for col in batch.columns() {
                    let pg_type = arrow_to_pg_type(col.data_type(), None);
                    let scalar = Scalar::try_from_array(col, row_idx, &pg_type)?;

                    if scalar.is_null() {
                        row_output.push("NULL".to_string());
                    } else {
                        let mut buf = BytesMut::new();
                        scalar.encode_with_format(Format::Text, &mut buf)?;

                        if buf.is_empty() {
                            row_output.push("(empty)".to_string())
                        } else {
                            let scalar = String::from_utf8(buf.to_vec()).map_err(|e| {
                                ExecError::Internal(format!(
                                    "invalid text formatted result from pg encoder: {e}"
                                ))
                            })?;
                            row_output.push(scalar.trim().to_owned());
                        }
                    }
                }
                output.push(row_output);
            }
        }
        if output.is_empty() && num_columns == 0 {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text; num_columns],
                rows: output,
            })
        }
    }
}

#[async_trait]
impl AsyncDB for TestClient {
    type Error = sqlexec::errors::ExecError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        match self {
            Self::Pg(pg_client) => pg_client.run(sql).await,
            Self::Rpc(rpc_client) => rpc_client.run(sql).await,
            Self::FlightSql(flight_client) => flight_client.run(sql).await,
        }
    }

    fn engine_name(&self) -> &str {
        match self {
            Self::Pg { .. } => "glaredb_pg",
            Self::Rpc { .. } => "glaredb_rpc",
            Self::FlightSql { .. } => "glaredb_flight",
        }
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}
