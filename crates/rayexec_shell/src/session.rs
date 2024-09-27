use rayexec_execution::hybrid::client::{HybridClient, HybridConnectConfig};
use rayexec_parser::parser;
use rayexec_parser::statement::RawStatement;
use std::collections::VecDeque;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use rayexec_execution::datasource::DataSourceRegistry;
use rayexec_execution::engine::{session::Session, Engine};
use rayexec_execution::runtime::{PipelineExecutor, Runtime};
use tokio::sync::Mutex;

use crate::result_table::StreamingTable;

/// A wrapper around a session and an engine for when running the database in a
/// local, single user mode (e.g. in the CLI or through wasm).
#[derive(Debug)]
pub struct SingleUserEngine<P: PipelineExecutor, R: Runtime> {
    pub runtime: R,
    pub engine: Engine<P, R>,
    pub session: SingleUserSession<P, R>,
}

impl<P, R> SingleUserEngine<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    /// Create a new single user engine using the provided runtime and registry.
    pub fn try_new(executor: P, runtime: R, registry: DataSourceRegistry) -> Result<Self> {
        let engine = Engine::new_with_registry(executor, runtime.clone(), registry)?;
        let session = SingleUserSession {
            session: Arc::new(Mutex::new(engine.new_session()?)),
        };

        Ok(SingleUserEngine {
            runtime,
            engine,
            session,
        })
    }

    pub fn session(&self) -> &SingleUserSession<P, R> {
        &self.session
    }

    /// Connect to a remote server for hybrid execution.
    pub async fn connect_hybrid(&self, connection_string: String) -> Result<()> {
        let config = HybridConnectConfig::try_from_connection_string(&connection_string)?;
        let client = self.runtime.http_client();
        let hybrid = HybridClient::new(client, config);

        hybrid.ping().await?;
        self.session.session.lock().await.set_hybrid(hybrid);

        Ok(())
    }
}

/// Session connected to the above engine.
///
/// Cheaply cloneable.
#[derive(Debug, Clone)]
pub struct SingleUserSession<P: PipelineExecutor, R: Runtime> {
    /// The underlying session.
    ///
    /// Wrapped in a mutex since planning may alter session state. Needs a tokio
    /// mutex since the lock is held across an await (for async binding).
    ///
    /// The lock should not be held during actual execution (reading of the
    /// result stream).
    pub(crate) session: Arc<Mutex<Session<P, R>>>,
}

impl<P, R> SingleUserSession<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    /// Execute a single sql query.
    pub async fn query(&self, sql: &str) -> Result<StreamingTable> {
        let mut statements = parser::parse(sql)?;
        let statement = match statements.len() {
            1 => statements.pop().unwrap(),
            other => {
                return Err(RayexecError::new(format!(
                    "Expected 1 statement, got {}",
                    other
                )))
            }
        };

        PendingQuery {
            session: self.session.clone(),
            statement,
        }
        .execute()
        .await
    }

    /// Execute multiple queries.
    ///
    /// Pending queries must be executed and streamed to completion in order.
    pub fn query_many(&self, sql: &str) -> Result<VecDeque<PendingQuery<P, R>>> {
        let statements = parser::parse(sql)?;

        // TODO: Implicit tx

        Ok(statements
            .into_iter()
            .map(|statement| PendingQuery {
                session: self.session.clone(),
                statement,
            })
            .collect())
    }
}

#[derive(Debug)]
pub struct PendingQuery<P: PipelineExecutor, R: Runtime> {
    pub(crate) statement: RawStatement,
    pub(crate) session: Arc<Mutex<Session<P, R>>>,
}

impl<P, R> PendingQuery<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    pub async fn execute(self) -> Result<StreamingTable> {
        const UNNAMED: &str = "";

        let mut session = self.session.lock().await;

        session.prepare(UNNAMED, self.statement)?;
        session.bind(UNNAMED, UNNAMED).await?;

        let result = session.execute(UNNAMED).await?;

        Ok(StreamingTable { result })
    }
}
