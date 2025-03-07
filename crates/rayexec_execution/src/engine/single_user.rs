use std::collections::VecDeque;
use std::sync::Arc;

use futures::lock::Mutex;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::parser;
use rayexec_parser::statement::RawStatement;

use super::query_result::QueryResult;
use super::session::Session;
use super::Engine;
use crate::runtime::{PipelineExecutor, Runtime};

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
    pub fn try_new(executor: P, runtime: R) -> Result<Self> {
        let engine = Engine::new(executor, runtime.clone())?;
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
}

/// Session connected to the above engine.
///
/// Cheaply cloneable.
#[derive(Debug, Clone)]
pub struct SingleUserSession<P: PipelineExecutor, R: Runtime> {
    /// The underlying session.
    ///
    /// Wrapped in a mutex since planning may alter session state. Needs a
    /// async-aware mutex since the lock is held across an await (for async
    /// binding).
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
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
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
    pub async fn execute(self) -> Result<QueryResult> {
        const UNNAMED: &str = "";

        let mut session = self.session.lock().await;

        session.prepare(UNNAMED, self.statement)?;
        session.bind(UNNAMED, UNNAMED).await?;

        session.execute(UNNAMED).await
    }
}
