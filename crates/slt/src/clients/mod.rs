use std::time::Duration;

use anyhow::Result;
use clap::builder::PossibleValue;
use clap::ValueEnum;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use tonic::async_trait;

use self::flightsql::FlightSqlTestClient;
use self::postgres::PgTestClient;
use self::rpc::RpcTestClient;

pub mod flightsql;
pub mod pg_proxy;
pub mod postgres;
pub mod rpc;

#[derive(Clone)]
pub enum TestClient {
    Pg(PgTestClient),
    PgProxy(pg_proxy::PgProxyTestClient),
    Rpc(RpcTestClient),
    FlightSql(FlightSqlTestClient),
}

impl TestClient {
    pub async fn close(self) -> Result<()> {
        match self {
            Self::Pg(pg_client) => pg_client.close().await,
            Self::PgProxy(client) => client.close().await,
            Self::Rpc(_) => Ok(()),
            Self::FlightSql(_) => Ok(()),
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
            Self::PgProxy(client) => client.run(sql).await,
            Self::Rpc(rpc_client) => rpc_client.run(sql).await,
            Self::FlightSql(flight_client) => flight_client.run(sql).await,
        }
    }

    fn engine_name(&self) -> &str {
        match self {
            Self::Pg { .. } => "glaredb_pg",
            Self::PgProxy(_) => todo!(),
            Self::Rpc { .. } => "glaredb_rpc",
            Self::FlightSql { .. } => "glaredb_flight",
        }
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum ClientProtocol {
    // Connect over a local postgres instance
    #[default]
    Postgres,
    /// Connect over a local PgProxy instance
    PgProxy,
    // Connect over a local RPC instance
    Rpc,
    // Connect over a local FlightSql instance
    FlightSql,
}

impl ValueEnum for ClientProtocol {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Postgres, Self::Rpc, Self::FlightSql, Self::PgProxy]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(match self {
            ClientProtocol::Postgres => PossibleValue::new("postgres"),
            ClientProtocol::PgProxy => PossibleValue::new("pgproxy"),
            ClientProtocol::Rpc => PossibleValue::new("rpc"),
            ClientProtocol::FlightSql => PossibleValue::new("flightsql")
                .alias("flight-sql")
                .alias("flight"),
        })
    }
}
