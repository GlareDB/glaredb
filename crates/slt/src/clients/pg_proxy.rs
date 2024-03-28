use std::sync::Arc;

use anyhow::{anyhow, Result};
use proxy::PgProxy;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_postgres::{config::Host, Config};
use tonic::async_trait;

use super::postgres::PgTestClient;

#[derive(Clone)]
pub struct PgProxyTestClient {
    proxy_handle: Arc<JoinHandle<Result<()>>>,
    client: PgTestClient,
}

impl PgProxyTestClient {
    pub async fn new(config: &Config) -> Result<Self> {
        let host = config.get_hosts().get(0).unwrap();
        let Host::Tcp(host) = host else {
            return Err(anyhow!("Host is not a TCP host"));
        };

        let port = config.get_ports().get(0).unwrap();
        let addr = format!("{}:{}", host, port);

        println!("addr = {:?}", addr);

        // assign to a random port
        let proxy_addr = "0.0.0.0:0";
        let pg_listener = TcpListener::bind(proxy_addr).await?;
        let proxied_port = pg_listener.local_addr()?.port();

        let proxy = PgProxy::new_test("0.0.0.0", proxied_port, "glaredb", "glaredb").await?;
        let proxy_handle = Arc::new(tokio::spawn(proxy.serve(pg_listener)));
        println!("handle = {:?}", proxy_handle);
        let mut proxied_client_cfg = Config::new();
        proxied_client_cfg
            .user("glaredb")
            .password("glaredb")
            .options("--org=glaredb")
            .dbname("glaredb")
            .host("0.0.0.0")
            .port(proxied_port);

        println!("proxied_client_cfg = {:?}", proxied_client_cfg);
        let client = PgTestClient::new(&proxied_client_cfg).await?;
        println!("PgTestClient created");
        Ok(Self {
            proxy_handle,
            client,
        })
    }
    pub(super) async fn close(&self) -> Result<()> {
        println!("Closing PgProxyTestClient");
        Ok(())
    }
}

#[async_trait]
impl AsyncDB for PgProxyTestClient {
    type Error = sqlexec::errors::ExecError;
    type ColumnType = DefaultColumnType;
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        println!("PgProxy");
        println!("sql: {}", sql);
        todo!()
    }
}
