use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use once_cell::sync::Lazy;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::{sleep as tokio_sleep, Instant};
use tokio_postgres::{Client, Config};
use tracing::{error, info, warn};

use super::test::Hook;
use crate::clients::TestClient;

/// This [`Hook`] is used to set some local variables that might change for
/// each test.
pub struct AllTestsHook;

impl AllTestsHook {
    const VAR_CURRENT_DATABASE: &'static str = "SLT_CURRENT_DATABASE";
    const TMP_DIR: &'static str = "TMP";
}

#[async_trait]
impl Hook for AllTestsHook {
    async fn pre(
        &self,
        config: &Config,
        _: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<bool> {
        // Create a unique temp dir and set the variable instead of using the
        // TMP environment variable.
        let tmp_dir = tempfile::tempdir()?;
        let tmp_dir = tmp_dir.into_path();
        vars.insert(
            Self::TMP_DIR.to_owned(),
            tmp_dir.to_string_lossy().into_owned(),
        );
        // Set the current database to test database
        vars.insert(
            Self::VAR_CURRENT_DATABASE.to_owned(),
            config.get_dbname().unwrap().to_owned(),
        );
        Ok(true)
    }

    async fn post(
        &self,
        _config: &Config,
        _client: TestClient,
        vars: &HashMap<String, String>,
    ) -> Result<()> {
        if let Some(tmp_dir) = vars.get(Self::TMP_DIR) {
            // It's ok if we fail to remove the tmp directory.
            let _res = tokio::fs::remove_dir_all(tmp_dir).await;
        }
        Ok(())
    }
}

/// This [`Hook`] is used to create an SSH Tunnel and setting up an OpenSSH
/// server in a Docker container.
///
/// Post test cleans up the container.
pub struct SshTunnelHook;

impl SshTunnelHook {
    const SSH_USER: &'static str = "glaredb";
    const TUNNEL_NAME_PREFIX: &'static str = "test_ssh_tunnel";

    /// Generate random port using operating system by using port 0.
    async fn generate_random_port() -> Result<u16> {
        // The 0 port indicates to the OS to assign a random port
        let listener = TcpListener::bind("localhost:0")
            .await
            .map_err(|e| anyhow!("Failed to bind to a random port: {e}"))?;
        let addr = listener.local_addr()?;
        Ok(addr.port())
    }

    async fn try_create_tunnel(try_num: i32, client: &Client) -> Result<(String, String)> {
        let tunnel_name = format!("{}_{}", Self::TUNNEL_NAME_PREFIX, try_num);
        let port = Self::generate_random_port().await?;
        // Create the tunnel and get public key.
        client
            .execute(
                &format!(
                    "
CREATE TUNNEL {}
    FROM ssh
    OPTIONS (
        connection_string = 'ssh://{}@localhost:{}',
    )
                    ",
                    tunnel_name,
                    Self::SSH_USER,
                    port,
                ),
                &[],
            )
            .await?;

        let row = client
            .query_one(
                &format!(
                    "
SELECT public_key
    FROM glare_catalog.ssh_keys
    WHERE ssh_tunnel_name = '{}'
                    ",
                    tunnel_name,
                ),
                &[],
            )
            .await?;
        let public_key: String = row.get(0);

        // Create the OpenSSH container with an exposed port.
        let cmd = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm", // Delete container when stopped.
                "-p",
                &format!("{port}:2222"), // Expose the container to the port
                "-e",
                "PUID=1000",
                "-e",
                "PGID=1000",
                "-e",
                "TZ=Etc/UTC",
                "-e",
                // Mod to enable SSH tunelling by default in the sshd_config.
                "DOCKER_MODS=linuxserver/mods:openssh-server-ssh-tunnel",
                "-e",
                &format!("USER_NAME={}", Self::SSH_USER),
                "-e",
                &format!("PUBLIC_KEY={public_key}"),
                "linuxserver/openssh-server",
            ])
            .output()
            .await?;
        if !cmd.status.success() {
            let stderr = String::from_utf8_lossy(&cmd.stderr);
            let stdout = String::from_utf8_lossy(&cmd.stdout);
            return Err(anyhow!(
                "Cannot create open-ssh container, stdout: {stdout}, stderr: {stderr}"
            ));
        }

        let container_id = String::from_utf8(cmd.stdout)
            .map_err(|e| anyhow!("Unable to get container ID for SSH container: {e}"))?;
        let container_id = container_id.trim().to_owned();

        Ok((container_id, tunnel_name))
    }

    async fn wait_for_container_start(container_id: &str) -> Result<()> {
        async fn check_container(container_id: &str) -> Result<()> {
            let out = Command::new("docker")
                .args(["exec", container_id, "ls", "/config/.ssh/authorized_keys"])
                .output()
                .await?;

            if out.status.success() {
                Ok(())
            } else {
                Err(anyhow!("container not started yet"))
            }
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if check_container(container_id).await.is_ok() {
                // Yes the check completed, but let's just wait a second for the
                // server to start (if there is anything pending).
                tokio_sleep(Duration::from_secs(1)).await;
                return Ok(());
            }
            tokio_sleep(Duration::from_millis(250)).await;
        }
        Err(anyhow!(
            "Timed-out waiting for container `{container_id}` to start"
        ))
    }
}

#[async_trait]
impl Hook for SshTunnelHook {
    async fn pre(
        &self,
        _: &Config,
        client: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<bool> {
        // TODO: make enum for skip/continue rather than booleans
        let client = match client {
            TestClient::Pg(client) => client,
            TestClient::Rpc(_) => {
                error!("cannot run SSH tunnel test with the RPC protocol. Skipping...");
                return Ok(false);
            }
            TestClient::FlightSql(_) => {
                error!("cannot run SSH tunnel test on FlightSQL protocol. Skipping...");
                return Ok(false);
            }
        };

        let mut err = None;
        // Try upto 5 times
        for try_num in 0..5 {
            match Self::try_create_tunnel(try_num, &client).await {
                Ok((container_id, tunnel_name)) => {
                    Self::wait_for_container_start(&container_id).await?;
                    vars.insert("CONTAINER_ID".to_owned(), container_id);
                    vars.insert("TUNNEL_NAME".to_owned(), tunnel_name);
                    return Ok(true);
                }
                Err(e) => {
                    err = Some(e);
                    // continue until we run out of tries.
                }
            }
            warn!(%try_num, "Unable to create SSH tunnel container. Retrying...");
        }
        Err(err.unwrap())
    }

    async fn post(
        &self,
        _config: &Config,
        _client: TestClient,
        vars: &HashMap<String, String>,
    ) -> Result<()> {
        Command::new("docker")
            .arg("stop")
            .arg(vars.get("CONTAINER_ID").unwrap())
            .output()
            .await?;
        Ok(())
    }
}

static SQLITE_DB_LOCATION: Lazy<Result<PathBuf>> = Lazy::new(|| {
    let path = PathBuf::from("testdata/sqllogictests_sqlite/data/db.sqlite3");
    let db = path.to_string_lossy();
    if path.exists() {
        info!(%db, "sqlite database exists, skipping setup; to re-create delete the old database file");
    } else {
        info!(%db, "creating sqlite database");
        let output = std::process::Command::new("sqlite3")
            .arg(&path)
            .arg(".read testdata/sqllogictests_sqlite/data/setup-test-sqlite-db.sql")
            .output()?;

        if !output.status.success() {
            return Err(anyhow!(
                "failed to setup sqlite db (status code: {}):\n  STDOUT: {}\n  STDERR: {}",
                output.status.code().unwrap_or_default(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    Ok(path)
});

pub struct SqliteTestsHook;

#[async_trait]
impl Hook for SqliteTestsHook {
    async fn pre(
        &self,
        _config: &Config,
        _client: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<bool> {
        let db_location = match SQLITE_DB_LOCATION.as_ref() {
            Ok(path) => path.to_string_lossy().into_owned(),
            Err(e) => return Err(anyhow!("{e}")),
        };
        vars.insert("SQLITE_DB_LOCATION".to_string(), db_location);
        Ok(true)
    }
}

pub struct IcebergFormatVersionHook(pub usize);

#[async_trait]
impl Hook for IcebergFormatVersionHook {
    async fn pre(
        &self,
        _config: &Config,
        _client: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<bool> {
        let Self(v) = self;
        vars.insert("ICEBERG_FORMAT_VERSION".to_string(), v.to_string());
        Ok(true)
    }
}
