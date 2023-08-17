use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use testing::slt::runner::{Hook, TestClient};
use tokio::{
    net::TcpListener,
    process::Command,
    time::{sleep as tokio_sleep, Instant},
};
use tokio_postgres::{Client, Config};
use tracing::warn;

/// This [`Hook`] is used to set some local variables that might change for
/// each test.
pub struct AllTestsHook;

impl AllTestsHook {
    const VAR_CURRENT_DATABASE: &str = "SLT_CURRENT_DATABASE";
    const TMP_DIR: &str = "TMP";
}

#[async_trait]
impl Hook for AllTestsHook {
    async fn pre(
        &self,
        config: &Config,
        _: TestClient,
        vars: &mut HashMap<String, String>,
    ) -> Result<()> {
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
        Ok(())
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
    const SSH_USER: &str = "glaredb";
    const TUNNEL_NAME_PREFIX: &str = "test_ssh_tunnel";

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
    ) -> Result<()> {
        let client = match client {
            TestClient::Pg(client) => client,
            TestClient::Rpc(_) => return Err(anyhow!("cannot run SSH tunnel test on rpc")),
        };

        let mut err = None;
        // Try upto 5 times
        for try_num in 0..5 {
            match Self::try_create_tunnel(try_num, &client).await {
                Ok((container_id, tunnel_name)) => {
                    Self::wait_for_container_start(&container_id).await?;
                    vars.insert("CONTAINER_ID".to_owned(), container_id);
                    vars.insert("TUNNEL_NAME".to_owned(), tunnel_name);
                    return Ok(());
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
