use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use testing::slt::runner::{FnTest, TestClient};
use tokio_postgres::Config;

macro_rules! test_assert {
    ($e:expr, $err:expr) => {
        if !($e) {
            return Err($err);
        }
    };
}

pub struct SshKeysTest;

#[async_trait]
impl FnTest for SshKeysTest {
    async fn run(
        &self,
        _config: &Config,
        client: TestClient,
        _vars: &mut HashMap<String, String>,
    ) -> Result<()> {
        let client = match client {
            TestClient::Pg(client) => client,
            TestClient::Rpc(_) => {
                return Err(anyhow!("cannot run ssh key test on rpc"));
            }
        };

        client
            .batch_execute(
                "
CREATE TUNNEL test_tunnel_1
    FROM ssh
    OPTIONS (
        connection_string = 'ssh://test_user1@host.com:2222',
    );

CREATE TUNNEL test_tunnel_2
    FROM ssh
    OPTIONS (
        connection_string = 'ssh://test_user2@host.com:2222',
    );
                ",
            )
            .await?;

        // Check if both the keys are different.
        let rows = client
            .query(
                "SELECT public_key FROM glare_catalog.ssh_keys ORDER BY ssh_tunnel_name",
                &[],
            )
            .await?;
        test_assert!(rows.len() == 2, anyhow!("query should return 2 rows"));

        let (key1, key2): (String, String) = (rows[0].get(0), rows[1].get(0));
        test_assert!(key1 != key2, anyhow!("both public keys must be different"));

        // Rotate key
        client
            .batch_execute("ALTER TUNNEL test_tunnel_1 ROTATE KEYS")
            .await?;

        let row = client
            .query_one(
                "
SELECT public_key
    FROM glare_catalog.ssh_keys
    WHERE ssh_tunnel_name = 'test_tunnel_1'
                ",
                &[],
            )
            .await?;
        let key1_new: String = row.get(0);
        test_assert!(
            key1 != key1_new,
            anyhow!("keys must be different after rotating")
        );

        // Test if keys end with username
        let test_cases = [
            (key1, "test_user1"),
            (key1_new, "test_user1"),
            (key2, "test_user2"),
        ];
        for (key, user) in test_cases {
            let parts: Vec<_> = key.split(' ').collect();
            test_assert!(
                parts.len() == 3,
                anyhow!("each public key should be of format `<algo> <key> <user>`")
            );
            test_assert!(
                parts[0] == "ssh-ed25519",
                anyhow!("invalid algorithm for ssh public key: {}", parts[0])
            );
            test_assert!(
                parts[2] == user,
                anyhow!(
                    "wrong user at end of public key: {}, expected {}",
                    parts[2],
                    user
                )
            );
        }

        Ok(())
    }
}

pub struct PgBinaryEncoding;

#[async_trait]
impl FnTest for PgBinaryEncoding {
    async fn run(
        &self,
        _config: &Config,
        client: TestClient,
        _vars: &mut HashMap<String, String>,
    ) -> Result<()> {
        let client = match client {
            TestClient::Pg(client) => client,
            TestClient::Rpc(_) => {
                return Err(anyhow!("cannot run pg binary encoding test on rpc"));
            }
        };

        let rows = client.query("select 1, 2.2::float4", &[]).await?;
        test_assert!(
            rows.len() == 1,
            anyhow!("number of rows returned ({}) != 1", rows.len())
        );

        let int: i64 = rows[0].try_get(0)?;
        test_assert!(int == 1, anyhow!("int value from column 0 ({}) != 1", int));

        let float: f32 = rows[0].try_get(1)?;
        test_assert!(
            float == 2.2,
            anyhow!("float value from column 1 ({}) != 2.2", float)
        );

        Ok(())
    }
}
