//!Ssh Connection type

use openssh::Session;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::errors::{internal, Result};

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshKey {
    pub private_key: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelAccess {
    pub host: String,
    pub user: String,
    pub port: u16,
    pub key: Option<SshKey>,
}

impl SshTunnelAccess {
    //FIXME: return port used in tunnel
    pub async fn create_tunnel(&self) -> Result<Session> {
        let tunnel = openssh::SessionBuilder::default()
            .user(self.user.clone())
            .port(self.port)
            .keyfile("~/.ssh/glaredb") //FIXME use temp file path
            .connect(self.host.clone())
            .await?;

        // delete temp private key/
        tunnel.check().await?;

        // Find open local port
        let mut attempts = 0;
        let local_port = loop {
            if attempts > 10 {
                return Err(internal!(
                    "failed to find an open port to open the SSH tunnel"
                ));
            } else {
                attempts += 1;
            }

            // let mut rng: rand::rngs::StdRng = rand::SeedableRng::from_entropy();
            // // Choosing a dycnamic port according to RFC 6335
            // let local_port: u16 = rng.gen_range(49152..65535);

            // FIXME: use 0 instead of 1212
            // let local = openssh::Socket::new(&("localhost", 0))?;
            let local = openssh::Socket::new(&("localhost", 1122))?;
            let remote = openssh::Socket::new(&(self.host.clone(), self.port))?;

            match tunnel
                .request_port_forward(openssh::ForwardType::Local, local, remote)
                .await
            {
                Err(err) => match err {
                    openssh::Error::Ssh(err)
                        if err.to_string().contains("forwarding request failed") =>
                    {
                        debug!("port already in use, testing another port");
                    }
                    e => {
                        return Err(internal!("Cannot establish SSH tunnel: {e}"));
                    }
                },
                Ok(_) => break 0,
            };
        };

        Ok(tunnel)
    }
}
