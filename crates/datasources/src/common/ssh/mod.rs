use std::fmt::Display;
use std::str::FromStr;

use super::errors::DatasourceCommonError;

pub mod key;
pub mod session;

#[derive(Debug, PartialEq, Eq)]
pub struct SshConnectionParameters {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
}

impl FromStr for SshConnectionParameters {
    type Err = DatasourceCommonError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use DatasourceCommonError::SshConnectionParseError;

        let s = match s.strip_prefix("ssh://") {
            None => {
                return Err(SshConnectionParseError(format!(
                    "connection string should start with `ssh://`: {s}"
                )))
            }
            Some(s) => s,
        };

        let (user, address) = match s.split_once('@') {
            None => {
                return Err(SshConnectionParseError(format!(
                    "connection string should have the format `ssh://user@address`: {s}"
                )))
            }
            Some((user, address)) => (user, address),
        };

        if user.is_empty() {
            return Err(SshConnectionParseError(format!(
                "user cannot be empty: {s}"
            )));
        }

        if address.is_empty() {
            return Err(SshConnectionParseError(format!(
                "address cannot be empty: {s}"
            )));
        }

        // Find the `:` and split on it.
        let (host, port) = match address.rsplit_once(':') {
            None => (address.to_owned(), None),
            Some((host, port)) => {
                let port: u16 = port.parse().map_err(|_e| {
                    SshConnectionParseError(format!("port should be a valid number: {port}"))
                })?;
                (host.to_owned(), Some(port))
            }
        };

        if host.is_empty() {
            return Err(SshConnectionParseError(format!(
                "host cannot be empty: {s}"
            )));
        }

        Ok(Self {
            host,
            port,
            user: user.to_owned(),
        })
    }
}

#[derive(Debug)]
pub enum SshConnection {
    ConnectionString(String),
    Parameters(SshConnectionParameters),
}

impl Display for SshConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionString(s) => write!(f, "{s}"),
            Self::Parameters(SshConnectionParameters { host, port, user }) => {
                write!(f, "ssh://{user}@{host}")?;
                if let Some(port) = port {
                    write!(f, ":{port}")?;
                }
                Ok(())
            }
        }
    }
}

impl SshConnection {
    pub fn connection_string(&self) -> String {
        format!("{self}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_connection_string() {
        let conn_str = SshConnection::ConnectionString("ssh://prod@127.0.0.1:5432".to_string())
            .connection_string();
        assert_eq!(&conn_str, "ssh://prod@127.0.0.1:5432");

        let conn_str = SshConnection::Parameters(SshConnectionParameters {
            host: "127.0.0.1".to_string(),
            port: Some(5432),
            user: "prod".to_string(),
        });
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "ssh://prod@127.0.0.1:5432");

        // Missing port.
        let conn_str = SshConnection::Parameters(SshConnectionParameters {
            host: "127.0.0.1".to_string(),
            port: None,
            user: "prod".to_string(),
        });
        let conn_str = conn_str.connection_string();
        assert_eq!(&conn_str, "ssh://prod@127.0.0.1");
    }

    #[test]
    fn parse_connection_string() {
        // Valid
        let test_cases = vec![
            (
                "ssh://user@host.com",
                SshConnectionParameters {
                    host: "host.com".to_string(),
                    port: None,
                    user: "user".to_string(),
                },
            ),
            (
                "ssh://user@host.com:1234",
                SshConnectionParameters {
                    host: "host.com".to_string(),
                    port: Some(1234),
                    user: "user".to_string(),
                },
            ),
            (
                "ssh://user@127.0.0.1:1234",
                SshConnectionParameters {
                    host: "127.0.0.1".to_string(),
                    port: Some(1234),
                    user: "user".to_string(),
                },
            ),
        ];
        for (s, v) in test_cases {
            let s: SshConnectionParameters = s.parse().unwrap();
            assert_eq!(s, v);
        }

        // Invalid
        let test_cases = vec![
            "random string",
            "user@host.com",          // doesn't start with `ssh://`
            "ssh://user_at_host.com", // missing `@`
            "ssh://@host.com",        // empty user
            "ssh://user@",            // empty address
            "ssh://user@:1234",       // empty host
            "ssh://host.com:abc",     // invalid port
        ];
        for s in test_cases {
            s.parse::<SshConnectionParameters>()
                .expect_err("invalid ssh connection string should error");
        }
    }
}
