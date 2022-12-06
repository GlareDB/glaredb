use crate::auth::{ConnectionAuthenticator, DatabaseDetails};
use crate::codec::{
    client::FramedClientConn,
    server::{FramedConn, PgCodec},
};
use crate::errors::{PgSrvError, Result};
use crate::messages::{
    BackendMessage, DescribeObjectType, ErrorResponse, FieldDescription, FrontendMessage,
    StartupMessage, TransactionStatus, VERSION_V3,
};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tracing::trace;

/// ProxyHandler proxies connections to some database instance. Connections are
/// authenticated via some authenticator.
///
/// In a production scenario, the provided authenticator will communicate with
/// Cloud to authenticate.
pub struct ProxyHandler<A> {
    authenticator: A,
}

impl<A: ConnectionAuthenticator> ProxyHandler<A> {
    pub fn new(authenticator: A) -> Self {
        Self { authenticator }
    }

    /// Proxy connection startup to some database.
    ///
    /// This will run through the startup phase of the protocol. The connection
    /// will be authenticated, and the database to proxy to will be determined
    /// by what the authenticator returns.
    pub async fn proxy_startup<C>(&self, conn: C, params: HashMap<String, String>) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let mut framed = FramedConn::new(conn);
        framed
            .send(BackendMessage::AuthenticationCleartextPassword)
            .await?;
        let msg = match framed.read().await? {
            Some(msg) => msg,
            None => return Ok(()), // Not an error, client disconnected.
        };

        // If we fail to auth, ensure an error response is sent to the
        // connection.
        let db_details = match self.authenticate_with_msg(msg, &params).await {
            Ok(details) => details,
            Err(e) => {
                framed
                    .send(ErrorResponse::fatal_internal(format!("cloud auth: {}", e)).into())
                    .await?;
                return Err(e);
            }
        };

        // At this point, open a connection to the database and initiate a startup message
        // We need to send the same parameters as the client sent us
        let db_addr = format!("{}:{}", db_details.ip, db_details.port);
        let db_conn = TcpStream::connect(db_addr).await?;
        let mut db_framed = FramedClientConn::new(db_conn);

        let startup = StartupMessage::StartupRequest {
            version: VERSION_V3,
            params,
        };
        db_framed.send_startup(startup).await?;

        // This implementation only supports AuthenticationCleartextPassword
        let auth_msg = db_framed.read().await?;
        match auth_msg {
            Some(BackendMessage::AuthenticationCleartextPassword) => {
                // TODO: rewrite password according to the response from the cloud api
                db_framed
                    .send(FrontendMessage::PasswordMessage {
                        password: "TODO: USE CLOUD PASSWORD".to_string(), // GlareDB doesn't currently check password.
                    })
                    .await?;

                // Check for AuthenticationOk and respond to the client with the same message
                let auth_ok = db_framed.read().await?;
                match auth_ok {
                    Some(BackendMessage::AuthenticationOk) => {
                        framed.send(BackendMessage::AuthenticationOk).await?;

                        // from here, we can just forward messages between the client to the database
                        let server_conn = db_framed.into_inner();
                        let client_conn = framed.into_inner();
                        tokio::io::copy_bidirectional(
                            &mut client_conn.into_inner(),
                            &mut server_conn.into_inner(),
                        )
                        .await?;

                        Ok(())
                    }
                    Some(other) => Err(PgSrvError::UnexpectedBackendMessage(other)),
                    None => Ok(()),
                }
            }
            Some(other) => Err(PgSrvError::UnexpectedBackendMessage(other)),
            None => Ok(()),
        }
    }

    /// Proxy a cancel request.
    ///
    /// Currently unimplemented. This will require that we broadcast cacel
    /// requests to the appropriate database instances.
    pub async fn proxy_cancel<C>(&self, mut _conn: C) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        trace!("received cancel request");
        Ok(())
    }

    /// Try to authenticate using the contents of a frontend message.
    ///
    /// Currently only supports the password message.
    async fn authenticate_with_msg(
        &self,
        msg: FrontendMessage,
        params: &HashMap<String, String>,
    ) -> Result<DatabaseDetails> {
        match msg {
            FrontendMessage::PasswordMessage { password } => {
                // Extract user (required) from startup params
                let user = match params.get("user") {
                    Some(user) => user,
                    None => return Err(PgSrvError::MissingStartupParameter("user")),
                };

                // Extract the database name (optional) from startup params
                // Defaults to the user
                let db_name = match params.get("database") {
                    Some(database) => database,
                    None => user,
                };

                // Get org id from the options startup parameter.
                let options =
                    parse_options(params).ok_or(PgSrvError::MissingStartupParameter("options"))?;
                let org_id = options
                    .get("org")
                    .ok_or(PgSrvError::MissingOptionsParameter("org"))?;

                let details = self
                    .authenticator
                    .authenticate(user, &password, db_name, org_id)
                    .await?;
                Ok(details)
            }
            other => Err(PgSrvError::UnexpectedFrontendMessage(other)),
        }
    }
}

/// Parse the options provided in the startup parameters.
fn parse_options(params: &HashMap<String, String>) -> Option<HashMap<String, String>> {
    let options = params.get("options")?;
    Some(
        options
            .split_whitespace()
            .map(|s| s.split('=').collect::<Vec<_>>())
            .map(|v| (v[0], v[1]))
            .map(|(k, v)| (k.replace("--", ""), v.to_string()))
            .collect::<HashMap<_, _>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_options_from_params() {
        let options_str = "--test-key=1 --another-key=2";
        let mut params = HashMap::new();
        params.insert("options".to_string(), options_str.to_string());

        let options = parse_options(&params).unwrap();
        assert_eq!(Some(&String::from("1")), options.get("test-key"));
        assert_eq!(Some(&String::from("2")), options.get("another-key"));
    }
}
