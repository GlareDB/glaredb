use crate::errors::{PgSrvError, Result};
use async_trait::async_trait;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub enum PasswordMode {
    /// A cleartext password is required.
    ///
    /// Should error if no password is provided.
    RequireCleartext,

    /// No password is required.
    NoPassword {
        /// Drop any authentication messages as well.
        ///
        /// A compliant frontend should not send any additional authentication
        /// messages after receiving AuthenticationOk. However, node-postgres
        /// will attempt to send a password message regardless. Setting this to
        /// true will drop that message.
        drop_auth_messages: bool,
    },
}

/// Authenticate connection on the glaredb node itself.
pub trait LocalAuthenticator: Sync + Send {
    fn password_mode(&self) -> PasswordMode;
    fn authenticate(&self, user: &str, password: &str, db_name: &str) -> Result<()>;
}

/// A simple single user authenticator.
#[derive(Clone)]
pub struct SingleUserAuthenticator {
    pub user: String,
    pub password: String,
}

impl LocalAuthenticator for SingleUserAuthenticator {
    fn password_mode(&self) -> PasswordMode {
        PasswordMode::RequireCleartext
    }

    fn authenticate(&self, user: &str, password: &str, _db_name: &str) -> Result<()> {
        if user != self.user {
            return Err(PgSrvError::InvalidUserOrPassword);
        }
        // TODO: Constant time compare.
        if password != self.password {
            return Err(PgSrvError::InvalidUserOrPassword);
        }
        Ok(())
    }
}

/// Require no password provided.
#[derive(Debug, Clone, Copy, Default)]
pub struct PasswordlessAuthenticator {
    pub drop_auth_messages: bool,
}

impl LocalAuthenticator for PasswordlessAuthenticator {
    fn password_mode(&self) -> PasswordMode {
        PasswordMode::NoPassword {
            drop_auth_messages: self.drop_auth_messages,
        }
    }

    fn authenticate(&self, _user: &str, _password: &str, _db_name: &str) -> Result<()> {
        Ok(())
    }
}
