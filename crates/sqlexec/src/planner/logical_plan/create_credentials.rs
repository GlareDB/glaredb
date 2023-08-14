use super::*;
#[derive(Clone, Debug)]
pub struct CreateCredentials {
    pub name: String,
    pub options: CredentialsOptions,
    pub comment: String,
}
