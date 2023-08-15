use super::*;

#[derive(Clone, Debug)]
pub struct CreateExternalDatabase {
    pub database_name: String,
    pub if_not_exists: bool,
    pub options: DatabaseOptions,
    pub tunnel: Option<String>,
}
