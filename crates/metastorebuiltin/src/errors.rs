#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {}

pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;
