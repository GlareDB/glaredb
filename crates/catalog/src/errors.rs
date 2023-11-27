use protogen::metastore::strategy::ResolveErrorStrategy;

#[derive(Debug, thiserror::Error)]
#[error("Catalog error: {msg}")]
pub struct CatalogError {
    /// Error message that may be presented to the user.
    pub msg: String,
    /// An error may be resolved by following the given strategy if provided.
    pub strategy: Option<ResolveErrorStrategy>,
}

impl CatalogError {
    pub fn new(msg: impl Into<String>) -> Self {
        CatalogError {
            msg: msg.into(),
            strategy: None,
        }
    }
}

pub type Result<T, E = CatalogError> = std::result::Result<T, E>;

impl From<protogen::errors::ProtoConvError> for CatalogError {
    fn from(value: protogen::errors::ProtoConvError) -> Self {
        Self::new(value.to_string())
    }
}

impl From<tonic::Status> for CatalogError {
    fn from(value: tonic::Status) -> Self {
        // Try to get the strategy from the response from metastore. This can be
        // used to try to resolve the error automatically.
        let strat = value
            .metadata()
            .get(protogen::metastore::strategy::RESOLVE_ERROR_STRATEGY_META)
            .map(|val| ResolveErrorStrategy::from_bytes(val.as_ref()))
            .unwrap_or(ResolveErrorStrategy::Unknown);

        Self {
            msg: value.message().to_string(),
            strategy: Some(strat),
        }
    }
}
