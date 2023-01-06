/// Context needed to ensure catalog lookups return the appropriate items.
///
/// Implementations should ensure clones are cheap.
pub trait CatalogContext: Sync + Send + Clone {}

#[derive(Debug, Clone)]
pub struct StubCatalogContext;

impl CatalogContext for StubCatalogContext {}

#[derive(Debug, Default, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(usize);
