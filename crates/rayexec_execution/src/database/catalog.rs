use std::fmt::Debug;

#[derive(Debug, Default)]
pub struct CatalogTx {}

impl CatalogTx {
    pub fn new() -> Self {
        Self::default()
    }
}
