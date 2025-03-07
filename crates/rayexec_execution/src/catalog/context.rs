use std::collections::HashMap;
use std::fmt::Debug;

use super::transaction::TransactionManager;
use super::Catalog;
use crate::arrays::scalar::ScalarValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachInfo {
    /// Name of the data source this attached database is for.
    pub datasource: String,
    /// Options used for connecting to the database.
    ///
    /// This includes things like connection strings, and other possibly
    /// sensitive info.
    pub options: HashMap<String, ScalarValue>,
}

pub trait Database: Debug + Sync + Send {
    type TransactionManager: TransactionManager;
    type Catalog: Catalog<CatalogTx = <Self::TransactionManager as TransactionManager>::Transaction>;
}

#[derive(Debug)]
pub struct DatabaseContext {}
