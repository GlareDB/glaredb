use std::fmt::Debug;

use rayexec_error::Result;

pub trait TransactionManager: Debug + Sync + Send {
    type Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn commit(&self, tx: &mut Self::Transaction) -> Result<()>;

    fn rollback(&self, tx: &mut Self::Transaction) -> Result<()>;
}
