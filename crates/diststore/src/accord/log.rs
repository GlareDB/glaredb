use super::timestamp::Timestamp;
use super::transaction::Transaction;
use super::{AccordError, Result};

#[derive(Debug)]
pub struct Log {
    latest_committed: Timestamp,
    latest_applied: Timestamp,
}

impl Log {
    pub fn new() -> Log {
        // TODO: Read from some backing store.
        Log {
            latest_applied: Timestamp::zero(),
            latest_committed: Timestamp::zero(),
        }
    }

    /// Get the timestamp of the last committed entry.
    pub fn get_latest_commit_ts(&self) -> &Timestamp {
        &self.latest_committed
    }

    /// Get the timestamp of the last applied.
    pub fn get_latest_applied_ts(&self) -> &Timestamp {
        &self.latest_applied
    }

    pub fn write_committed<K>(&mut self, tx: &Transaction<K>, ts: &Timestamp) -> Result<()> {
        if ts > &self.latest_committed {
            self.latest_committed = ts.clone();
        }
        Ok(())
    }

    pub fn write_applied<K>(&mut self, tx: &Transaction<K>, ts: &Timestamp) -> Result<()> {
        if ts > &self.latest_applied {
            self.latest_applied = ts.clone();
        }
        Ok(())
    }
}
