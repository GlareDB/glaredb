use std::collections::HashMap;

use glaredb_error::{RayexecError, Result};
use tracing::trace;

use super::action::{Action, ActionAddFile, ActionChangeMetadata, ActionRemoveFile};
use super::schema::StructType;

/// Snapshot of a table reconstructed from delta logs.
///
/// See <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#action-reconciliation>
#[derive(Debug)]
pub struct Snapshot {
    /// Latest metadata seen.
    pub(crate) metadata: ActionChangeMetadata,

    /// Add actions we've seen.
    pub(crate) add: HashMap<FileKey, ActionAddFile>,

    /// Remove actions we've seen.
    pub(crate) remove: HashMap<FileKey, ActionRemoveFile>,
}

impl Snapshot {
    /// Try to create a new snapshot from the provided actions.
    ///
    /// There should be exactly one "metadata" action in the list of actions.
    /// This should either be retrieved from the first version of the table, or
    /// a checkpoint.
    pub fn try_new_from_actions(actions: Vec<Action>) -> Result<Self> {
        let metadata = actions
            .iter()
            .find_map(|action| match action {
                Action::ChangeMetadata(metadata) => Some(metadata.clone()),
                _ => None,
            })
            .ok_or_else(|| {
                RayexecError::new("Cannot construct snapshop without a metadata action")
            })?;

        let mut snapshot = Snapshot {
            metadata,
            add: HashMap::new(),
            remove: HashMap::new(),
        };

        // Technically this reapplies the metadata change, but that doesn't
        // matter since it's the same thing.
        snapshot.apply_actions(actions)?;

        Ok(snapshot)
    }

    /// Apply actions to produce an updated snapshot.
    pub fn apply_actions(&mut self, actions: impl IntoIterator<Item = Action>) -> Result<()> {
        for action in actions {
            trace!(?action, "reconciling action for snapshot");

            match action {
                Action::ChangeMetadata(metadata) => {
                    self.metadata = metadata;
                }
                Action::AddFile(add) => {
                    let key = FileKey {
                        path: add.path.clone(), // TODO: Avoid clone (probably just make path private and wrap in rc)
                        dv_id: None,            // TODO: Include deletion vector in action.
                    };

                    let _ = self.remove.remove(&key);
                    self.add.insert(key, add);
                }
                Action::RemoveFile(remove) => {
                    let key = FileKey {
                        path: remove.path.clone(),
                        dv_id: None,
                    };

                    let _ = self.add.remove(&key);
                    self.remove.insert(key, remove);
                }
                Action::AddCdcFile(_) => {
                    // Nothing to do.
                }
                Action::Transaction(_txn) => {
                    // TODO: Track latest tx version per app id.
                }
                Action::Protocol(_) => {
                    // TODO: Track protocol, store on table for reader/writer compat
                }
                Action::CommitInfo(_) => {
                    // TODO: This holds arbitrary json. We could potentially
                    // structure it specific to our use case, then fall back to
                    // just a json Value if it doesn't fit.
                }
            }
        }

        Ok(())
    }

    pub fn schema(&self) -> Result<StructType> {
        self.metadata.deserialize_schema()
    }
}

/// Key representing the "primary key" for a file.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileKey {
    /// Path to the file.
    pub(crate) path: String,
    /// Deletion vector ID if there is one associated for this key.
    pub(crate) dv_id: Option<String>,
}
