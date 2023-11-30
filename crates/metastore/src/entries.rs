use std::collections::HashMap;

use crate::errors::{MetastoreError, Result};
use protogen::metastore::types::catalog::{CatalogEntry, EntryMeta, EntryType};

/// Absolute max number of database objects that can exist in the catalog. This
/// is not configurable when creating a session.
const MAX_DATABASE_OBJECTS: usize = 2048;

/// Determine behavior of inserting new objects into the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreatePolicy {
    /// Corresponds to 'IF NOT EXISTS' clauses.
    ///
    /// Suppresses error if there already exists an object with the same name.
    CreateIfNotExists,

    /// Corresponds to 'OR REPLACE' clauses.
    ///
    /// Replaces an existing object with the same name.
    CreateOrReplace,

    /// A plain 'CREATE'.
    ///
    /// Errors if there exists an object with the same name.
    Create,
}

impl CreatePolicy {
    pub fn new(if_not_exists: bool, or_replace: bool) -> Result<Self> {
        match (if_not_exists, or_replace) {
            (true, true) => Err(MetastoreError::InvalidCreatePolicy),
            (true, false) => Ok(CreatePolicy::CreateIfNotExists),
            (false, true) => Ok(CreatePolicy::CreateOrReplace),
            (false, false) => Ok(CreatePolicy::Create),
        }
    }
}

/// A thin wrapper around a hashmap for database entries which will also produce
/// undo actions on modifying methods.
///
/// Mutating methods on this type prevent mutating default (builtin) objects.
#[derive(Debug, Clone)]
pub struct DatabaseEntries {
    /// All entries in the catalog.
    entries: HashMap<u32, CatalogEntry>,

    namespaces: GlobalNamespaces,

    /// The undo log for undoing a set of actions against the catalog.
    undo_log: UndoLog,
}

impl DatabaseEntries {
    /// Get an immutable reference to the catalog entry if it exists. Errors if
    /// the entry is builtin.
    pub fn get(&self, oid: &u32) -> Result<Option<&CatalogEntry>> {
        match self.entries.get(oid) {
            Some(ent) if ent.get_meta().builtin => {
                Err(MetastoreError::CannotModifyBuiltin(ent.clone()))
            }
            Some(ent) => Ok(Some(ent)),
            None => Ok(None),
        }
    }

    /// Remove an entry. Errors if the entry is builtin.
    pub fn remove(&mut self, oid: u32) -> Result<()> {
        if let Some(ent) = self.entries.get(&oid) {
            if ent.get_meta().builtin {
                return Err(MetastoreError::CannotModifyBuiltin(ent.clone()));
            }
        }

        let existing = self.entries.remove(&oid);
        let undo = UndoAction::UndoRemove {
            oid,
            entry: existing,
        };
        self.undo_log.push(undo);

        Ok(())
    }

    /// Insert an entry.
    pub fn insert(&mut self, oid: u32, ent: CatalogEntry, policy: CreatePolicy) -> Result<()> {
        if self.entries.len() > MAX_DATABASE_OBJECTS {
            return Err(MetastoreError::MaxNumberOfObjects {
                max: MAX_DATABASE_OBJECTS,
            });
        }

        if let Some(ent) = self.entries.get(&oid) {
            if ent.get_meta().builtin {
                return Err(MetastoreError::CannotModifyBuiltin(ent.clone()));
            }
        }

        let prev = self.entries.insert(oid, ent);
        let undo = UndoAction::UndoInsert { oid, entry: prev };
        self.undo_log.push(undo);

        Ok(())
    }

    /// Alter an entry with the provided closure.
    pub fn alter_entry<F>(&mut self, oid: u32, alter_fn: F) -> Result<()>
    where
        F: Fn(CatalogEntry) -> CatalogEntry,
    {
        // Make sure the entry exists and we can change it.
        match self.entries.get(&oid) {
            Some(ent) if ent.get_meta().builtin => {
                return Err(MetastoreError::CannotModifyBuiltin(ent.clone()))
            }
            Some(_) => (),
            None => return Err(MetastoreError::MissingEntry(oid)),
        }

        let ent = self.entries.remove(&oid).unwrap(); // Checked above.
        let mut meta_diff = EntryMetaDiff::init_old(ent.get_meta());

        self.undo_log.push(UndoAction::UndoRemove {
            oid,
            entry: Some(ent.clone()),
        });

        let new_ent = alter_fn(ent);
        meta_diff.add_new(new_ent.get_meta());

        // self.entries.insert(oid, new_ent);
        // self.undo_log.push(UndoAction::UndoInsert {
        //     oid,
        //     entry: Some(new_ent),
        // });

        Ok(())
    }

    /// Take the current undo log.
    pub fn take_undo_log(&mut self) -> UndoLog {
        std::mem::take(&mut self.undo_log)
    }
}

// impl From<HashMap<u32, CatalogEntry>> for DatabaseEntries {
//     fn from(value: HashMap<u32, CatalogEntry>) -> Self {
//         DatabaseEntries {
//             entries: value,
//             undo_log: UndoLog::default(),
//         }
//     }
// }

impl AsRef<HashMap<u32, CatalogEntry>> for DatabaseEntries {
    fn as_ref(&self) -> &HashMap<u32, CatalogEntry> {
        &self.entries
    }
}

/// An action to undo a mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum UndoAction {
    UndoRemove {
        oid: u32,
        /// The entry in the catalog prior to being removed if it existed.
        entry: Option<CatalogEntry>,
    },
    UndoInsert {
        oid: u32,
        /// The entry in the catalog prior to being overwritten by the new entry
        /// if it existed.
        entry: Option<CatalogEntry>,
    },
}

/// Vector of undo actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoLog(pub Vec<UndoAction>);

impl UndoLog {
    fn push(&mut self, action: UndoAction) {
        self.0.push(action);
    }

    fn last(&self) -> &UndoAction {
        self.0.last().unwrap()
    }
}

impl Default for UndoLog {
    fn default() -> Self {
        UndoLog(Vec::with_capacity(1))
    }
}

#[derive(Debug)]
struct DiffValue<T> {
    old: T,
    new: Option<T>,
}

impl<T: PartialEq> DiffValue<T> {
    fn init(v: T) -> Self {
        DiffValue { old: v, new: None }
    }

    fn set_new(&mut self, v: T) {
        self.new = Some(v);
    }

    fn did_change(&self) -> bool {
        if let Some(new) = &self.new {
            return &self.old != new;
        }
        false
    }
}

/// Diff of fields that may be changed in an entry's meta object.
#[derive(Debug)]
struct EntryMetaDiff {
    parent: DiffValue<u32>,
    name: DiffValue<String>,
}

impl EntryMetaDiff {
    /// Initialize a diff from an entry's meta.
    fn init_old(meta: &EntryMeta) -> EntryMetaDiff {
        EntryMetaDiff {
            parent: DiffValue::init(meta.parent),
            name: DiffValue::init(meta.name.clone()),
        }
    }

    /// Add new values to the diff.
    fn add_new(&mut self, meta: &EntryMeta) {
        self.parent.set_new(meta.parent);
        self.name.set_new(meta.name.clone());
    }
}

/// Namespaces at the top level of the database.
#[derive(Debug, Clone)]
struct GlobalNamespaces {
    /// Map database names to their ids.
    database_names: HashMap<String, u32>,
    /// Map tunnel names to their ids.
    tunnel_names: HashMap<String, u32>,
    /// Map credentials names to their ids.
    credentials_names: HashMap<String, u32>,
    /// Map schema names to their ids.
    schema_names: HashMap<String, u32>,
    /// Nested schema namespaces.
    schema_namespaces: HashMap<u32, SchemaNamespaces>,
}

#[derive(Debug, Clone)]
struct SchemaNamespaces {
    /// The "table" namespace in this schema. Views and external tables should
    /// be included in this namespace.
    ///
    /// Maps names to object ids.
    tables: HashMap<String, u32>,
    /// The "function" namespace.
    functions: HashMap<String, u32>,
}
