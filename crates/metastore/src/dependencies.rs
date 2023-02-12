use crate::types::catalog::{Dependency, DependencyList, DependencyType};
use std::collections::{HashMap, HashSet};

#[derive(Debug, thiserror::Error)]
pub enum DependencyError {}

type Result<T, E = DependencyError> = std::result::Result<T, E>;

/// Manages the dependencies across a catalog for a single database.
///
/// This should be consulted before making modifications to the catalog state,
/// and modified once the new state has been applied.
#[derive(Debug)]
pub struct DependencyManager {
    /// A map of "dependants" to "references".
    ///
    /// This is what gets persisted.
    dependants: HashMap<u32, DependencyList>,

    /// A map of "references" to "dependants".
    ///
    /// This a reverse lookup of the `dependants` map, and allows quickly
    /// checking if an object can be dropped, or if there's some dependants that
    /// need to be dropped first.
    references: HashMap<u32, HashSet<Dependant>>,
}

impl DependencyManager {
    /// Create a new dependency manager.
    pub fn new(dependants: HashMap<u32, DependencyList>) -> Self {
        let mut references = HashMap::with_capacity(dependants.len()); // Likely larger.

        // Build reverse lookup.
        for (dependant_oid, dep_list) in &dependants {
            for dep_reference in &dep_list.dependencies {
                let deps = references
                    .entry(dep_reference.reference)
                    .or_insert(HashSet::new());
                deps.insert(Dependant(*dependant_oid, dep_reference.dep_type));
            }
        }

        DependencyManager {
            dependants,
            references,
        }
    }

    /// Add new dependencies for an object.
    pub fn add_deps_for_object(&mut self, oid: u32, deps: impl IntoIterator<Item = Dependency>) {
        let list = self
            .dependants
            .entry(oid)
            .or_insert(DependencyList::default());

        for dep in deps.into_iter() {
            // Add to "dependants"
            list.dependencies.insert(dep.clone());

            // Keep reverse lookup up to date.
            let dependants = self
                .references
                .entry(dep.reference)
                .or_insert(HashSet::new());
            dependants.insert(Dependant(oid, dep.dep_type));
        }
    }

    pub fn get_dependants(&self) -> &HashMap<u32, DependencyList> {
        &self.dependants
    }
}

/// Value type for use in the reverse lookup map.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Dependant(u32, DependencyType);
