#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaseInformation {
    /// Current state of the lease.
    #[prost(enumeration = "lease_information::State", tag = "1")]
    pub state: i32,
    /// Monotonically increasing generation of the lock.
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    /// Expiration of the lease. May be continually updated.
    ///
    /// If the state of the lock is 'LOCKED', and we're past this timestamp, then
    /// the lock can be taken by another process. Processes should be updating this
    /// in the background.
    ///
    /// This protects against a process acquiring the lock then crashing, causing
    /// the lock to never be unlocked.
    #[prost(message, optional, tag = "3")]
    pub expires_at: ::core::option::Option<::prost_types::Timestamp>,
    /// UUID of the process holding this lock. May be empty if the lock state is
    /// 'UNLOCKED'.
    #[prost(bytes = "vec", tag = "4")]
    pub held_by: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `LeaseInformation`.
pub mod lease_information {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum State {
        Unkown = 0,
        Unlocked = 1,
        Locked = 2,
    }
    impl State {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                State::Unkown => "UNKOWN",
                State::Unlocked => "UNLOCKED",
                State::Locked => "LOCKED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "UNKOWN" => Some(Self::Unkown),
                "UNLOCKED" => Some(Self::Unlocked),
                "LOCKED" => Some(Self::Locked),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogMetadata {
    /// Latest version of the persisted catalog. Used to build the object name for
    /// the catalog blob.
    #[prost(uint64, tag = "1")]
    pub latest_version: u64,
    /// Byte serialized UUID for the process that last wrote this metadata.
    #[prost(bytes = "vec", tag = "2")]
    pub last_written_by: ::prost::alloc::vec::Vec<u8>,
}
/// The catalog as it exists in object storage.
///
/// Note that this is very similar to `CatalogState`, however this is very likely
/// a candidate of being broken up into separate objects (e.g. by entry type, or
/// dependent entries). A single blob does the job for now, and its unlikely that
/// this blob will get egregiously large.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistedCatalog {
    /// The version of this catalog.
    #[prost(uint64, tag = "1")]
    pub version: u64,
    /// All entries in this catalog.
    ///
    /// ID -> Entry
    #[prost(map = "uint32, message", tag = "2")]
    pub entries: ::std::collections::HashMap<u32, super::catalog::CatalogEntry>,
    /// Persisted oid counter. Used for oid generation for new database objects.
    #[prost(uint32, tag = "3")]
    pub oid_counter: u32,
    /// Dependencies for objects in the catalog.
    ///
    /// The key in this map is the "dependant". Every object in a dependency list
    /// is a "reference". A "dependant" depends on a "reference". A "reference"
    /// cannot be dropped without first all "dependants" beging dropped.
    ///
    /// An object may only be dropped if there are no dependants that reference it.
    #[prost(map = "uint32, message", tag = "4")]
    pub dependency_lists: ::std::collections::HashMap<
        u32,
        super::catalog::DependencyList,
    >,
}
