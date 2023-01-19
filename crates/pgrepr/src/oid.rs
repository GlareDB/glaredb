//! Postgres OID constants.
//!
//! GlareDB follows Postgres OIDs closely for catalog entries.
//!
//! See <https://github.com/postgres/postgres/blob/a3b65fb28238fe7f4a1ae9685d39ff0c11bdb9d3/src/include/access/transam.h#L156-L194>

/// First normal object id in Postgres. To avoid conflicting with any of their
/// builtin tables, we start our builtins at the first normal id.
pub const FIRST_GLAREDB_BUILTIN_ID: u32 = 16384;

/// First id available for user objects.
pub const FIRST_AVAILABLE_ID: u32 = 20000;
