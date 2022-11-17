//! Constants related to system tables.
use catalog_types::keys::TableId;

pub const BUILTIN_TYPES_TABLE_ID: TableId = 1;
pub const BUILTIN_TYPES_TABLE_NAME: &str = "builtin_types";

pub const SEQUENCES_TABLE_ID: TableId = 2;
pub const SEQUENCES_TABLE_NAME: &str = "sequences";

pub const SCHEMAS_TABLE_ID: TableId = 3;
pub const SCHEMAS_TABLE_NAME: &str = "schemas";

pub const RELATIONS_TABLE_ID: TableId = 4;
pub const RELATIONS_TABLE_NAME: &str = "relations";

pub const ATTRIBUTES_TABLE_ID: TableId = 5;
pub const ATTRIBUTES_TABLE_NAME: &str = "attributes";
