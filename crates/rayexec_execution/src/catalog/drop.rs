/// The object we're dropping.
///
/// Most objects are namespaced with a schema, and so will also have their names
/// included.
///
/// If we're dropping a scheme, that schema's name is already included in the
/// drop info.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropObject {
    Index(String),
    Function(String),
    Table(String),
    View(String),
    Schema,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropInfo {
    pub schema: String,
    pub object: DropObject,
    pub cascade: bool,
    pub if_exists: bool,
}
